#include "rpc_server.hpp"
#include "message.hpp"
#include "rpc_log_type.hpp"
#include "rpc_storage_extension.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/render_tree.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/common/types/blob.hpp"

using namespace duckdb;

RpcServer::RpcServer(ClientContext &context_p) : db(context_p.db) {
}

RpcServer::~RpcServer() {
}

optional_ptr<RpcConnection> RpcServer::GetConnection(const string &connection_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second.get();
	}
	return nullptr;
}

string RpcServer::CreateNewConnection(const string &session_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	D_ASSERT(active_connections.find(session_id) == active_connections.end());

	auto new_connection = make_uniq<RpcConnection>();
	new_connection->duckdb_connection = make_uniq<Connection>(*db);
	new_connection->duckdb_connection->context->config.enable_progress_bar = false;
	// new_connection->duckdb_connection->context->config.streaming_buffer_size = 10 * 1000000; // 10 MB
	active_connections[session_id] = std::move(new_connection);
	return session_id;
}

static string GetSettingString(DatabaseInstance &db, const string &setting_name) {
	Value setting_val;
	auto &config = DBConfig::GetConfig(db);

	auto lookup_result = config.TryGetCurrentSetting(setting_name, setting_val);
	D_ASSERT(lookup_result);
	D_ASSERT(setting_val.type().id() == LogicalTypeId::VARCHAR);
	auto setting_str = setting_val.GetValue<string>();
	D_ASSERT(!setting_str.empty());
	return setting_str;
}

// Run the configured authentication callback as `SELECT <fn>(?, ?)` and return
// its BOOLEAN result. Errors thrown by the callback are swallowed as `false`;
// callers surface a generic "Authentication failed" message.
static bool EvaluateAuthenticate(DatabaseInstance &db, const string &sid, const string &auth_string) {
	Connection dummy_connection(db);
	auto sql = StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "rpc_authentication_function"));
	auto result = dummy_connection.Query(sql, Value(sid), Value(auth_string));
	if (!result || result->HasError()) {
		return false;
	}
	auto chunk = result->Fetch();
	if (!chunk || !chunk->GetValue(0, 0).GetValue<bool>()) {
		return false;
	}
	return true;
}

// Run the configured authorization callback as `SELECT <fn>(?, ?, ?)` and
// return the VARCHAR it produced. Throws on any error from the callback (e.g.
// `error('msg')`), on missing rows, or on a non-VARCHAR/NULL return.
static string EvaluateAuthorize(DatabaseInstance &db, const string &sid, const string &op_kind,
                                const string &payload) {
	Connection dummy_connection(db);
	auto sql = StringUtil::Format("SELECT %s(?, ?, ?)", GetSettingString(db, "rpc_authorization_function"));
	auto result = dummy_connection.Query(sql, Value(sid), Value(op_kind), Value(payload));
	if (!result || result->HasError()) {
		throw InvalidInputException(result ? result->GetError() : "authorization callback failed");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw InvalidInputException("authorization callback returned no rows");
	}
	auto val = chunk->GetValue(0, 0);
	if (val.IsNull() || val.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("authorization callback must return VARCHAR");
	}
	return val.GetValue<string>();
}

// Cached APPEND authz: fires the callback once per (connection, schema.table),
// this is to avoid repeated calls for every chunk in a large append.
static const AppendAuthzCacheEntry &AuthorizeAppendCached(DatabaseInstance &db, RpcConnection &conn,
                                                          const string &sid, const string &target) {
	auto it = conn.append_authz_cache.find(target);
	if (it != conn.append_authz_cache.end()) {
		return it->second;
	}
	AppendAuthzCacheEntry entry;
	try {
		EvaluateAuthorize(db, sid, "append", target);
		entry.allowed = true;
	} catch (const std::exception &e) {
		entry.allowed = false;
		entry.reject_message = string("Authorization failed: ") + e.what();
	}
	auto inserted = conn.append_authz_cache.emplace(target, std::move(entry));
	return inserted.first->second;
}

string RpcServer::GenerateSessionId() {
	return StringUtil::GenerateRandomName(32);
}

// Extract connection_id from a message if available
static string ExtractConnectionId(ProtocolMessage &msg) {
	switch (msg.Type()) {
	case MessageType::PREPARE_REQUEST:
		return msg.Cast<PrepareRequestMessage>().ConnectionId();
	case MessageType::FETCH_REQUEST:
		return msg.Cast<FetchRequestMessage>().ConnectionId();
	case MessageType::CATALOG_REQUEST:
		return msg.Cast<CatalogRequestMessage>().ConnectionId();
	case MessageType::APPEND_REQUEST:
		return msg.Cast<AppendRequestMessage>().ConnectionId();
	default:
		return "";
	}
}

static optional_idx ExtractClientQueryId(ProtocolMessage &msg) {
	return msg.ClientQueryId();
}

static string ExtractQuery(ProtocolMessage &msg) {
	if (msg.Type() == MessageType::PREPARE_REQUEST) {
		return msg.Cast<PrepareRequestMessage>().Query();
	}
	return "";
}

// main switcheroo happens here
unique_ptr<ProtocolMessage> RpcServer::HandleMessage(ProtocolMessage &received_message) {
	auto &logger = Logger::Get(*db);
	bool should_log = logger.ShouldLog(RPCLogType::NAME, RPCLogType::LEVEL);

	string rpc_connection_id;
	string query;
	optional_idx client_query_id;
	int64_t start_time = 0;
	if (should_log) {
		rpc_connection_id = ExtractConnectionId(received_message);
		client_query_id = ExtractClientQueryId(received_message);
		query = ExtractQuery(received_message);
		start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                 .time_since_epoch()
		                 .count();
	}

	auto response = HandleMessageInternal(received_message);

	if (should_log) {
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();
		string error;
		if (response->Type() == MessageType::ERROR) {
			error = response->Cast<ErrorMessage>().Error();
		}
		auto msg = RPCLogType::ConstructLogMessage(received_message.Type(), rpc_connection_id, client_query_id, query,
		                                           "", end_time - start_time, response->Type(), error);
		logger.WriteLog(RPCLogType::NAME, RPCLogType::LEVEL, msg);
	}

	return response;
}

static vector<unique_ptr<DataChunk>> CreateBatch(unique_ptr<QueryResult> &query_result, idx_t max_chunks) {
	vector<unique_ptr<DataChunk>> batch;
	while (batch.size() < max_chunks) {
		auto result_chunk = query_result->Fetch();
		// error case
		if (!result_chunk && query_result->HasError()) {
			batch.clear();
			return std::move(batch);
		}
		// we are done case
		if (!result_chunk || result_chunk->size() == 0) {
			query_result.reset();
			break;
		}
		batch.push_back(std::move(result_chunk));
	}
	return std::move(batch);
}

unique_ptr<ProtocolMessage> RpcServer::HandleMessageInternal(ProtocolMessage &received_message) {
	switch (received_message.Type()) {
	case MessageType::CONNECTION_REQUEST: {
		auto &connection_request_message = received_message.Cast<ConnectionRequestMessage>();
		string session_id = GenerateSessionId();
		if (!EvaluateAuthenticate(*db, session_id, connection_request_message.AuthString())) {
			return make_uniq<ErrorMessage>("Authentication failed");
		}
		return make_uniq<ConnectionResponseMessage>(CreateNewConnection(session_id));
	}
	case MessageType::PREPARE_REQUEST: {
		auto &prepare_request_message = received_message.Cast<PrepareRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(prepare_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}

		string query_to_run;
		try {
			query_to_run = EvaluateAuthorize(*db, prepare_request_message.ConnectionId(), "query",
			                                 prepare_request_message.Query());
		} catch (const std::exception &e) {
			return make_uniq<ErrorMessage>(string("Authorization failed: ") + e.what());
		}

		std::unique_lock<std::mutex> lock(rpc_connection->lock);
		rpc_connection->duckdb_query_result.reset();

		if (!prepare_request_message.ImmediatelyExecute()) {
			return make_uniq<ErrorMessage>("EEEK");
		}

		{
			auto query_result = rpc_connection->duckdb_connection->SendQuery(query_to_run);
			if (query_result->HasError()) {
				return make_uniq<ErrorMessage>(query_result->GetError());
			}
			if (query_result->names.empty()) {
				return make_uniq<ErrorMessage>("Query did not return any columns");
			}

			rpc_connection->duckdb_query_result = std::move(query_result);
		}
		// Fresh query → restart batch numbering. Clients' local state is re-initialized on
		// a new PREPARE, so indices start at 0 again.
		rpc_connection->next_batch_index = 0;

		Value max_chunks_val;
		DBConfig::GetConfig(*db).TryGetCurrentSetting("quack_fetch_batch_chunks", max_chunks_val);
		auto max_chunks_per_batch = max_chunks_val.GetValue<uint64_t>();

		auto names = rpc_connection->duckdb_query_result->names;
		auto types = rpc_connection->duckdb_query_result->types;

		auto batch = CreateBatch(rpc_connection->duckdb_query_result, max_chunks_per_batch);
		if (rpc_connection->duckdb_query_result && rpc_connection->duckdb_query_result->HasError()) {
			auto error_message = rpc_connection->duckdb_query_result->GetError();
			rpc_connection->duckdb_query_result.reset();
			return make_uniq<ErrorMessage>(error_message);
		}
		auto needs_more_fetch = batch.size() == max_chunks_per_batch;

		return make_uniq<PrepareResponseMessage>(types, names,

		                                         std::move(batch), needs_more_fetch);
	}

	case MessageType::FETCH_REQUEST: {
		auto &fetch_request_message = received_message.Cast<FetchRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(fetch_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}
		std::unique_lock<std::mutex> lock(rpc_connection->lock);

		if (!rpc_connection->duckdb_query_result) {
			return make_uniq<FetchResponseMessage>();
		}
		if (rpc_connection->duckdb_query_result->HasError()) {
			return make_uniq<ErrorMessage>(rpc_connection->duckdb_query_result->GetError());
		}

		Value max_chunks_val;
		DBConfig::GetConfig(*db).TryGetCurrentSetting("quack_fetch_batch_chunks", max_chunks_val);
		auto max_chunks_per_batch = max_chunks_val.GetValue<uint64_t>();

		auto batch = CreateBatch(rpc_connection->duckdb_query_result, max_chunks_per_batch);
		if (rpc_connection->duckdb_query_result && rpc_connection->duckdb_query_result->HasError()) {
			auto error_message = rpc_connection->duckdb_query_result->GetError();
			rpc_connection->duckdb_query_result.reset();
			return make_uniq<ErrorMessage>(error_message);
		}
		auto assigned_batch_index = rpc_connection->next_batch_index++;
		return make_uniq<FetchResponseMessage>(std::move(batch), optional_idx(assigned_batch_index));
	}

	case MessageType::CATALOG_REQUEST: {
		auto &catalog_request_message = received_message.Cast<CatalogRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(catalog_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}
		std::unique_lock<std::mutex> lock(rpc_connection->lock);
		auto &context = *rpc_connection->duckdb_connection->context;

		// FIXME handle other types!
		auto parse_info = catalog_request_message.GetParseInfo();

		switch (parse_info->info_type) {
		case ParseInfoType::CREATE_INFO: {
			auto &create_info = parse_info->Cast<CreateInfo>();
			auto &catalog = Catalog::GetCatalog(context, create_info.catalog);
			switch (create_info.type) {
			case CatalogType::TABLE_ENTRY: {
				try {
					EvaluateAuthorize(*db, catalog_request_message.ConnectionId(), "create_table",
					                  create_info.ToString());
				} catch (const std::exception &e) {
					return make_uniq<ErrorMessage>(string("Authorization failed: ") + e.what());
				}
				unique_ptr<CreateTableInfo> create_table_info(
				    reinterpret_cast<CreateTableInfo *>(parse_info.release()));
				auto &meta_transaction = MetaTransaction::Get(context);
				meta_transaction.ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::CREATE_CATALOG_ENTRY);
				auto create_result = catalog.CreateTable(context, std::move(create_table_info));
				return make_uniq<CatalogResponseMessage>(create_result->GetInfo());
			}
			case CatalogType::SCHEMA_ENTRY: {
				try {
					EvaluateAuthorize(*db, catalog_request_message.ConnectionId(), "create_schema",
					                  create_info.ToString());
				} catch (const std::exception &e) {
					return make_uniq<ErrorMessage>(string("Authorization failed: ") + e.what());
				}
				auto &meta_transaction = MetaTransaction::Get(context);
				meta_transaction.ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::CREATE_CATALOG_ENTRY);
				auto create_result = catalog.CreateSchema(context, parse_info->Cast<CreateSchemaInfo>());
				return make_uniq<CatalogResponseMessage>(create_result->GetInfo());
			}
			default:
				return make_uniq<ErrorMessage>(
				    StringUtil::Format("Unimplemented catalog type %s", CatalogTypeToString(create_info.type)));
			}
		}
		case ParseInfoType::DROP_INFO: {
			auto &drop_info = parse_info->Cast<DropInfo>();
			auto &catalog = Catalog::GetCatalog(context, drop_info.catalog);
			switch (drop_info.type) {
			case CatalogType::TABLE_ENTRY: {
				try {
					EvaluateAuthorize(*db, catalog_request_message.ConnectionId(), "drop_table",
					                  drop_info.ToString());
				} catch (const std::exception &e) {
					return make_uniq<ErrorMessage>(string("Authorization failed: ") + e.what());
				}
				auto &meta_transaction = MetaTransaction::Get(context);
				meta_transaction.ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::DROP_CATALOG_ENTRY);
				catalog.DropEntry(context, drop_info);
				return make_uniq<CatalogResponseMessage>(drop_info.Copy()); // The copy is not used but oh well
			}
			default:
				return make_uniq<ErrorMessage>(
				    StringUtil::Format("Unimplemented catalog type %s", CatalogTypeToString(drop_info.type)));
			}
		}
		default:
			return make_uniq<ErrorMessage>("Unimplemented parse info type");
		}
	}
	case MessageType::APPEND_REQUEST: {
		auto &append_request_message = received_message.Cast<AppendRequestMessage>();
		optional_ptr<RpcConnection> rpc_connection = GetConnection(append_request_message.ConnectionId());
		if (!rpc_connection) {
			return make_uniq<ErrorMessage>("Invalid connection id");
		}
		std::unique_lock<std::mutex> lock(rpc_connection->lock);

		auto target = append_request_message.SchemaName() + "." + append_request_message.TableName();
		auto &authz_outcome =
		    AuthorizeAppendCached(*db, *rpc_connection, append_request_message.ConnectionId(), target);
		if (!authz_outcome.allowed) {
			return make_uniq<ErrorMessage>(authz_outcome.reject_message);
		}

		auto &context = *rpc_connection->duckdb_connection->context;
		auto table_info = context.TableInfo(append_request_message.SchemaName(), append_request_message.TableName());
		ColumnDataCollection collection(Allocator::Get(context), append_request_message.AppendChunk().GetTypes());
		collection.Append(append_request_message.AppendChunk());
		rpc_connection->duckdb_connection->Append(*table_info, collection);
		return make_uniq<AppendResponseMessage>();
	}
	default: {
		return make_uniq<ErrorMessage>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(received_message.Type())));
	}
	}
}
