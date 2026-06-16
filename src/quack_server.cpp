#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_storage.hpp"

namespace duckdb {
QuackConnection::QuackConnection(string session_id_p) : session_id(std::move(session_id_p)) {
}

QuackConnection::~QuackConnection() {
	duckdb_query_result.reset();
}

void QuackServer::ValidateToken(const string &token) {
	if (token.size() < 4) {
		throw InvalidInputException("Quack server token must be at least 4 characters long");
	}
}

QuackServer::QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p)
    : db_ptr(context_p.db), uri(uri_p), token(token_p) {
	ValidateToken(token);
}

QuackServer::~QuackServer() {
}

vector<QuackConnectionSnapshot> QuackServer::GetActiveConnectionSnap() {
	vector<QuackConnectionSnapshot> result;
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	for (auto &[id, conn] : active_connections) {
		QuackConnectionSnapshot snapshot;
		snapshot.session_id = conn->session_id;
		snapshot.client_id_hash = conn->client_id_hash;
		snapshot.sql_query = conn->sql_query;
		snapshot.query_state = conn->query_state;
		snapshot.query_started_at = conn->query_started_at;
		result.push_back(std::move(snapshot));
	}
	return result;
}

shared_ptr<QuackConnection> QuackServer::GetConnection(const string &connection_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second;
	}
	return nullptr;
}

string QuackServer::CreateNewConnection(const string &session_id, const string &client_id_hash) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	D_ASSERT(active_connections.find(session_id) == active_connections.end());

	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto new_connection = make_shared_ptr<QuackConnection>(session_id);
	new_connection->client_id_hash = client_id_hash;
	new_connection->duckdb_connection = make_uniq<Connection>(*db);
	new_connection->duckdb_connection->context->config.enable_progress_bar = false;
	// new_connection->duckdb_connection->context->config.streaming_buffer_size = 10 * 1000000; // 10 MB
	active_connections[session_id] = std::move(new_connection);
	return session_id;
}

bool QuackServer::DisconnectConnection(const string &session_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	auto entry = active_connections.find(session_id);
	if (entry == active_connections.end()) {
		// unknown client
		return false;
	}
	active_connections.erase(entry);
	return true;
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

template <typename... ARGS>
static Value EvaluateAuthQuery(DatabaseInstance &db, const string &sql, ARGS... values) {
	Connection dummy_connection(db);
	auto auth_result = dummy_connection.Query(sql, values...);
	if (!auth_result || auth_result->HasError()) {
		return Value(false);
	}
	auto auth_result_chunk = auth_result->Fetch();
	if (!auth_result_chunk || auth_result_chunk->size() == 0) {
		return Value(false);
	}
	return auth_result_chunk->GetValue(0, 0);
}

static constexpr idx_t kTokenBytes = 16; // 128 bits

static string ComputeSessionHash(const string &session_id, const string &token, const string &client_id) {
	MD5Context context;
	context.Add(session_id);
	context.Add(token);
	context.Add(client_id);
	return context.FinishHex();
}

static string HexEncode(const data_t *bytes, idx_t n) {
	string result(n * 2, '\0');
	for (idx_t i = 0; i < n; i++) {
		result[2 * i] = Blob::HEX_TABLE[bytes[i] >> 4];
		result[2 * i + 1] = Blob::HEX_TABLE[bytes[i] & 0x0F];
	}
	return result;
}

string QuackServer::GenerateRandomToken(DatabaseInstance &db) {
	auto encryption_util = db.GetEncryptionUtil(false);
	auto metadata =
	    make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, kTokenBytes, EncryptionTypes::EncryptionVersion::NONE);
	auto rng = encryption_util->CreateEncryptionState(std::move(metadata));

	data_t bytes[kTokenBytes];
	rng->GenerateRandomData(bytes, kTokenBytes);
	return HexEncode(bytes, kTokenBytes);
}

string QuackServer::GenerateSessionId() {
	{
		std::lock_guard<std::mutex> lock(session_id_rng_mutex);
		if (!session_id_rng) {
			auto db = db_ptr.lock();
			if (!db) {
				throw InternalException("Database was closed");
			}
			auto encryption_util = db->GetEncryptionUtil(false);
			auto metadata = make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, kTokenBytes,
			                                                   EncryptionTypes::EncryptionVersion::NONE);
			session_id_rng = encryption_util->CreateEncryptionState(std::move(metadata));
		}
	}

	data_t bytes[kTokenBytes];
	session_id_rng->GenerateRandomData(bytes, kTokenBytes);
	return HexEncode(bytes, kTokenBytes);
}

static string ExtractQuery(QuackMessage &msg) {
	if (msg.Type() == MessageType::PREPARE_REQUEST) {
		return msg.Cast<PrepareRequestMessage>().Query();
	}
	return "";
}

bool ServerSupportsMessage(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
	case MessageType::PREPARE_REQUEST:
	case MessageType::FETCH_REQUEST:
	case MessageType::APPEND_REQUEST:
	case MessageType::DISCONNECT_MESSAGE:
	case MessageType::CANCEL_REQUEST:
		return true;
	default:
		return false;
	}
}

bool MessageRequiresConnection(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
		return false;
	default:
		return true;
	}
}

// main switcheroo happens here
unique_ptr<QuackMessage> QuackServer::HandleMessage(MemoryStream &read_stream) {
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
	}
	auto &logger = Logger::Get(*db);
	bool should_log = logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL);

	int64_t start_time = 0;
	if (should_log) {
		start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                 .time_since_epoch()
		                 .count();
	}

	// start deserializing the message
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);

	// first read the header
	auto header = QuackMessage::DeserializeHeader(deserializer);

	// validate if the server can handle this type of message - the server cannot handle all message types
	if (!ServerSupportsMessage(header.type)) {
		return make_uniq<ErrorResponse>("Unsupported message type for server");
	}

	// if the message requires it, obtain a connection
	// these are basically all messages aside from connect request
	shared_ptr<QuackConnection> connection;
	if (MessageRequiresConnection(header.type)) {
		connection = GetConnection(header.connection_id);
		if (!connection) {
			return make_uniq<ErrorResponse>("Invalid connection id");
		}
	}

	// now deserialize the actual message
	auto received_message = QuackMessage::DeserializeMessage(deserializer, header);

	// process the message
	auto response = HandleMessageInternal(*db, *received_message, connection);

	if (should_log) {
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();
		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		auto msg = QuackLogType::ConstructLogMessage(header.type, header.connection_id, header.client_query_id,
		                                             ExtractQuery(*received_message), "", end_time - start_time,
		                                             response->Type(), error);
		logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
	}

	return response;
}

static vector<unique_ptr<DataChunkWrapper>> CreateBatch(Allocator &allocator, unique_ptr<QueryResult> &query_result,
                                                        idx_t max_chunks) {
	vector<unique_ptr<DataChunkWrapper>> results;

	while (results.size() < max_chunks) {
		auto result_chunk = query_result->Fetch();
		// error case
		if (!result_chunk && query_result->HasError()) {
			results.clear();
			return results;
		}
		// we are done case
		if (!result_chunk || result_chunk->size() == 0) {
			query_result.reset();
			break;
		}
		results.push_back(make_uniq<DataChunkWrapper>(*result_chunk));
	}
	return results;
}

unique_ptr<QuackMessage> QuackServer::HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
                                                            optional_ptr<QuackConnection> connection_p) {
	switch (received_message.Type()) {
	case MessageType::CONNECTION_REQUEST: {
		auto &connection_request_message = received_message.Cast<ConnectionRequestMessage>();
		if (connection_request_message.MinimumSupportedQuackVersion() > 1ULL) {
			return make_uniq<ErrorResponse>("Unsupported Quack version - server only supports version 1 of quack");
		}
		string session_id = GenerateSessionId();
		auto auth_result = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?, ?)", GetSettingString(db, "quack_authentication_function")),
		    Value(session_id), Value(connection_request_message.AuthString()), Value(Token()));

		if (auth_result.IsNull() ||
		    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
			return make_uniq<ErrorResponse>("Authentication failed");
		}
		string client_id_hash;
		if (!connection_request_message.ClientId().empty()) {
			client_id_hash = ComputeSessionHash(session_id, Token(), connection_request_message.ClientId());
		}
		return make_uniq<ConnectionResponseMessage>(CreateNewConnection(session_id, client_id_hash));
	}
	case MessageType::DISCONNECT_MESSAGE: {
		auto &connection = *connection_p;
		if (!DisconnectConnection(connection.session_id)) {
			return make_uniq<ErrorResponse>("Connection does not exist / already disconnected");
		}
		return make_uniq<SuccessResponse>();
	}
	case MessageType::PREPARE_REQUEST: {
		auto &prepare_request_message = received_message.Cast<PrepareRequestMessage>();
		auto &connection = *connection_p;

		// TODO do not do this if there is no fun set
		auto auth_result = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
		    Value(prepare_request_message.ConnectionId()), Value(prepare_request_message.Query()));
		if (auth_result.IsNull() ||
		    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
			return make_uniq<ErrorResponse>("Authorization failed");
		}
		auto effective_sql = (auth_result.type().id() == LogicalTypeId::VARCHAR) ? auth_result.GetValue<string>()
		                                                                         : prepare_request_message.Query();

		std::unique_lock<std::mutex> lock(connection.lock);
		connection.duckdb_query_result.reset();
		connection.sql_query = prepare_request_message.Query();
		connection.query_state = QuackQueryState::ACTIVE;
		connection.query_started_at = Timestamp::GetCurrentTimestamp();

		{
			auto query_result = connection.duckdb_connection->SendQuery(effective_sql);
			if (query_result->HasError()) {
				connection.sql_query = "";
				auto response = make_uniq<ErrorResponse>(query_result->GetErrorObject());
				connection.duckdb_query_result.reset();
				connection.query_state = QuackQueryState::CANCELLED;
				return response;
			}
			if (query_result->names.empty()) {
				connection.sql_query = "";
				auto response = make_uniq<ErrorResponse>(query_result->GetErrorObject());
				connection.duckdb_query_result.reset();
				connection.query_state = QuackQueryState::QUACK_ERROR;
				return make_uniq<ErrorResponse>("Query did not return any columns");
			}

			connection.duckdb_query_result = std::move(query_result);
		}
		// Fresh query → restart batch numbering. Clients' local state is re-initialized on
		// a new PREPARE, so indices start at 0 again.
		connection.next_batch_index = 1;
		connection.query_uuid = prepare_request_message.QueryUUID();

		Value max_chunks_val;
		DBConfig::GetConfig(db).TryGetCurrentSetting("quack_fetch_batch_chunks", max_chunks_val);
		auto max_chunks_per_batch = max_chunks_val.GetValue<uint64_t>();

		auto names = connection.duckdb_query_result->names;
		auto types = connection.duckdb_query_result->types;

		auto results = CreateBatch(Allocator::Get(db), connection.duckdb_query_result, max_chunks_per_batch);
		if (connection.duckdb_query_result && connection.duckdb_query_result->HasError()) {
			D_ASSERT(results.empty());

			auto error_message = connection.duckdb_query_result->GetErrorObject();
			connection.duckdb_query_result.reset();
			return make_uniq<ErrorResponse>(std::move(error_message));
		}
		auto needs_more_fetch = results.size() == max_chunks_per_batch;
		if (!needs_more_fetch && connection.query_state == QuackQueryState::ACTIVE) {
			connection.query_state = QuackQueryState::FINISHED;
		}
		return make_uniq<PrepareResponseMessage>(types, names, std::move(results), needs_more_fetch,
		                                         connection.query_uuid);
	}

	case MessageType::FETCH_REQUEST: {
		auto &fetch_request_message = received_message.Cast<FetchRequestMessage>();
		auto &connection = *connection_p;
		std::unique_lock<std::mutex> lock(connection.lock);

		if (connection.query_uuid != fetch_request_message.uuid) {
			return make_uniq<ErrorResponse>("Result has been closed");
		}
		if (connection.query_state == QuackQueryState::CANCELLED) {
			return make_uniq<ErrorResponse>("Query was interrupted");
		}
		if (!connection.duckdb_query_result) {
			return make_uniq<FetchResponseMessage>();
		}
		if (connection.duckdb_query_result->HasError()) {
			return make_uniq<ErrorResponse>(connection.duckdb_query_result->GetErrorObject());
		}

		Value max_chunks_val;
		DBConfig::GetConfig(db).TryGetCurrentSetting("quack_fetch_batch_chunks", max_chunks_val);
		auto max_chunks_per_batch = max_chunks_val.GetValue<uint64_t>();

		auto results = CreateBatch(Allocator::Get(db), connection.duckdb_query_result, max_chunks_per_batch);
		if (connection.duckdb_query_result && connection.duckdb_query_result->HasError()) { // TODO this is duplicated
			D_ASSERT(results.empty());
			auto error_message = connection.duckdb_query_result->GetErrorObject();
			connection.duckdb_query_result.reset();
			return make_uniq<ErrorResponse>(std::move(error_message));
		}
		auto assigned_batch_index = connection.next_batch_index++;
		if (results.size() < max_chunks_per_batch && connection.query_state == QuackQueryState::ACTIVE) {
			connection.query_state = QuackQueryState::FINISHED;
		}
		return make_uniq<FetchResponseMessage>(std::move(results), optional_idx(assigned_batch_index));
	}

	case MessageType::APPEND_REQUEST: {
		auto &append_request_message = received_message.Cast<AppendRequestMessage>();
		auto &connection = *connection_p;

		// we never execute this query, but throw it at the authorization function so it can check if this user gets to
		// insert into this table
		auto dummy_insert_query =
		    StringUtil::Format("INSERT INTO %s.%s VALUES (NULL)", SQLIdentifier(append_request_message.SchemaName()),
		                       SQLIdentifier(append_request_message.TableName()));

		// TODO do not do this if there is no fun set
		{
			auto auth_result = EvaluateAuthQuery(
			    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
			    Value(append_request_message.ConnectionId()), Value(dummy_insert_query));
			if (auth_result.IsNull() ||
			    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
				return make_uniq<ErrorResponse>("Authorization failed");
			}
		}

		std::unique_lock<std::mutex> lock(connection.lock);
		auto &context = *connection.duckdb_connection->context;
		auto table_info = context.TableInfo(Identifier(append_request_message.SchemaName()),
		                                    Identifier(append_request_message.TableName()));
		if (!table_info) {
			return make_uniq<ErrorResponse>("Table %s.%s does not exist",
			                                SQLIdentifier(append_request_message.SchemaName()),
			                                SQLIdentifier(append_request_message.TableName()));
		}
		try {
			ColumnDataCollection collection(Allocator::Get(context), append_request_message.AppendChunk().GetTypes());
			collection.Append(append_request_message.AppendChunk());
			connection.duckdb_connection->Append(*table_info, collection);
		} catch (std::exception &ex) {
			// apend failed - directly pass error to user
			return make_uniq<ErrorResponse>(ErrorData(ex));
		}
		return make_uniq<SuccessResponse>();
	}
	case MessageType::CANCEL_REQUEST: {
		auto &cancel_request_message = received_message.Cast<CancelRequestMessage>();
		auto &connection = *connection_p;
		if (connection.query_uuid != cancel_request_message.query_uuid) {
			return make_uniq<ErrorResponse>("Attempted to cancel a different query with id '%d' instead of '%d'",
			                                cancel_request_message.query_uuid, connection.query_uuid);
		}
		connection.duckdb_connection->Interrupt();
		connection.query_state = QuackQueryState::CANCELLED;
		connection.duckdb_query_result.reset();
		return make_uniq<SuccessResponse>();
	}
	default: {
		return make_uniq<ErrorResponse>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(received_message.Type())));
	}
	}
}
} // namespace duckdb
