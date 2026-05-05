#include "rpc_scan_function.hpp"
#include "client.hpp"
#include "catalog.hpp"

#include "duckdb/function/table_function.hpp"
#include "rpc_bind_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"

#include <queue>
using namespace duckdb;

static unique_ptr<FunctionData> RpcBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	// Set logging to be pretty verbose (everything except message payloads)
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("call_rpc_server URI and query parameters cannot be NULL");
	}

	auto query = input.inputs[1].GetValue<string>();
	auto initial_uri = RpcUri(input.inputs[0].GetValue<string>());

	// no ssl on local by default
	auto enable_ssl = !initial_uri.IsLocal();
	if (input.named_parameters.find("disable_ssl") != input.named_parameters.end()) {
		enable_ssl = !input.named_parameters["disable_ssl"].GetValue<bool>();
	}

	auto bind_data = make_uniq<RpcBindData>();

	bind_data->server_uri = RpcUri(initial_uri.Uri(), enable_ssl);

	bind_data->initial_client = RpcClient::GetClient(context, bind_data->server_uri);

	// Resolve auth token: prefer a quack secret scoped to this URI; fall back to the
	// global rpc_default_token setting. Mirrors the logic in RpcCatalog::RpcCatalog.
	string token;
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, bind_data->server_uri.Uri(), "quack");
	if (match.HasMatch()) {
		const auto &kv = dynamic_cast<const KeyValueSecret &>(*match.secret_entry->secret);
		token = kv.TryGetValue("token", true).ToString();
	} else {
		Value default_token_val;
		auto &config = DBConfig::GetConfig(context);
		auto lookup_result_token = config.TryGetCurrentSetting("rpc_default_token", default_token_val);
		D_ASSERT(lookup_result_token);
		if (default_token_val.IsNull() || default_token_val.type().id() != LogicalTypeId::VARCHAR) {
			throw InvalidConfigurationException("No RPC token found");
		}
		token = default_token_val.GetValue<string>();
	}

	// Bundle CONNECT + PREPARE in a single round-trip. Empty connection_id on
	// PREPARE is filled in server-side from the CONNECTION_RESPONSE produced
	// by the CONNECT message earlier in the batch (chain_connection_id).
	vector<unique_ptr<ProtocolMessage>> batch_messages;
	batch_messages.push_back(make_uniq<ConnectionRequestMessage>(token));
	batch_messages.push_back(make_uniq<PrepareRequestMessage>("", query, true));

	auto batch_response = bind_data->initial_client->Request<BatchResponseMessage>(
	    make_uniq<BatchRequestMessage>(std::move(batch_messages), /*chain_connection_id*/ true));

	auto &responses = batch_response->MutableResponses();
	if (responses.empty()) {
		throw IOException("Batch response was empty");
	}
	// If the server stopped on error, the last entry is an ERROR.
	if (responses.back()->Type() == MessageType::ERROR) {
		throw IOException(responses.back()->Cast<ErrorMessage>().Error());
	}
	if (responses.size() != 2 || responses[0]->Type() != MessageType::CONNECTION_RESPONSE ||
	    responses[1]->Type() != MessageType::PREPARE_RESPONSE) {
		throw IOException("Unexpected batch response shape");
	}
	bind_data->connection_id = responses[0]->Cast<ConnectionResponseMessage>().ConnectionId();
	auto &bind_response = responses[1]->Cast<PrepareResponseMessage>();

	return_types = bind_response.Types();
	names = bind_response.Names();

	bind_data->results = std::move(bind_response.MutableResults());
	bind_data->needs_more_fetch = bind_response.NeedsMoreFetch();

	return bind_data;
}

RpcCatalog &GetRpcCatalog(ClientContext &context, Value &catalog_name) {
	if (catalog_name.IsNull()) {
		throw BinderException("Catalog cannot be NULL");
	}
	// look up the database to query
	auto db_name = catalog_name.GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "quack") {
		throw BinderException("Attached database \"%s\" does not refer to a RPC database", db_name);
	}
	return catalog.Cast<RpcCatalog>();
}

static unique_ptr<FunctionData> RpcBindCatalogName(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("catalog_name and query parameters cannot be NULL");
	}

	auto &rpc_catalog = GetRpcCatalog(context, input.inputs[0]);

	// TODO some of this stuff below is duplicated af

	auto query = input.inputs[1].GetValue<string>();
	auto bind_data = make_uniq<RpcBindData>();
	bind_data->server_uri = rpc_catalog.GetServerUri();
	bind_data->connection_id = rpc_catalog.GetConnectionId();

	auto bind_response = rpc_catalog.GetRawClient().Request<PrepareResponseMessage>(
	    make_uniq<PrepareRequestMessage>(bind_data->connection_id, query, true));

	return_types = bind_response->Types();
	names = bind_response->Names();

	// new stuff
	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();

	return bind_data;
}

struct RpcLocalState : public LocalTableFunctionState {
	unique_ptr<RpcClient> client;
	//! batch_index of the batch that `fetched_results` currently holds chunks from (server-assigned).
	//! Surfaced to DuckDB via get_partition_data so downstream order-preserving operators
	//! (CTAS, COPY TO, INSERT SELECT) can run the scan in parallel without losing order.
	optional_idx current_batch_index;

	queue<unique_ptr<DataChunk>> results;
	ColumnDataScanState scan_state;

	explicit RpcLocalState() {
	}
	~RpcLocalState() override {
	}
};

struct RpcGlobalState : GlobalTableFunctionState {
	explicit RpcGlobalState(vector<column_t> column_ids_p, vector<idx_t> projection_id_p, bool needs_more_fetch_p)
	    : max_threads(needs_more_fetch_p ? MAX_THREADS : 1), column_ids(column_ids_p), projection_ids(projection_id_p),
	      needs_more_fetch(needs_more_fetch_p) {
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	vector<column_t> column_ids;
	vector<idx_t> projection_ids;
	atomic<bool> needs_more_fetch;
};

static bool CanPushdownFilter(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
	case TableFilterType::IN_FILTER:
		return true;
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction = filter.Cast<ConjunctionAndFilter>();
		for (auto &child : conjunction.child_filters) {
			if (!CanPushdownFilter(*child)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction = filter.Cast<ConjunctionOrFilter>();
		for (auto &child : conjunction.child_filters) {
			if (!CanPushdownFilter(*child)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

static string BuildPushdownQuery(const RpcBindData &bind_data, const TableFunctionInitInput &input) {
	string query;

	// Projection: select only the columns DuckDB actually needs in the output.
	// With filter_prune, projection_ids indexes into column_ids for output columns only.
	// Filter-only columns are in column_ids but NOT in projection_ids — they go in WHERE, not SELECT.
	// if (!input.column_ids.empty() && !bind_data.column_names.empty()) {
	// 	vector<string> selected_columns;
	// 	if (!input.projection_ids.empty()) {
	// 		for (auto &proj_id : input.projection_ids) {
	// 			auto col_id = input.column_ids[proj_id];
	// 			if (IsRowIdColumnId(col_id) || col_id >= bind_data.column_names.size()) {
	// 				continue;
	// 			}
	// 			selected_columns.push_back(KeywordHelper::WriteOptionallyQuoted(bind_data.column_names[col_id]));
	// 		}
	// 	} else {
	// 		for (auto &col_id : input.column_ids) {
	// 			if (IsRowIdColumnId(col_id) || col_id >= bind_data.column_names.size()) {
	// 				continue;
	// 			}
	// 			selected_columns.push_back(KeywordHelper::WriteOptionallyQuoted(bind_data.column_names[col_id]));
	// 		}
	// 	}
	// 	if (!selected_columns.empty()) {
	// 		query = "SELECT " + StringUtil::Join(selected_columns, ", ") + " ";
	// 	}
	// }
	query += StringUtil::Format("FROM %s", bind_data.table_name);
	//
	// // Filters: build WHERE clause from pushable filters
	// if (input.filters) {
	// 	vector<string> where_clauses;
	// 	for (auto &entry : input.filters->filters) {
	// 		auto col_idx = entry.second.GetIndex();
	// 		if (col_idx >= bind_data.column_names.size()) {
	// 			continue;
	// 		}
	// 		auto &filter = entry.Filter();
	// 		if (!CanPushdownFilter(filter)) {
	// 			continue;
	// 		}
	// 		auto col_name = KeywordHelper::WriteOptionallyQuoted(bind_data.column_names[col_idx]);
	// 		where_clauses.push_back(filter.ToString(col_name));
	// 	}
	// 	if (!where_clauses.empty()) {
	// 		query += " WHERE " + StringUtil::Join(where_clauses, " AND ");
	// 	}
	// }

	return query;
}

unique_ptr<GlobalTableFunctionState> RpcInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<RpcBindData>();

	// For the catalog path (ATTACH), LookupEntry only prepares without executing
	// to avoid the server-side result being overwritten by subsequent lookups.
	// We execute the query here, right before scanning, so the result is fresh.

	if (!bind_data.table_name.empty()) {
		auto query = BuildPushdownQuery(bind_data, input);
		auto client = RpcClient::GetClient(context, bind_data.server_uri);
		auto response_message = client->Request<PrepareResponseMessage>(
		    make_uniq<PrepareRequestMessage>(bind_data.connection_id, query, true));
		bind_data.needs_more_fetch = response_message->NeedsMoreFetch();
		bind_data.results = std::move(response_message->MutableResults());
	}

	// we only multithread if there is more to fetch
	return make_uniq<RpcGlobalState>(input.column_ids, input.projection_ids, bind_data.needs_more_fetch);
}

unique_ptr<LocalTableFunctionState> RpcInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                 GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->CastNoConst<RpcBindData>();
	lock_guard<mutex> guard(bind_data.lock);

	auto local_state = make_uniq<RpcLocalState>();
	// re-use initial client from bind if possible
	if (bind_data.initial_client) {
		local_state->client = std::move(bind_data.initial_client);
	} else {
		local_state->client = RpcClient::GetClient(context.client, bind_data.server_uri);
	}

	if (!bind_data.results.empty()) {
		for (auto &chunk : bind_data.results) {
			local_state->results.push(std::move(chunk));
		}
		bind_data.results.clear();
	}
	return local_state;
}

static void RpcScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = input.global_state->Cast<RpcGlobalState>();
	auto &local_state = input.local_state->Cast<RpcLocalState>();

	while (true) {
		// first we try to scan from our local results buffer if we have any
		if (!local_state.results.empty()) {
			auto chunk = std::move(local_state.results.front());
			local_state.results.pop();

			auto &response_chunk = *chunk;
			const auto &col_ids = global_state.column_ids;
			const auto &proj_ids = global_state.projection_ids;
			if (response_chunk.ColumnCount() == output.ColumnCount() && col_ids.empty() && proj_ids.empty()) {
				output.Reference(response_chunk);
			} else {
				// Map each output column to the right response column. DuckDB's pushdown contract:
				//   - column_ids lists which source columns the scan should read (in that order).
				//   - projection_ids (when filter_prune=true) indexes into column_ids for the outputs.
				// The server ignores pushdown and returns every column, so we apply both hops here.
				for (idx_t i = 0; i < output.ColumnCount(); i++) {
					idx_t src;
					if (!proj_ids.empty()) {
						src = col_ids[proj_ids[i]];
					} else if (!col_ids.empty()) {
						src = col_ids[i];
					} else {
						src = i;
					}
					D_ASSERT(src < response_chunk.ColumnCount());
					D_ASSERT(response_chunk.data[src].GetType() == output.data[i].GetType());
					output.data[i].Reference(response_chunk.data[src]);
				}
			}
			output.SetCardinality(chunk->size());

			return;
		}

		// if that did not work, we request more results
		if (local_state.results.empty() && global_state.needs_more_fetch) {
			auto fetch_response = local_state.client->Request<FetchResponseMessage>(
			    make_uniq<FetchRequestMessage>(bind_data.connection_id));

			if (fetch_response->MutableResults().empty()) {
				// server is done, we are done
				global_state.needs_more_fetch = false;
				return;
			}
			// set up buffer for scan in next iteration
			for (auto &chunk : fetch_response->MutableResults()) {
				local_state.results.push(std::move(chunk));
			}
			local_state.current_batch_index = fetch_response->BatchIndex();
			continue;
		}
		// we did not have anything cached and then request to the server did not yield anything - we are done
		break;
	}
}

static OperatorPartitionData RpcGetPartitionData(ClientContext &, TableFunctionGetPartitionInput &input) {
	auto &local_state = input.local_state->Cast<RpcLocalState>();
	// If we haven't received a batch yet, fall back to 0 so downstream doesn't choke; the
	// planner only calls this after RpcScan has returned rows, by which point the current
	// batch index is always set.
	auto idx = local_state.current_batch_index.IsValid() ? local_state.current_batch_index.GetIndex() : 0;
	return OperatorPartitionData(idx);
}

InsertionOrderPreservingMap<string> RpcCallToString(TableFunctionToStringInput &input) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	InsertionOrderPreservingMap<string> result;
	result["Server"] = bind_data.server_uri.Uri();
	return result;
}

TableFunction RpcScanFunction::GetFunction() {
	auto fun = TableFunction("rpc_call", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcScan, RpcBind, RpcInitGlobal,
	                         RpcInitLocal);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.projection_pushdown = true;
	fun.get_partition_data = RpcGetPartitionData;
	fun.to_string = RpcCallToString;
	// fun.filter_pushdown = true;
	// fun.filter_prune = true;
	return fun;
}

TableFunction RpcScanByNameFunction::GetFunction() {
	auto fun = TableFunction("rpc_call_by_name", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcScan,
	                         RpcBindCatalogName, RpcInitGlobal, RpcInitLocal);
	fun.projection_pushdown = true;
	fun.get_partition_data = RpcGetPartitionData;
	fun.to_string = RpcCallToString;

	// fun.filter_pushdown = true;
	// fun.filter_prune = true;
	return fun;
}
