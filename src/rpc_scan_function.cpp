#include "rpc_scan_function.hpp"
#include "client.hpp"
#include "catalog.hpp"

#include "duckdb/function/table_function.hpp"
#include "rpc_bind_data.hpp"
#include "duckdb/main/database.hpp"
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
	auto disable_ssl = input.named_parameters.find("disable_ssl") != input.named_parameters.end() &&
	                   input.named_parameters["disable_ssl"].GetValue<bool>();

	auto bind_data = make_uniq<RpcBindData>();
	bind_data->server_uri = RpcUri(input.inputs[0].GetValue<string>(), !disable_ssl);

	bind_data->initial_client = RpcClient::GetClient(bind_data->server_uri);
	bind_data->initial_client->SetContext(&context);

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

	auto connection_request_response =
	    bind_data->initial_client->Request<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>(token));
	bind_data->connection_id = connection_request_response->ConnectionId();

	auto bind_response = bind_data->initial_client->Request<PrepareResponseMessage>(
	    make_uniq<PrepareRequestMessage>(bind_data->connection_id, query, true));

	bind_data->estimated_cardinality = bind_response->EstimatedCardinality();
	return_types = bind_response->Types();
	names = bind_response->Names();

	bind_data->needs_more_fetch = true;
	if (bind_response->HasResults()) {
		bind_data->initial_results = std::move(bind_response->MutableChunks());
		bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	}

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

	bind_data->estimated_cardinality = bind_response->EstimatedCardinality();
	return_types = bind_response->Types();
	names = bind_response->Names();

	// new stuff
	bind_data->needs_more_fetch = true;
	if (bind_response->HasResults()) {
		bind_data->initial_results = std::move(bind_response->MutableChunks());
		bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	}
	return bind_data;
}

struct RpcLocalState : public LocalTableFunctionState {
	unique_ptr<RpcClient> client;
	std::queue<unique_ptr<DataChunk>> pending;
	bool server_exhausted = false;
	//! batch_index of the batch that `pending` currently holds chunks from (server-assigned).
	//! Surfaced to DuckDB via get_partition_data so downstream order-preserving operators
	//! (CTAS, COPY TO, INSERT SELECT) can run the scan in parallel without losing order.
	optional_idx current_batch_index;

	explicit RpcLocalState() {
	}
	~RpcLocalState() override {
	}
};

struct RpcGlobalState : GlobalTableFunctionState {
	explicit RpcGlobalState(idx_t max_threads_p) : max_threads(max_threads_p), done(false) {
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	atomic<bool> done;
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
	if (!input.column_ids.empty() && !bind_data.column_names.empty()) {
		vector<string> selected_columns;
		if (!input.projection_ids.empty()) {
			for (auto &proj_id : input.projection_ids) {
				auto col_id = input.column_ids[proj_id];
				if (IsRowIdColumnId(col_id) || col_id >= bind_data.column_names.size()) {
					continue;
				}
				selected_columns.push_back(KeywordHelper::WriteOptionallyQuoted(bind_data.column_names[col_id]));
			}
		} else {
			for (auto &col_id : input.column_ids) {
				if (IsRowIdColumnId(col_id) || col_id >= bind_data.column_names.size()) {
					continue;
				}
				selected_columns.push_back(KeywordHelper::WriteOptionallyQuoted(bind_data.column_names[col_id]));
			}
		}
		if (!selected_columns.empty()) {
			query = "SELECT " + StringUtil::Join(selected_columns, ", ");
		}
	}
	if (query.empty()) {
		query = "SELECT *";
	}
	query += StringUtil::Format(" FROM %s", bind_data.table_name);
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
		auto client = RpcClient::GetClient(bind_data.server_uri);
		client->SetContext(&context);
		auto response_message = client->Request<PrepareResponseMessage>(
		    make_uniq<PrepareRequestMessage>(bind_data.connection_id, query, true));
		bind_data.needs_more_fetch = response_message->NeedsMoreFetch();
		bind_data.initial_results = std::move(response_message->MutableChunks());
	}

	// FIXME
	return make_uniq<RpcGlobalState>(1);
}

unique_ptr<LocalTableFunctionState> RpcInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                 GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->CastNoConst<RpcBindData>();
	auto &global_state = global_state_p->Cast<RpcGlobalState>();
	if (global_state.done) {
		return nullptr;
	}

	auto local_state = make_uniq<RpcLocalState>();
	// re-use initial client from bind if possible
	if (bind_data.initial_client) { // TODO possible race here, too?
		local_state->client = unique_ptr<RpcClient>(bind_data.initial_client.release());
	} else {
		local_state->client = RpcClient::GetClient(bind_data.server_uri);
		local_state->client->SetContext(&context.client);
	}

	// TODO we need a lock here
	if (!bind_data.initial_results.empty()) {
		for (auto &chunk : bind_data.initial_results) {
			local_state->pending.push(std::move(chunk));
		}
		bind_data.initial_results.clear();
	}
	return local_state;
}

static void RpcScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<RpcBindData>();
	auto &global_state = input.global_state->Cast<RpcGlobalState>();
	auto &local_state = input.local_state->Cast<RpcLocalState>();

	// Only issue a new FETCH if we've drained our local batch and the stream
	// hasn't signalled end. global_state.done is an optimization hint that
	// skips wasted FETCHes — it must not pre-empt draining local pending.
	if (local_state.pending.empty() && !local_state.server_exhausted && !global_state.done &&
	    bind_data.needs_more_fetch) {
		auto fetch_response =
		    local_state.client->Request<FetchResponseMessage>(make_uniq<FetchRequestMessage>(bind_data.connection_id));
		if (fetch_response->Chunks().empty()) {
			local_state.server_exhausted = true;
			global_state.done = true;
		} else {
			// Record this batch's server-assigned index so DuckDB can restore order
			// downstream when chunks from this local state are interleaved with those
			// produced by sibling threads.
			local_state.current_batch_index = fetch_response->BatchIndex();
			for (auto &chunk : fetch_response->MutableChunks()) {
				local_state.pending.push(std::move(chunk));
			}
		}
	}

	if (local_state.pending.empty()) {
		return;
	}

	auto chunk = std::move(local_state.pending.front());
	local_state.pending.pop();

	auto &response_chunk = *chunk;
	if (response_chunk.ColumnCount() == output.ColumnCount()) {
		output.Reference(response_chunk);
	} else {
		// Server returned more columns than needed (e.g. rpc_call with projection pushdown).
		// Copy only the columns DuckDB expects.
		for (idx_t i = 0; i < output.ColumnCount(); i++) {
			output.data[i].Reference(response_chunk.data[i]);
		}
	}
	output.SetCardinality(response_chunk.size());
}

static unique_ptr<NodeStatistics> RpcCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<RpcBindData>();
	if (bind_data.estimated_cardinality.IsValid()) {
		return make_uniq<NodeStatistics>(bind_data.estimated_cardinality.GetIndex());
	}
	return nullptr;
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
	result["Cardinality"] =
	    bind_data.estimated_cardinality.IsValid() ? to_string(bind_data.estimated_cardinality.GetIndex()) : "?";
	return result;
}

TableFunction RpcScanFunction::GetFunction() {
	auto fun = TableFunction("rpc_call", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RpcScan, RpcBind, RpcInitGlobal,
	                         RpcInitLocal);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.cardinality = RpcCardinality;
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
	fun.cardinality = RpcCardinality;
	fun.projection_pushdown = true;
	fun.get_partition_data = RpcGetPartitionData;
	fun.to_string = RpcCallToString;

	// fun.filter_pushdown = true;
	// fun.filter_prune = true;
	return fun;
}
