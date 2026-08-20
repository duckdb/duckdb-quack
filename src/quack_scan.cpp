#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "quack_scan.hpp"
#include "quack_client.hpp"
#include "quack_fetch_ahead.hpp"
#include "include/storage/quack_catalog.hpp"
#include "storage/quack_transaction.hpp"

#include <queue>
namespace duckdb {

static unique_ptr<FunctionData> QuackScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	// Set logging to be pretty verbose (everything except message payloads)
	if (input.inputs.empty()) {
		throw InternalException("No input to quack scan?");
	}
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("quack_query URI and query parameters cannot be NULL");
	}

	auto query = input.inputs[1].GetValue<string>();
	auto initial_uri = QuackUri(input.inputs[0].GetValue<string>());

	// no ssl on local by default
	auto enable_ssl = !initial_uri.IsLocal();
	if (input.named_parameters.find("disable_ssl") != input.named_parameters.end()) {
		enable_ssl = !input.named_parameters["disable_ssl"].GetValue<bool>();
	}

	auto bind_data = make_uniq<QuackScanBindData>();
	auto server_uri = QuackUri(initial_uri.Uri(), enable_ssl);

	// Resolve auth token: prefer a quack secret scoped to this URI; fall back to the
	// global rpc_default_token setting. Mirrors the logic in QuackCatalog::QuackCatalog.
	string token;
	if (input.named_parameters.find("token") != input.named_parameters.end()) {
		token = input.named_parameters["token"].GetValue<string>();
	}
	auto client_id_entry = input.named_parameters.find("client_id");
	auto client_id = QuackClient::ResolveClientId(
	    context, client_id_entry != input.named_parameters.end() ? &client_id_entry->second : nullptr);
	auto heartbeat_timeout_entry = input.named_parameters.find("heartbeat_timeout");
	auto heartbeat_timeout = QuackClient::ResolveHeartbeatTimeout(
	    context, heartbeat_timeout_entry != input.named_parameters.end() ? &heartbeat_timeout_entry->second : nullptr);
	bind_data->client_connection =
	    QuackClient::ConnectToServer(context, server_uri, token, std::move(client_id), heartbeat_timeout);
	auto &client_connection = *bind_data->client_connection;

	auto client_wrapper = client_connection.GetClient(context);
	auto &client = client_wrapper->GetClient();

	bind_data->query_uuid = UUID::GenerateRandomUUID();
	auto bind_response = client.Request<PrepareResponseMessage>(
	    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query, bind_data->query_uuid));

	return_types = bind_response->Types();
	for (auto &col_name : bind_response->Names()) {
		names.push_back(Identifier(col_name));
	}

	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data->query_uuid = bind_response->QueryUUID();

	return bind_data;
}

static unique_ptr<FunctionData> QuackScanBindCatalogName(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("catalog_name and query parameters cannot be NULL");
	}
	bool use_transaction = false;
	auto entry = input.named_parameters.find("use_transaction");
	if (entry != input.named_parameters.end()) {
		if (entry->second.IsNull()) {
			throw InvalidInputException("use_transaction cannot be null");
		}
		use_transaction = BooleanValue::Get(entry->second);
	}

	bool refresh_catalog = false;
	auto refresh_entry = input.named_parameters.find("refresh_catalog");
	if (refresh_entry != input.named_parameters.end()) {
		if (refresh_entry->second.IsNull()) {
			throw InvalidInputException("refresh_catalog cannot be null");
		}
		refresh_catalog = BooleanValue::Get(refresh_entry->second);
	}

	auto &catalog = QuackCatalog::GetQuackCatalog(context, input.inputs[0]);
	if (use_transaction) {
		// start a transaction if "use_transaction" is specified
		auto &transaction = QuackTransaction::Get(context, catalog);
		transaction.ForceStart();
	}

	// TODO some of this stuff below is duplicated af
	auto query = input.inputs[1].GetValue<string>();
	auto bind_data = make_uniq<QuackScanBindData>();
	bind_data->client_connection = catalog.GetClientConnection();
	auto client_wrapper = bind_data->client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	bind_data->query_uuid = UUID::GenerateRandomUUID();
	auto bind_response = client.Request<PrepareResponseMessage>(
	    context,
	    make_uniq<PrepareRequestMessage>(bind_data->client_connection->ConnectionId(), query, bind_data->query_uuid));

	return_types = bind_response->Types();
	for (auto &col_name : bind_response->Names()) {
		names.push_back(Identifier(col_name));
	}

	// new stuff
	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data->query_uuid = bind_response->QueryUUID();
	if (refresh_catalog) {
		// the query was a statement pushed down by the RemotePushdownOptimizer - the server has already
		// executed it (PREPARE runs the query), so reload our snapshot of the remote catalog. Such a
		// statement is always bound on its own, so no catalog entry is in use here
		catalog.Refresh(context);
	}
	return bind_data;
}

enum class ChunkResultPushdownType { REQUIRES_PUSHDOWN, PUSHDOWN_ALREADY_APPLIED };

class ChunkResult {
public:
	explicit ChunkResult(DataChunk &chunk_p, ChunkResultPushdownType pushdown_type_p) : pushdown_type(pushdown_type_p) {
		chunk = make_uniq<DataChunk>();
		chunk->InitializeEmpty(chunk_p.GetTypes());
		chunk->Reference(chunk_p);
	}
	DataChunk &Chunk() {
		return *chunk;
	}
	bool RequiresPushdown() const {
		return pushdown_type == ChunkResultPushdownType::REQUIRES_PUSHDOWN;
	}

private:
	unique_ptr<DataChunk> chunk;
	ChunkResultPushdownType pushdown_type;
};

struct QuackScanLocalState : public LocalTableFunctionState {
	explicit QuackScanLocalState() {
	}
	~QuackScanLocalState() override {
	}

	//! Server-assigned index of the batch currently being drained; surfaced via get_partition_data
	//! so order-preserving operators (CTAS, COPY TO) can run the scan in parallel without losing order.
	optional_idx current_batch_index;

	//! This thread's outstanding batch claim; persists across BLOCKED yields until the batch arrives.
	optional_idx fetch_claim;
	//! The stream ended below this thread's claim; no more batches for this thread.
	bool fetch_exhausted = false;
	//! This thread already reported its completion to the global scanner countdown.
	bool finish_reported = false;

	queue<ChunkResult> results;
	ColumnDataScanState scan_state;
};

struct QuackScanGlobalState : GlobalTableFunctionState {
	explicit QuackScanGlobalState(vector<ColumnIndex> column_ids_p, vector<idx_t> projection_id_p,
	                              vector<ChunkResult> results_p, bool needs_more_fetch_p, hugeint_t query_uuid_p,
	                              bool reconnects_enabled_p)
	    : max_threads(needs_more_fetch_p ? MAX_THREADS : 1), column_ids(std::move(column_ids_p)),
	      projection_ids(std::move(projection_id_p)), query_uuid(query_uuid_p),
	      reconnects_enabled(reconnects_enabled_p), results(std::move(results_p)) {
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	vector<ColumnIndex> column_ids;
	vector<idx_t> projection_ids;
	hugeint_t query_uuid;
	//! FETCH read-ahead pipeline; set when the PREPARE response signals more batches to fetch.
	shared_ptr<QuackFetcher> fetcher;
	bool reconnects_enabled;
	//! Number of active scanners
	atomic<idx_t> active_scanners {0};
	atomic<bool> ack_sent {false};

	vector<ChunkResult> TryGetResults() {
		lock_guard<mutex> guard(lock);
		return std::move(results);
	}

private:
	mutex lock;
	vector<ChunkResult> results;
};

static string BuildPushdownQuery(const QuackScanBindData &bind_data, const TableFunctionInitInput &input) {
	string query;

	// Projection: select only the columns DuckDB actually needs in the output.
	// With filter_prune, projection_ids indexes into column_ids for output columns only.
	// Filter-only columns are in column_ids but NOT in projection_ids — they go in WHERE, not SELECT.
	if (!input.column_indexes.empty()) {
		for (auto &col_id : input.column_indexes) {
			if (!query.empty()) {
				query += ", ";
			}
			if (col_id.IsVirtualColumn()) {
				auto virtual_column = col_id.GetPrimaryIndex();
				if (virtual_column == COLUMN_IDENTIFIER_EMPTY || virtual_column == COLUMN_IDENTIFIER_ROW_ID) {
					query += "NULL::BIGINT";
				} else {
					throw InternalException("Unsupported virtual column index");
				}
			} else {
				query += "#" + to_string(col_id.GetPrimaryIndex() + 1);
			}
		}
		query = "SELECT " + query + " ";
	}
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
	query += StringUtil::Format("FROM %s", SQLIdentifier(bind_data.table_name));
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

static bool ReconnectsEnabled(ClientContext &context) {
	Value val;
	if (!context.TryGetCurrentSetting("quack_enable_reconnects", val)) {
		return false;
	}
	return val.GetValue<bool>();
}

unique_ptr<GlobalTableFunctionState> QuackScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();

	// For the catalog path (ATTACH), LookupEntry only prepares without executing
	// to avoid the server-side result being overwritten by subsequent lookups.
	// We execute the query here, right before scanning, so the result is fresh.
	vector<ChunkResult> results;
	bool needs_more_fetch = bind_data.needs_more_fetch;
	hugeint_t query_uuid;
	if (!bind_data.table_name.empty()) {
		// apply pushdown to the query
		auto query = BuildPushdownQuery(bind_data, input);
		auto &client_connection = *bind_data.client_connection;
		auto client_wrapper = client_connection.GetClient(context);
		auto &client = client_wrapper->GetClient();
		query_uuid = UUID::GenerateRandomUUID();
		auto response_message = client.Request<PrepareResponseMessage>(
		    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query, query_uuid));
		needs_more_fetch = response_message->NeedsMoreFetch();
		// fetch the result
		for (auto &chunk_ref : response_message->MutableResults()) {
			auto &chunk = chunk_ref->Chunk();
			results.emplace_back(chunk, ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED);
		}
	} else {
		for (auto &chunk_ref : bind_data.results) {
			auto &chunk = chunk_ref->Chunk();
			results.emplace_back(chunk, ChunkResultPushdownType::REQUIRES_PUSHDOWN);
		}
		query_uuid = bind_data.query_uuid;
	}
	// we only multithread if there is more to fetch
	auto global_state = make_uniq<QuackScanGlobalState>(input.column_indexes, input.projection_ids, std::move(results),
	                                                    needs_more_fetch, query_uuid, ReconnectsEnabled(context));
	if (needs_more_fetch) {
		// start pipelining FETCH requests on the ASYNC pool before the first scan call
		global_state->fetcher = make_shared_ptr<QuackFetcher>(context, *bind_data.client_connection, query_uuid,
		                                                      QuackFetcher::GetReadAheadDepth(context));
	}
	return std::move(global_state);
}

unique_ptr<LocalTableFunctionState> QuackScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	auto &global_state = global_state_p->Cast<QuackScanGlobalState>();
	auto local_state = make_uniq<QuackScanLocalState>();

	global_state.active_scanners++;
	auto results = global_state.TryGetResults();
	for (auto &chunk : results) {
		local_state->results.push(std::move(chunk));
	}
	return local_state;
}

static void SendAcknowledgement(ClientContext &context, QuackScanGlobalState &global_state,
                                QuackScanBindData &bind_data) {
	if (global_state.ack_sent.exchange(true)) {
		return;
	}
	try {
		auto client_wrapper = bind_data.client_connection->GetClient(context);
		client_wrapper->GetClient().Request<SuccessResponse>(
		    context,
		    make_uniq<AcknowledgementMessage>(bind_data.client_connection->ConnectionId(), global_state.query_uuid));
	} catch (const std::exception &e) {
		// The query is already complete, so we swallow the failure rather than fail the query
		DUCKDB_LOG_WARNING(
		    context, StringUtil::Format("Quack: acknowledgement failed and was ignored (query already completed): %s",
		                                e.what()));
	}
}

static void FinishScan(ClientContext &context, QuackScanGlobalState &global_state, QuackScanLocalState &local_state,
                       QuackScanBindData &bind_data) {
	bind_data.completed = true;
	if (local_state.finish_reported) {
		return;
	}
	local_state.finish_reported = true;
	if (global_state.active_scanners.fetch_sub(1) != 1) {
		return;
	}
	if (!global_state.reconnects_enabled) {
		return;
	}
	if (global_state.fetcher) {
		global_state.fetcher->StopAndDrain();
	}
	SendAcknowledgement(context, global_state, bind_data);
}

static void QuackScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->CastNoConst<QuackScanBindData>();
	auto &global_state = input.global_state->Cast<QuackScanGlobalState>();
	auto &local_state = input.local_state->Cast<QuackScanLocalState>();

	while (true) {
		// first we try to scan from our local results buffer if we have any
		while (!local_state.results.empty()) {
			auto chunk = std::move(local_state.results.front());
			local_state.results.pop();

			auto &response_chunk = chunk.Chunk();
			if (response_chunk.size() > 0) {
				if (!chunk.RequiresPushdown()) {
					output.Reference(response_chunk);
				} else {
					for (idx_t i = 0; i < global_state.column_ids.size(); i++) {
						auto &index = global_state.column_ids[i];
						if (index.IsVirtualColumn()) {
							// TODO
							output.data[i].Reference(Value(output.data[i].GetType()), count_t(response_chunk.size()));
							return;
						}
						auto col_idx = index.GetPrimaryIndex();
						output.data[i].Reference(response_chunk.data[col_idx]);
					}
					output.CheckCardinality(response_chunk.size());
				}
				return;
			}
		}

		// if that did not work, we consume this thread's claimed batch from the fetcher
		if (local_state.results.empty() && global_state.fetcher && !local_state.fetch_exhausted) {
			idx_t batch_index;
			vector<unique_ptr<DataChunk>> chunks;
			switch (global_state.fetcher->GetBatch(context, input, local_state.fetch_claim, batch_index, chunks)) {
			case QuackFetchResult::BATCH: {
				// tag fetched chunks like the initial batch (see QuackScanInitGlobal): direct queries
				// return full-width chunks that still need projection, the catalog path already projected
				auto fetched_pushdown_type = bind_data.table_name.empty()
				                                 ? ChunkResultPushdownType::REQUIRES_PUSHDOWN
				                                 : ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED;
				for (auto &chunk : chunks) {
					local_state.results.emplace(*chunk, fetched_pushdown_type);
				}
				local_state.current_batch_index = batch_index;
				continue;
			}
			case QuackFetchResult::FINISHED:
				// server is done, this thread is done
				local_state.fetch_exhausted = true;
				FinishScan(context, global_state, local_state, bind_data);
				return;
			case QuackFetchResult::BLOCKED:
				return;
			}
		}
		// we did not have anything cached and then request to the server did not yield anything - we are done
		FinishScan(context, global_state, local_state, bind_data);
		break;
	}
}

static OperatorPartitionData QuackScanGetPartitionData(ClientContext &, TableFunctionGetPartitionInput &input) {
	auto &local_state = input.local_state->Cast<QuackScanLocalState>();
	// If we haven't received a batch yet, fall back to 0 so downstream doesn't choke; the
	// planner only calls this after QuackScan has returned rows, by which point the current
	// batch index is always set.
	auto idx = local_state.current_batch_index.IsValid() ? local_state.current_batch_index.GetIndex() : 0;
	return OperatorPartitionData(idx);
}

InsertionOrderPreservingMap<string> QuackScanToString(TableFunctionToStringInput &input) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	InsertionOrderPreservingMap<string> result;
	result["Server"] = bind_data.client_connection->ServerURI().Uri();
	return result;
}

void QuackScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                        const TableFunction &function) {
	throw NotImplementedException("Quack scans cannot be serialized (yet?)");
}

unique_ptr<FunctionData> QuackScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("Quack scans cannot be deserialized (yet?)");
}

BindInfo QuackScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->CastNoConst<QuackScanBindData>();
	if (bind_data.table_entry) {
		return BindInfo(*bind_data.table_entry);
	}
	return BindInfo(ScanType::EXTERNAL);
}

TableFunction QuackScanFunction::GetFunction() {
	auto fun = TableFunction("quack_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, QuackScan, QuackScanBind,
	                         QuackScanInitGlobal, QuackScanInitLocal);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.named_parameters["token"] = LogicalType::VARCHAR;
	fun.named_parameters["client_id"] = LogicalType::VARCHAR;
	fun.named_parameters["heartbeat_timeout"] = LogicalType::UBIGINT;

	fun.projection_pushdown = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.get_bind_info = QuackScanGetBindInfo;
	// fun.filter_pushdown = true;
	// fun.filter_prune = true;
	return fun;
}

TableFunction QuackScanByNameFunction::GetFunction() {
	auto fun = TableFunction("quack_query_by_name", {LogicalType::VARCHAR, LogicalType::VARCHAR}, QuackScan,
	                         QuackScanBindCatalogName, QuackScanInitGlobal, QuackScanInitLocal);
	fun.projection_pushdown = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.get_bind_info = QuackScanGetBindInfo;
	fun.named_parameters["use_transaction"] = LogicalType::BOOLEAN;
	fun.named_parameters["refresh_catalog"] = LogicalType::BOOLEAN;
	// fun.filter_pushdown = true;
	// fun.filter_prune = true;
	return fun;
}

bool QuackCatalog::IsQuackScan(const string &name) {
	return name == "quack_query" || name == "quack_query_by_name";
}

} // namespace duckdb
