#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "quack_scan.hpp"
#include "quack_client.hpp"
#include "quack_filter.hpp"
#include "include/storage/quack_catalog.hpp"

#include <queue>
namespace duckdb {

static unique_ptr<FunctionData> QuackScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
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
	bind_data->client_connection = QuackClient::ConnectToServer(context, server_uri, token);
	auto &client_connection = *bind_data->client_connection;

	auto client_wrapper = client_connection.GetClient(context);
	auto &client = client_wrapper->GetClient();

	auto bind_response = client.Request<PrepareResponseMessage>(
	    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query));

	return_types = bind_response->Types();
	names = bind_response->Names();

	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data->result_uuid = bind_response->ResultUUID();

	return std::move(bind_data);
}

QuackCatalog &GetQuackCatalog(ClientContext &context, Value &catalog_name) {
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
	return catalog.Cast<QuackCatalog>();
}

static unique_ptr<FunctionData> QuackScanBindCatalogName(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("catalog_name and query parameters cannot be NULL");
	}

	auto &catalog = GetQuackCatalog(context, input.inputs[0]);

	// TODO some of this stuff below is duplicated af

	auto query = input.inputs[1].GetValue<string>();
	auto bind_data = make_uniq<QuackScanBindData>();
	bind_data->client_connection = catalog.GetClientConnection();
	auto client_wrapper = bind_data->client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	auto bind_response = client.Request<PrepareResponseMessage>(
	    context, make_uniq<PrepareRequestMessage>(bind_data->client_connection->ConnectionId(), query));

	return_types = bind_response->Types();
	names = bind_response->Names();

	// new stuff
	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data->result_uuid = bind_response->ResultUUID();
	return std::move(bind_data);
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

	unique_ptr<QuackClientWrapper> client_wrapper;
	//! batch_index of the batch that `fetched_results` currently holds chunks from (server-assigned).
	//! Surfaced to DuckDB via get_partition_data so downstream order-preserving operators
	//! (CTAS, COPY TO, INSERT SELECT) can run the scan in parallel without losing order.
	optional_idx current_batch_index;

	queue<ChunkResult> results;
	ColumnDataScanState scan_state;
};

struct QuackScanGlobalState : GlobalTableFunctionState {
	explicit QuackScanGlobalState(vector<ColumnIndex> column_ids_p, vector<idx_t> projection_id_p,
	                              vector<ChunkResult> results_p, bool needs_more_fetch_p, hugeint_t result_uuid_p,
	                              string pushed_sql_p)
	    : max_threads(needs_more_fetch_p ? MAX_THREADS : 1), column_ids(std::move(column_ids_p)),
	      projection_ids(std::move(projection_id_p)), needs_more_fetch(needs_more_fetch_p), result_uuid(result_uuid_p),
	      pushed_sql(std::move(pushed_sql_p)), results(std::move(results_p)) {
		// Pre-compute the effective output-column list once: with filter_prune on the
		// scan emits only the projection_ids subset of column_ids; with it off the
		// effective set is column_ids itself. Used by the URI-path fallback that
		// references chunks position-by-position.
		effective_column_ids.reserve(projection_ids.empty() ? column_ids.size() : projection_ids.size());
		if (projection_ids.empty()) {
			for (auto &c : column_ids) {
				effective_column_ids.push_back(c);
			}
		} else {
			for (auto pid : projection_ids) {
				if (pid < column_ids.size()) {
					effective_column_ids.push_back(column_ids[pid]);
				}
			}
		}
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	vector<ColumnIndex> column_ids;
	vector<idx_t> projection_ids;
	//! `projection_ids`-resolved subset of column_ids (or column_ids itself when no
	//! filter_prune is in effect). Lives here so the per-row scan path doesn't have to
	//! recompute it for every output chunk.
	vector<ColumnIndex> effective_column_ids;
	atomic<bool> needs_more_fetch;
	hugeint_t result_uuid;
	//! The SQL string that was actually sent to the server for this scan (after all
	//! pushdown rewriting). Surfaced via the function's dynamic_to_string callback so
	//! it shows up in EXPLAIN ANALYZE. Empty for the raw quack_query(uri, sql) path
	//! where the user's SQL is sent verbatim.
	string pushed_sql;

	vector<ChunkResult> TryGetResults() {
		lock_guard<mutex> guard(lock);
		return std::move(results);
	}

private:
	mutex lock;
	vector<ChunkResult> results;
};

// Returns the positional reference (e.g. "#3") to use in pushed-down SQL for the column
// at index `column_indexes_idx` of the scan's `column_indexes`. Virtual columns are emitted
// as a typed NULL placeholder. Used both for SELECT-list and WHERE-clause emission so that
// projection and filter pushdown reference the same underlying table columns by position.
static string PositionalColumnRef(const ColumnIndex &col_id) {
	if (col_id.IsVirtualColumn()) {
		auto virtual_column = col_id.GetPrimaryIndex();
		if (virtual_column == COLUMN_IDENTIFIER_EMPTY || virtual_column == COLUMN_IDENTIFIER_ROW_ID) {
			return "NULL::BIGINT";
		}
		throw InternalException("Unsupported virtual column index in quack pushdown");
	}
	return "#" + to_string(col_id.GetPrimaryIndex() + 1);
}

static bool IsFilterPushdownEnabled(ClientContext &context) {
	Value v;
	if (!context.TryGetCurrentSetting("quack_pushdown_filters", v)) {
		return true;
	}
	return v.GetValue<bool>();
}

// Returns the effective list of column_indexes the scan must output, honoring filter_prune:
// with filter_prune=true, only the `projection_ids` subset of column_indexes is emitted
// (filter-only columns are referenced in WHERE via their original primary index but not
// shipped back from the server). With filter_prune=false, returns column_indexes unchanged.
static vector<ColumnIndex> EffectiveOutputColumns(const TableFunctionInitInput &input) {
	if (input.projection_ids.empty()) {
		return input.column_indexes;
	}
	vector<ColumnIndex> result;
	result.reserve(input.projection_ids.size());
	for (auto pid : input.projection_ids) {
		if (pid < input.column_indexes.size()) {
			result.push_back(input.column_indexes[pid]);
		}
	}
	return result;
}

static string BuildPushdownQuery(ClientContext &context, const QuackScanBindData &bind_data,
                                 const TableFunctionInitInput &input) {
	string query;

	if (!bind_data.pushed_aggregates.empty() || !bind_data.pushed_group_keys.empty()) {
		// --- Aggregation pushdown (M3a) --------------------------------------
		// The SELECT list is the concatenation of group keys (positions 0..|G|-1) and
		// aggregate expressions (positions |G|..|G|+|A|-1). Projection pushdown above
		// may have pruned to a subset; column_indexes references positions in this
		// combined list.
		vector<string> full_select;
		full_select.reserve(bind_data.pushed_group_keys.size() + bind_data.pushed_aggregates.size());
		for (auto &g : bind_data.pushed_group_keys) {
			full_select.push_back(g);
		}
		for (auto &a : bind_data.pushed_aggregates) {
			full_select.push_back(a);
		}
		auto effective = EffectiveOutputColumns(input);
		string select_list;
		bool any = false;
		if (!effective.empty()) {
			for (auto &col_id : effective) {
				if (col_id.IsVirtualColumn()) {
					continue;
				}
				auto idx = col_id.GetPrimaryIndex();
				if (idx >= full_select.size()) {
					continue;
				}
				if (any) {
					select_list += ", ";
				}
				select_list += full_select[idx];
				any = true;
			}
		}
		if (!any) {
			for (idx_t i = 0; i < full_select.size(); i++) {
				if (i > 0) {
					select_list += ", ";
				}
				select_list += full_select[i];
			}
		}
		query = "SELECT " + select_list + " FROM " + StringUtil::Format("%s", SQLIdentifier(bind_data.table_name));
		if (!bind_data.pushed_where_sql.empty()) {
			query += " WHERE " + bind_data.pushed_where_sql;
		}
		if (!bind_data.pushed_group_keys.empty()) {
			query += " GROUP BY ";
			for (idx_t i = 0; i < bind_data.pushed_group_keys.size(); i++) {
				if (i > 0) {
					query += ", ";
				}
				query += bind_data.pushed_group_keys[i];
			}
		}
		return query;
	} else {
		// --- Projection pushdown ---------------------------------------------
		// Emit a positional ref per output column. With filter_prune on, the effective
		// output set is the projection_ids subset of column_indexes — filter-only
		// columns are referenced in WHERE via their primary index but not shipped back.
		auto effective = EffectiveOutputColumns(input);
		if (!effective.empty()) {
			for (auto &col_id : effective) {
				if (!query.empty()) {
					query += ", ";
				}
				query += PositionalColumnRef(col_id);
			}
			query = "SELECT " + query + " ";
		}
		query += StringUtil::Format("FROM %s", SQLIdentifier(bind_data.table_name));
	}

	// --- Filter pushdown -----------------------------------------------------
	// At scan-init / runtime, TableFunctionInitInput.filters keys have already been
	// remapped by DuckDB's physical-plan generation (CreateTableFilterSet in
	// plan_get.cpp:17-35) from absolute table column indices to *positions in
	// column_ids/column_indexes*. So look up the underlying primary index here via
	// input.column_indexes[key].GetPrimaryIndex() before emitting a positional ref.
	if (input.filters && IsFilterPushdownEnabled(context)) {
		string where_clause;
		for (auto &entry : input.filters->filters) {
			auto column_indexes_idx = entry.first;
			if (column_indexes_idx >= input.column_indexes.size()) {
				continue;
			}
			auto &col_id = input.column_indexes[column_indexes_idx];
			if (col_id.IsVirtualColumn()) {
				continue;
			}
			auto &filter = *entry.second;
			if (!CanPushdownFilter(filter)) {
				continue;
			}
			auto col_ref = PositionalColumnRef(col_id);
			auto fragment = FilterToSql(filter, col_ref);
			if (fragment.empty()) {
				continue;
			}
			if (!where_clause.empty()) {
				where_clause += " AND ";
			}
			where_clause += "(" + fragment + ")";
		}
		if (!where_clause.empty()) {
			query += " WHERE " + where_clause;
		}
	}

	// --- ORDER BY pushdown (only when a LogicalTopN was rewritten) -----------
	if (!bind_data.pushed_order_by.empty()) {
		query += " ORDER BY ";
		for (idx_t i = 0; i < bind_data.pushed_order_by.size(); i++) {
			auto &ob = bind_data.pushed_order_by[i];
			if (i > 0) {
				query += ", ";
			}
			query += "#" + to_string(ob.column_id + 1);
			query += ob.order_type == OrderType::DESCENDING ? " DESC" : " ASC";
			switch (ob.null_order) {
			case OrderByNullType::NULLS_FIRST:
				query += " NULLS FIRST";
				break;
			case OrderByNullType::NULLS_LAST:
				query += " NULLS LAST";
				break;
			default:
				// ORDER_DEFAULT: leave to remote server's default; matches how DuckDB resolved
				// the order on the client side.
				break;
			}
		}
	}

	// --- LIMIT / OFFSET pushdown --------------------------------------------
	if (bind_data.pushed_limit.IsValid()) {
		query += " LIMIT " + to_string(bind_data.pushed_limit.GetIndex());
		if (bind_data.pushed_offset.IsValid() && bind_data.pushed_offset.GetIndex() > 0) {
			query += " OFFSET " + to_string(bind_data.pushed_offset.GetIndex());
		}
	}

	return query;
}

unique_ptr<GlobalTableFunctionState> QuackScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();

	// For the catalog path (ATTACH), LookupEntry only prepares without executing
	// to avoid the server-side result being overwritten by subsequent lookups.
	// We execute the query here, right before scanning, so the result is fresh.
	vector<ChunkResult> results;
	bool needs_more_fetch = bind_data.needs_more_fetch;
	hugeint_t result_uuid;
	string pushed_sql;
	if (!bind_data.table_name.empty()) {
		// apply pushdown to the query
		auto query = BuildPushdownQuery(context, bind_data, input);
		pushed_sql = query;
		auto &client_connection = *bind_data.client_connection;
		auto client_wrapper = client_connection.GetClient(context);
		auto &client = client_wrapper->GetClient();
		auto response_message = client.Request<PrepareResponseMessage>(
		    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query));
		needs_more_fetch = response_message->NeedsMoreFetch();
		// fetch the result
		for (auto &chunk_ref : response_message->MutableResults()) {
			auto &chunk = chunk_ref->Chunk();
			results.emplace_back(chunk, ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED);
		}
		result_uuid = response_message->ResultUUID();
	} else {
		for (auto &chunk_ref : bind_data.results) {
			auto &chunk = chunk_ref->Chunk();
			results.emplace_back(chunk, ChunkResultPushdownType::REQUIRES_PUSHDOWN);
		}
		result_uuid = bind_data.result_uuid;
	}
	// we only multithread if there is more to fetch
	return make_uniq<QuackScanGlobalState>(input.column_indexes, input.projection_ids, std::move(results),
	                                       needs_more_fetch, result_uuid, std::move(pushed_sql));
}

unique_ptr<LocalTableFunctionState> QuackScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	auto &global_state = global_state_p->Cast<QuackScanGlobalState>();
	auto local_state = make_uniq<QuackScanLocalState>();

	// re-use initial client from bind if possible
	local_state->client_wrapper = bind_data.client_connection->GetClient(context.client);
	auto results = global_state.TryGetResults();
	for (auto &chunk : results) {
		local_state->results.push(std::move(chunk));
	}
	return std::move(local_state);
}

static void QuackScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
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
					// Raw quack_query(uri, sql) path: response_chunk has the FULL user-SQL
					// column set; the scan picks the subset matching the scan's output
					// shape. With filter_prune=true, only `projection_ids` are output
					// (column_ids still includes filter-only columns).
					auto &output_selection = global_state.projection_ids.empty() ? global_state.column_ids
					                                                              : global_state.effective_column_ids;
					for (idx_t i = 0; i < output_selection.size(); i++) {
						auto &index = output_selection[i];
						if (index.IsVirtualColumn()) {
							// TODO
							output.data[i].Reference(Value(output.data[i].GetType()));
							return;
						}
						auto col_idx = index.GetPrimaryIndex();
						output.data[i].Reference(response_chunk.data[col_idx]);
					}
					output.SetCardinality(response_chunk.size());
				}
				return;
			}
		}

		// if that did not work, we request more results
		if (local_state.results.empty() && global_state.needs_more_fetch) {
			auto &client = local_state.client_wrapper->GetClient();
			auto fetch_response = client.Request<FetchResponseMessage>(
			    context,
			    make_uniq<FetchRequestMessage>(bind_data.client_connection->ConnectionId(), global_state.result_uuid));

			if (fetch_response->MutableResults().empty()) {
				// server is done, we are done
				global_state.needs_more_fetch = false;
				return;
			}
			// set up buffer for scan in next iteration
			for (auto &chunk : fetch_response->MutableResults()) {
				local_state.results.emplace(chunk->Chunk(), ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED);
			}
			local_state.current_batch_index = fetch_response->BatchIndex();
			continue;
		}
		// we did not have anything cached and then request to the server did not yield anything - we are done
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

// EXPLAIN ANALYZE — fires after InitGlobal has set the rewritten SQL, so we can show
// users the actual string that crossed the wire to the remote server. Empty for the
// raw quack_query(uri, sql) path (which sends the SQL verbatim — no rewrite happens).
InsertionOrderPreservingMap<string> QuackScanDynamicToString(TableFunctionDynamicToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (input.global_state) {
		auto &gstate = input.global_state->Cast<QuackScanGlobalState>();
		if (!gstate.pushed_sql.empty()) {
			result["Pushed SQL"] = gstate.pushed_sql;
		}
	}
	return result;
}

void QuackScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                        const TableFunction &function) {
	throw NotImplementedException("Quack scans cannot be serialized (yet?)");
}

unique_ptr<FunctionData> QuackScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("Quack scans cannot be deserialized (yet?)");
}

// Surface the piggybacked remote `duckdb_tables.estimated_size` to the DuckDB optimizer
// (M4). Returns nullptr when the remote estimate was NULL — yielding the same behavior as
// before: no statistics, optimizer falls back to its defaults.
static unique_ptr<NodeStatistics> QuackScanCardinality(ClientContext &, const FunctionData *bind_data_p) {
	if (!bind_data_p) {
		return nullptr;
	}
	auto &bind_data = bind_data_p->Cast<QuackScanBindData>();
	if (!bind_data.estimated_cardinality.IsValid()) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(bind_data.estimated_cardinality.GetIndex());
}

TableFunction QuackScanFunction::GetFunction() {
	auto fun = TableFunction("quack_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, QuackScan, QuackScanBind,
	                         QuackScanInitGlobal, QuackScanInitLocal);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.named_parameters["token"] = LogicalType::VARCHAR;

	fun.projection_pushdown = true;
	fun.filter_pushdown = true;
	fun.filter_prune = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.dynamic_to_string = QuackScanDynamicToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.cardinality = QuackScanCardinality;
	return fun;
}

TableFunction QuackScanByNameFunction::GetFunction() {
	auto fun = TableFunction("quack_query_by_name", {LogicalType::VARCHAR, LogicalType::VARCHAR}, QuackScan,
	                         QuackScanBindCatalogName, QuackScanInitGlobal, QuackScanInitLocal);
	fun.projection_pushdown = true;
	fun.filter_pushdown = true;
	fun.filter_prune = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.dynamic_to_string = QuackScanDynamicToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.cardinality = QuackScanCardinality;
	return fun;
}

bool QuackCatalog::IsQuackScan(const string &name) {
	return name == "quack_query" || name == "quack_query_by_name";
}

} // namespace duckdb
