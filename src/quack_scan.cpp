#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/progress_bar/progress_bar_display.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "quack_scan.hpp"
#include "quack_client.hpp"
#include "include/storage/quack_catalog.hpp"
#include "storage/quack_transaction.hpp"

#include <queue>
#include <thread>
#include <chrono>
namespace duckdb {

//! Polls server-side query progress on a *separate* HTTP connection (reusing the scan's
//! connection_id) so progress can reach the client while a query runs server-side.
//!
//! Two usage modes:
//!  - feed-only: the latest percentage is exposed via LatestProgress() and surfaced to DuckDB's
//!    own progress bar through table_scan_progress. Used during the streaming FETCH phase, where
//!    the client's main thread is free to pump the bar.
//!  - direct-render: the poller additionally drives a ProgressBarDisplay itself. Used while the
//!    main thread is blocked inside a bind-phase PREPARE and therefore cannot pump the bar. The
//!    display is created from the client's own ClientConfig, so it only happens when the embedding
//!    (e.g. the shell) actually wants a progress bar printed.
class QuackProgressPoller {
public:
	QuackProgressPoller(shared_ptr<QuackClientConnection> connection_p, shared_ptr<DatabaseInstance> db_p)
	    : connection(std::move(connection_p)), db(std::move(db_p)) {
	}
	~QuackProgressPoller() {
		Stop();
	}

	//! Start polling, feeding LatestProgress() only. Waits `wait_time_ms` before connecting.
	void Start(idx_t wait_time_ms) {
		StartInternal(nullptr, wait_time_ms);
	}
	//! Start polling and render directly to `display` (no-op rendering if `display` is null), waiting
	//! `wait_time_ms` before connecting to mirror DuckDB's own progress-bar startup delay.
	void StartWithDisplay(unique_ptr<ProgressBarDisplay> display_p, idx_t wait_time_ms) {
		StartInternal(std::move(display_p), wait_time_ms);
	}
	void Stop() {
		stop = true;
		if (poll_thread.joinable()) {
			poll_thread.join();
		}
	}
	//! Latest server-reported progress as a percentage in [0, 100], or negative if unknown.
	double LatestProgress() const {
		return latest_progress.load();
	}

private:
	void StartInternal(unique_ptr<ProgressBarDisplay> display_p, idx_t wait_time_ms) {
		display = std::move(display_p);
		wait_time_ms_v = wait_time_ms;
		poll_thread = std::thread(&QuackProgressPoller::PollLoop, this);
	}

	//! Sleep up to `ms` in small slices so a stop request is honored promptly.
	//! Returns false if a stop was requested while sleeping.
	bool SleepInterruptible(idx_t ms) {
		for (idx_t slept = 0; slept < ms; slept += 20) {
			if (stop) {
				return false;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
		return !stop;
	}

	void PollLoop() {
		// Defer connecting until the progress-bar wait time has elapsed — the same delay DuckDB waits
		// before showing its own progress bar. Queries that finish within this window never open the
		// extra polling connection: the owning scan is torn down, Stop() fires, and we exit here.
		if (!SleepInterruptible(wait_time_ms_v)) {
			return;
		}
		auto uri = connection->ServerURI();
		auto connection_id = connection->ConnectionId();
		unique_ptr<QuackClient> client;
		bool rendered = false;
		while (!stop) {
			try {
				if (!client) {
					client = QuackClient::GetClient(*db, uri);
				}
				auto response =
				    client->Request<ProgressResponseMessage>(nullptr, make_uniq<ProgressRequestMessage>(connection_id));
				auto progress = response->Progress();
				latest_progress = progress;

				if (display && progress >= 0) {
					display->Update(progress);
					rendered = true;
				}
			} catch (...) {
				// Progress is best-effort: drop the client and retry on the next tick.
				client.reset();
			}
			// Sleep between polls, honoring stop promptly.
			SleepInterruptible(200);
		}
		if (display && rendered) {
			display->Finish();
		}
	}

	shared_ptr<QuackClientConnection> connection;
	shared_ptr<DatabaseInstance> db;
	atomic<double> latest_progress {-1};
	atomic<bool> stop {false};
	unique_ptr<ProgressBarDisplay> display;
	idx_t wait_time_ms_v = 0;
	std::thread poll_thread;
};

//! Build the same progress-bar display DuckDB would use for this client, but only when the client
//! actually wants a progress bar printed (e.g. an interactive shell). Returns null otherwise.
static unique_ptr<ProgressBarDisplay> MaybeCreateProgressDisplay(ClientContext &context) {
	auto &config = context.config;
	if (!config.enable_progress_bar || !config.print_progress_bar) {
		return nullptr;
	}
	auto create_func = config.display_create_func ? config.display_create_func : ProgressBar::DefaultProgressBarDisplay;
	return create_func();
}

//! RAII helper that renders server-side progress directly while a blocking bind-phase PREPARE runs
//! on the (otherwise stalled) main thread. A no-op when no progress bar is wanted.
struct ScopedBindProgress {
	ScopedBindProgress(ClientContext &context, const shared_ptr<QuackClientConnection> &connection) {
		auto display = MaybeCreateProgressDisplay(context);
		if (!display) {
			return;
		}
		poller = make_uniq<QuackProgressPoller>(connection, context.db);
		poller->StartWithDisplay(std::move(display), NumericCast<idx_t>(context.config.wait_time));
	}
	unique_ptr<QuackProgressPoller> poller;
};

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

	bind_data->query_uuid = UUID::GenerateRandomUUID();
	unique_ptr<PrepareResponseMessage> bind_response;
	{
		// The query runs server-side while this request blocks the main thread; render progress directly.
		ScopedBindProgress progress(context, bind_data->client_connection);
		bind_response = client.Request<PrepareResponseMessage>(
		    context,
		    make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query, bind_data->query_uuid));
	}

	return_types = bind_response->Types();
	names = bind_response->Names();

	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data->query_uuid = bind_response->QueryUUID();

	return bind_data;
}

static unique_ptr<FunctionData> QuackScanBindCatalogName(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
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
	unique_ptr<PrepareResponseMessage> bind_response;
	{
		ScopedBindProgress progress(context, bind_data->client_connection);
		bind_response = client.Request<PrepareResponseMessage>(
		    context, make_uniq<PrepareRequestMessage>(bind_data->client_connection->ConnectionId(), query,
		                                              bind_data->query_uuid));
	}

	return_types = bind_response->Types();
	names = bind_response->Names();

	// new stuff
	bind_data->results = std::move(bind_response->MutableResults());
	bind_data->needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data->query_uuid = bind_response->QueryUUID();
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
	                              vector<ChunkResult> results_p, bool needs_more_fetch_p, hugeint_t query_uuid_p,
	                              shared_ptr<QuackClientConnection> client_connection_p,
	                              shared_ptr<DatabaseInstance> db_p)
	    : max_threads(needs_more_fetch_p ? MAX_THREADS : 1), column_ids(std::move(column_ids_p)),
	      projection_ids(std::move(projection_id_p)), needs_more_fetch(needs_more_fetch_p), query_uuid(query_uuid_p),
	      poller(std::move(client_connection_p), std::move(db_p)), results(std::move(results_p)) {
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	vector<ColumnIndex> column_ids;
	vector<idx_t> projection_ids;
	atomic<bool> needs_more_fetch;
	hugeint_t query_uuid;

	vector<ChunkResult> TryGetResults() {
		lock_guard<mutex> guard(lock);
		return std::move(results);
	}

	//! Start polling server progress for the streaming phase (surfaced via table_scan_progress).
	//! No-op when the query already finished in a single batch, so there is nothing left to stream.
	//! Waits `wait_time_ms` before connecting, mirroring DuckDB's own progress-bar startup delay.
	void StartProgressPolling(idx_t wait_time_ms) {
		if (!needs_more_fetch) {
			return;
		}
		poller.Start(wait_time_ms);
	}
	double LatestProgress() const {
		return poller.LatestProgress();
	}

private:
	mutex lock;
	QuackProgressPoller poller;
	vector<ChunkResult> results;
};

static double QuackScanProgress(ClientContext &, const FunctionData *, const GlobalTableFunctionState *global_state_p) {
	auto &global_state = global_state_p->Cast<QuackScanGlobalState>();
	auto progress = global_state.LatestProgress();
	// A negative value means the server hasn't reported progress yet; surface 0 so DuckDB shows a
	// fresh bar rather than a nonsensical percentage.
	return progress < 0 ? 0.0 : progress;
}

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
		unique_ptr<PrepareResponseMessage> response_message;
		{
			ScopedBindProgress progress(context, bind_data.client_connection);
			response_message = client.Request<PrepareResponseMessage>(
			    context, make_uniq<PrepareRequestMessage>(client_connection.ConnectionId(), query, query_uuid));
		}
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
	auto global_state =
	    make_uniq<QuackScanGlobalState>(input.column_indexes, input.projection_ids, std::move(results),
	                                    needs_more_fetch, query_uuid, bind_data.client_connection, context.db);
	// Surface server-side query progress to the local progress bar while we stream results.
	global_state->StartProgressPolling(NumericCast<idx_t>(context.config.wait_time));
	return std::move(global_state);
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
	return local_state;
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
			    make_uniq<FetchRequestMessage>(bind_data.client_connection->ConnectionId(), global_state.query_uuid));

			if (fetch_response->MutableResults().empty()) {
				// server is done, we are done
				global_state.needs_more_fetch = false;
				bind_data.completed = true;
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
		bind_data.completed = true;
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

	fun.projection_pushdown = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.get_bind_info = QuackScanGetBindInfo;
	fun.table_scan_progress = QuackScanProgress;
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
	fun.table_scan_progress = QuackScanProgress;
	fun.named_parameters["use_transaction"] = LogicalType::BOOLEAN;
	// fun.filter_pushdown = true;
	// fun.filter_prune = true;
	return fun;
}

bool QuackCatalog::IsQuackScan(const string &name) {
	return name == "quack_query" || name == "quack_query_by_name";
}

} // namespace duckdb
