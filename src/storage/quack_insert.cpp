#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/common/serializer/async_task_queue.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "storage/quack_catalog.hpp"
#include "quack_message.hpp"
#include "storage/quack_insert.hpp"
#include "storage/quack_table.hpp"
#include "quack_client.hpp"

#include <chrono>
#include <set>

using namespace duckdb;

QuackInsert::QuackInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr) {
}

QuackInsert::QuackInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                         unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class QuackInsertGlobalState : public GlobalSinkState {
public:
	QuackInsertGlobalState(QuackTableCatalogEntry &table_p, idx_t flush_rows_p)
	    : table(table_p), insert_count(0), flush_rows(flush_rows_p), query_uuid(UUID::GenerateRandomUUID()) {
	}
	~QuackInsertGlobalState() override {
		if (queue) {
			try {
				queue->Close();
			} catch (...) { // NOLINT: destructor must not throw
			}
		}
	}

	QuackTableCatalogEntry &table;
	atomic<idx_t> insert_count;
	idx_t flush_rows;
	//! Per-statement id for the server-side insert stream.
	hugeint_t query_uuid;
	//! Shared async upload queue: regular threads register serialized messages, ASYNC-pool threads POST them.
	unique_ptr<ManagedAsyncTaskQueue> queue;
	//! Monotonic batch counter for SERIAL_ORDERED mode (single-threaded, so plain idx_t is fine).
	idx_t serial_batch_counter = 0;

	//! Dead-range gap tracking (PARALLEL_ORDERED), guarded by gap_lock: a batch index is "live" once a thread
	//! produces rows for it; never-claimed indices below the floor are dead and get a dead-range marker.
	annotated_mutex gap_lock;
	std::set<idx_t> live_batches;
	//! Max min_batch_index observed. A live batch strictly below it is settled: no lower batch can appear.
	idx_t max_floor = 0;
	//! Largest settled live batch; the lower bound for the next inter-batch dead range.
	optional_idx last_settled_live;
	//! Firm lower bound on the lowest batch, sent as the constant seed watermark. Set in CloseDeadGaps.
	optional_idx stream_floor;
};

class QuackInsertLocalState : public LocalSinkState {
public:
	vector<unique_ptr<DataChunk>> buffer;
	idx_t buffered_rows = 0;
	idx_t local_count = 0;
	//! PARALLEL_ORDERED: executor batch currently being buffered (invalid before first Sink/NextBatch).
	optional_idx current_batch;
	//! PARALLEL_ORDERED: monotonic per-batch sequence counter, reset at each NextBatch transition.
	idx_t sequence_counter = 0;
};

static constexpr idx_t QUACK_SEND_DATA_FLUSH_ROWS = STANDARD_VECTOR_SIZE * 100ULL;

static idx_t GetFlushRows(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("quack_send_data_flush_rows", val) && !val.IsNull()) {
		auto rows = val.GetValue<uint64_t>();
		if (rows > 0) {
			return rows;
		}
	}
	return QUACK_SEND_DATA_FLUSH_ROWS;
}

unique_ptr<GlobalSinkState> QuackInsert::GetGlobalSinkState(ClientContext &context) const {
	auto flush_rows = GetFlushRows(context);
	unique_ptr<QuackInsertGlobalState> global_state;
	if (table) {
		global_state =
		    make_uniq<QuackInsertGlobalState>(table.get_mutable()->Cast<QuackTableCatalogEntry>(), flush_rows);
	} else {
		auto &quack_schema = schema.get_mutable()->Cast<QuackSchemaCatalogEntry>();
		auto &quack_catalog = quack_schema.catalog.Cast<QuackCatalog>();
		auto entry = quack_schema.CreateTable(CatalogTransaction(quack_catalog, context), *info);
		global_state = make_uniq<QuackInsertGlobalState>(entry->Cast<QuackTableCatalogEntry>(), flush_rows);
	}
	global_state->queue = make_uniq<ManagedAsyncTaskQueue>(context);
	return std::move(global_state);
}

unique_ptr<LocalSinkState> QuackInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<QuackInsertLocalState>();
}

//===--------------------------------------------------------------------===//
// Async send task
//===--------------------------------------------------------------------===//
static int64_t NowMillis() {
	return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	    .time_since_epoch()
	    .count();
}

// Performs the blocking SEND_DATA POST on an ASYNC-pool thread. The payload was serialized on
// the producing (regular) execution thread; this task only does the network send and checks the ack.
class QuackSendDataTask : public AsyncTask {
public:
	QuackSendDataTask(unique_ptr<QuackClientWrapper> client_wrapper_p, unique_ptr<MemoryStream> payload_p,
	                  idx_t payload_size_p, string connection_id_p, optional_idx client_query_id_p,
	                  shared_ptr<Logger> logger_p)
	    : client_wrapper(std::move(client_wrapper_p)), payload(std::move(payload_p)), payload_size(payload_size_p),
	      connection_id(std::move(connection_id_p)), client_query_id(client_query_id_p), logger(std::move(logger_p)) {
	}

	void Execute() override {
		auto &client = client_wrapper->GetClient();
		auto start_time = NowMillis();
		// context=nullptr: called off the execution thread, must not touch ClientContext.
		auto response_body = client.PostRaw(nullptr, payload->GetData(), payload_size);
		auto duration_ms = NowMillis() - start_time;

		auto response = QuackClient::DecodeResponse(response_body);

		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		// Log through the context logger captured on the producer thread, so async SEND_DATA logs carry the
		// query's connection/transaction/query id (the pool thread has no ClientContext).
		client.LogRequest(Logger::Get(logger), MessageType::SEND_DATA_REQUEST, connection_id, client_query_id, string(),
		                  duration_ms, response->Type(), error);

		if (response->Type() == MessageType::ERROR_RESPONSE) {
			response->Cast<ErrorResponse>().Error().Throw();
		}
		if (response->Type() != SendDataResponseMessage::TYPE) {
			throw IOException("Expected send_data_response, got %s instead", MessageTypeToString(response->Type()));
		}
	}

private:
	unique_ptr<QuackClientWrapper> client_wrapper;
	unique_ptr<MemoryStream> payload;
	idx_t payload_size;
	string connection_id;
	optional_idx client_query_id;
	shared_ptr<Logger> logger;
};

//===--------------------------------------------------------------------===//
// Send helpers
//===--------------------------------------------------------------------===//
// Serialize `chunks` into one SEND_DATA_REQUEST and register it with the async queue. The payload is
// serialized on this (regular) execution thread; an ASYNC-pool thread does the blocking POST.
// For PARALLEL_ORDERED: lstate.current_batch and lstate.sequence_counter must already be set.
// For SERIAL_ORDERED: gstate.serial_batch_counter is incremented here (single-threaded path).
static void SendChunks(ClientContext &context, const QuackInsert &insert, QuackInsertGlobalState &gstate,
                       QuackInsertLocalState &lstate, vector<unique_ptr<DataChunk>> chunks, bool is_last_in_batch) {
	auto &tbl = gstate.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();

	vector<unique_ptr<DataChunkWrapper>> wrappers;
	wrappers.reserve(chunks.size());
	for (auto &chunk : chunks) {
		wrappers.push_back(make_uniq<DataChunkWrapper>(*chunk));
	}

	auto send_msg =
	    make_uniq<SendDataRequestMessage>(quack_catalog.GetConnectionId(), tbl.schema.name.GetIdentifierName(),
	                                      tbl.name.GetIdentifierName(), std::move(wrappers), gstate.query_uuid);

	switch (insert.order_mode) {
	case AppendOrderMode::PARALLEL_ORDERED: {
		D_ASSERT(lstate.current_batch.IsValid());
		send_msg->SetOrdering(lstate.current_batch, lstate.sequence_counter++, is_last_in_batch);
		annotated_lock_guard<annotated_mutex> lk(gstate.gap_lock);
		send_msg->SetBatchWatermark(gstate.stream_floor);
		break;
	}
	case AppendOrderMode::SERIAL_ORDERED: {
		idx_t batch = gstate.serial_batch_counter++;
		send_msg->SetOrdering(optional_idx(batch), 0, true);
		annotated_lock_guard<annotated_mutex> lk(gstate.gap_lock);
		if (!gstate.stream_floor.IsValid()) {
			gstate.stream_floor = optional_idx(0); // serial batches are dense from 0
		}
		send_msg->SetBatchWatermark(gstate.stream_floor);
		break;
	}
	case AppendOrderMode::UNORDERED:
		break;
	}

	// Encode on the producer thread: the async task runs off-thread and must not touch ClientContext.
	auto payload = make_uniq<MemoryStream>();
	QuackClient::EncodeRequest(context, *send_msg, *payload);
	auto payload_size = payload->GetPosition();

	auto connection_id = send_msg->ConnectionId();
	auto client_query_id = send_msg->ClientQueryId();
	auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
	gstate.queue->Register(make_uniq<QuackSendDataTask>(std::move(client_wrapper), std::move(payload), payload_size,
	                                                    std::move(connection_id), client_query_id, context.logger),
	                       payload_size);
}

// Emit a dead-range marker: batches [lo, hi) produced no rows and will never arrive. Carries no chunks;
// the server records the range so its delivery cursor can skip the gap instead of stalling on it.
static void SendDeadRange(ClientContext &context, QuackInsertGlobalState &gstate, idx_t lo, idx_t hi) {
	auto &tbl = gstate.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();

	auto send_msg = make_uniq<SendDataRequestMessage>(quack_catalog.GetConnectionId(),
	                                                  tbl.schema.name.GetIdentifierName(), tbl.name.GetIdentifierName(),
	                                                  vector<unique_ptr<DataChunkWrapper>>(), gstate.query_uuid);
	send_msg->SetDeadRange(lo, hi);

	// Encode on the producer thread: the async task runs off-thread and must not touch ClientContext.
	auto payload = make_uniq<MemoryStream>();
	QuackClient::EncodeRequest(context, *send_msg, *payload);
	auto payload_size = payload->GetPosition();

	auto connection_id = quack_catalog.GetConnectionId();
	auto client_query_id = send_msg->ClientQueryId();
	auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
	gstate.queue->Register(make_uniq<QuackSendDataTask>(std::move(client_wrapper), std::move(payload), payload_size,
	                                                    std::move(connection_id), client_query_id, context.logger),
	                       payload_size);
}

// PARALLEL_ORDERED only: record `current_batch` as live and, as the floor rises, emit one dead-range marker
// per run of never-claimed indices bounded above by a live batch. `floor` is min_batch_index (idx_t max at finalize).
static void CloseDeadGaps(ClientContext &context, const QuackInsert &insert, QuackInsertGlobalState &gstate,
                          optional_idx current_batch, idx_t floor) {
	if (insert.order_mode != AppendOrderMode::PARALLEL_ORDERED) {
		return;
	}
	vector<std::pair<idx_t, idx_t>> to_emit;
	{
		annotated_lock_guard<annotated_mutex> guard(gstate.gap_lock);
		if (current_batch.IsValid()) {
			// A live batch must never appear at or below already-settled territory.
			D_ASSERT(!gstate.last_settled_live.IsValid() ||
			         current_batch.GetIndex() > gstate.last_settled_live.GetIndex());
			gstate.live_batches.insert(current_batch.GetIndex());
		}
		if (floor > gstate.max_floor) {
			gstate.max_floor = floor;
		}
		// Lowest min_batch_index seen (skip the idx_t max that FINALIZE passes to flush everything): a firm lower
		// bound sent as the server's constant seed watermark. It can be one below the lowest real batch (an
		// unproduced thread's placeholder), which the leading marker covers.
		if (floor != NumericLimits<idx_t>::Maximum() &&
		    (!gstate.stream_floor.IsValid() || floor < gstate.stream_floor.GetIndex())) {
			gstate.stream_floor = optional_idx(floor);
		}
		// Settle every live batch now strictly below the floor, emitting the dead gap to its predecessor.
		while (!gstate.live_batches.empty()) {
			idx_t b = *gstate.live_batches.begin();
			if (b >= gstate.max_floor) {
				break; // not settled — a lower batch could still appear
			}
			gstate.live_batches.erase(gstate.live_batches.begin());
			if (gstate.last_settled_live.IsValid()) {
				if (b > gstate.last_settled_live.GetIndex() + 1) {
					to_emit.emplace_back(gstate.last_settled_live.GetIndex() + 1, b);
				}
			} else if (gstate.stream_floor.IsValid() && b > gstate.stream_floor.GetIndex()) {
				// Leading gap: advance the server's cursor from the seed up to the first real batch.
				to_emit.emplace_back(gstate.stream_floor.GetIndex(), b);
			}
			gstate.last_settled_live = optional_idx(b);
		}
	}
	for (auto &r : to_emit) {
		SendDeadRange(context, gstate, r.first, r.second);
	}
}

// Flush lstate.buffer with the given is_last_in_batch flag. For PARALLEL_ORDERED, sends a zero-chunk
// end-of-batch marker even if buffer is empty (so the server knows the batch is complete).
// For other modes, skips if the buffer is empty.
static void FlushBuffer(ClientContext &context, const QuackInsert &insert, QuackInsertGlobalState &gstate,
                        QuackInsertLocalState &lstate, bool is_last_in_batch) {
	bool parallel_ordered = insert.order_mode == AppendOrderMode::PARALLEL_ORDERED;
	if (lstate.buffer.empty() && !parallel_ordered) {
		return;
	}
	if (parallel_ordered && !lstate.current_batch.IsValid()) {
		lstate.buffer.clear();
		lstate.buffered_rows = 0;
		return;
	}
	auto chunks = std::move(lstate.buffer);
	lstate.buffer.clear();
	lstate.buffered_rows = 0;
	SendChunks(context, insert, gstate, lstate, std::move(chunks), is_last_in_batch);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType QuackInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();

	// Update current_batch before the empty check so NextBatch can always flush the right batch.
	if (order_mode == AppendOrderMode::PARALLEL_ORDERED) {
		local_state.current_batch = input.local_state.partition_info.batch_index;
	}
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Deep-copy the chunk: the executor reuses the source chunk across Sink calls.
	auto owned = make_uniq<DataChunk>();
	owned->Initialize(context.client, chunk.GetTypes(), chunk.size());
	owned->Append(chunk);
	local_state.buffered_rows += owned->size();
	local_state.local_count += chunk.size();
	local_state.buffer.push_back(std::move(owned));

	// Flush when the row threshold is reached. For PARALLEL_ORDERED, mid-batch flushes use is_last=false
	// (the batch boundary is determined by NextBatch, not the row count); others always use is_last=true.
	if (local_state.buffered_rows >= global_state.flush_rows) {
		bool is_last = (order_mode != AppendOrderMode::PARALLEL_ORDERED);
		FlushBuffer(context.client, *this, global_state, local_state, is_last);
		global_state.queue->ApplyBackpressure();
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// NextBatch (PARALLEL_ORDERED path)
//===--------------------------------------------------------------------===//
// The executor has crossed a batch boundary. Flush all data for lstate.current_batch (the batch that
// just ended) with is_last=true, then advance to the new batch. NextBatch fires BEFORE Sink() is called
// for the new batch's first chunk, so lstate.current_batch is the OLD batch when we enter here.
SinkNextBatchType QuackInsert::NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const {
	if (order_mode != AppendOrderMode::PARALLEL_ORDERED) {
		return SinkNextBatchType::READY;
	}
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();
	auto new_batch = input.local_state.partition_info.batch_index;

	// Record the new batch as live, capture the firm floor, and emit dead-range markers for confirmed gaps.
	auto floor = input.local_state.partition_info.min_batch_index;
	CloseDeadGaps(context.client, *this, global_state, new_batch, floor.IsValid() ? floor.GetIndex() : 0);

	// Flush old batch (or send zero-chunk end-of-batch marker if no data came through).
	// current_batch is invalid only before the very first NextBatch call; skip in that case.
	if (local_state.current_batch.IsValid()) {
		FlushBuffer(context.client, *this, global_state, local_state, /*is_last=*/true);
		global_state.queue->ApplyBackpressure();
	}

	// Advance to the new batch.
	local_state.current_batch = new_batch;
	local_state.sequence_counter = 0;
	return SinkNextBatchType::READY;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType QuackInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();

	// Flush remaining buffer as the final (is_last=true) message for the current batch/serial batch.
	FlushBuffer(context.client, *this, global_state, local_state, /*is_last=*/true);
	global_state.insert_count += local_state.local_count;
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType QuackInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &tbl = global_state.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();

	// At finalize the floor is effectively +inf: settle every remaining live batch, emitting the last
	// internal-gap markers before the queue is drained.
	CloseDeadGaps(context, *this, global_state, optional_idx(), NumericLimits<idx_t>::Maximum());

	// Drain all async sends before FINALIZE: ensures every row is committed atomically on the server.
	global_state.queue->Close();

	auto client_connection = quack_catalog.GetClientConnection();
	auto client_wrapper = client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	auto finalize_msg = make_uniq<FinalizeMessage>(quack_catalog.GetConnectionId(), global_state.query_uuid);
	optional_idx stream_floor;
	{
		annotated_lock_guard<annotated_mutex> lk(global_state.gap_lock);
		stream_floor = global_state.stream_floor;
	}
	finalize_msg->SetMinBatchWatermark(stream_floor);
	client.Request<SuccessResponse>(context, std::move(finalize_msg));
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType QuackInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<QuackInsertGlobalState>();
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count.load())));
	chunk.SetCardinality(1);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string QuackInsert::GetName() const {
	return table ? "RPC_INSERT" : "RPC_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> QuackInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name.GetIdentifierName() : info->Base().table.GetIdentifierName();
	return result;
}

// Decide ordering strategy at plan time (mirrors core's plan_insert.cpp):
//  - preserve_insertion_order=false → UNORDERED: no stamps, server applies on arrival.
//  - preserve order + source has executor batch index → PARALLEL_ORDERED: stamp with (batch, seq, is_last).
//  - preserve order + no batch index → SERIAL_ORDERED: single producer mints a new batch per flush.
static void ConfigureOrdering(ClientContext &context, QuackInsert &insert, PhysicalOperator &source) {
	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, source)) {
		insert.order_mode = AppendOrderMode::UNORDERED;
	} else if (PhysicalPlanGenerator::UseBatchIndex(context, source)) {
		insert.order_mode = AppendOrderMode::PARALLEL_ORDERED;
	} else {
		insert.order_mode = AppendOrderMode::SERIAL_ORDERED;
	}
}

PhysicalOperator &QuackCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw NotImplementedException("RETURNING not yet supported for QUACK_INSERT");
	}
	D_ASSERT(plan);
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &insert = planner.Make<QuackInsert>(op, op.table);
	insert.children.push_back(*plan);
	ConfigureOrdering(context, insert.Cast<QuackInsert>(), *plan);
	return insert;
}

PhysicalOperator &QuackCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &insert = planner.Make<QuackInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	ConfigureOrdering(context, insert.Cast<QuackInsert>(), plan);
	return insert;
}
