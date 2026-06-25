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
	    : table(table_p), insert_count(0), flush_rows(flush_rows_p) {
	}
	~QuackInsertGlobalState() override {
		// Defensive: if the statement errored before Finalize, the queue may still own in-flight sends.
		// Close drains/cancels them so the queue's drained-on-destroy invariant holds. Best-effort.
		if (queue) {
			try {
				queue->Close();
			} catch (...) { // NOLINT: a destructor must not throw
			}
		}
	}

	QuackTableCatalogEntry &table;
	//! Total rows inserted, summed from per-thread counts at Combine time
	atomic<idx_t> insert_count;
	//! Rows a thread buffers before shipping one APPEND_REQUEST (from quack_append_flush_rows)
	idx_t flush_rows;
	//! Shared async upload queue: regular threads register serialized batches, ASYNC-pool threads POST them.
	unique_ptr<ManagedAsyncTaskQueue> queue;
	//! Dense order coordinator (mirrors core's PhysicalBatchCopyToFile). The sparse, out-of-order executor
	//! batch indices are re-mapped here to a contiguous 0-based sequence: finished batches are buffered keyed
	//! by their executor batch index, and released — in ascending order, as `min_batch_index` finalizes them —
	//! each getting the next `next_dense`. The server then reorders by this dense sequence (trivially: from 0).
	mutex remap_lock;
	map<idx_t, vector<unique_ptr<DataChunk>>> pending; // executor batch_index -> its chunks
	idx_t next_dense = 0;                              // next dense sequence number to hand out
	//! Self-mint sequence for sources without an executor batch index (single producer mints dense directly).
	idx_t mint_counter = 0;
};

class QuackInsertLocalState : public LocalSinkState {
public:
	//! Self-owned chunks buffered client-side for the executor batch this thread is currently producing.
	vector<unique_ptr<DataChunk>> buffer;
	idx_t buffered_rows = 0;
	idx_t local_count = 0;
	//! Order-preserving path only: the executor batch index the buffer currently holds.
	optional_idx current_batch;
};

// Default rows a thread buffers before shipping one APPEND_REQUEST; overridable via the
// quack_append_flush_rows setting. Mimics the appender threshold.
static constexpr idx_t QUACK_APPEND_FLUSH_ROWS = STANDARD_VECTOR_SIZE * 100ULL;

static idx_t GetFlushRows(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("quack_append_flush_rows", val) && !val.IsNull()) {
		auto rows = val.GetValue<uint64_t>();
		if (rows > 0) {
			return rows;
		}
	}
	return QUACK_APPEND_FLUSH_ROWS;
}

unique_ptr<GlobalSinkState> QuackInsert::GetGlobalSinkState(ClientContext &context) const {
	auto flush_rows = GetFlushRows(context);
	unique_ptr<QuackInsertGlobalState> global_state;
	if (table) {
		global_state =
		    make_uniq<QuackInsertGlobalState>(table.get_mutable()->Cast<QuackTableCatalogEntry>(), flush_rows);
	} else {
		// CREATE TABLE AS path: create the table on the remote side first
		auto &quack_schema = schema.get_mutable()->Cast<QuackSchemaCatalogEntry>();
		auto &quack_catalog = quack_schema.catalog.Cast<QuackCatalog>();

		auto entry = quack_schema.CreateTable(CatalogTransaction(quack_catalog, context), *info);
		global_state = make_uniq<QuackInsertGlobalState>(entry->Cast<QuackTableCatalogEntry>(), flush_rows);
	}
	// One shared async upload queue per statement; concurrency K defaults to async_threads, decoupled from
	// `threads`. Order preservation is handled by stamping + server-side reorder, so this is always async.
	global_state->queue = make_uniq<ManagedAsyncTaskQueue>(context);
	return std::move(global_state);
}

unique_ptr<LocalSinkState> QuackInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<QuackInsertLocalState>();
}

//===--------------------------------------------------------------------===//
// Async send task
//===--------------------------------------------------------------------===//
// Performs the blocking APPEND POST on an ASYNC-pool thread. The payload was serialized on the producing
// (regular) execution thread; this task only does the low-CPU network send and checks the server's ack. It
// owns a pooled connection for the duration of the request (one socket cannot do concurrent POSTs).
static int64_t NowMillis() {
	return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	    .time_since_epoch()
	    .count();
}

class QuackAppendSendTask : public AsyncTask {
public:
	QuackAppendSendTask(unique_ptr<QuackClientWrapper> client_wrapper_p, unique_ptr<MemoryStream> payload_p,
	                    idx_t payload_size_p, string connection_id_p, optional_idx client_query_id_p)
	    : client_wrapper(std::move(client_wrapper_p)), payload(std::move(payload_p)), payload_size(payload_size_p),
	      connection_id(std::move(connection_id_p)), client_query_id(client_query_id_p) {
	}

	void Execute() override {
		auto &client = client_wrapper->GetClient();
		// context=nullptr: this runs off the execution thread, so it must not touch the ClientContext.
		auto start_time = NowMillis();
		// TODO: PostRaw + the manual LogRequest below are a temporary split of Client::Request (which couples
		// serialization with the network round-trip). For proper async IO we want a request abstraction that
		// separates serialization (done on the producing thread) from the send/recv (done here), so this task
		// can reuse the logged request path without re-serializing. Tracked for a follow-up PR.
		auto response_body = client.PostRaw(nullptr, payload->GetData(), payload_size);
		auto duration_ms = NowMillis() - start_time;
		MemoryStream read_stream((data_ptr_t)response_body.data(), response_body.size());
		auto response = QuackMessage::FromMemoryStream(read_stream);

		// Surface the append on the Quack request log path (the synchronous path logs too); PostRaw alone
		// only exercises the raw HTTP transport.
		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		client.LogRequest(MessageType::APPEND_REQUEST, connection_id, client_query_id, string(), duration_ms,
		                  response->Type(), error);

		if (response->Type() == MessageType::ERROR_RESPONSE) {
			response->Cast<ErrorResponse>().Error().Throw();
		}
		if (response->Type() != SuccessResponse::TYPE) {
			throw IOException("Expected success response for append, got %s instead",
			                  MessageTypeToString(response->Type()));
		}
	}

private:
	unique_ptr<QuackClientWrapper> client_wrapper;
	unique_ptr<MemoryStream> payload;
	idx_t payload_size;
	string connection_id;
	optional_idx client_query_id;
};

//===--------------------------------------------------------------------===//
// Append buffering
//===--------------------------------------------------------------------===//
// Serialize the buffered chunks into one APPEND_REQUEST on this (regular) execution thread, then hand the
// bytes to the async task queue: an ASYNC-pool thread performs the blocking POST over a pooled connection
// while this thread returns to producing the next batch. Concurrency is bounded by async_threads, and the
// queue's TemporaryMemoryManager reservation bounds how much serialized-but-unsent data we retain.
// Serialize `chunks` into one APPEND_REQUEST on this (regular) execution thread and hand the bytes to the
// async queue (an ASYNC-pool thread does the blocking POST). `dense_batch` valid → the order-preserving path:
// the append carries a dense, contiguous source-order index that the server reorders by. Invalid → the fast
// path (server applies on arrival). The caller owns `chunks`.
static void SendChunks(ClientContext &context, QuackInsertGlobalState &global_state,
                       const vector<unique_ptr<DataChunk>> &chunks, optional_idx dense_batch) {
	if (chunks.empty()) {
		return;
	}
	auto &tbl = global_state.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();

	vector<unique_ptr<DataChunkWrapper>> wrappers;
	wrappers.reserve(chunks.size());
	for (auto &chunk : chunks) {
		wrappers.push_back(make_uniq<DataChunkWrapper>(*chunk));
	}
	auto append_message =
	    make_uniq<AppendRequestMessage>(quack_catalog.GetConnectionId(), tbl.schema.name.GetIdentifierName(),
	                                    tbl.name.GetIdentifierName(), std::move(wrappers));
	if (dense_batch.IsValid()) {
		// One complete batch per append (chunk 0, last_chunk) at this dense source-order index.
		append_message->SetAppendOrder(dense_batch, optional_idx(0), /*last_chunk=*/true, optional_idx());
	}

	// Correlate with the server-side query for logging. Read the active query on this regular thread; the
	// async task must not touch the ClientContext.
	optional_idx client_query_id;
	if (context.transaction.HasActiveTransaction()) {
		auto raw_query_id = context.transaction.GetActiveQuery();
		if (raw_query_id != DConstants::INVALID_INDEX) {
			client_query_id = raw_query_id;
			append_message->SetClientQueryId(raw_query_id);
		}
	}

	// Serialize on this regular thread (the CPU part); the ASYNC pool only does the network send.
	auto payload = make_uniq<MemoryStream>();
	append_message->ToMemoryStream(*payload);
	auto payload_size = payload->GetPosition();

	auto connection_id = quack_catalog.GetConnectionId();
	auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
	global_state.queue->Register(make_uniq<QuackAppendSendTask>(std::move(client_wrapper), std::move(payload),
	                                                            payload_size, std::move(connection_id),
	                                                            client_query_id),
	                             payload_size);
}

// Order-preserving release: hand this thread's finished executor batch to the global coordinator, then
// release every batch the watermark has finalized (executor index < min_index) in ascending order, each
// stamped with the next dense sequence number. Serialization/send happens outside the lock (stays parallel).
static void ReleaseDenseBatches(ClientContext &context, QuackInsertGlobalState &gstate, QuackInsertLocalState &lstate,
                                idx_t min_index) {
	vector<std::pair<idx_t, vector<unique_ptr<DataChunk>>>> ready;
	{
		lock_guard<mutex> guard(gstate.remap_lock);
		if (lstate.current_batch.IsValid() && !lstate.buffer.empty()) {
			gstate.pending[lstate.current_batch.GetIndex()] = std::move(lstate.buffer);
		}
		lstate.buffer.clear();
		lstate.buffered_rows = 0;
		while (!gstate.pending.empty() && gstate.pending.begin()->first < min_index) {
			auto it = gstate.pending.begin();
			ready.emplace_back(gstate.next_dense++, std::move(it->second));
			gstate.pending.erase(it);
		}
	}
	for (auto &entry : ready) {
		SendChunks(context, gstate, entry.second, optional_idx(entry.first));
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType QuackInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();
	if (order_mode == AppendOrderMode::EXECUTOR) {
		// Every chunk between NextBatch boundaries belongs to one executor batch index; accumulate the whole
		// batch and hand it to the coordinator at the boundary.
		local_state.current_batch = input.local_state.partition_info.batch_index;
	}
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Buffer a self-owned copy of the chunk: the executor reuses the source chunk across Sink calls.
	auto owned = make_uniq<DataChunk>();
	owned->Initialize(context.client, chunk.GetTypes(), chunk.size());
	owned->Append(chunk);
	local_state.buffered_rows += owned->size();
	local_state.local_count += chunk.size();
	local_state.buffer.push_back(std::move(owned));

	// EXECUTOR flushes at the batch boundary (NextBatch); MINTED/NONE flush at the row threshold.
	if (order_mode != AppendOrderMode::EXECUTOR && local_state.buffered_rows >= global_state.flush_rows) {
		optional_idx dense; // MINTED: single producer mints the next dense index; NONE: fast path (unstamped).
		if (order_mode == AppendOrderMode::MINTED) {
			dense = global_state.mint_counter++;
		}
		SendChunks(context.client, global_state, local_state.buffer, dense);
		local_state.buffer.clear();
		local_state.buffered_rows = 0;
		global_state.queue->ApplyBackpressure();
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// NextBatch (order-preserving executor path)
//===--------------------------------------------------------------------===//
// The owning thread has crossed from `current_batch` to a new executor batch index — so `current_batch` is
// final. Hand it to the coordinator and release every now-finalized batch (executor index < min) in dense order.
SinkNextBatchType QuackInsert::NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const {
	if (order_mode != AppendOrderMode::EXECUTOR) {
		return SinkNextBatchType::READY;
	}
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();
	auto min_index = input.local_state.partition_info.min_batch_index.GetIndex();
	ReleaseDenseBatches(context.client, global_state, local_state, min_index);
	global_state.queue->ApplyBackpressure();
	local_state.current_batch = input.local_state.partition_info.batch_index;
	return SinkNextBatchType::READY;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType QuackInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();
	if (order_mode == AppendOrderMode::EXECUTOR) {
		// Hand this thread's final batch to the coordinator; Finalize releases everything still pending in order.
		lock_guard<mutex> guard(global_state.remap_lock);
		if (local_state.current_batch.IsValid() && !local_state.buffer.empty()) {
			global_state.pending[local_state.current_batch.GetIndex()] = std::move(local_state.buffer);
		}
		local_state.buffer.clear();
	} else {
		optional_idx dense; // MINTED: final minted batch; NONE: fast-path remainder.
		if (order_mode == AppendOrderMode::MINTED && !local_state.buffer.empty()) {
			dense = global_state.mint_counter++;
		}
		SendChunks(context.client, global_state, local_state.buffer, dense);
		local_state.buffer.clear();
	}
	global_state.insert_count += local_state.local_count;
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType QuackInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	if (order_mode == AppendOrderMode::EXECUTOR) {
		// Everything is now final: release the remaining buffered batches in ascending executor order, each at
		// the next dense index (runs single-threaded after all Combines).
		vector<std::pair<idx_t, vector<unique_ptr<DataChunk>>>> ready;
		{
			lock_guard<mutex> guard(global_state.remap_lock);
			for (auto &entry : global_state.pending) {
				ready.emplace_back(global_state.next_dense++, std::move(entry.second));
			}
			global_state.pending.clear();
		}
		for (auto &entry : ready) {
			SendChunks(context, global_state, entry.second, optional_idx(entry.first));
		}
	}
	// Drain all async sends so that by the time Finalize returns, every row is committed on the server (matching
	// the visibility/rollback semantics of a local insert) and any async send error is surfaced.
	global_state.queue->Close();
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

// Decide the insert ordering strategy at plan time (mirrors core's plan_insert.cpp):
//  - preserve_insertion_order=false → fast concurrent path, no stamping (server applies on arrival).
//  - preserve order + source has an executor batch index → stamp with it (parallel producers).
//  - preserve order + source has no batch index → MINTED: single producer mints its own sequence.
// Both order-preserving variants stay async; the server reorders by the (batch_index, chunk_index) stamp.
static void ConfigureOrdering(ClientContext &context, QuackInsert &insert, PhysicalOperator &source) {
	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, source)) {
		insert.order_mode = AppendOrderMode::NONE;
	} else if (PhysicalPlanGenerator::UseBatchIndex(context, source)) {
		insert.order_mode = AppendOrderMode::EXECUTOR;
	} else {
		insert.order_mode = AppendOrderMode::MINTED;
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
