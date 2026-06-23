#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
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
};

class QuackInsertLocalState : public LocalSinkState {
public:
	//! Self-owned chunks buffered client-side, not yet shipped to the server
	vector<unique_ptr<DataChunk>> buffer;
	idx_t buffered_rows = 0;
	idx_t local_count = 0;
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
	// One shared upload queue per statement; concurrency K defaults to async_threads, decoupled from `threads`.
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
class QuackAppendSendTask : public AsyncTask {
public:
	QuackAppendSendTask(unique_ptr<QuackClientWrapper> client_wrapper_p, unique_ptr<MemoryStream> payload_p,
	                    idx_t payload_size_p)
	    : client_wrapper(std::move(client_wrapper_p)), payload(std::move(payload_p)), payload_size(payload_size_p) {
	}

	void Execute() override {
		auto &client = client_wrapper->GetClient();
		// context=nullptr: this runs off the execution thread, so it must not touch the ClientContext.
		auto response_body = client.PostRaw(nullptr, payload->GetData(), payload_size);
		MemoryStream read_stream((data_ptr_t)response_body.data(), response_body.size());
		auto response = QuackMessage::FromMemoryStream(read_stream);
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
};

//===--------------------------------------------------------------------===//
// Append buffering
//===--------------------------------------------------------------------===//
// Serialize the buffered chunks into one APPEND_REQUEST on this (regular) execution thread, then hand the
// bytes to the async task queue: an ASYNC-pool thread performs the blocking POST over a pooled connection
// while this thread returns to producing the next batch. Concurrency is bounded by async_threads, and the
// queue's TemporaryMemoryManager reservation bounds how much serialized-but-unsent data we retain.
static void FlushAppendBuffer(ClientContext &context, QuackInsertGlobalState &global_state,
                              QuackInsertLocalState &local_state) {
	if (local_state.buffer.empty()) {
		return;
	}
	auto &tbl = global_state.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();

	vector<unique_ptr<DataChunkWrapper>> wrappers;
	wrappers.reserve(local_state.buffer.size());
	for (auto &chunk : local_state.buffer) {
		wrappers.push_back(make_uniq<DataChunkWrapper>(*chunk));
	}
	auto append_message =
	    make_uniq<AppendRequestMessage>(quack_catalog.GetConnectionId(), tbl.schema.name.GetIdentifierName(),
	                                    tbl.name.GetIdentifierName(), std::move(wrappers));

	// Correlate with the server-side query for logging. Read the active query on this regular thread; the
	// async task must not touch the ClientContext.
	if (context.transaction.HasActiveTransaction()) {
		auto raw_query_id = context.transaction.GetActiveQuery();
		if (raw_query_id != DConstants::INVALID_INDEX) {
			append_message->SetClientQueryId(raw_query_id);
		}
	}

	// Serialize on this regular thread (the CPU part); the ASYNC pool only does the network send.
	auto payload = make_uniq<MemoryStream>();
	append_message->ToMemoryStream(*payload);
	auto payload_size = payload->GetPosition();

	auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
	global_state.queue->Register(
	    make_uniq<QuackAppendSendTask>(std::move(client_wrapper), std::move(payload), payload_size), payload_size);

	local_state.buffer.clear();
	local_state.buffered_rows = 0;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType QuackInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Buffer a self-owned copy of the chunk: the executor reuses the source chunk across Sink calls,
	// so we cannot Reference it and defer the send. Size the copy to the actual row count (not a full
	// STANDARD_VECTOR_SIZE) so a stream of partial chunks doesn't inflate memory.
	auto owned = make_uniq<DataChunk>();
	owned->Initialize(context.client, chunk.GetTypes(), chunk.size());
	owned->Append(chunk);
	local_state.buffered_rows += owned->size();
	local_state.local_count += chunk.size();
	local_state.buffer.push_back(std::move(owned));

	if (local_state.buffered_rows >= global_state.flush_rows) {
		FlushAppendBuffer(context.client, global_state, local_state);
		// Bound queued upload memory and surface async send errors promptly.
		global_state.queue->ApplyBackpressure();
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType QuackInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();
	// Register this thread's remaining rows; the shared queue is drained once in Finalize.
	FlushAppendBuffer(context.client, global_state, local_state);
	global_state.insert_count += local_state.local_count;
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType QuackInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	// Every thread registered its rows in Combine; drain all async sends here so that by the time Finalize
	// returns, all rows for the statement are on the server (matching the unbuffered path's visibility/rollback
	// semantics) and any async send error is surfaced. Close waits, releases the memory reservation, and
	// rejects further registration.
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
	return insert;
}

PhysicalOperator &QuackCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &insert = planner.Make<QuackInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	return insert;
}
