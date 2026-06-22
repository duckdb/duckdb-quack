#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

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

	QuackTableCatalogEntry &table;
	//! Total rows inserted, summed from per-thread counts at Combine time
	atomic<idx_t> insert_count;
	//! Rows a thread buffers before shipping one APPEND_REQUEST (from quack_append_flush_rows)
	idx_t flush_rows;
};

class QuackInsertLocalState : public LocalSinkState {
public:
	//! Self-owned chunks buffered client-side, not yet shipped to the server
	vector<unique_ptr<DataChunk>> buffer;
	idx_t buffered_rows = 0;
	idx_t local_count = 0;
	//! One warm connection held for this thread's lifetime, mirroring QuackScanLocalState
	unique_ptr<QuackClientWrapper> client_wrapper;
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
	if (table) {
		return make_uniq<QuackInsertGlobalState>(table.get_mutable()->Cast<QuackTableCatalogEntry>(), flush_rows);
	}
	// CREATE TABLE AS path: create the table on the remote side first
	auto &quack_schema = schema.get_mutable()->Cast<QuackSchemaCatalogEntry>();
	auto &quack_catalog = quack_schema.catalog.Cast<QuackCatalog>();

	auto entry = quack_schema.CreateTable(CatalogTransaction(quack_catalog, context), *info);
	return make_uniq<QuackInsertGlobalState>(entry->Cast<QuackTableCatalogEntry>(), flush_rows);
}

unique_ptr<LocalSinkState> QuackInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<QuackInsertLocalState>();
}

//===--------------------------------------------------------------------===//
// Append buffering
//===--------------------------------------------------------------------===//
// Ship all buffered chunks to the server in one APPEND_REQUEST. The wrappers reference the buffered
// chunks (no copy) and the request is serialized synchronously inside Request(), so the buffer only
// needs to stay alive until Request() returns. Each thread reuses one warm connection across flushes,
// like the scan path; server-side appends serialize under the connection lock.
static void FlushAppendBuffer(ClientContext &context, QuackTableCatalogEntry &tbl,
                              QuackInsertLocalState &local_state) {
	if (local_state.buffer.empty()) {
		return;
	}
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();

	vector<unique_ptr<DataChunkWrapper>> wrappers;
	wrappers.reserve(local_state.buffer.size());
	for (auto &chunk : local_state.buffer) {
		wrappers.push_back(make_uniq<DataChunkWrapper>(*chunk));
	}
	auto append_message =
	    make_uniq<AppendRequestMessage>(quack_catalog.GetConnectionId(), tbl.schema.name.GetIdentifierName(),
	                                    tbl.name.GetIdentifierName(), std::move(wrappers));

	if (!local_state.client_wrapper) {
		local_state.client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
	}
	auto &client = local_state.client_wrapper->GetClient();
	client.Request<SuccessResponse>(context, std::move(append_message));

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
		FlushAppendBuffer(context.client, global_state.table, local_state);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType QuackInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &local_state = input.local_state.Cast<QuackInsertLocalState>();
	// Ship this thread's remaining rows, then fold its count into the global total.
	FlushAppendBuffer(context.client, global_state.table, local_state);
	global_state.insert_count += local_state.local_count;
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType QuackInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	// Every thread drained its buffer in Combine, so by the time Finalize runs all rows for the
	// statement have been sent — transaction visibility/rollback semantics match the unbuffered path.
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
