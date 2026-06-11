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
	explicit QuackInsertGlobalState(QuackTableCatalogEntry &table_p)
	    : table(table_p), insert_count(0), buffered_rows(0) {
	}

	QuackTableCatalogEntry &table;
	idx_t insert_count;
	//! Self-owned chunks buffered client-side, not yet shipped to the server
	vector<unique_ptr<DataChunk>> buffer;
	idx_t buffered_rows;
};

unique_ptr<GlobalSinkState> QuackInsert::GetGlobalSinkState(ClientContext &context) const {
	if (table) {
		return make_uniq<QuackInsertGlobalState>(table.get_mutable()->Cast<QuackTableCatalogEntry>());
	}
	// CREATE TABLE AS path: create the table on the remote side first
	auto &quack_schema = schema.get_mutable()->Cast<QuackSchemaCatalogEntry>();
	auto &quack_catalog = quack_schema.catalog.Cast<QuackCatalog>();

	auto entry = quack_schema.CreateTable(CatalogTransaction(quack_catalog, context), *info);
	return make_uniq<QuackInsertGlobalState>(entry->Cast<QuackTableCatalogEntry>());
}

//===--------------------------------------------------------------------===//
// Append buffering
//===--------------------------------------------------------------------===//
// This is just some threshold that mimics the appender threshold; could be made configurable
static constexpr idx_t QUACK_APPEND_FLUSH_ROWS = STANDARD_VECTOR_SIZE * 100ULL;

// Ship all buffered chunks to the server in one APPEND_REQUEST. The wrappers reference the buffered
// chunks (no copy) and the request is serialized synchronously inside Request(), so the buffer only
// needs to stay alive until Request() returns.
static void FlushAppendBuffer(ClientContext &context, QuackInsertGlobalState &global_state) {
	if (global_state.buffer.empty()) {
		return;
	}
	auto &tbl = global_state.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();

	vector<unique_ptr<DataChunkWrapper>> wrappers;
	wrappers.reserve(global_state.buffer.size());
	for (auto &chunk : global_state.buffer) {
		wrappers.push_back(make_uniq<DataChunkWrapper>(*chunk));
	}
	auto append_message = make_uniq<AppendRequestMessage>(quack_catalog.GetConnectionId(), tbl.schema.name, tbl.name,
	                                                      std::move(wrappers));

	auto client_connection = quack_catalog.GetClientConnection();
	auto client_wrapper = client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	client.Request<SuccessResponse>(context, std::move(append_message));

	global_state.buffer.clear();
	global_state.buffered_rows = 0;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType QuackInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	// Buffer a self-owned copy of the chunk: the executor reuses the source chunk across Sink calls,
	// so we cannot Reference it and defer the send. Size the copy to the actual row count (not a full
	// STANDARD_VECTOR_SIZE) so a stream of partial chunks doesn't inflate memory.
	auto owned = make_uniq<DataChunk>();
	owned->Initialize(context.client, chunk.GetTypes(), chunk.size());
	owned->Append(chunk);
	global_state.buffered_rows += owned->size();
	global_state.insert_count += chunk.size();
	global_state.buffer.push_back(std::move(owned));

	if (global_state.buffered_rows >= QUACK_APPEND_FLUSH_ROWS) {
		FlushAppendBuffer(context.client, global_state);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType QuackInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	// Ship any remaining buffered rows. After this returns every row for the statement has been sent,
	// so transaction visibility/rollback semantics match the unbuffered path.
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	FlushAppendBuffer(context, global_state);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType QuackInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<QuackInsertGlobalState>();
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));
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
	result["Table Name"] = table ? table->name : info->Base().table;
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
