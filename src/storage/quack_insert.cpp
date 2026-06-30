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
	    : table(table_p), insert_count(0), query_uuid(UUID::GenerateRandomUUID()) {
	}

	QuackTableCatalogEntry &table;
	idx_t insert_count;
	//! Per-statement id for the server-side insert stream. Sent on every SEND_DATA and the FINALIZE so
	//! the server keys the stream by (connection_id, query_uuid).
	hugeint_t query_uuid;
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
// Sink
//===--------------------------------------------------------------------===//
SinkResultType QuackInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &tbl = global_state.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();
	// Wrap (references chunk, no copy); serialized synchronously in Request below while `chunk` is alive.
	auto chunk_wrapper = make_uniq<DataChunkWrapper>(chunk);
	auto append_message = make_uniq<SendDataRequestMessage>(
	    quack_catalog.GetConnectionId(), tbl.schema.name.GetIdentifierName(), tbl.name.GetIdentifierName(),
	    std::move(chunk_wrapper), global_state.query_uuid);

	auto client_connection = quack_catalog.GetClientConnection();
	auto client_wrapper = client_connection->GetClient(context.client);
	auto &client = client_wrapper->GetClient();
	// The response carries a placeholder accept-budget for future flow control; errors arrive as an
	// ErrorResponse, which Request<>() rethrows.
	client.Request<SendDataResponseMessage>(context.client, std::move(append_message));

	// Client-side optimistic count (rows sent); exact because any server failure throws above before reporting.
	global_state.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType QuackInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &tbl = global_state.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();
	auto client_connection = quack_catalog.GetClientConnection();
	auto client_wrapper = client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	// Signal end-of-stream so the server drains the source, completes the INSERT statement, and
	// commits it. A server-side failure (e.g. a constraint violation) surfaces here as an error.
	client.Request<SuccessResponse>(
	    context, make_uniq<FinalizeMessage>(quack_catalog.GetConnectionId(), global_state.query_uuid));
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
