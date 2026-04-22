#include "rpc_insert.hpp"

#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "catalog.hpp"
#include "message.hpp"

using namespace duckdb;

RpcInsert::RpcInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                     physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

RpcInsert::RpcInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                     unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class RpcInsertGlobalState : public GlobalSinkState {
public:
	explicit RpcInsertGlobalState(RpcTableCatalogEntry &table_p) : table(table_p), insert_count(0) {
	}

	RpcTableCatalogEntry &table;
	idx_t insert_count;
};

unique_ptr<GlobalSinkState> RpcInsert::GetGlobalSinkState(ClientContext &context) const {
	if (table) {
		return make_uniq<RpcInsertGlobalState>(table.get_mutable()->Cast<RpcTableCatalogEntry>());
	}
	// CREATE TABLE AS path: create the table on the remote side, stash it in the schema's
	// table cache, and hand the sink a reference into the cache. The cache owns lifetime.
	auto &rpc_schema = schema.get_mutable()->Cast<RpcSchemaCatalogEntry>();
	auto &rpc_catalog = rpc_schema.catalog.Cast<RpcCatalog>();
	auto &txn = RpcTransaction::Get(context, rpc_catalog);

	auto create_table_info = info->Base().Copy();
	create_table_info->catalog = rpc_schema.GetInfo()->catalog;
	create_table_info->schema = rpc_schema.GetInfo()->schema;

	auto catalog_request_message =
	    make_uniq<CatalogRequestMessage>(rpc_catalog.GetConnectionId(), std::move(create_table_info));
	auto catalog_response =
	    txn.GetClient().Request<CatalogResponseMessage>(std::move(catalog_request_message));

	auto entry = make_uniq<RpcTableCatalogEntry>(rpc_schema.catalog, rpc_schema,
	                                             catalog_response->GetParseInfo()->Cast<CreateTableInfo>());
	auto &cache_ref = *entry;
	rpc_schema.AddToTableCache(info->Base().table, std::move(entry));
	return make_uniq<RpcInsertGlobalState>(cache_ref);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType RpcInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<RpcInsertGlobalState>();
	auto &tbl = global_state.table;
	auto &rpc_catalog = tbl.catalog.Cast<RpcCatalog>();
	auto &txn = RpcTransaction::Get(context.client, rpc_catalog);
	auto append_chunk = make_uniq<DataChunk>();
	append_chunk->Initialize(context.client, chunk.GetTypes());
	append_chunk->Reference(chunk);
	auto append_message = make_uniq<AppendRequestMessage>(rpc_catalog.GetConnectionId(), tbl.schema.name, tbl.name,
	                                                      std::move(append_chunk));
	txn.GetClient().Request<AppendResponseMessage>(std::move(append_message));
	global_state.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType RpcInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                     OperatorSinkFinalizeInput &input) const {
	// TODO nop?
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType RpcInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<RpcInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string RpcInsert::GetName() const {
	return table ? "RPC_INSERT" : "RPC_CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> RpcInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

PhysicalOperator &RpcCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
	D_ASSERT(plan);
	auto &insert = planner.Make<RpcInsert>(op, op.table, op.column_index_map);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &RpcCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                LogicalCreateTable &op, PhysicalOperator &plan) {
	auto &insert = planner.Make<RpcInsert>(op, op.schema, std::move(op.info));
	insert.children.push_back(plan);
	return insert;
}
