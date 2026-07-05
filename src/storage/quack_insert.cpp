#include "duckdb/common/types/column/column_data_collection.hpp"
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
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      return_chunk(op.Cast<LogicalInsert>().return_chunk) {
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
	QuackInsertGlobalState(ClientContext &context, QuackTableCatalogEntry &table_p, bool return_chunk,
	                       const vector<LogicalType> &return_types)
	    : table(table_p), insert_count(0) {
		if (return_chunk) {
			return_collection = make_uniq<ColumnDataCollection>(context, return_types);
		}
	}

	QuackTableCatalogEntry &table;
	idx_t insert_count;
	//! When RETURNING is used, holds the inserted rows to stream back as the result
	unique_ptr<ColumnDataCollection> return_collection;
};

unique_ptr<GlobalSinkState> QuackInsert::GetGlobalSinkState(ClientContext &context) const {
	if (table) {
		return make_uniq<QuackInsertGlobalState>(context, table.get_mutable()->Cast<QuackTableCatalogEntry>(),
		                                         return_chunk, types);
	}
	// CREATE TABLE AS path: create the table on the remote side first
	auto &quack_schema = schema.get_mutable()->Cast<QuackSchemaCatalogEntry>();
	auto &quack_catalog = quack_schema.catalog.Cast<QuackCatalog>();

	auto entry = quack_schema.CreateTable(CatalogTransaction(quack_catalog, context), *info);
	return make_uniq<QuackInsertGlobalState>(context, entry->Cast<QuackTableCatalogEntry>(), return_chunk, types);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType QuackInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<QuackInsertGlobalState>();
	auto &tbl = global_state.table;
	auto &quack_catalog = tbl.catalog.Cast<QuackCatalog>();
	auto append_chunk = make_uniq<DataChunk>();
	append_chunk->Initialize(context.client, chunk.GetTypes());
	append_chunk->Reference(chunk);
	auto chunk_wrapper = make_uniq<DataChunkWrapper>(*append_chunk);
	auto append_message = make_uniq<AppendRequestMessage>(quack_catalog.GetConnectionId(), tbl.schema.name, tbl.name,
	                                                      std::move(chunk_wrapper));

	auto client_connection = quack_catalog.GetClientConnection();
	auto client_wrapper = client_connection->GetClient(context.client);
	auto &client = client_wrapper->GetClient();
	client.Request<SuccessResponse>(context.client, std::move(append_message));

	global_state.insert_count += chunk.size();
	if (return_chunk) {
		global_state.return_collection->Append(chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType QuackInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	// TODO nop?
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
class QuackInsertSourceState : public GlobalSourceState {
public:
	explicit QuackInsertSourceState(const QuackInsert &op) {
		if (op.return_chunk) {
			D_ASSERT(op.sink_state);
			auto &g = op.sink_state->Cast<QuackInsertGlobalState>();
			g.return_collection->InitializeScan(scan_state);
		}
	}

	ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> QuackInsert::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<QuackInsertSourceState>(*this);
}

SourceResultType QuackInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<QuackInsertGlobalState>();
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(insert_gstate.insert_count)));
		return SourceResultType::FINISHED;
	}

	auto &state = input.global_state.Cast<QuackInsertSourceState>();
	insert_gstate.return_collection->Scan(state.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
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
