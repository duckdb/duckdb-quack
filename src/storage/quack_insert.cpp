#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "duckdb/common/types/uuid.hpp"

#include "quack_client.hpp"
#include "quack_message.hpp"
#include "quack_send_data.hpp"
#include "storage/quack_catalog.hpp"
#include "storage/quack_insert.hpp"
#include "storage/quack_schema.hpp"
#include "storage/quack_table.hpp"

using namespace duckdb;

namespace {

//! The scan takes its schema from a prototype value, so the statement can be planned before any
//! batch arrives: NULL::STRUCT(col0 INTEGER, col1 VARCHAR).
string StructPrototype(const vector<LogicalType> &types) {
	string result = "STRUCT(";
	for (idx_t i = 0; i < types.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += StringUtil::Format("col%llu %s", i, types[i].ToString());
	}
	return result + ")";
}

} // namespace

QuackInsert::QuackInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr) {
}

QuackInsert::QuackInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
                         unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// Sink interface
//===--------------------------------------------------------------------===//
// The shared rebalancer does the work. This operator only supplies the send emitter.
unique_ptr<GlobalSinkState> QuackInsert::GetGlobalSinkState(ClientContext &context) const {
	optional_ptr<QuackTableCatalogEntry> table_entry;
	if (table) {
		table_entry = &table.get_mutable()->Cast<QuackTableCatalogEntry>();
	} else {
		auto &quack_schema = schema.get_mutable()->Cast<QuackSchemaCatalogEntry>();
		auto &quack_catalog = quack_schema.catalog.Cast<QuackCatalog>();
		auto entry = quack_schema.CreateTable(CatalogTransaction(quack_catalog, context), *info);
		table_entry = &entry->Cast<QuackTableCatalogEntry>();
	}

	auto debug_delay_ms = QuackGetUBigintSetting(context, "quack_debug_send_delay_ms", 0);
	auto debug_duplicate_sends = QuackGetUBigintSetting(context, "quack_debug_duplicate_sends", 0) > 0;
	auto debug_drop_batch = QuackGetUBigintSetting(context, "quack_debug_drop_batch", 0);
	auto &quack_catalog = table_entry->catalog.Cast<QuackCatalog>();
	auto &types = children[0].get().GetTypes();

	// The client names the statement, so the server composes no SQL. The stream id only needs to be
	// unique inside this connection: the connection id is the secret.
	auto stream_id = UUID::ToString(UUID::GenerateRandomUUID());
	auto query_uuid = UUID::GenerateRandomUUID();
	// The remote name carries the full nested-schema path, so the server resolves the same table.
	auto remote_name = table_entry->schema.Cast<QuackSchemaCatalogEntry>().GetRemoteName(table_entry->name);
	auto sql = StringUtil::Format(
	    "INSERT INTO %s SELECT * FROM scan_data_from_quack_client(%s, NULL::%s, ordered := %s)", remote_name.ToString(),
	    SQLString(stream_id), StructPrototype(types), order_mode != AppendOrderMode::UNORDERED ? "true" : "false");

	// inline_rows = 0, because the statement waits for our batches and has no row to answer with. The
	// scan registers the stream while it binds, so PREPARE must land before the first batch.
	auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
	auto prepare = make_uniq<PrepareRequestMessage>(quack_catalog.GetConnectionId(), sql, query_uuid);
	prepare->SetInlineRows(optional_idx(0));
	client_wrapper->GetClient().Request<PrepareResponseMessage>(context, std::move(prepare));

	auto emitter = MakeQuackSendDataEmitter(context, *table_entry, std::move(stream_id), query_uuid, debug_delay_ms,
	                                        debug_duplicate_sends, debug_drop_batch);
	return MakeQuackRebalancerGlobalState(context, types, order_mode, std::move(emitter));
}

unique_ptr<LocalSinkState> QuackInsert::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<QuackRebalancerLocalState>();
}

SinkResultType QuackInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	return QuackRebalancerSink(context, chunk, input);
}

SinkNextBatchType QuackInsert::NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const {
	return QuackRebalancerNextBatch(context, input);
}

SinkCombineResultType QuackInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	return QuackRebalancerCombine(context, input);
}

SinkFinalizeType QuackInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	// FINALIZE then carries the batch count, so the server can check the stream is complete.
	return QuackRebalancerFinalize(pipeline, event, context, input.global_state.Cast<QuackRebalancerGlobalState>());
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType QuackInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<QuackRebalancerGlobalState>();
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(gstate.row_count.load())));
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
	result["Table Name"] = table ? table->name.GetIdentifierName() : info->Base().GetTableName().GetIdentifierName();
	return result;
}

// The order strategy, chosen at plan time. It mirrors core's plan_insert.cpp:
//  - no preserve_insertion_order -> UNORDERED.
//  - preserve order, and the source has an executor batch index -> PARALLEL_ORDERED.
//  - preserve order, and no batch index -> SERIAL_ORDERED.
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
