#include "storage/quack_optimizer.hpp"
#include "storage/quack_catalog.hpp"
#include "quack_filter.hpp"
#include "quack_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

struct QuackOperatorInfo {
	vector<reference<LogicalGet>> scans;
	idx_t insert_count = 0;
};

struct QuackOperators {
	// map of connection id -> operator info
	unordered_map<string, QuackOperatorInfo> op_info;
};

void GatherQuackScans(LogicalOperator &op, QuackOperators &result) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto &table_scan = get.function;
		if (QuackCatalog::IsQuackScan(table_scan.name)) {
			// add a quack scan
			auto &bind_data = get.bind_data->Cast<QuackScanBindData>();
			auto connection_id = bind_data.client_connection->ConnectionId();
			result.op_info[connection_id].scans.push_back(get);
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_CREATE_TABLE) {
		auto &insert = op.Cast<LogicalCreateTable>();
		auto &catalog = insert.schema.ParentCatalog();
		if (catalog.GetCatalogType() == "quack") {
			auto &quack_catalog = catalog.Cast<QuackCatalog>();
			auto connection_id = quack_catalog.GetConnectionId();
			result.op_info[connection_id].insert_count += 1;
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_INSERT) {
		auto &insert = op.Cast<LogicalInsert>();
		auto &catalog = insert.table.ParentCatalog();
		if (catalog.GetCatalogType() == "quack") {
			auto &quack_catalog = catalog.Cast<QuackCatalog>();
			auto connection_id = quack_catalog.GetConnectionId();
			result.op_info[connection_id].insert_count += 1;
		}
	}
	// recurse into children
	for (auto &child : op.children) {
		GatherQuackScans(*child, result);
	}
}

// Returns the wrapped quack LogicalGet if `op` is a direct quack scan, else nullptr.
// Used by the LIMIT / TopN pushdown walker to decide whether to push.
static LogicalGet *AsQuackGet(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	if (!QuackCatalog::IsQuackScan(get.function.name)) {
		return nullptr;
	}
	return &get;
}

// Resolve a BoundOrderByNode whose expression is a plain ColumnRef into the underlying
// table column id (0-based). Returns false if the order key is anything other than a
// direct column reference into the scan we're pushing into.
static bool TryResolveOrderColumn(const BoundOrderByNode &order, const LogicalGet &get,
                                  QuackPushedOrderBy &out) {
	if (!order.expression || order.expression->type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col_ref = order.expression->Cast<BoundColumnRefExpression>();
	if (col_ref.binding.table_index != get.table_index) {
		return false;
	}
	auto &column_ids = get.GetColumnIds();
	if (col_ref.binding.column_index >= column_ids.size()) {
		return false;
	}
	auto &col_idx = column_ids[col_ref.binding.column_index];
	if (col_idx.IsVirtualColumn()) {
		return false;
	}
	out.column_id = col_idx.GetPrimaryIndex();
	out.order_type = order.type;
	out.null_order = order.null_order;
	return true;
}

static bool IsLimitPushdownEnabled(ClientContext &context) {
	Value v;
	if (!context.TryGetCurrentSetting("quack_pushdown_limit", v)) {
		return true;
	}
	return v.GetValue<bool>();
}

// Attempt to push a LogicalLimit into a directly-underlying quack scan. Returns true if
// fully pushed (caller should replace the node with its child); false if not pushable.
static bool TryPushLimit(LogicalLimit &limit) {
	if (limit.children.size() != 1) {
		return false;
	}
	auto *get = AsQuackGet(*limit.children[0]);
	if (!get) {
		return false;
	}
	if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return false;
	}
	if (limit.offset_val.Type() != LimitNodeType::UNSET &&
	    limit.offset_val.Type() != LimitNodeType::CONSTANT_VALUE) {
		return false;
	}
	auto &bind_data = get->bind_data->Cast<QuackScanBindData>();
	bind_data.pushed_limit = limit.limit_val.GetConstantValue();
	if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
		bind_data.pushed_offset = limit.offset_val.GetConstantValue();
	}
	return true;
}

// Same for LogicalTopN. Requires every ORDER BY key to be a plain column ref into the scan.
static bool TryPushTopN(LogicalTopN &top_n) {
	if (top_n.children.size() != 1) {
		return false;
	}
	auto *get = AsQuackGet(*top_n.children[0]);
	if (!get) {
		return false;
	}
	vector<QuackPushedOrderBy> resolved;
	resolved.reserve(top_n.orders.size());
	for (auto &order : top_n.orders) {
		QuackPushedOrderBy entry;
		if (!TryResolveOrderColumn(order, *get, entry)) {
			return false;
		}
		resolved.push_back(entry);
	}
	auto &bind_data = get->bind_data->Cast<QuackScanBindData>();
	bind_data.pushed_limit = top_n.limit;
	if (top_n.offset > 0) {
		bind_data.pushed_offset = top_n.offset;
	}
	bind_data.pushed_order_by = std::move(resolved);
	return true;
}

// Walk the plan and for each child slot that is a pushable LogicalLimit / LogicalTopN
// directly above a quack scan, fold the limit/topN into the scan's bind_data and replace
// the slot with the scan itself. We can't simply leave the limit in place because
// OFFSET re-applied by DuckDB after a pushed OFFSET would yield zero rows.
static void PushLimitsDown(unique_ptr<LogicalOperator> &op_ref) {
	auto &op = *op_ref;
	// Try to fold THIS node if it is a limit/topN over a quack scan. This handles the
	// (rare) case where the limit is the plan root.
	if (op.type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit = op.Cast<LogicalLimit>();
		if (TryPushLimit(limit)) {
			op_ref = std::move(limit.children[0]);
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &top_n = op.Cast<LogicalTopN>();
		if (TryPushTopN(top_n)) {
			op_ref = std::move(top_n.children[0]);
		}
	}
	// Recurse into the (possibly new) children.
	for (auto &child : op_ref->children) {
		PushLimitsDown(child);
	}
}

// ===-------------------------------------------------------------------=== //
// M3a-narrow: total aggregation pushdown
// ===-------------------------------------------------------------------=== //
//
// Recognized pattern:
//   LogicalAggregate (no GROUP BY, no DISTINCT, no FILTER, no ORDER BY on aggs,
//                     all aggregates are count_star / count(col) / sum(col) /
//                     min(col) / max(col) over a bare column ref into the
//                     directly-underlying quack scan)
//      -> LogicalGet (quack)
//
// Rewrite: the LogicalAggregate is dropped from the plan, and the underlying
// LogicalGet is mutated to:
//   - emit exactly one row whose columns are the aggregate results
//   - report its output via table_index = aggregate_index (so existing
//     ColumnBinding(aggregate_index, i) references upstream resolve cleanly)
// The aggregate SELECT-list strings are stashed on QuackScanBindData; the
// per-scan BuildPushdownQuery emits them in place of the per-row projection
// (still combined with any WHERE pushed by M1).

static bool IsAggregatePushdownEnabled(ClientContext &context) {
	Value v;
	if (!context.TryGetCurrentSetting("quack_pushdown_aggregates", v)) {
		return true;
	}
	return v.GetValue<bool>();
}

// Maps a BoundAggregateExpression to its server-side SQL fragment using a
// positional ref into the underlying quack scan. Returns false for any shape
// outside the M3a-narrow pushable set.
static bool TryEmitAggregateSql(const BoundAggregateExpression &agg, const LogicalGet &get, string &out_sql) {
	// Reject distinct, filter, order-by aggregates: pushed semantics would differ.
	if (agg.aggr_type != AggregateType::NON_DISTINCT) {
		return false;
	}
	if (agg.filter || !agg.order_bys || !agg.order_bys->orders.empty()) {
		// order_bys is a unique_ptr; null means none, but if present and empty
		// we still want to allow. Be conservative: if any order-by entries, skip.
		if (agg.order_bys && !agg.order_bys->orders.empty()) {
			return false;
		}
		if (agg.filter) {
			return false;
		}
	}
	auto name = StringUtil::Lower(agg.function.name);
	auto resolve_col = [&](const Expression &expr, string &col_ref) -> bool {
		if (expr.type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &col = expr.Cast<BoundColumnRefExpression>();
		if (col.binding.table_index != get.table_index) {
			return false;
		}
		auto &column_ids = get.GetColumnIds();
		if (col.binding.column_index >= column_ids.size()) {
			return false;
		}
		auto &idx = column_ids[col.binding.column_index];
		if (idx.IsVirtualColumn()) {
			return false;
		}
		col_ref = "#" + to_string(idx.GetPrimaryIndex() + 1);
		return true;
	};

	if (name == "count_star") {
		if (!agg.children.empty()) {
			return false;
		}
		out_sql = "count(*)";
		return true;
	}
	if (name == "count" || name == "sum" || name == "min" || name == "max") {
		if (agg.children.size() != 1) {
			return false;
		}
		string col_ref;
		if (!resolve_col(*agg.children[0], col_ref)) {
			return false;
		}
		out_sql = name + "(" + col_ref + ")";
		return true;
	}
	return false;
}

// Attempt to push a LogicalAggregate directly above a quack LogicalGet.
// On success, the LogicalGet is mutated and the caller should replace the
// LogicalAggregate slot with the (now-rewired) LogicalGet.
static bool TryPushAggregate(LogicalAggregate &agg, LogicalGet &get) {
	// Aggregation rewrite relies on BuildPushdownQuery actually rewriting the SQL
	// at scan-init time, which only happens for catalog-attached scans (where
	// bind_data.table_name is set). The raw quack_query(uri, sql) path uses the
	// user's verbatim SQL — pushing here would silently corrupt the result schema.
	auto &bind_data = get.bind_data->Cast<QuackScanBindData>();
	if (bind_data.table_name.empty()) {
		return false;
	}
	if (!agg.groups.empty() || agg.grouping_sets.size() > 1) {
		return false;
	}
	// Reject GROUPING() function calls.
	if (!agg.grouping_functions.empty()) {
		return false;
	}
	if (agg.expressions.empty()) {
		return false;
	}
	vector<string> agg_sqls;
	vector<LogicalType> agg_types;
	vector<string> agg_names;
	agg_sqls.reserve(agg.expressions.size());
	agg_types.reserve(agg.expressions.size());
	agg_names.reserve(agg.expressions.size());
	for (auto &expr : agg.expressions) {
		if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &bound_agg = expr->Cast<BoundAggregateExpression>();
		string sql;
		if (!TryEmitAggregateSql(bound_agg, get, sql)) {
			return false;
		}
		agg_sqls.push_back(std::move(sql));
		agg_types.push_back(bound_agg.return_type);
		agg_names.push_back(bound_agg.GetName());
	}

	// Capture any filters DuckDB already pushed into LogicalGet.table_filters BEFORE we
	// reshape column_ids — their column indexes point into the OLD layout (original
	// table columns). Emit them as SQL using the old column_ids and stash on bind_data.
	// Then clear table_filters so DuckDB doesn't re-validate them against the new
	// (1-row-of-aggregate-outputs) shape and panic with "Could not find column index".
	string captured_where;
	for (auto &entry : get.table_filters.filters) {
		auto column_id = entry.first;
		auto &filter = *entry.second;
		if (!CanPushdownFilter(filter)) {
			continue;
		}
		auto col_ref = "#" + to_string(column_id + 1);
		auto fragment = FilterToSql(filter, col_ref);
		if (fragment.empty()) {
			continue;
		}
		if (!captured_where.empty()) {
			captured_where += " AND ";
		}
		captured_where += "(" + fragment + ")";
	}
	get.table_filters.filters.clear();

	// Mutate the LogicalGet: take over the aggregate's bindings & output schema.
	bind_data.pushed_aggregates = std::move(agg_sqls);
	bind_data.pushed_where_sql = std::move(captured_where);
	bind_data.column_names = agg_names;
	bind_data.column_types = agg_types;
	// LIMIT / ORDER BY pushed by M2 would be invalid alongside whole-table aggs.
	// We hit this only if optimizer ordering changes; defensive clear.
	bind_data.pushed_limit = optional_idx();
	bind_data.pushed_offset = optional_idx();
	bind_data.pushed_order_by.clear();
	// Aggregate output is single-row.
	bind_data.estimated_cardinality = 1;

	get.table_index = agg.aggregate_index;
	get.returned_types = agg_types;
	get.names = agg_names;
	vector<ColumnIndex> new_column_ids;
	for (idx_t i = 0; i < agg_types.size(); i++) {
		new_column_ids.emplace_back(i);
	}
	get.SetColumnIds(std::move(new_column_ids));
	get.projection_ids.clear();
	get.types = agg_types;
	return true;
}

static void PushAggregatesDown(unique_ptr<LogicalOperator> &op_ref) {
	auto &op = *op_ref;
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && op.children.size() == 1) {
		auto &agg = op.Cast<LogicalAggregate>();
		auto *get = AsQuackGet(*agg.children[0]);
		if (get && TryPushAggregate(agg, *get)) {
			op_ref = std::move(agg.children[0]);
		}
	}
	for (auto &child : op_ref->children) {
		PushAggregatesDown(child);
	}
}

void QuackOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// look at the query plan and check if we can enable streaming query scans
	QuackOperators operators;
	GatherQuackScans(*plan, operators);
	if (operators.op_info.empty()) {
		// no scans
		return;
	}
	for (auto &entry : operators.op_info) {
		auto &op_info = entry.second;
		auto multiple_scans = (op_info.scans.size() + op_info.insert_count) > 1;
		if (!multiple_scans) {
			continue;
		}
		for (auto &_ : op_info.scans) {
			throw NotImplementedException("Multiple streaming scans or streaming scans + CTAS / insert in the same "
			                              "query are not currently supported");
		}
	}
	// M2: push LIMIT / TopN into quack scans.
	if (IsLimitPushdownEnabled(input.context)) {
		PushLimitsDown(plan);
	}
	// M3a-narrow: push total aggregations (no GROUP BY) into quack scans.
	if (IsAggregatePushdownEnabled(input.context)) {
		PushAggregatesDown(plan);
	}
}

} // namespace duckdb
