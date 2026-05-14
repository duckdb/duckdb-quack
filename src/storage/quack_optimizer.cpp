#include "storage/quack_optimizer.hpp"
#include "storage/quack_catalog.hpp"
#include "quack_filter.hpp"
#include "quack_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
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

// Resolves a BoundColumnRefExpression that may point through a trivial LogicalProjection
// down to the underlying quack scan. On success returns the LogicalGet and updates
// `out_column_index` to the position in get.column_ids corresponding to the input ref.
//
// "Trivial projection" = every projection expression is a bare BoundColumnRef.
// Returns nullptr when the binding doesn't resolve cleanly to a scan column.
static LogicalGet *ResolveColumnRefToScan(const BoundColumnRefExpression &ref, LogicalOperator &op,
                                          idx_t &out_column_index) {
	if (auto *get = AsQuackGet(op)) {
		if (ref.binding.table_index != get->table_index) {
			return nullptr;
		}
		if (ref.binding.column_index >= get->GetColumnIds().size()) {
			return nullptr;
		}
		out_column_index = ref.binding.column_index;
		return get;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION && op.children.size() == 1) {
		auto &proj = op.Cast<LogicalProjection>();
		if (ref.binding.table_index != proj.table_index) {
			return nullptr;
		}
		if (ref.binding.column_index >= proj.expressions.size()) {
			return nullptr;
		}
		auto &child_expr = *proj.expressions[ref.binding.column_index];
		if (child_expr.type != ExpressionType::BOUND_COLUMN_REF) {
			return nullptr;
		}
		auto &child_ref = child_expr.Cast<BoundColumnRefExpression>();
		return ResolveColumnRefToScan(child_ref, *proj.children[0], out_column_index);
	}
	return nullptr;
}

// Walk past a trivial single-child LogicalProjection chain to reach the underlying
// quack LogicalGet. Returns nullptr if the chain isn't trivial (any projection
// expression isn't a bare column ref).
static LogicalGet *FindQuackGetBelowTrivialProjection(LogicalOperator &op) {
	auto *cur = &op;
	while (cur && cur->type == LogicalOperatorType::LOGICAL_PROJECTION && cur->children.size() == 1) {
		auto &proj = cur->Cast<LogicalProjection>();
		for (auto &e : proj.expressions) {
			if (e->type != ExpressionType::BOUND_COLUMN_REF) {
				return nullptr;
			}
		}
		cur = proj.children[0].get();
	}
	if (cur) {
		return AsQuackGet(*cur);
	}
	return nullptr;
}

// Resolve a BoundOrderByNode whose expression is a plain ColumnRef into the underlying
// table column id (0-based). Walks through a possible trivial LogicalProjection
// between the TopN and the scan, so plans like
//   TopN -> Projection(bare col refs) -> Get
// can still be pushed. Returns false if the order key is anything other than a
// bare column reference that ultimately resolves to a non-virtual scan column.
static bool TryResolveOrderColumn(const BoundOrderByNode &order, LogicalOperator &child_op,
                                  const LogicalGet &target_get, QuackPushedOrderBy &out) {
	if (!order.expression || order.expression->type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col_ref = order.expression->Cast<BoundColumnRefExpression>();
	idx_t scan_col_index = 0;
	auto *get = ResolveColumnRefToScan(col_ref, child_op, scan_col_index);
	if (!get || get != &target_get) {
		return false;
	}
	auto &col_idx = get->GetColumnIds()[scan_col_index];
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

// Attempt to push a LogicalLimit into the underlying quack scan, possibly traversing
// a trivial LogicalProjection chain between the limit and the scan. Returns true if
// fully pushed (caller should replace the node with its child); false if not pushable.
static bool TryPushLimit(LogicalLimit &limit) {
	if (limit.children.size() != 1) {
		return false;
	}
	auto *get = FindQuackGetBelowTrivialProjection(*limit.children[0]);
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

// Same for LogicalTopN. Requires every ORDER BY key to be a plain column ref that
// resolves through any intervening trivial projection chain into the scan.
static bool TryPushTopN(LogicalTopN &top_n) {
	if (top_n.children.size() != 1) {
		return false;
	}
	auto *get = FindQuackGetBelowTrivialProjection(*top_n.children[0]);
	if (!get) {
		return false;
	}
	vector<QuackPushedOrderBy> resolved;
	resolved.reserve(top_n.orders.size());
	for (auto &order : top_n.orders) {
		QuackPushedOrderBy entry;
		if (!TryResolveOrderColumn(order, *top_n.children[0], *get, entry)) {
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

// Description of the column-binding remap to apply to the rest of the plan after a
// LogicalAggregate has been folded into a quack LogicalGet.
struct AggregateBindingRemap {
	idx_t old_group_index;       // LogicalAggregate.group_index
	idx_t old_aggregate_index;   // LogicalAggregate.aggregate_index
	idx_t new_table_index;       // LogicalGet's (post-rewrite) table_index
	idx_t group_count;           // |G| — aggregate column j moves from position j to (G+j)
};

// Visitor that rewrites BoundColumnRefExpression bindings touching the removed
// LogicalAggregate's two table indexes into the unified new scan layout:
//   (old_group_index, i)   -> (new_table_index, i)
//   (old_aggregate_index,j)-> (new_table_index, group_count + j)
class AggregateBindingRemapper : public LogicalOperatorVisitor {
public:
	explicit AggregateBindingRemapper(const AggregateBindingRemap &remap_p) : remap(remap_p) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.binding.table_index == remap.old_group_index) {
			expr.binding.table_index = remap.new_table_index;
			// column_index already corresponds to the group position, no shift needed.
		} else if (expr.binding.table_index == remap.old_aggregate_index) {
			expr.binding.table_index = remap.new_table_index;
			expr.binding.column_index += remap.group_count;
		}
		return nullptr;
	}

private:
	const AggregateBindingRemap &remap;
};

// Resolve a group key expression to SQL. Bare BoundColumnRefs become a positional ref
// (e.g. "#3"); expressions are emitted via TryEmitExpressionSql, which recursively
// handles a whitelist of pure scalar operators / functions over bare cols and constants.
// Returns false when any subexpression isn't pushable.
static bool TryEmitGroupKeySql(const Expression &expr, const LogicalGet &get, string &out_sql) {
	return TryEmitExpressionSql(expr, get, out_sql);
}

// Attempt to push a LogicalAggregate directly above a quack LogicalGet.
// On success, the LogicalGet is mutated, the caller replaces the LogicalAggregate
// slot with the LogicalGet, and the returned `remap` is non-empty so the binding
// rewriter can run over the rest of the plan.
static bool TryPushAggregate(LogicalAggregate &agg, LogicalGet &get, AggregateBindingRemap &remap_out) {
	// Aggregation rewrite relies on BuildPushdownQuery actually rewriting the SQL
	// at scan-init time, which only happens for catalog-attached scans.
	auto &bind_data = get.bind_data->Cast<QuackScanBindData>();
	if (bind_data.table_name.empty()) {
		return false;
	}
	// Reject ROLLUP / CUBE / GROUPING SETS (multiple grouping sets).
	if (agg.grouping_sets.size() > 1) {
		return false;
	}
	// Reject GROUPING() function calls — depend on grouping_sets.
	if (!agg.grouping_functions.empty()) {
		return false;
	}
	if (agg.expressions.empty() && agg.groups.empty()) {
		return false;
	}

	// Group keys — every entry must be a bare column ref into the scan.
	vector<string> group_sqls;
	vector<LogicalType> group_types;
	vector<string> group_names;
	group_sqls.reserve(agg.groups.size());
	group_types.reserve(agg.groups.size());
	group_names.reserve(agg.groups.size());
	for (auto &g : agg.groups) {
		string sql;
		if (!TryEmitGroupKeySql(*g, get, sql)) {
			return false;
		}
		group_sqls.push_back(std::move(sql));
		group_types.push_back(g->return_type);
		group_names.push_back(g->GetName());
	}

	// Aggregate expressions.
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

	// Capture filters using the old column_ids (primary indices) before reshaping.
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

	// Combined output layout: groups first, then aggregates.
	vector<LogicalType> out_types;
	vector<string> out_names;
	out_types.reserve(group_types.size() + agg_types.size());
	out_names.reserve(group_names.size() + agg_names.size());
	for (idx_t i = 0; i < group_types.size(); i++) {
		out_types.push_back(group_types[i]);
		out_names.push_back(group_names[i]);
	}
	for (idx_t i = 0; i < agg_types.size(); i++) {
		out_types.push_back(agg_types[i]);
		out_names.push_back(agg_names[i]);
	}

	// Stash everything on bind_data.
	bind_data.pushed_aggregates = std::move(agg_sqls);
	bind_data.pushed_group_keys = std::move(group_sqls);
	bind_data.pushed_where_sql = std::move(captured_where);
	bind_data.column_names = out_names;
	bind_data.column_types = out_types;
	// LIMIT / ORDER BY pushed by M2 would be invalid alongside agg rewrite.
	bind_data.pushed_limit = optional_idx();
	bind_data.pushed_offset = optional_idx();
	bind_data.pushed_order_by.clear();
	// Single row for total aggregation; otherwise unknown but bounded by base table.
	if (agg.groups.empty()) {
		bind_data.estimated_cardinality = 1;
	} else {
		bind_data.estimated_cardinality = optional_idx();
	}

	// Reshape the LogicalGet's output schema. The new table_index unifies what used
	// to be the aggregate's two indices; we pick aggregate_index by convention.
	get.table_index = agg.aggregate_index;
	get.returned_types = out_types;
	get.names = out_names;
	vector<ColumnIndex> new_column_ids;
	new_column_ids.reserve(out_types.size());
	for (idx_t i = 0; i < out_types.size(); i++) {
		new_column_ids.emplace_back(i);
	}
	get.SetColumnIds(std::move(new_column_ids));
	get.projection_ids.clear();
	get.types = out_types;

	remap_out.old_group_index = agg.group_index;
	remap_out.old_aggregate_index = agg.aggregate_index;
	remap_out.new_table_index = get.table_index;
	remap_out.group_count = group_types.size();
	return true;
}

static void PushAggregatesDownRec(unique_ptr<LogicalOperator> &op_ref, vector<AggregateBindingRemap> &remaps) {
	auto &op = *op_ref;
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && op.children.size() == 1) {
		auto &agg = op.Cast<LogicalAggregate>();
		auto *get = AsQuackGet(*agg.children[0]);
		if (get) {
			AggregateBindingRemap remap;
			if (TryPushAggregate(agg, *get, remap)) {
				op_ref = std::move(agg.children[0]);
				remaps.push_back(remap);
			}
		}
	}
	for (auto &child : op_ref->children) {
		PushAggregatesDownRec(child, remaps);
	}
}

static void PushAggregatesDown(unique_ptr<LogicalOperator> &plan) {
	vector<AggregateBindingRemap> remaps;
	PushAggregatesDownRec(plan, remaps);
	// Apply the binding remaps over the whole plan. Each remap touches only the two
	// table indices that belonged to the removed LogicalAggregate; sibling subtrees
	// use different indices so they're unaffected.
	for (auto &remap : remaps) {
		AggregateBindingRemapper rewriter(remap);
		rewriter.VisitOperator(*plan);
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
	// Same-server multi-scan used to throw NotImplementedException here because the
	// server held a single streaming QueryResult per connection — a second PREPARE
	// from a parallel scan would clobber the first. The server now materializes
	// results into a uuid-keyed map on QuackConnection so concurrent in-flight
	// results coexist safely; lifting the throw lets queries like
	//   SELECT * FROM rpc.a JOIN rpc.b ON a.k = b.k
	// execute by running both scans against the same connection in parallel.
	(void)operators;
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
