#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include "quack_filter.hpp"
#include "quack_scan.hpp"
#include "storage/quack_catalog.hpp"
#include "storage/quack_partial_agg.hpp"

#include <unordered_map>

namespace duckdb {

// Recursively scan `expr` for BoundColumnRefExpressions targeting `table_index`. Each
// unique referenced column's primary id (from `column_ids`) is appended to
// `out_primaries`. Returns false (and sets out_ok=false) if any ref is into a virtual
// or out-of-range column.
static void CollectPrimaries(Expression &expr, idx_t table_index, const vector<ColumnIndex> &column_ids,
                             vector<idx_t> &out_primaries, bool &out_ok) {
	if (!out_ok) {
		return;
	}
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &c = expr.Cast<BoundColumnRefExpression>();
		if (c.binding.table_index == table_index) {
			if (c.binding.column_index >= column_ids.size()) {
				out_ok = false;
				return;
			}
			auto &idx = column_ids[c.binding.column_index];
			if (idx.IsVirtualColumn()) {
				out_ok = false;
				return;
			}
			auto primary = idx.GetPrimaryIndex();
			for (auto p : out_primaries) {
				if (p == primary) {
					return;
				}
			}
			out_primaries.push_back(primary);
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) {
		CollectPrimaries(*child, table_index, column_ids, out_primaries, out_ok);
	});
}

// Rewrite every BoundColumnRef into `table_index` so that its column_index points at
// the new layout: the old column_ids entry's primary id is looked up in `primary_to_new`
// and replaced. Returns false if any ref lacks a mapping.
static bool RewriteRefsToNewLayout(Expression &expr, idx_t table_index, const vector<ColumnIndex> &old_column_ids,
                                   const std::unordered_map<idx_t, idx_t> &primary_to_new) {
	bool ok = true;
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &c = expr.Cast<BoundColumnRefExpression>();
		if (c.binding.table_index != table_index) {
			return true;
		}
		if (c.binding.column_index >= old_column_ids.size()) {
			return false;
		}
		auto primary = old_column_ids[c.binding.column_index].GetPrimaryIndex();
		auto it = primary_to_new.find(primary);
		if (it == primary_to_new.end()) {
			return false;
		}
		c.binding.column_index = it->second;
		return true;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) {
		if (!RewriteRefsToNewLayout(*child, table_index, old_column_ids, primary_to_new)) {
			ok = false;
		}
	});
	return ok;
}

// True iff `agg` is sum/min/max over a bare BoundColumnRef into target_get.
static bool IsPushablePartialAggregate(const BoundAggregateExpression &agg, idx_t target_table_index,
                                       const vector<ColumnIndex> &column_ids, string &out_func_name,
                                       idx_t &out_arg_primary) {
	if (agg.aggr_type != AggregateType::NON_DISTINCT) {
		return false;
	}
	if (agg.filter) {
		return false;
	}
	if (agg.order_bys && !agg.order_bys->orders.empty()) {
		return false;
	}
	auto name = StringUtil::Lower(agg.function.name);
	if (name != "sum" && name != "min" && name != "max") {
		return false;
	}
	if (agg.children.size() != 1) {
		return false;
	}
	auto &child = *agg.children[0];
	if (child.type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &cr = child.Cast<BoundColumnRefExpression>();
	if (cr.binding.table_index != target_table_index) {
		return false;
	}
	if (cr.binding.column_index >= column_ids.size()) {
		return false;
	}
	auto &idx = column_ids[cr.binding.column_index];
	if (idx.IsVirtualColumn()) {
		return false;
	}
	out_func_name = name;
	out_arg_primary = idx.GetPrimaryIndex();
	return true;
}

// Build a fresh BoundAggregateExpression for the "combiner" — same function name, but
// applied over the partial column's type.
static unique_ptr<Expression> BuildCombinerAggregate(ClientContext &context, const string &func_name,
                                                    const LogicalType &partial_type, idx_t scan_table_index,
                                                    idx_t scan_column_position, const LogicalType &expected_return_type,
                                                    const string &alias) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, func_name);
	auto bound_fun = entry.functions.GetFunctionByArguments(context, {partial_type});
	FunctionBinder binder(context);

	auto child = make_uniq<BoundColumnRefExpression>(partial_type, ColumnBinding(scan_table_index, scan_column_position));
	vector<unique_ptr<Expression>> args;
	args.push_back(std::move(child));
	auto expr = binder.BindAggregateFunction(bound_fun, std::move(args));
	if (!alias.empty()) {
		expr->SetAlias(alias);
	}
	if (expr->return_type != expected_return_type) {
		// Combiner doesn't naturally produce the original aggregate's declared type;
		// fall back rather than risk silent wrong types.
		return nullptr;
	}
	return unique_ptr<Expression>(expr.release());
}

bool QuackPartialAggregate::TryPushPartialAggregate(ClientContext &context, LogicalAggregate &agg,
                                                    LogicalOperator &agg_child, LogicalGet &target_get) {
	auto &bind_data = target_get.bind_data->Cast<QuackScanBindData>();
	if (bind_data.table_name.empty()) {
		return false;
	}
	if (!bind_data.pushed_aggregates.empty() || !bind_data.pushed_group_keys.empty() ||
	    bind_data.pushed_limit.IsValid()) {
		return false;
	}
	if (agg.grouping_sets.size() > 1 || !agg.grouping_functions.empty()) {
		return false;
	}
	if (agg_child.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false; // v1: only directly under a comparison join
	}
	auto &join = agg_child.Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER) {
		return false;
	}

	// Capture original column_ids — we'll mutate target_get.column_ids later but the
	// upstream BoundColumnRefs still encode positions into this old list.
	auto old_column_ids = target_get.GetColumnIds();
	auto target_table_index = target_get.table_index;

	// 1) Collect base-key primaries from join conditions and agg group expressions.
	vector<idx_t> base_key_primaries;
	bool ok = true;
	for (auto &cond : join.conditions) {
		CollectPrimaries(*cond.left, target_table_index, old_column_ids, base_key_primaries, ok);
		CollectPrimaries(*cond.right, target_table_index, old_column_ids, base_key_primaries, ok);
	}
	for (auto &e : join.expressions) {
		CollectPrimaries(*e, target_table_index, old_column_ids, base_key_primaries, ok);
	}
	for (auto &g : agg.groups) {
		CollectPrimaries(*g, target_table_index, old_column_ids, base_key_primaries, ok);
	}
	if (!ok) {
		return false;
	}

	// 2) Classify aggregate expressions. Any aggregate that touches target_get must be
	//    a pushable partial; otherwise we'd silently change its semantics.
	struct PushableAggInfo {
		idx_t agg_index;
		string func_name;
		idx_t arg_primary;
	};
	vector<PushableAggInfo> pushable;
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &e = *agg.expressions[i];
		if (e.expression_class != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		vector<idx_t> refs;
		bool inner_ok = true;
		CollectPrimaries(e, target_table_index, old_column_ids, refs, inner_ok);
		if (!inner_ok) {
			return false;
		}
		if (refs.empty()) {
			continue; // aggregate doesn't reference target_get
		}
		auto &ba = e.Cast<BoundAggregateExpression>();
		string func_name;
		idx_t arg_primary;
		if (!IsPushablePartialAggregate(ba, target_table_index, old_column_ids, func_name, arg_primary)) {
			return false;
		}
		pushable.push_back({i, std::move(func_name), arg_primary});
	}

	if (pushable.empty() && base_key_primaries.empty()) {
		return false; // nothing to push
	}

	// 3) Build the new column layout: base_keys first, partial_aggs second.
	vector<LogicalType> new_types;
	vector<string> new_names;
	std::unordered_map<idx_t, idx_t> primary_to_new;
	for (auto pid : base_key_primaries) {
		primary_to_new[pid] = new_types.size();
		new_types.push_back(bind_data.column_types[pid]);
		new_names.push_back(bind_data.column_names[pid]);
	}
	struct PartialColInfo {
		idx_t agg_index;
		string func_name;
		idx_t arg_primary;
		idx_t partial_col_position;
		LogicalType partial_type;
	};
	vector<PartialColInfo> partial_cols;
	partial_cols.reserve(pushable.size());
	for (auto &p : pushable) {
		auto pos = new_types.size();
		auto &orig_agg = agg.expressions[p.agg_index]->Cast<BoundAggregateExpression>();
		auto partial_type = orig_agg.return_type;
		new_types.push_back(partial_type);
		new_names.push_back(p.func_name + "_" + bind_data.column_names[p.arg_primary] + "_partial");
		partial_cols.push_back({p.agg_index, p.func_name, p.arg_primary, pos, partial_type});
	}

	// 4) Build combiner aggregates BEFORE mutating anything, so failures don't half-
	//    apply the rewrite.
	vector<unique_ptr<Expression>> combiners;
	combiners.reserve(partial_cols.size());
	for (auto &pc : partial_cols) {
		auto &orig_agg = agg.expressions[pc.agg_index]->Cast<BoundAggregateExpression>();
		auto combiner = BuildCombinerAggregate(context, pc.func_name, pc.partial_type, target_table_index,
		                                       pc.partial_col_position, orig_agg.return_type, orig_agg.GetAlias());
		if (!combiner) {
			return false;
		}
		combiners.push_back(std::move(combiner));
	}

	// 5) Rewrite refs in join conditions, join.expressions, and agg.groups to point at
	//    the new layout. These refs are encoded against the OLD column_ids positions.
	for (auto &cond : join.conditions) {
		if (!RewriteRefsToNewLayout(*cond.left, target_table_index, old_column_ids, primary_to_new) ||
		    !RewriteRefsToNewLayout(*cond.right, target_table_index, old_column_ids, primary_to_new)) {
			return false;
		}
	}
	for (auto &e : join.expressions) {
		if (!RewriteRefsToNewLayout(*e, target_table_index, old_column_ids, primary_to_new)) {
			return false;
		}
	}
	for (auto &g : agg.groups) {
		if (!RewriteRefsToNewLayout(*g, target_table_index, old_column_ids, primary_to_new)) {
			return false;
		}
	}

	// 6) Capture WHERE filters before reshaping (filter keys are old primary ids).
	string captured_where;
	for (auto &entry : target_get.table_filters.filters) {
		auto column_id = entry.first;
		auto &filter = *entry.second;
		if (!CanPushdownFilter(filter)) {
			continue;
		}
		auto fragment = FilterToSql(filter, "#" + to_string(column_id + 1));
		if (fragment.empty()) {
			continue;
		}
		if (!captured_where.empty()) {
			captured_where += " AND ";
		}
		captured_where += "(" + fragment + ")";
	}
	target_get.table_filters.filters.clear();

	// 7) Mutate the scan's bind_data and shape.
	bind_data.pushed_group_keys.clear();
	bind_data.pushed_aggregates.clear();
	for (auto pid : base_key_primaries) {
		bind_data.pushed_group_keys.push_back("#" + to_string(pid + 1));
	}
	for (auto &pc : partial_cols) {
		bind_data.pushed_aggregates.push_back(pc.func_name + "(#" + to_string(pc.arg_primary + 1) + ")");
	}
	bind_data.pushed_where_sql = std::move(captured_where);
	bind_data.column_names = new_names;
	bind_data.column_types = new_types;

	target_get.returned_types = new_types;
	target_get.names = new_names;
	vector<ColumnIndex> new_column_ids;
	for (idx_t i = 0; i < new_types.size(); i++) {
		new_column_ids.emplace_back(i);
	}
	target_get.SetColumnIds(std::move(new_column_ids));
	target_get.projection_ids.clear();
	target_get.types = new_types;

	// 8) Replace each pushed agg expression with its combiner.
	for (idx_t i = 0; i < partial_cols.size(); i++) {
		agg.expressions[partial_cols[i].agg_index] = std::move(combiners[i]);
	}

	return true;
}

// Rewrite every BoundColumnRef in `expr` so that any ref through a trivial
// LogicalProjection chain is replaced with the equivalent direct ref into the
// projection's child. Returns false if any ref doesn't resolve to a bare col ref.
static bool RewriteAcrossTrivialProjection(Expression &expr, idx_t proj_table_index,
                                           const vector<unique_ptr<Expression>> &proj_exprs, idx_t child_table_index) {
	bool ok = true;
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &c = expr.Cast<BoundColumnRefExpression>();
		if (c.binding.table_index != proj_table_index) {
			return true;
		}
		if (c.binding.column_index >= proj_exprs.size()) {
			return false;
		}
		auto &target = *proj_exprs[c.binding.column_index];
		if (target.type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &target_ref = target.Cast<BoundColumnRefExpression>();
		c.binding = target_ref.binding;
		return true;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) {
		if (!RewriteAcrossTrivialProjection(*child, proj_table_index, proj_exprs, child_table_index)) {
			ok = false;
		}
	});
	return ok;
}

void QuackPartialAggregate::Apply(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	std::function<void(unique_ptr<LogicalOperator> &)> walk = [&](unique_ptr<LogicalOperator> &op_ref) {
		auto &op = *op_ref;
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && op.children.size() == 1) {
			auto &agg = op.Cast<LogicalAggregate>();
			// Resolve through a possible LogicalProjection between agg and join. We
			// rewrite the agg's groups/expressions to bypass the projection so the
			// pushdown logic below sees the simpler agg-over-join pattern.
			auto *child = agg.children[0].get();
			while (child && child->type == LogicalOperatorType::LOGICAL_PROJECTION && child->children.size() == 1) {
				auto &proj = child->Cast<LogicalProjection>();
				bool trivial = true;
				for (auto &e : proj.expressions) {
					if (e->type != ExpressionType::BOUND_COLUMN_REF) {
						trivial = false;
						break;
					}
				}
				if (!trivial) {
					break;
				}
				bool ok = true;
				for (auto &g : agg.groups) {
					ok = RewriteAcrossTrivialProjection(*g, proj.table_index, proj.expressions, /*unused*/ 0) && ok;
				}
				for (auto &e : agg.expressions) {
					ok = RewriteAcrossTrivialProjection(*e, proj.table_index, proj.expressions, /*unused*/ 0) && ok;
				}
				if (!ok) {
					break;
				}
				// Drop this projection — the agg now references the projection's child
				// directly.
				agg.children[0] = std::move(proj.children[0]);
				child = agg.children[0].get();
			}
			if (child && child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
				auto &join = child->Cast<LogicalComparisonJoin>();
				for (auto &side : join.children) {
					// Walk past a trivial projection on the join's side to reach the get.
					auto *side_op = side.get();
					while (side_op && side_op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
					       side_op->children.size() == 1) {
						auto &p = side_op->Cast<LogicalProjection>();
						bool trivial = true;
						for (auto &e : p.expressions) {
							if (e->type != ExpressionType::BOUND_COLUMN_REF) {
								trivial = false;
								break;
							}
						}
						if (!trivial) {
							break;
						}
						side_op = p.children[0].get();
					}
					if (side_op && side_op->type == LogicalOperatorType::LOGICAL_GET) {
						auto &get = side_op->Cast<LogicalGet>();
						if (QuackCatalog::IsQuackScan(get.function.name)) {
							TryPushPartialAggregate(context, agg, *child, get);
						}
					}
				}
			}
		}
		for (auto &c : op_ref->children) {
			walk(c);
		}
	};
	walk(plan);
}

} // namespace duckdb
