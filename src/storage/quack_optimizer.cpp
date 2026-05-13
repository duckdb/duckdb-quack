#include "storage/quack_optimizer.hpp"
#include "storage/quack_catalog.hpp"
#include "quack_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
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
}

} // namespace duckdb
