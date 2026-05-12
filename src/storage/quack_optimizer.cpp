#include "storage/quack_optimizer.hpp"
#include "storage/quack_catalog.hpp"
#include "quack_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

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
}

} // namespace duckdb
