//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class QuackOptimizer {
public:
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
