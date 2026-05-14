//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_partial_agg.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;
class LogicalAggregate;
class LogicalGet;

//! Pushes a *partial* aggregation into a quack scan that sits underneath a
//! LogicalAggregate via a join. The aggregate stays in the plan but its sum/min/max
//! expressions referencing the pushed scan are rewritten into combiner form
//! (sum-of-partial-sums etc.), and the scan now emits a small set of partially-
//! aggregated rows rather than the full table.
class QuackPartialAggregate {
public:
	static void Apply(ClientContext &context, unique_ptr<LogicalOperator> &plan);

private:
	static bool TryPushPartialAggregate(ClientContext &context, LogicalAggregate &agg, LogicalOperator &agg_child,
	                                    LogicalGet &target_get);
};

} // namespace duckdb
