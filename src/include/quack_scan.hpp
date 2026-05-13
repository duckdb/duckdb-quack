#pragma once

#include "quack_uri.hpp"
#include "quack_client.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

//! A single ORDER BY entry forwarded to the remote server, expressed in terms of the
//! positional column index into the underlying remote table (`column_id`, 0-based).
struct QuackPushedOrderBy {
	idx_t column_id;
	OrderType order_type;
	OrderByNullType null_order;
};

struct QuackScanBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackScanBindData>();
		return other.client_connection->ConnectionId() == client_connection->ConnectionId() &&
		       other.client_connection->ServerURI() == client_connection->ServerURI() &&
		       other.table_name == table_name && other.column_names == column_names &&
		       other.column_types == column_types;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackScanBindData>();
		result->client_connection = client_connection;
		result->table_name = table_name;
		result->column_names = column_names;
		result->column_types = column_types;
		return std::move(result);
	}

	string table_name;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<unique_ptr<DataChunkWrapper>> results;
	shared_ptr<QuackClientConnection> client_connection;
	bool needs_more_fetch = true;
	hugeint_t result_uuid;
	//! LIMIT pushed down by the optimizer (M2). When set, the rewritten scan SQL appends
	//! "LIMIT <pushed_limit> [OFFSET <pushed_offset>]". The corresponding LogicalLimit /
	//! LogicalTopN node is left in the plan as a client-side safety net.
	optional_idx pushed_limit;
	optional_idx pushed_offset;
	//! ORDER BY clauses pushed alongside a LogicalTopN. Empty for a plain LogicalLimit.
	vector<QuackPushedOrderBy> pushed_order_by;
	//! Best-effort row count for the underlying remote table (M4). Sourced from the
	//! remote `duckdb_tables().estimated_size` at catalog-load time. When set, surfaced
	//! to the optimizer via the table function's `cardinality` callback so cross-server
	//! join orderings make sense. Stale until the catalog is re-attached / refreshed.
	optional_idx estimated_cardinality;
	//! Aggregation pushdown (M3a): when non-empty, the rewritten scan SQL emits these
	//! aggregate expressions as the suffix of the SELECT list. The corresponding
	//! LogicalAggregate has been removed; the LogicalGet now produces one row per group
	//! (or a single row when pushed_group_keys is empty).
	vector<string> pushed_aggregates;
	//! GROUP BY column SQL fragments (positional refs like "#3"). When non-empty they
	//! are emitted both as the prefix of the SELECT list AND as the GROUP BY clause.
	//! Empty for total (no-GROUP-BY) aggregations.
	vector<string> pushed_group_keys;
	//! WHERE clause (without the leading "WHERE") captured at agg-pushdown time. Filters
	//! that DuckDB had folded into LogicalGet.table_filters need to be moved here because
	//! the agg rewrite reshapes column_ids and breaks the filters' positional references.
	//! Empty when no aggregate is pushed; ignored in that case (the per-scan path consults
	//! input.filters directly).
	string pushed_where_sql;
};

class TableFunction;

class QuackScanFunction {
public:
	static TableFunction GetFunction();
};

class QuackScanByNameFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
