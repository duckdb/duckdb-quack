#include "quack_filter.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Value pushability
//===--------------------------------------------------------------------===//
// We restrict the literal types we round-trip through SQL text to ones whose
// Value::ToSQLString() is unambiguous and parses identically on the remote
// DuckDB. Composite / extension types are excluded to avoid version skew.
static bool IsPushableValueType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::UUID:
		return true;
	default:
		return false;
	}
}

static bool IsPushableComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		// COMPARE_DISTINCT_FROM / COMPARE_NOT_DISTINCT_FROM have specific NULL semantics
		// that are unsafe to push without careful handling. Skip for now.
		return false;
	}
}

bool CanPushdownFilter(const TableFilter &filter) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &f = filter.Cast<ConstantFilter>();
		if (!IsPushableComparison(f.comparison_type)) {
			return false;
		}
		if (f.constant.IsNull()) {
			// `col = NULL` is never true; do not push, let DuckDB handle it.
			return false;
		}
		return IsPushableValueType(f.constant.type());
	}
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
		return true;
	case TableFilterType::IN_FILTER: {
		auto &f = filter.Cast<InFilter>();
		if (f.values.empty()) {
			return false;
		}
		for (auto &v : f.values) {
			if (v.IsNull()) {
				return false;
			}
			if (!IsPushableValueType(v.type())) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &f = filter.Cast<ConjunctionAndFilter>();
		// AND is monotone: we can drop unpushable children server-side and still apply them
		// client-side. But we require at least one pushable child to be worth emitting.
		for (auto &child : f.child_filters) {
			if (CanPushdownFilter(*child)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &f = filter.Cast<ConjunctionOrFilter>();
		// OR is all-or-nothing: dropping a child would weaken the predicate (return more rows)
		// which is OK for correctness with a client-side re-check, BUT we must still emit ALL
		// children to avoid making the server-side filter useless. Require every child pushable.
		if (f.child_filters.empty()) {
			return false;
		}
		for (auto &child : f.child_filters) {
			if (!CanPushdownFilter(*child)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &f = filter.Cast<OptionalFilter>();
		if (!f.child_filter) {
			return false;
		}
		return CanPushdownFilter(*f.child_filter);
	}
	default:
		// DYNAMIC_FILTER, EXPRESSION_FILTER, STRUCT_EXTRACT, BLOOM_FILTER: not safe / not
		// implemented for SQL emission.
		return false;
	}
}

string FilterToSql(const TableFilter &filter, const string &column_ref) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &f = filter.Cast<ConstantFilter>();
		return column_ref + ExpressionTypeToOperator(f.comparison_type) + f.constant.ToSQLString();
	}
	case TableFilterType::IS_NULL:
		return column_ref + " IS NULL";
	case TableFilterType::IS_NOT_NULL:
		return column_ref + " IS NOT NULL";
	case TableFilterType::IN_FILTER: {
		auto &f = filter.Cast<InFilter>();
		string list;
		for (auto &v : f.values) {
			if (!list.empty()) {
				list += ", ";
			}
			list += v.ToSQLString();
		}
		return column_ref + " IN (" + list + ")";
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &f = filter.Cast<ConjunctionAndFilter>();
		string result;
		for (auto &child : f.child_filters) {
			if (!CanPushdownFilter(*child)) {
				continue;
			}
			if (!result.empty()) {
				result += " AND ";
			}
			result += "(" + FilterToSql(*child, column_ref) + ")";
		}
		return result;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &f = filter.Cast<ConjunctionOrFilter>();
		string result;
		for (auto &child : f.child_filters) {
			if (!result.empty()) {
				result += " OR ";
			}
			result += "(" + FilterToSql(*child, column_ref) + ")";
		}
		return result;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &f = filter.Cast<OptionalFilter>();
		return FilterToSql(*f.child_filter, column_ref);
	}
	default:
		throw InternalException("FilterToSql called on a non-pushable filter (type=%d) - "
		                        "CanPushdownFilter should have been checked first",
		                        static_cast<int>(filter.filter_type));
	}
}

} // namespace duckdb
