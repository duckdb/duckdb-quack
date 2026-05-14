#include "quack_filter.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
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

//===--------------------------------------------------------------------===//
// Expression -> SQL (for partial group key pushdown)
//===--------------------------------------------------------------------===//
// Whitelist of operator / function names whose token parses identically on a
// remote DuckDB. Each entry is the name as emitted by DuckDB's binder.
static bool IsPushableInfixOperator(const string &name) {
	return name == "+" || name == "-" || name == "*" || name == "/" || name == "%" || name == "mod" ||
	       name == "||" || name == "<<" || name == ">>" || name == "&" || name == "|" || name == "#" ||
	       name == "=" || name == "<>" || name == "!=" || name == "<" || name == "<=" || name == ">" ||
	       name == ">=";
}

// Functions / scalar names that we'll emit as prefix calls `name(args)`. We restrict to
// well-known pure scalar functions whose definition is stable across DuckDB versions.
static bool IsPushablePrefixFunction(const string &name) {
	auto lc = StringUtil::Lower(name);
	return lc == "abs" || lc == "ceil" || lc == "floor" || lc == "round" || lc == "trunc" ||
	       lc == "lower" || lc == "upper" || lc == "length" || lc == "strlen" ||
	       lc == "trim" || lc == "ltrim" || lc == "rtrim" || lc == "reverse" ||
	       lc == "concat" || lc == "left" || lc == "right" || lc == "substring" || lc == "substr" ||
	       lc == "coalesce" || lc == "ifnull" || lc == "nullif" ||
	       lc == "date_trunc" || lc == "datetrunc" || lc == "date_part" || lc == "datepart" ||
	       lc == "year" || lc == "month" || lc == "day" || lc == "hour" || lc == "minute" ||
	       lc == "second" || lc == "quarter" || lc == "dayofweek" || lc == "dayofyear" ||
	       lc == "epoch" || lc == "epoch_ms" || lc == "to_timestamp" ||
	       lc == "greatest" || lc == "least" || lc == "sign" || lc == "ln" || lc == "log" ||
	       lc == "log10" || lc == "log2" || lc == "exp" || lc == "sqrt" || lc == "power" || lc == "pow" ||
	       lc == "md5" || lc == "sha1" || lc == "sha256" || lc == "hash" ||
	       lc == "cast" || lc == "try_cast";
}

static bool IsPushableConstantType(const LogicalType &type) {
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

bool TryEmitExpressionSql(const Expression &expr, const LogicalGet &get, string &out_sql) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &c = expr.Cast<BoundColumnRefExpression>();
		if (c.binding.table_index != get.table_index) {
			return false;
		}
		auto &column_ids = get.GetColumnIds();
		if (c.binding.column_index >= column_ids.size()) {
			return false;
		}
		auto &idx = column_ids[c.binding.column_index];
		if (idx.IsVirtualColumn()) {
			return false;
		}
		out_sql = "#" + std::to_string(idx.GetPrimaryIndex() + 1);
		return true;
	}
	case ExpressionClass::BOUND_CONSTANT: {
		auto &c = expr.Cast<BoundConstantExpression>();
		if (c.value.IsNull()) {
			out_sql = "NULL";
			return true;
		}
		if (!IsPushableConstantType(c.value.type())) {
			return false;
		}
		out_sql = c.value.ToSQLString();
		return true;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &c = expr.Cast<BoundCastExpression>();
		string inner;
		if (!TryEmitExpressionSql(*c.child, get, inner)) {
			return false;
		}
		if (!IsPushableConstantType(c.return_type)) {
			return false;
		}
		out_sql = "CAST(" + inner + " AS " + c.return_type.ToString() + ")";
		return true;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &f = expr.Cast<BoundFunctionExpression>();
		// Reject non-consistent functions (random, current_timestamp at server time,
		// etc.) — they'd give different results on server vs client.
		if (f.function.stability != FunctionStability::CONSISTENT) {
			return false;
		}
		vector<string> args;
		args.reserve(f.children.size());
		for (auto &child : f.children) {
			string s;
			if (!TryEmitExpressionSql(*child, get, s)) {
				return false;
			}
			args.push_back(std::move(s));
		}
		auto name = f.function.name;
		if (f.is_operator && args.size() == 2 && IsPushableInfixOperator(name)) {
			out_sql = "(" + args[0] + " " + name + " " + args[1] + ")";
			return true;
		}
		if (f.is_operator && args.size() == 1) {
			// unary prefix operator, e.g. `-x`
			if (name == "-" || name == "+" || name == "~") {
				out_sql = "(" + name + args[0] + ")";
				return true;
			}
		}
		if (IsPushablePrefixFunction(name)) {
			out_sql = name + "(";
			for (idx_t i = 0; i < args.size(); i++) {
				if (i > 0) {
					out_sql += ", ";
				}
				out_sql += args[i];
			}
			out_sql += ")";
			return true;
		}
		return false;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		// Most logical operators (AND/OR/NOT, IS NULL etc.) — emit only the
		// shapes we recognize.
		auto &op = expr.Cast<BoundOperatorExpression>();
		auto type = op.GetExpressionType();
		vector<string> args;
		args.reserve(op.children.size());
		for (auto &child : op.children) {
			string s;
			if (!TryEmitExpressionSql(*child, get, s)) {
				return false;
			}
			args.push_back(std::move(s));
		}
		switch (type) {
		case ExpressionType::OPERATOR_NOT:
			if (args.size() == 1) {
				out_sql = "(NOT " + args[0] + ")";
				return true;
			}
			return false;
		case ExpressionType::OPERATOR_IS_NULL:
			if (args.size() == 1) {
				out_sql = "(" + args[0] + " IS NULL)";
				return true;
			}
			return false;
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			if (args.size() == 1) {
				out_sql = "(" + args[0] + " IS NOT NULL)";
				return true;
			}
			return false;
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

} // namespace duckdb
