#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class Expression;
class LogicalGet;
class TableFilter;

//! Returns true if `filter` (and all of its descendants) can be safely emitted as SQL and
//! pushed down to a remote quack server. Conservative: when in doubt, returns false.
bool CanPushdownFilter(const TableFilter &filter);

//! Emits the filter as a SQL WHERE-clause fragment, using `column_ref` as the LHS for the
//! column the filter is keyed on. `column_ref` is typically a positional reference like "#3"
//! that resolves against the FROM clause of the rewritten scan query.
//!
//! Precondition: `CanPushdownFilter(filter)` returned true.
string FilterToSql(const TableFilter &filter, const string &column_ref);

//! Try to emit `expr` as SQL referencing the FROM clause of the given quack scan via
//! positional `#N` refs. Supports bare column refs, constants of pushable types, casts,
//! and a whitelist of pure scalar function calls / operators whose name parses identically
//! on a remote DuckDB. Returns true on success and writes the SQL fragment to `out_sql`;
//! returns false when any subexpression isn't pushable.
bool TryEmitExpressionSql(const Expression &expr, const LogicalGet &get, string &out_sql);

} // namespace duckdb
