#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

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

} // namespace duckdb
