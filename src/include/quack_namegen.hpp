#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

string GenerateWhoamiName();

ScalarFunction GetWhoamiRandomNameFunction();

} // namespace duckdb
