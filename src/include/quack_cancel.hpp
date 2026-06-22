#pragma once

namespace duckdb {

class TableFunction;

class QuackCancelFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
