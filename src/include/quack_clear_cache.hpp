#pragma once

namespace duckdb {

class TableFunction;

class QuackClearCacheFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
