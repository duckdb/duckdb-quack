#pragma once

namespace duckdb {

class TableFunction;
class TableFunctionSet;

class QuackClearCacheFunction {
public:
	static TableFunctionSet GetFunction();
};

} // namespace duckdb
