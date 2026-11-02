#pragma once

namespace duckdb {

class TableFunction;

class QuackActiveConnectionsFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
