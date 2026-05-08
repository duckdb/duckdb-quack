#pragma once

namespace duckdb {

class TableFunction;

class QuacktivityFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
