#pragma once

namespace duckdb {

class TableFunction;

class QuackServeFunction {
public:
	static TableFunction GetFunction();
};

class QuackStopFunction {
public:
	static TableFunction GetFunction();
};

class QuackServerListFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
