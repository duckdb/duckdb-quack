#pragma once

namespace duckdb {

class TableFunction;

class QuackServeFunction {
public:
	static TableFunctionSet GetFunction();
};

class QuackStopFunction {
public:
	static TableFunction GetFunction();
};

class QuackServerListFunction {
public:
	static TableFunction GetFunction();
};

class QuackGenerateKeysFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
