#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! Internal table function that reads the DataChunks a client sends via SEND_DATA from the stream's buffer.
//! The server drives `INSERT INTO tbl SELECT * FROM scan_data_from_quack_client('<id>', NULL::STRUCT(...))`.
class QuackScanFromClientFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
