#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! Internal table function that pops the DataChunks a client sends via SEND_DATA out of a per-stream
//! queue. The server drives `INSERT INTO tbl SELECT * FROM scan_data_from_quack_client('<id>')`.
class QuackScanFromClientFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb
