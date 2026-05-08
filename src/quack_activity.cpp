#include "quack_activity.hpp"
#include "duckdb.hpp"

namespace duckdb {

static void QuacktivityScan(ClientContext &, TableFunctionInput &, DataChunk &) {
}

static unique_ptr<FunctionData> QuacktivityBind(ClientContext &, TableFunctionBindInput &,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"connection_id", "query"};
	return nullptr;
}

TableFunction QuacktivityFunction::GetFunction() {
	return TableFunction("quacktivity", {}, QuacktivityScan, QuacktivityBind);
}

} // namespace duckdb
