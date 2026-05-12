#include "quack_clear_cache.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"

#include "storage/quack_catalog.hpp"

namespace duckdb {

struct ClearCacheFunctionData : public TableFunctionData {
	bool finished = false;
};

unique_ptr<FunctionData> ClearCacheBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                                        vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return make_uniq<ClearCacheFunctionData>();
}

void ClearQuackCaches(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &catalog = db_ref->GetCatalog();
		if (catalog.GetCatalogType() != "quack") {
			continue;
		}
		catalog.Cast<QuackCatalog>().Refresh(context);
	}
}

void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &) {
	auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
	if (data.finished) {
		return;
	}
	ClearQuackCaches(context);
	data.finished = true;
}

TableFunction QuackClearCacheFunction::GetFunction() {
	return TableFunction("quack_clear_cache", {}, ClearCacheFunction, ClearCacheBind);
}

} // namespace duckdb
