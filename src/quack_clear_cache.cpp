#include "quack_clear_cache.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"

#include "storage/quack_catalog.hpp"

namespace duckdb {

struct ClearCacheFunctionData : public TableFunctionData {
	bool finished = false;
	string catalog_name;
};

unique_ptr<FunctionData> ClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<ClearCacheFunctionData>();
	if (!input.inputs.empty()) {
		QuackCatalog::GetQuackCatalog(context, input.inputs[0]);
		bind_data->catalog_name = input.inputs[0].GetValue<string>();
	}
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(bind_data);
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

void ClearQuackCache(ClientContext &context, const string &catalog_name) {
	auto name = Value(catalog_name);
	QuackCatalog::GetQuackCatalog(context, name).Refresh(context);
}

void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ClearCacheFunctionData>();
	if (data.finished) {
		return;
	}
	if (data.catalog_name.empty()) {
		ClearQuackCaches(context);
	} else {
		ClearQuackCache(context, data.catalog_name);
	}
	output.data[0].SetValue(0, Value::BOOLEAN(true));
	output.SetChildCardinality(1);
	data.finished = true;
}

TableFunctionSet QuackClearCacheFunction::GetFunction() {
	TableFunctionSet set("quack_clear_cache");
	set.AddFunction(TableFunction("quack_clear_cache", {}, ClearCacheFunction, ClearCacheBind));
	set.AddFunction(TableFunction("quack_clear_cache", {LogicalType::VARCHAR}, ClearCacheFunction, ClearCacheBind));
	return set;
}

} // namespace duckdb
