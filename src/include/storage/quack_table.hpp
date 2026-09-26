//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
class QuackCatalog;
class QuackSchemaCatalogEntry;

class QuackTableSet : public QuackCatalogSet {
public:
	QuackTableSet(ClientContext &context, QuackSchemaCatalogEntry &parent, const QuackLoadCatalogData &load_data);
	explicit QuackTableSet(QuackSchemaCatalogEntry &parent);

	static string GetLoadQuery();

private:
	QuackSchemaCatalogEntry &schema;
};

class QuackTableCatalogEntry : public TableCatalogEntry {
public:
	QuackTableCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateTableInfo &info_p)
	    : TableCatalogEntry(catalog_p, schema_p, info_p), columns(std::move(info_p.columns)) {
	}
	const ColumnList &GetColumns() const override {
		return columns;
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

protected:
	ColumnList columns;
};

} // namespace duckdb
