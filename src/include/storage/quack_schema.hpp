//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "storage/quack_catalog_set.hpp"

namespace duckdb {

class QuackCatalog;
class QuackTableSet;

class QuackSchemaSet : public QuackCatalogSet {
public:
	QuackSchemaSet(ClientContext &context, QuackCatalog &catalog, const QuackLoadCatalogData &load_data);

	static string GetLoadQuery();

	void Reload(ClientContext &context, QuackCatalog &catalog, const QuackLoadCatalogData &load_data);
};

class QuackSchemaCatalogEntry : public SchemaCatalogEntry {
public:
	QuackSchemaCatalogEntry(Catalog &catalog_p, CreateSchemaInfo &info_p);
	QuackSchemaCatalogEntry(ClientContext &context, Catalog &catalog_p, CreateSchemaInfo &info_p,
	                        const QuackLoadCatalogData &load_data);
	~QuackSchemaCatalogEntry() override;

	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;

	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;

private:
	optional_ptr<CatalogEntry> TryLoadBuiltInFunction(const string &entry_name);
	optional_ptr<CatalogEntry> LoadBuiltInFunction(DefaultTableMacro macro);

private:
	unique_ptr<QuackTableSet> tables;

private:
	mutex default_function_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> default_function_map;
};

} // namespace duckdb
