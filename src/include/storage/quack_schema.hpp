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
class QuackSchemaCatalogEntry;
class QuackTableSet;

//! Remote oid used for a schema that does not exist as a schema on the server: the stand-in entry for a
//! remote catalog, which is a level of the local schema tree but not a remote schema of its own
static constexpr int64_t QUACK_INVALID_SCHEMA_OID = -1;

//! The set of top-level schemas of a quack catalog. The schemas of a quack catalog form a tree that mirrors
//! the server: a remote nested schema becomes a nested schema here as well, and a remote catalog other than
//! the server's default one becomes a top-level schema holding that catalog's schemas.
class QuackSchemaSet : public QuackCatalogSet {
public:
	QuackSchemaSet(ClientContext &context, QuackCatalog &catalog, const QuackLoadCatalogData &load_data);

	static string GetLoadQuery();

	void Reload(ClientContext &context, QuackCatalog &catalog, const QuackLoadCatalogData &load_data);

private:
	//! Register a schema at the given (possibly nested) local path, creating any missing parent level
	void RegisterSchema(ClientContext &context, QuackCatalog &catalog, const vector<Identifier> &local_path,
	                    int64_t remote_oid, const QuackLoadCatalogData &load_data);
};

class QuackSchemaCatalogEntry : public SchemaCatalogEntry {
public:
	//! A schema that is not loaded from the server: one that was just created, or the stand-in entry for a
	//! remote catalog
	QuackSchemaCatalogEntry(Catalog &catalog_p, CreateSchemaInfo &info_p,
	                        optional_ptr<SchemaCatalogEntry> parent_schema_p = nullptr,
	                        int64_t remote_oid_p = QUACK_INVALID_SCHEMA_OID);
	//! A schema loaded from the server, together with the tables/views it holds
	QuackSchemaCatalogEntry(ClientContext &context, Catalog &catalog_p, CreateSchemaInfo &info_p,
	                        optional_ptr<SchemaCatalogEntry> parent_schema_p, int64_t remote_oid_p,
	                        const QuackLoadCatalogData &load_data);
	~QuackSchemaCatalogEntry() override;

	unique_ptr<CreateInfo> GetInfo() const override;

	optional_ptr<SchemaCatalogEntry> GetParentSchema() const override {
		return parent_schema;
	}

	//! The schemas nested inside this schema
	QuackCatalogSet &Schemas() {
		return *schemas;
	}
	//! The oid of this schema on the server, or QUACK_INVALID_SCHEMA_OID if it has none
	int64_t RemoteOid() const {
		return remote_oid;
	}
	//! The fully qualified name of an entry in this schema as the server sees it. The local schema path is
	//! exactly the remote qualification - the catalog name is only the local ATTACH alias, so it is dropped
	QualifiedName GetRemoteName(const Identifier &entry_name) const;

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
	optional_ptr<SchemaCatalogEntry> parent_schema;
	int64_t remote_oid;
	unique_ptr<QuackCatalogSet> schemas;
	unique_ptr<QuackTableSet> tables;

private:
	mutex default_function_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> default_function_map;
};

} // namespace duckdb
