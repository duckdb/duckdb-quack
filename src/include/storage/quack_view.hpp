//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {
class QuackCatalog;
class QuackSchemaCatalogEntry;

class QuackViewCatalogEntry : public ViewCatalogEntry {
public:
	QuackViewCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateViewInfo &info_p);

	//! Generate SQL for querying a view from quack
	//! This SQL will always be "FROM {catalog}.query('FROM {view_name}');"
	//! Ensuring the view is executed remotely
	static string CreateViewSQL(const string &catalog_name, const string &schema_name, const string &view_name,
	                            const string &server_catalog = string());
};

} // namespace duckdb
