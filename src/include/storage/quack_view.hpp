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
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {
class QuackCatalog;
class QuackSchemaCatalogEntry;

class QuackViewCatalogEntry : public ViewCatalogEntry {
public:
	QuackViewCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateViewInfo &info_p);

	//! Generate SQL for querying a view from quack
	//! This SQL will always be "FROM {catalog}.query('FROM {remote_view_name}');"
	//! Ensuring the view is executed remotely. `remote_view_name` is the view as the server sees it, i.e.
	//! qualified with the (possibly nested) remote schema path
	static string CreateViewSQL(const string &catalog_name, const QualifiedName &remote_view_name);
};

} // namespace duckdb
