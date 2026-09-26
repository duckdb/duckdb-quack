//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {
class QuackCatalog;
class QuackTransaction;

struct QuackLoadCatalogData {
	unique_ptr<ColumnDataCollection> schemas;
	unique_ptr<ColumnDataCollection> tables;
};

class QuackCatalogSet {
public:
	explicit QuackCatalogSet(QuackCatalog &catalog);
	virtual ~QuackCatalogSet() = default;

	optional_ptr<CatalogEntry> GetEntry(const string &entry_name);
	void DropEntry(const string &entry_name);
	vector<reference<CatalogEntry>> GetAllCatalogEntries();
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry, OnCreateConflict on_conflict);
	void Clear();

protected:
	QuackCatalog &catalog;

private:
	mutable mutex entry_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
};

} // namespace duckdb
