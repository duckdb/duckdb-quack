#include "storage/quack_catalog_set.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

#include <vector>

namespace duckdb {

QuackCatalogSet::QuackCatalogSet(QuackCatalog &catalog) : catalog(catalog) {
}

optional_ptr<CatalogEntry> QuackCatalogSet::GetEntry(const string &name) {
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		// entry found
		return entry->second.get();
	}
	return nullptr;
}

vector<reference<CatalogEntry>> QuackCatalogSet::GetAllCatalogEntries() {
	lock_guard<mutex> l(entry_lock);
	vector<reference<CatalogEntry>> result;
	for (auto &entry : entries) {
		result.push_back(*entry.second);
	}
	return result;
}

void QuackCatalogSet::DropEntry(const string &entry_name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(entry_name);
}

void QuackCatalogSet::Clear() {
	lock_guard<mutex> l(entry_lock);
	entries.clear();
}

optional_ptr<CatalogEntry> QuackCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry, OnCreateConflict on_conflict) {
	lock_guard<mutex> l(entry_lock);
	auto &entry_name = entry->name;
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		entries[entry_name] = std::move(entry);
		return entries[entry_name].get();
	} else {
		auto inserted_entry = entries.insert(make_pair(entry_name, std::move(entry)));
		return inserted_entry.first->second.get();
	}
}

} // namespace duckdb
