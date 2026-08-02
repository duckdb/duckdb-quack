#include "storage/quack_view.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

QuackViewCatalogEntry::QuackViewCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateViewInfo &info_p)
    : ViewCatalogEntry(catalog_p, schema_p, info_p) {
}

string QuackViewCatalogEntry::CreateViewSQL(const string &catalog_name, const string &schema_name,
                                            const string &view_name, const string &remote_catalog) {
	//! This SQL will always be "FROM quack_query({catalog}, 'FROM {view_name}');"
	auto remote_sql = StringUtil::Format("FROM %s.%s", SQLIdentifier(schema_name), SQLIdentifier(view_name));
	if (!remote_catalog.empty()) {
		remote_sql = StringUtil::Format("FROM %s.%s.%s", SQLIdentifier(remote_catalog), SQLIdentifier(schema_name),
		                                SQLIdentifier(view_name));
	}
	return StringUtil::Format("FROM quack_query_by_name(%s, %s)", SQLString(catalog_name), SQLString(remote_sql));
}

} // namespace duckdb
