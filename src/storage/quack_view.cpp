#include "storage/quack_view.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

QuackViewCatalogEntry::QuackViewCatalogEntry(Catalog &catalog_p, SchemaCatalogEntry &schema_p, CreateViewInfo &info_p)
    : ViewCatalogEntry(catalog_p, schema_p, info_p) {
}

string QuackViewCatalogEntry::CreateViewSQL(const string &catalog_name, const QualifiedName &remote_view_name) {
	//! This SQL will always be "FROM quack_query({catalog}, 'FROM {remote_view_name}');"
	auto remote_sql = StringUtil::Format("FROM %s", remote_view_name.ToString());
	return StringUtil::Format("FROM quack_query_by_name(%s, %s)", SQLString(catalog_name), SQLString(remote_sql));
}

} // namespace duckdb
