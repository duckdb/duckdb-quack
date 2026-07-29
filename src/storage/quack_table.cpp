#include "storage/quack_catalog.hpp"
#include "storage/quack_table.hpp"
#include "quack_scan.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "storage/quack_view.hpp"

namespace duckdb {

unique_ptr<CreateInfo> ParseCreateTable(const string &sql) {
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw BinderException(
		    "Failed to parse CREATE TABLE from remote catalog SQL - \"%s\" - expected a single CREATE statement",
		    sql);
	}
	auto &create = parser.statements[0]->Cast<CreateStatement>();
	return std::move(create.info);
}

//! Client-side catalog entries for attached quack tables only need name + type for
//! local planning. Computed DEFAULTs (e.g. `1+0`, `current_timestamp`) fail
//! BindCreateTableInfo on the client and abort the entire ATTACH with a misleading
//! "Catalog \"…\" does not exist" (https://github.com/duckdb/duckdb-quack/issues/132).
//! Postgres/Snowflake attaches similarly expose remote columns without rebinding
//! server-side DEFAULT expressions into the client catalog. Real DEFAULTs remain
//! on the server; INSERT DEFAULT VALUES is executed remotely.
static void StripColumnDefaultsForClientCatalog(CreateTableInfo &info) {
	for (idx_t i = 0; i < info.columns.LogicalColumnCount(); i++) {
		auto &col = info.columns.GetColumnMutable(LogicalIndex(i));
		if (col.HasDefaultValue()) {
			col.SetDefaultValue(nullptr);
		}
	}
}

QuackTableSet::QuackTableSet(ClientContext &context, QuackSchemaCatalogEntry &parent,
                             const QuackLoadCatalogData &load_data)
    : QuackCatalogSet(parent.ParentCatalog().Cast<QuackCatalog>()), schema(parent) {
	for (auto &row : load_data.tables->Rows()) {
		auto schema_name = row.GetValue(0).GetValue<string>();
		if (schema_name != parent.name) {
			// does not belong to this schema
			continue;
		}
		// parse the SQL to get the table definition
		auto type = row.GetValue(2).GetValue<string>();
		unique_ptr<CatalogEntry> entry;
		if (type == "table") {
			auto sql = row.GetValue(1).GetValue<string>();
			auto info = ParseCreateTable(sql);
			if (info->type != CatalogType::TABLE_ENTRY) {
				throw InternalException("Expected a CREATE TABLE");
			}
			auto &create_table = info->Cast<CreateTableInfo>();
			StripColumnDefaultsForClientCatalog(create_table);
			// bind to resolve the types
			auto binder = Binder::CreateBinder(context);
			unique_ptr<BoundCreateTableInfo> bound_info;
			try {
				bound_info = binder->BindCreateTableInfo(std::move(info), schema);
			} catch (std::exception &ex) {
				throw BinderException(
				    "Failed to bind remote table while attaching quack catalog \"%s\" (schema \"%s\"): %s\nSQL: %s",
				    catalog.GetName(), schema.name, ex.what(), sql);
			}
			auto table = make_uniq<QuackTableCatalogEntry>(catalog, parent, bound_info->Base());
			entry = std::move(table);
		} else {
			auto view_name = row.GetValue(1).GetValue<string>();
			// bind a remote procedure call to the view on the server side
			// we don't actually care what the view contains server-side, we just treat it like an opaque object we can
			// query
			CreateViewInfo info(schema, view_name);
			info.sql = QuackViewCatalogEntry::CreateViewSQL(catalog.GetName(), schema.name, view_name);
			info.query = CreateViewInfo::ParseSelect(info.sql);

			// bind to resolve the types
			auto view = make_uniq<QuackViewCatalogEntry>(catalog, parent, info);
			entry = std::move(view);
		}
		CreateEntry(std::move(entry), OnCreateConflict::REPLACE_ON_CONFLICT);
	}
}

QuackTableSet::QuackTableSet(QuackSchemaCatalogEntry &parent)
    : QuackCatalogSet(parent.ParentCatalog().Cast<QuackCatalog>()), schema(parent) {
}

string QuackTableSet::GetLoadQuery() {
	return R"(
SELECT schema_name, sql, 'table'
FROM duckdb_tables()
UNION ALL
SELECT schema_name, view_name, 'view'
FROM duckdb_views()
	)";
}

TableFunction QuackTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data_p) {
	auto &quack_catalog = catalog.Cast<QuackCatalog>();
	auto bind_data = make_uniq<QuackScanBindData>();
	bind_data->client_connection = quack_catalog.GetClientConnection();
	bind_data->table_name = name;
	for (auto &col : GetColumns().Physical()) {
		bind_data->column_names.push_back(col.Name());
		bind_data->column_types.push_back(col.Type());
	}
	bind_data_p = std::move(bind_data);
	return QuackScanFunction::GetFunction();
}

unique_ptr<BaseStatistics> QuackTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	throw NotImplementedException("GetStatistics not implemented yet");
}

TableStorageInfo QuackTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	throw NotImplementedException("GetStorageInfo not implemented yet");
}

} // namespace duckdb
