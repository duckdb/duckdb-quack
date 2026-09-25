#include "storage/quack_catalog.hpp"
#include "storage/quack_schema.hpp"
#include "storage/quack_table.hpp"
#include "quack_client.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/common/types/value.hpp"
#include "storage/quack_transaction.hpp"
#include "storage/quack_view.hpp"

namespace duckdb {

QuackSchemaSet::QuackSchemaSet(ClientContext &context, QuackCatalog &catalog, const QuackLoadCatalogData &load_data)
    : QuackCatalogSet(catalog) {
	Reload(context, catalog, load_data);
}

//! Build the CreateSchemaInfo for a schema entry. The parent chain is tracked through the entry itself, so the
//! info only carries the schema's own name
static CreateSchemaInfo MakeSchemaInfo(const Identifier &schema_name) {
	CreateSchemaInfo info;
	info.SetQualifiedName(QualifiedName({schema_name}, Identifier()));
	return info;
}

void QuackSchemaSet::RegisterSchema(ClientContext &context, QuackCatalog &catalog, const vector<Identifier> &local_path,
                                    int64_t remote_oid, const QuackLoadCatalogData &load_data) {
	// navigate to the set the schema belongs in
	reference<QuackCatalogSet> target_set = *this;
	optional_ptr<QuackSchemaCatalogEntry> parent;
	for (idx_t i = 0; i + 1 < local_path.size(); i++) {
		auto entry = target_set.get().GetEntry(local_path[i].GetIdentifierName());
		if (!entry) {
			// a level of the path the server has no schema for: the stand-in entry for a remote catalog
			auto info = MakeSchemaInfo(local_path[i]);
			auto stand_in = make_uniq<QuackSchemaCatalogEntry>(catalog, info, parent.get());
			entry = target_set.get().CreateEntry(std::move(stand_in), OnCreateConflict::REPLACE_ON_CONFLICT);
		}
		parent = &entry->Cast<QuackSchemaCatalogEntry>();
		target_set = parent->Schemas();
	}
	auto &name = local_path.back();
	if (target_set.get().GetEntry(name.GetIdentifierName())) {
		// the name is already taken - keep the schema that was registered first (the schemas of the server's
		// default catalog are registered before those of any other catalog) rather than replacing it and
		// losing the tables it holds. The shadowed schema stays reachable through the query() table function
		return;
	}
	auto info = MakeSchemaInfo(name);
	auto schema = make_uniq<QuackSchemaCatalogEntry>(context, catalog, info, parent.get(), remote_oid, load_data);
	target_set.get().CreateEntry(std::move(schema), OnCreateConflict::REPLACE_ON_CONFLICT);
}

void QuackSchemaSet::Reload(ClientContext &context, QuackCatalog &catalog, const QuackLoadCatalogData &load_data) {
	Clear();
	for (auto &row : load_data.schemas->Rows()) {
		auto remote_oid = row.GetValue(0).GetValue<int64_t>();
		auto database_name = row.GetValue(1).GetValue<string>();
		auto remote_path = ListValue::GetChildren(row.GetValue(2));
		auto default_catalog = row.GetValue(3).GetValue<string>();

		vector<Identifier> local_path;
		if (!StringUtil::CIEquals(database_name, default_catalog)) {
			// a remote catalog other than the server's default one: expose the catalog itself as a schema that
			// holds the catalog's schemas, so that schemas of the same name in different catalogs stay distinct
			local_path.emplace_back(database_name);
		}
		for (auto &part : remote_path) {
			local_path.emplace_back(part.GetValue<string>());
		}
		RegisterSchema(context, catalog, local_path, remote_oid, load_data);
	}
}

string QuackSchemaSet::GetLoadQuery() {
	// walk the (possibly nested) schema tree of every catalog the server has attached, so that each schema is
	// reported with its full path - e.g. a schema "child" nested in "s1" comes back as ['s1', 'child']
	return R"(
WITH RECURSIVE schema_tree AS (
	SELECT oid, database_name, [schema_name] AS schema_path
	FROM duckdb_schemas()
	WHERE parent_schema_oid IS NULL
	UNION ALL
	SELECT nested.oid, nested.database_name, list_append(parent.schema_path, nested.schema_name)
	FROM duckdb_schemas() nested
	JOIN schema_tree parent ON nested.parent_schema_oid = parent.oid
)
SELECT oid, database_name, schema_path, current_database()
FROM schema_tree
WHERE database_name NOT IN ('system', 'temp')
ORDER BY (database_name = current_database()) DESC, database_name, length(schema_path), schema_path
	)";
}

QuackSchemaCatalogEntry::QuackSchemaCatalogEntry(Catalog &catalog_p, CreateSchemaInfo &info_p,
                                                 optional_ptr<SchemaCatalogEntry> parent_schema_p, int64_t remote_oid_p)
    : SchemaCatalogEntry(catalog_p, info_p), parent_schema(parent_schema_p), remote_oid(remote_oid_p) {
	schemas = make_uniq<QuackCatalogSet>(catalog_p.Cast<QuackCatalog>());
	tables = make_uniq<QuackTableSet>(*this);
}

QuackSchemaCatalogEntry::QuackSchemaCatalogEntry(ClientContext &context, Catalog &catalog_p, CreateSchemaInfo &info_p,
                                                 optional_ptr<SchemaCatalogEntry> parent_schema_p, int64_t remote_oid_p,
                                                 const QuackLoadCatalogData &load_data)
    : SchemaCatalogEntry(catalog_p, info_p), parent_schema(parent_schema_p), remote_oid(remote_oid_p) {
	schemas = make_uniq<QuackCatalogSet>(catalog_p.Cast<QuackCatalog>());
	tables = make_uniq<QuackTableSet>(context, *this, load_data);
}

QuackSchemaCatalogEntry::~QuackSchemaCatalogEntry() {
}

unique_ptr<CreateInfo> QuackSchemaCatalogEntry::GetInfo() const {
	auto result = make_uniq<CreateSchemaInfo>();
	// a nested schema roots its path at the catalog so the full path can be navigated: [catalog, parents..., name]
	// (a top-level schema keeps the plain [name] form)
	auto path = GetSchemaPath();
	if (path.size() > 1) {
		path.insert(path.begin(), catalog.GetName());
	}
	result->SetQualifiedName(QualifiedName(std::move(path), Identifier()));
	result->comment = comment;
	result->tags = tags;
	return std::move(result);
}

QualifiedName QuackSchemaCatalogEntry::GetRemoteName(const Identifier &entry_name) const {
	return QualifiedName(GetSchemaPath(), entry_name);
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                const EntryLookupInfo &lookup_info) {
	auto catalog_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	switch (catalog_type) {
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return TryLoadBuiltInFunction(entry_name);
	case CatalogType::SCHEMA_ENTRY:
		return schemas->GetEntry(entry_name);
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables->GetEntry(entry_name);
	default:
		return nullptr;
	}
}

void QuackSchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                                   const std::function<void(CatalogEntry &)> &callback) {
	Scan(type, callback);
}

void QuackSchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	switch (type) {
	case CatalogType::SCHEMA_ENTRY:
		for (auto &entry : schemas->GetAllCatalogEntries()) {
			callback(entry.get());
		}
		break;
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		// tables and views share a set - only report the entries of the requested type
		for (auto &entry : tables->GetAllCatalogEntries()) {
			if (entry.get().type == type) {
				callback(entry.get());
			}
		}
		break;
	default:
		break;
	}
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                                TableCatalogEntry &table) {
	throw NotImplementedException("CreateIndex not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateFunction(CatalogTransaction transaction,
                                                                   CreateFunctionInfo &info) {
	throw NotImplementedException("CreateFunction not implemented yet");
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateTable(CatalogTransaction transaction,
                                                                BoundCreateTableInfo &info) {
	auto create_table_info = info.Base().Copy();
	create_table_info->SetQualifiedName(GetRemoteName(create_table_info->GetQualifiedName().Name()));

	auto &quack_transaction = QuackTransaction::Get(transaction);
	quack_transaction.Query(create_table_info->ToString());
	auto quack_entry = make_uniq<QuackTableCatalogEntry>(catalog, *this, create_table_info->Cast<CreateTableInfo>());
	return tables->CreateEntry(std::move(quack_entry), info.Base().on_conflict);
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	auto create_view_info = info.Copy();
	auto remote_name = GetRemoteName(create_view_info->GetQualifiedName().Name());
	create_view_info->SetQualifiedName(remote_name);

	// create the view verbatim in the serer
	auto &quack_transaction = QuackTransaction::Get(transaction);
	quack_transaction.Query(create_view_info->ToString());

	// locally, override the query with a remote procedure call to ensure the view is evaluated remotely
	info.sql = QuackViewCatalogEntry::CreateViewSQL(ParentCatalog().GetName().GetIdentifierName(), remote_name);
	info.query = CreateViewInfo::ParseSelect(info.sql);

	auto quack_entry = make_uniq<QuackViewCatalogEntry>(catalog, *this, info);
	return tables->CreateEntry(std::move(quack_entry), info.on_conflict);
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateSequence(CatalogTransaction transaction,
                                                                   CreateSequenceInfo &info) {
	throw NotImplementedException("CreateSequence not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                        CreateTableFunctionInfo &info) {
	throw NotImplementedException("CreateTableFunction not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                       CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CreateCopyFunction not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                         CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CreatePragmaFunction not implemented yet");
}
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateCollation(CatalogTransaction transaction,
                                                                    CreateCollationInfo &info) {
	throw NotImplementedException("CreateCollation not implemented yet");
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("CreateType not implemented yet");
}

void QuackSchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo &info_p) {
	auto drop_info = info_p.Copy();
	drop_info->SetQualifiedName(GetRemoteName(info_p.GetQualifiedName().Name()));
	switch (drop_info->type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		break;
	default:
		throw NotImplementedException("Drop not supported yet for this entry");
	}
	auto &transaction = QuackTransaction::Get(context, ParentCatalog());
	transaction.Query(drop_info->ToString());
	tables->DropEntry(info_p.GetQualifiedName().Name().GetIdentifierName());
}
void QuackSchemaCatalogEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Alter not implemented yet, Alter!");
}

// clang-format off
static const DefaultTableMacro quack_table_macros[] = {
	{DEFAULT_SCHEMA, "query", {"remote_sql_query", nullptr}, {{nullptr, nullptr}},  "FROM quack_query_by_name({CATALOG}, remote_sql_query)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

// 'borrowed' from ducklake
optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::LoadBuiltInFunction(DefaultTableMacro macro) {
	string macro_def = macro.macro;
	macro_def = StringUtil::Replace(macro_def, "{CATALOG}",
	                                KeywordHelper::WriteQuoted(catalog.GetName().GetIdentifierName(), '\''));
	macro_def = StringUtil::Replace(macro_def, "{SCHEMA}", KeywordHelper::WriteQuoted(name.GetIdentifierName(), '\''));
	macro.macro = macro_def.c_str();
	auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(macro);
	auto table_macro =
	    make_uniq_base<CatalogEntry, TableMacroCatalogEntry>(catalog, *this, info->Cast<CreateMacroInfo>());
	auto result = table_macro.get();
	default_function_map.emplace(macro.name, std::move(table_macro));
	return result;
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::TryLoadBuiltInFunction(const string &entry_name) {
	lock_guard<mutex> guard(default_function_lock);
	auto entry = default_function_map.find(entry_name);
	if (entry != default_function_map.end()) {
		return entry->second.get();
	}
	for (idx_t index = 0; quack_table_macros[index].name != nullptr; index++) {
		if (StringUtil::CIEquals(quack_table_macros[index].name, entry_name)) {
			return LoadBuiltInFunction(quack_table_macros[index]);
		}
	}
	return nullptr;
}

} // namespace duckdb
