#include "storage/quack_catalog.hpp"
#include "storage/quack_schema.hpp"
#include "storage/quack_table.hpp"
#include "quack_client.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "storage/quack_transaction.hpp"
#include "storage/quack_view.hpp"

namespace duckdb {

QuackSchemaSet::QuackSchemaSet(ClientContext &context, QuackCatalog &catalog, const QuackLoadCatalogData &load_data)
    : QuackCatalogSet(catalog) {
	Reload(context, catalog, load_data);
}

void QuackSchemaSet::Reload(ClientContext &context, QuackCatalog &catalog, const QuackLoadCatalogData &load_data) {
	Clear();
	for (auto &row : load_data.schemas->Rows()) {
		CreateSchemaInfo info;
		info.catalog = row.GetValue(0).GetValue<string>();
		info.schema = row.GetValue(1).GetValue<string>();
		// TODO this will fail if there are two schemas with the same name in different catalogs :/
		auto schema = make_uniq<QuackSchemaCatalogEntry>(context, catalog, info, load_data);
		CreateEntry(std::move(schema), OnCreateConflict::REPLACE_ON_CONFLICT);
	}
}

string QuackSchemaSet::GetLoadQuery() {
	return R"(
SELECT catalog_name, schema_name
FROM information_schema.schemata
WHERE catalog_name NOT IN ('system', 'temp')
ORDER BY ALL
	)";
}

QuackSchemaCatalogEntry::QuackSchemaCatalogEntry(Catalog &catalog_p, CreateSchemaInfo &info_p)
    : SchemaCatalogEntry(catalog_p, info_p) {
	tables = make_uniq<QuackTableSet>(*this);
}

QuackSchemaCatalogEntry::QuackSchemaCatalogEntry(ClientContext &context, Catalog &catalog_p, CreateSchemaInfo &info_p,
                                                 const QuackLoadCatalogData &load_data)
    : SchemaCatalogEntry(catalog_p, info_p) {
	tables = make_uniq<QuackTableSet>(context, *this, load_data);
}

QuackSchemaCatalogEntry::~QuackSchemaCatalogEntry() {
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                                const EntryLookupInfo &lookup_info) {
	auto catalog_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	switch (catalog_type) {
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return TryLoadBuiltInFunction(entry_name);
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return tables->GetEntry(lookup_info.GetEntryName());
	default:
		return nullptr;
	}
}

void QuackSchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                                   const std::function<void(CatalogEntry &)> &callback) {
	// TODO
}
void QuackSchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// TODO
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
	create_table_info->catalog = GetInfo()->catalog;
	create_table_info->schema = GetInfo()->schema;

	auto &quack_transaction = QuackTransaction::Get(transaction);
	quack_transaction.Query(create_table_info->ToString());
	auto quack_entry = make_uniq<QuackTableCatalogEntry>(catalog, *this, create_table_info->Cast<CreateTableInfo>());
	return tables->CreateEntry(std::move(quack_entry), info.Base().on_conflict);
}

optional_ptr<CatalogEntry> QuackSchemaCatalogEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	auto create_view_info = info.Copy();
	create_view_info->catalog = GetInfo()->catalog;
	create_view_info->schema = GetInfo()->schema;

	// create the view verbatim in the serer
	auto &quack_transaction = QuackTransaction::Get(transaction);
	quack_transaction.Query(create_view_info->ToString());

	// locally, override the query with a remote procedure call to ensure the view is evaluated remotely
	info.sql = QuackViewCatalogEntry::CreateViewSQL(ParentCatalog().GetName(), name, info.view_name);
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
	drop_info->catalog = GetInfo()->catalog;
	drop_info->schema = name;
	switch (drop_info->type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		break;
	default:
		throw NotImplementedException("Drop not supported yet for this entry");
	}
	auto &transaction = QuackTransaction::Get(context, ParentCatalog());
	transaction.Query(drop_info->ToString());
	tables->DropEntry(info_p.name);
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
	macro_def = StringUtil::Replace(macro_def, "{CATALOG}", KeywordHelper::WriteQuoted(catalog.GetName(), '\''));
	macro_def = StringUtil::Replace(macro_def, "{SCHEMA}", KeywordHelper::WriteQuoted(name, '\''));
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
