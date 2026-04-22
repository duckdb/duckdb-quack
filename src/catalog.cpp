#include "catalog.hpp"

#include "rpc_scan_function.hpp"
#include "rpc_bind_data.hpp"
#include "rpc_insert.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

// FIXME bunch of stuff copied from postgres scanner, can probably be simplified!

using namespace duckdb;

RpcTransaction::RpcTransaction(RpcCatalog &rpc_catalog_p, TransactionManager &manager_p, ClientContext &context_p)
    : Transaction(manager_p, context_p), rpc_catalog(rpc_catalog_p) {
}

RpcClient &RpcTransaction::GetClient() {
	if (transaction_state == RpcTransactionState::TRANSACTION_FINISHED) {
		throw InternalException("Cannot use RPC transaction after commit/rollback");
	}
	if (transaction_state == RpcTransactionState::TRANSACTION_NOT_YET_STARTED) {
		rpc_catalog.ExecuteCommand("BEGIN TRANSACTION");
		transaction_state = RpcTransactionState::TRANSACTION_STARTED;
	}
	return rpc_catalog.GetRawClient();
}

void RpcTransaction::Commit() {
	if (transaction_state == RpcTransactionState::TRANSACTION_STARTED) {
		transaction_state = RpcTransactionState::TRANSACTION_FINISHED;
		rpc_catalog.ExecuteCommand("COMMIT");
	}
}

void RpcTransaction::Rollback() {
	if (transaction_state == RpcTransactionState::TRANSACTION_STARTED) {
		transaction_state = RpcTransactionState::TRANSACTION_FINISHED;
		rpc_catalog.ExecuteCommand("ROLLBACK");
	}
}

RpcTransaction &RpcTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<RpcTransaction>();
}

RpcTransaction &RpcTransaction::Get(CatalogTransaction &transaction) {
	D_ASSERT(transaction.transaction);
	return transaction.transaction->Cast<RpcTransaction>();
}

RpcTransaction::~RpcTransaction() {
}

RpcTransactionManager::RpcTransactionManager(AttachedDatabase &db_p, RpcCatalog &rpc_catalog_p)
    : TransactionManager(db_p), rpc_catalog(rpc_catalog_p) {
}

Transaction &RpcTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<RpcTransaction>(rpc_catalog, *this, context);
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}
ErrorData RpcTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &rpc_transaction = transaction.Cast<RpcTransaction>();
	rpc_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void RpcTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &rpc_transaction = transaction.Cast<RpcTransaction>();
	rpc_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void RpcTransactionManager::Checkpoint(ClientContext &context, bool force) {
	throw NotImplementedException("Checkpoint not implemented yet");
}

RpcCatalog::RpcCatalog(AttachedDatabase &db_p, const RpcUri &server_uri_p, ClientContext &context)
    : Catalog(db_p), server_uri(server_uri_p), client(RpcClient::GetClient(server_uri)), client_context(context) {
	client->SetContext(&context);

	// Resolve auth token: prefer a quack secret scoped to this URI; fall back to the
	// global rpc_default_token setting.
	string token;
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, server_uri.Uri(), "quack");
	if (match.HasMatch()) {
		const auto &kv = dynamic_cast<const KeyValueSecret &>(*match.secret_entry->secret);
		token = kv.TryGetValue("token", true).ToString();
	} else {
		Value default_token_val;
		auto &config = DBConfig::GetConfig(db_p.GetDatabase());
		auto lookup_result_token = config.TryGetCurrentSetting("rpc_default_token", default_token_val);
		D_ASSERT(lookup_result_token);
		if (!default_token_val.IsNull()) {
			token = default_token_val.GetValue<string>();
		}
	}

	auto connection_response = client->Request<ConnectionResponseMessage>(make_uniq<ConnectionRequestMessage>(token));
	connection_id = connection_response->ConnectionId();

	// TODO a tiiny bit clunky this
	auto schemata = ExecuteCommand("FROM information_schema.schemata SELECT catalog_name, schema_name WHERE "
	                               "catalog_name NOT IN ('system', 'temp') ORDER BY ALL");
	auto row_collection = make_uniq<ColumnDataRowCollection>(schemata->GetRows());
	for (idx_t row_idx = 0; row_idx < row_collection->size(); row_idx++) {
		CreateSchemaInfo info;
		info.catalog = row_collection->GetValue(0, row_idx).GetValue<string>();
		info.schema = row_collection->GetValue(1, row_idx).GetValue<string>();
		// TODO this will fail if there are two schemas with the same name in different catalogs :/
		schemas[info.schema] = make_uniq<RpcSchemaCatalogEntry>(*this, info);
	}
	for (auto &schema : schemas) {
		schema.second->LoadTableCache();
	}
}

RpcCatalog::~RpcCatalog() {
}

void RpcCatalog::Initialize(bool load_builtin) {
}

optional_ptr<SchemaCatalogEntry> RpcCatalog::LookupSchema(CatalogTransaction transaction,
                                                          const EntryLookupInfo &schema_lookup,
                                                          OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	if (schemas.find(schema_name) == schemas.end()) {
		switch (if_not_found) {
		case OnEntryNotFound::THROW_EXCEPTION:
			throw BinderException("Schema with name \"%s\" not found", schema_name);
		case OnEntryNotFound::RETURN_NULL:
			return nullptr;
		}
	}
	return schemas[schema_name].get();
}

const RpcUri &RpcCatalog::GetServerUri() {
	return server_uri;
}

unique_ptr<ColumnDataCollection> RpcCatalog::ExecuteCommand(const string &query) {
	// FIXME this will break with many results!
	auto chunk_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
	auto response =
	    client->Request<PrepareResponseMessage>(make_uniq<PrepareRequestMessage>(connection_id, query, true));
	chunk_collection->Initialize(response->Types());
	for (auto &chunk : response->MutableChunks()) {
		chunk_collection->Append(*chunk);
	}
	return chunk_collection;
}

RpcClient &RpcCatalog::GetRawClient() {
	return *client;
}

const string &RpcCatalog::GetConnectionId() {
	return connection_id;
}

ClientContext &RpcCatalog::GetContext() {
	return client_context;
}

optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::LookupEntry(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup_info) {
	auto &schema_create_info = GetInfo()->Cast<RpcSchemaInfo>();

	CreateTableInfo create_info(*this, lookup_info.GetEntryName());
	auto &rpc_catalog = catalog.Cast<RpcCatalog>();
	auto catalog_type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	if (catalog_type == CatalogType::TABLE_FUNCTION_ENTRY) {
		auto entry = TryLoadBuiltInFunction(entry_name);
		if (entry) {
			return entry;
		}
	}

	try {
		auto bind_response =
		    rpc_catalog.GetRawClient().Request<PrepareResponseMessage>(make_uniq<PrepareRequestMessage>(
		        rpc_catalog.GetConnectionId(), StringUtil::Format("FROM %s WHERE FALSE", lookup_info.GetEntryName()),
		        true));
		for (idx_t i = 0; i < bind_response->Types().size(); i++) {
			create_info.columns.AddColumn(ColumnDefinition(bind_response->Names()[i], bind_response->Types()[i]));
		}
		return new RpcTableCatalogEntry(catalog, *this, create_info);
	} catch (IOException &ex) { // FIXME this should not be a catch on IOError
		return nullptr;
	}
}

TableFunction RpcTableCatalogEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data_p) {
	auto &rpc_catalog = catalog.Cast<RpcCatalog>();
	auto bind_data = make_uniq<RpcBindData>();
	bind_data->server_uri = rpc_catalog.GetServerUri();
	bind_data->connection_id = rpc_catalog.GetConnectionId();
	bind_data->table_name = name;
	for (auto &col : GetColumns().Physical()) {
		bind_data->column_names.push_back(col.Name());
		bind_data->column_types.push_back(col.Type());
	}
	bind_data_p = std::move(bind_data);
	return RpcScanFunction::GetFunction();
}

optional_ptr<CatalogEntry> RpcCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &txn = RpcTransaction::Get(transaction);
	auto create_schema_info = info.Copy();
	create_schema_info->catalog = "memory";

	auto catalog_request_message = make_uniq<CatalogRequestMessage>(GetConnectionId(), std::move(create_schema_info));
	auto catalog_response = txn.GetClient().Request<CatalogResponseMessage>(std::move(catalog_request_message));

	auto response_parse_info = catalog_response->GetParseInfo();
	auto &response_info = response_parse_info->Cast<CreateSchemaInfo>();
	auto entry = make_uniq<RpcSchemaCatalogEntry>(*this, response_info);
	auto result = entry.get();
	schemas[response_info.schema] = std::move(entry);
	return result;
}

void RpcCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	for (auto &schema : schemas) {
		callback(*schema.second);
	}
}

PhysicalOperator &RpcCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("PlanDelete not implemented yet");
}
PhysicalOperator &RpcCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
	throw NotImplementedException("PlanUpdate not implemented yet");
}

unique_ptr<LogicalOperator> RpcCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                        unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not implemented yet");
}

DatabaseSize RpcCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not implemented yet");
}

//! Whether or not this is an in-memory SQLite database
bool RpcCatalog::InMemory() {
	throw NotImplementedException("InMemory not implemented yet");
}
string RpcCatalog::GetDBPath() {
	throw NotImplementedException("GetDBPath not implemented yet");
}

void RpcCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO should we just send over the drop info in a dropmessage???
	throw NotImplementedException("DropSchema not implemented yet");
}

void RpcSchemaCatalogEntry::AddToTableCache(const string &table_name, unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> guard(table_cache_lock);
	table_cache[table_name] = std::move(entry);
}

void RpcSchemaCatalogEntry::RemoveFromTableCache(const string &table_name) {
	lock_guard<mutex> guard(table_cache_lock);
	table_cache.erase(table_name);
}

void RpcSchemaCatalogEntry::LoadTableCache() {
	{
		lock_guard<mutex> guard(table_cache_lock);
		if (table_cache_loaded) {
			return;
		}
	}

	auto &rpc_catalog = catalog.Cast<RpcCatalog>();
	auto &context = rpc_catalog.GetContext();

	auto query = StringUtil::Format("SELECT table_name, column_name, data_type "
	                                "FROM duckdb_columns() "
	                                "WHERE schema_name = %s AND NOT internal "
	                                "ORDER BY table_name, column_index",
	                                KeywordHelper::WriteQuoted(name, '\''));
	auto result = rpc_catalog.ExecuteCommand(query);
	auto rows = result->GetRows();

	case_insensitive_map_t<unique_ptr<CatalogEntry>> new_entries;
	string current_table;
	unique_ptr<CreateTableInfo> current_info;

	for (idx_t i = 0; i < rows.size(); i++) {
		auto table_name = rows.GetValue(0, i).GetValue<string>();
		auto col_name = rows.GetValue(1, i).GetValue<string>();
		auto type_str = rows.GetValue(2, i).GetValue<string>();

		if (table_name != current_table) {
			if (current_info) {
				new_entries[current_table] = make_uniq<RpcTableCatalogEntry>(catalog, *this, *current_info);
			}
			current_table = table_name;
			current_info = make_uniq<CreateTableInfo>(*this, table_name);
		}
		current_info->columns.AddColumn(
		    ColumnDefinition(col_name, TransformStringToLogicalType(type_str, context)));
	}
	if (current_info) {
		new_entries[current_table] = make_uniq<RpcTableCatalogEntry>(catalog, *this, *current_info);
	}

	lock_guard<mutex> guard(table_cache_lock);
	if (table_cache_loaded) {
		return;
	}
	table_cache = std::move(new_entries);
	table_cache_loaded = true;
}

void RpcSchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                                 const std::function<void(CatalogEntry &)> &callback) {
	Scan(type, callback);
}

void RpcSchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	LoadTableCache();
	lock_guard<mutex> guard(table_cache_lock);
	for (auto &entry : table_cache) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                              TableCatalogEntry &table) {
	throw NotImplementedException("CreateIndex not implemented yet");
}
optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateFunction(CatalogTransaction transaction,
                                                                 CreateFunctionInfo &info) {
	throw NotImplementedException("CreateFunction not implemented yet");
}

optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateTable(CatalogTransaction transaction,
                                                              BoundCreateTableInfo &info) {
	auto &txn = RpcTransaction::Get(transaction);
	auto &rpc_catalog = catalog.Cast<RpcCatalog>();

	auto create_table_info = info.Base().Copy();
	create_table_info->catalog = GetInfo()->catalog;
	create_table_info->schema = GetInfo()->schema;

	auto catalog_request_message =
	    make_uniq<CatalogRequestMessage>(rpc_catalog.GetConnectionId(), std::move(create_table_info));
	auto catalog_response =
	    txn.GetClient().Request<CatalogResponseMessage>(std::move(catalog_request_message));

	auto entry = make_uniq<RpcTableCatalogEntry>(catalog, *this,
	                                             catalog_response->GetParseInfo()->Cast<CreateTableInfo>());
	auto result = entry.get();
	AddToTableCache(info.Base().table, std::move(entry));
	return result;
}

optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("CreateView not implemented yet");
}
optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateSequence(CatalogTransaction transaction,
                                                                 CreateSequenceInfo &info) {
	throw NotImplementedException("CreateSequence not implemented yet");
}
optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                      CreateTableFunctionInfo &info) {
	throw NotImplementedException("CreateTableFunction not implemented yet");
}
optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                     CreateCopyFunctionInfo &info) {
	throw NotImplementedException("CreateCopyFunction not implemented yet");
}
optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                       CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("CreatePragmaFunction not implemented yet");
}
optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateCollation(CatalogTransaction transaction,
                                                                  CreateCollationInfo &info) {
	throw NotImplementedException("CreateCollation not implemented yet");
}

optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("CreateType not implemented yet");
}

void RpcSchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo &info_p) {
	auto &txn = RpcTransaction::Get(context, catalog);
	auto &rpc_catalog = catalog.Cast<RpcCatalog>();
	auto drop_info = info_p.Copy();
	drop_info->catalog = GetInfo()->catalog;
	drop_info->schema = GetInfo()->schema;

	auto catalog_request_message =
	    make_uniq<CatalogRequestMessage>(rpc_catalog.GetConnectionId(), std::move(drop_info));

	txn.GetClient().Request<CatalogResponseMessage>(std::move(catalog_request_message));
	RemoveFromTableCache(info_p.name);
}
void RpcSchemaCatalogEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Alter not implemented yet, Alter!");
}

unique_ptr<BaseStatistics> RpcTableCatalogEntry::GetStatistics(ClientContext &context, column_t column_id) {
	throw NotImplementedException("GetStatistics not implemented yet");
}

TableStorageInfo RpcTableCatalogEntry::GetStorageInfo(ClientContext &context) {
	// Minimal info: cardinality is unknown, and we have no local index/column segment metadata
	// to report. Returning a default-constructed value is enough for SHOW TABLES / duckdb_tables().
	return TableStorageInfo();
}

// clang-format off
static const DefaultTableMacro rpc_table_macros[] = {
	{DEFAULT_SCHEMA, "call", {"remote_sql_query", nullptr}, {{nullptr, nullptr}},  "FROM rpc_call_by_name({CATALOG}, remote_sql_query)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

// 'borrowed' from ducklake
optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::LoadBuiltInFunction(DefaultTableMacro macro) {
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

optional_ptr<CatalogEntry> RpcSchemaCatalogEntry::TryLoadBuiltInFunction(const string &entry_name) {
	lock_guard<mutex> guard(default_function_lock);
	auto entry = default_function_map.find(entry_name);
	if (entry != default_function_map.end()) {
		return entry->second.get();
	}
	for (idx_t index = 0; rpc_table_macros[index].name != nullptr; index++) {
		if (StringUtil::CIEquals(rpc_table_macros[index].name, entry_name)) {
			return LoadBuiltInFunction(rpc_table_macros[index]);
		}
	}
	return nullptr;
}
