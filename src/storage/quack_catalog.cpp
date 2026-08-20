#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "storage/quack_catalog.hpp"
#include "storage/quack_table.hpp"
#include "quack_scan.hpp"
#include "storage/quack_insert.hpp"
#include "quack_message.hpp"
#include "quack_client.hpp"
#include "storage/quack_transaction.hpp"

// FIXME bunch of stuff copied from postgres scanner, can probably be simplified!

namespace duckdb {

QuackCatalog::QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri, ClientContext &context,
                           const string &token, string client_id, idx_t heartbeat_timeout_seconds)
    : Catalog(db_p) {
	// connect to the server
	client_connection =
	    QuackClient::ConnectToServer(context, server_uri, token, std::move(client_id), heartbeat_timeout_seconds);

	// load the entire catalog up-front
	auto load_info = LoadCatalog(context);
	schemas = make_uniq<QuackSchemaSet>(context, *this, load_info);
}

QuackLoadCatalogData QuackCatalog::LoadCatalog(ClientContext &context) {
	QuackLoadCatalogData result;
	result.schemas = ExecuteCommandInternal(context, QuackSchemaSet::GetLoadQuery());
	result.tables = ExecuteCommandInternal(context, QuackTableSet::GetLoadQuery());
	return result;
}

QuackCatalog::~QuackCatalog() {
}

void QuackCatalog::Initialize(bool load_builtin) {
}

//! The (possibly nested) schema path a lookup refers to. A qualified lookup always leads with the catalog
//! component (e.g. [catalog, schema] or a nested [catalog, s1, s2]); a bare lookup is just [schema]. The
//! leading catalog component and any empty placeholders are dropped.
static vector<Identifier> GetSchemaPath(const EntryLookupInfo &schema_lookup) {
	auto &qualified_path = schema_lookup.GetQualifiedName().Path();
	vector<Identifier> schema_path;
	for (idx_t i = 0; i < qualified_path.size(); i++) {
		if (i == 0 && qualified_path.size() > 1) {
			continue;
		}
		if (qualified_path[i].empty()) {
			continue;
		}
		schema_path.push_back(qualified_path[i]);
	}
	return schema_path;
}

optional_ptr<SchemaCatalogEntry> QuackCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto schema_path = GetSchemaPath(schema_lookup);
	// navigate the schema chain: the outermost schema lives in the catalog, each nested one in its parent
	reference<QuackCatalogSet> current_set = *schemas;
	optional_ptr<CatalogEntry> entry;
	for (idx_t i = 0; i < schema_path.size(); i++) {
		entry = current_set.get().GetEntry(schema_path[i].GetIdentifierName());
		if (!entry) {
			switch (if_not_found) {
			case OnEntryNotFound::THROW_EXCEPTION:
				throw BinderException("Schema with name \"%s\" not found", schema_path[i].GetIdentifierName());
			case OnEntryNotFound::RETURN_NULL:
			default:
				return nullptr;
			}
		}
		current_set = entry->Cast<QuackSchemaCatalogEntry>().Schemas();
	}
	if (!entry) {
		return nullptr;
	}
	return entry->Cast<SchemaCatalogEntry>();
}

const QuackUri &QuackCatalog::GetServerUri() {
	return client_connection->ServerURI();
}

unique_ptr<ColumnDataCollection> QuackCatalog::ExecuteCommandInternal(ClientContext &context, const string &query) {
	// FIXME this will break with many results!
	auto chunk_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
	// get a client to query
	auto client_wrapper = client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	auto response =
	    client.Request<PrepareResponseMessage>(context, make_uniq<PrepareRequestMessage>(GetConnectionId(), query, 0));
	chunk_collection->Initialize(response->Types());
	for (auto &chunk : response->MutableResults()) {
		chunk_collection->Append(chunk->Chunk());
	}
	return chunk_collection;
}

shared_ptr<QuackClientConnection> QuackCatalog::GetClientConnection() {
	return client_connection;
}

void QuackCatalog::Refresh(ClientContext &context) {
	auto load_info = LoadCatalog(context);
	schemas->Reload(context, *this, load_info);
}

const string &QuackCatalog::GetConnectionId() {
	return client_connection->ConnectionId();
}

QuackCatalog &QuackCatalog::GetQuackCatalog(ClientContext &context, Value &catalog_name) {
	if (catalog_name.IsNull()) {
		throw BinderException("Catalog cannot be NULL");
	}
	// look up the database to query
	auto db_name = catalog_name.GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, Identifier(db_name));
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\"", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "quack") {
		throw BinderException("Attached database \"%s\" does not refer to a Quack database", db_name);
	}
	return catalog.Cast<QuackCatalog>();
}

optional_ptr<CatalogEntry> QuackCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	// find the set the schema goes in - the catalog itself for a top-level schema, the deepest parent for a
	// nested one. This runs before the schema is created on the server so a bad path does not leave the
	// server and the local catalog out of sync
	reference<QuackCatalogSet> target_set = *schemas;
	optional_ptr<QuackSchemaCatalogEntry> parent;
	for (auto &parent_name : info.ParentSchemas()) {
		auto parent_entry = target_set.get().GetEntry(parent_name.GetIdentifierName());
		if (!parent_entry) {
			throw CatalogException("Cannot create nested schema \"%s\": parent schema \"%s\" does not exist",
			                       info.SchemaName().GetIdentifierName(), parent_name.GetIdentifierName());
		}
		parent = &parent_entry->Cast<QuackSchemaCatalogEntry>();
		target_set = parent->Schemas();
	}

	// create the schema remotely - the local schema path is exactly how the server sees it, so the statement
	// only needs the local catalog name (the ATTACH alias) removed
	auto remote_info = unique_ptr_cast<CreateInfo, CreateSchemaInfo>(info.Copy());
	remote_info->StripCatalogQualification();
	auto &quack_transaction = QuackTransaction::Get(transaction);
	quack_transaction.Query(remote_info->ToString());

	auto schema_entry = make_uniq<QuackSchemaCatalogEntry>(*this, info, parent.get());
	return target_set.get().CreateEntry(std::move(schema_entry), info.on_conflict);
}

//! Report a schema and every schema nested inside it
static void ScanNestedSchemas(QuackSchemaCatalogEntry &schema,
                              const std::function<void(SchemaCatalogEntry &)> &callback) {
	callback(schema);
	for (auto &nested : schema.Schemas().GetAllCatalogEntries()) {
		ScanNestedSchemas(nested.get().Cast<QuackSchemaCatalogEntry>(), callback);
	}
}

void QuackCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	for (auto &schema : schemas->GetAllCatalogEntries()) {
		ScanNestedSchemas(schema.get().Cast<QuackSchemaCatalogEntry>(), callback);
	}
}

PhysicalOperator &QuackCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("PlanDelete not implemented yet");
}
PhysicalOperator &QuackCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("PlanUpdate not implemented yet");
}

unique_ptr<LogicalOperator> QuackCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                          TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not implemented yet");
}

DatabaseSize QuackCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not implemented yet");
}

unique_ptr<TableRef> QuackCatalog::RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) {
	return RemoteExecute(context, node->ToString());
}

unique_ptr<TableRef> QuackCatalog::RemoteExecute(ClientContext &context, unique_ptr<SQLStatement> statement) {
	// a statement pushed down as a whole is DDL - it changes the catalog on the server, so the local
	// snapshot of the remote catalog has to be reloaded once the statement has run
	return CreateRemoteQueryRef(statement->ToString(), true);
}

unique_ptr<TableRef> QuackCatalog::RemoteExecute(ClientContext &context, const string &sql) {
	return CreateRemoteQueryRef(sql, false);
}

unique_ptr<TableRef> QuackCatalog::CreateRemoteQueryRef(const string &sql, bool refresh_catalog) {
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(GetName())));
	args.push_back(make_uniq<ConstantExpression>(Value(sql)));
	auto use_transaction = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
	use_transaction->SetAlias("use_transaction");
	args.push_back(std::move(use_transaction));
	if (refresh_catalog) {
		auto refresh = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
		refresh->SetAlias("refresh_catalog");
		args.push_back(std::move(refresh));
	}
	auto func_ref = make_uniq<TableFunctionRef>();
	func_ref->function = make_uniq<FunctionExpression>("quack_query_by_name", std::move(args));
	return std::move(func_ref);
}

bool QuackCatalog::InMemory() {
	return false;
}
string QuackCatalog::GetDBPath() {
	return client_connection->ServerURI().Uri();
}

void QuackCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// the resolved path is [catalog, parent schemas..., schema]; drop it remotely under the name the server
	// knows it by (everything but the local catalog alias)
	auto &path = info.GetQualifiedName().Path();
	if (path.empty()) {
		throw InternalException("DropSchema called without a schema name");
	}
	vector<Identifier> schema_path(path.begin() + (path.size() > 1 ? 1 : 0), path.end());
	auto drop_info = info.Copy();
	drop_info->SetQualifiedName(
	    QualifiedName(vector<Identifier>(schema_path.begin(), schema_path.end() - 1), schema_path.back()));
	auto &transaction = QuackTransaction::Get(context, *this);
	transaction.Query(drop_info->ToString());

	// remove the schema from the local set it lives in
	reference<QuackCatalogSet> target_set = *schemas;
	for (idx_t i = 0; i + 1 < schema_path.size(); i++) {
		auto parent_entry = target_set.get().GetEntry(schema_path[i].GetIdentifierName());
		if (!parent_entry) {
			return;
		}
		target_set = parent_entry->Cast<QuackSchemaCatalogEntry>().Schemas();
	}
	target_set.get().DropEntry(schema_path.back().GetIdentifierName());
}

bool QuackCatalog::SupportsPushdown(const TableRef &ref) {
	if (ref.type != TableReferenceType::TABLE_FUNCTION) {
		return true;
	}
	auto &table_func_ref = ref.Cast<TableFunctionRef>();
	if (table_func_ref.function->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return true;
	}
	auto &func_expr = table_func_ref.function->Cast<FunctionExpression>();
	if (func_expr.FunctionName() == "query") {
		return false;
	}
	return true;
}

} // namespace duckdb
