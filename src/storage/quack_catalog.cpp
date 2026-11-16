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
#include "duckdb/parser/statement/create_statement.hpp"
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
#include "quack_session_state.hpp"

#include <exception>

// FIXME bunch of stuff copied from postgres scanner, can probably be simplified!

namespace duckdb {

//! The server connections of one attached quack catalog, one per client context that uses it
class QuackConnectionRegistry {
public:
	shared_ptr<QuackClientConnection> Find(ClientContext &context) {
		lock_guard<mutex> guard(lock);
		auto entry = connections.find(&context);
		return entry == connections.end() ? nullptr : entry->second;
	}

	shared_ptr<QuackClientConnection> FindById(const string &connection_id) {
		lock_guard<mutex> guard(lock);
		for (auto &entry : connections) {
			if (entry.second->ConnectionId() == connection_id) {
				return entry.second;
			}
		}
		return nullptr;
	}

	//! Returns the connection that won if "context" raced to connect from two threads
	shared_ptr<QuackClientConnection> Add(ClientContext &context, shared_ptr<QuackClientConnection> connection) {
		lock_guard<mutex> guard(lock);
		return connections.emplace(&context, std::move(connection)).first->second;
	}

	//! Hands the connection back so it disconnects outside the lock
	shared_ptr<QuackClientConnection> Remove(ClientContext &context) {
		lock_guard<mutex> guard(lock);
		auto entry = connections.find(&context);
		if (entry == connections.end()) {
			return nullptr;
		}
		auto connection = std::move(entry->second);
		connections.erase(entry);
		return connection;
	}

private:
	mutex lock;
	unordered_map<ClientContext *, shared_ptr<QuackClientConnection>> connections;
};

//! Drops the connections a client context opened once the context goes away
class QuackContextConnections : public ClientContextState {
public:
	static constexpr const char *KEY = "quack_context_connections";

	explicit QuackContextConnections(ClientContext &context_p) : context(context_p) {
	}

	~QuackContextConnections() override {
		vector<weak_ptr<QuackConnectionRegistry>> to_release;
		{
			lock_guard<mutex> guard(lock);
			to_release = std::move(registries);
		}
		for (auto &registry : to_release) {
			if (auto live = registry.lock()) {
				live->Remove(context);
			}
		}
	}

	void Track(const shared_ptr<QuackConnectionRegistry> &registry) {
		lock_guard<mutex> guard(lock);
		registries.push_back(registry);
	}

private:
	ClientContext &context;
	mutex lock;
	vector<weak_ptr<QuackConnectionRegistry>> registries;
};

QuackCatalog::QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri_p, ClientContext &context,
                           const string &token_p, string client_id_p, idx_t heartbeat_timeout_seconds_p)
    : Catalog(db_p), server_uri(server_uri_p), token(token_p), client_id(std::move(client_id_p)),
      heartbeat_timeout_seconds(heartbeat_timeout_seconds_p), connections(make_shared_ptr<QuackConnectionRegistry>()) {
	// connect to the server; the attaching context keeps using this connection
	client_connection = QuackClient::ConnectToServer(context, server_uri, token, client_id, heartbeat_timeout_seconds);
	connections->Add(context, client_connection);
	context.registered_state->GetOrCreate<QuackContextConnections>(QuackContextConnections::KEY, context)
	    ->Track(connections);

	// load the entire catalog up-front
	auto load_info = LoadCatalog(context);
	snapshot = make_shared_ptr<QuackSchemaSet>(context, *this, load_info);
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
	if (!transaction.HasContext()) {
		throw InternalException("Quack catalog lookups require a client context");
	}
	auto schema_path = GetSchemaPath(schema_lookup);
	// navigate the schema chain: the outermost schema lives in the catalog, each nested one in its parent
	// temp.main: the server's temp catalog; a lone temp is a schema of that name in its default catalog
	auto in_temp = schema_path.size() > 1 && StringUtil::CIEquals(schema_path[0].GetIdentifierName(), TEMP_CATALOG);
	reference<QuackCatalogSet> current_set =
	    in_temp ? GetTempSchemas(transaction.GetContext()) : GetSchemas(transaction.GetContext());
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
	auto chunk_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
	// get a client to query
	auto connection = GetClientConnection(context);
	auto &connection_id = connection->ConnectionId();
	auto client_wrapper = connection->GetClient(context);
	auto &client = client_wrapper->GetClient();
	auto response =
	    client.Request<PrepareResponseMessage>(context, make_uniq<PrepareRequestMessage>(connection_id, query, 0));
	chunk_collection->Initialize(response->Types());
	for (auto &chunk : response->MutableResults()) {
		chunk_collection->Append(chunk->Chunk());
	}
	// The PREPARE response only carries the leading batches (up to quack_prepare_inline_rows rows).
	// These commands load the catalog, so anything left behind is not a truncated result set - it
	// is a schema, table or view that silently ceases to exist. Drain the rest. Internal catalog
	// traffic keeps uuid 0 (see PrepareRequestMessage above), and FETCH names that same uuid.
	auto query_uuid = response->QueryUUID();
	if (response->NeedsMoreFetch()) {
		for (idx_t batch_index = 1;; batch_index++) {
			// ack everything before this batch: we never ask for an earlier batch again
			auto fetch_response = client.Request<FetchResponseMessage>(
			    context, make_uniq<FetchRequestMessage>(connection_id, query_uuid, batch_index, batch_index - 1));
			if (fetch_response->MutableResults().empty()) {
				// an empty FETCH is how the server says the result is exhausted (cf. QuackScan)
				break;
			}
			for (auto &chunk : fetch_response->MutableResults()) {
				chunk_collection->Append(*chunk);
			}
		}
	}
	return chunk_collection;
}

shared_ptr<QuackClientConnection> QuackCatalog::GetClientConnection(ClientContext &context) {
	// a server session of a self-attached server stays on the connection it serves - GetClient then refuses to
	// route back through it, where a fresh connection would open yet another session for every lookup
	if (auto session = QuackSessionState::Get(context)) {
		if (auto own = connections->FindById(session->ConnectionId())) {
			return own;
		}
	}
	if (auto existing = connections->Find(context)) {
		return existing;
	}
	auto connection = QuackClient::ConnectToServer(context, server_uri, token, client_id, heartbeat_timeout_seconds);
	auto result = connections->Add(context, std::move(connection));
	context.registered_state->GetOrCreate<QuackContextConnections>(QuackContextConnections::KEY, context)
	    ->Track(connections);
	return result;
}

void QuackCatalog::Refresh(ClientContext &context) {
	Invalidate();
	ForgetTemp(context);
	auto &transaction = QuackTransaction::Get(context, *this);
	if (transaction.CatalogChanged()) {
		transaction.MarkCatalogChanged();
	}
	GetSchemas(context);
}

void QuackCatalog::Invalidate() {
	lock_guard<mutex> guard(snapshot_lock);
	invalidated_version++;
}

void QuackCatalog::Invalidate(ClientContext &context) {
	QuackTransaction::Get(context, *this).MarkCatalogChanged();
	ForgetTemp(context);
}

void QuackCatalog::ForgetTemp(ClientContext &context) {
	if (auto connection = connections->Find(context)) {
		connection->SetTempSchemas(nullptr);
	}
}

QuackSchemaSet &QuackCatalog::GetTempSchemas(ClientContext &context) {
	// another connection's session has a temp catalog of its own, so a shared snapshot would show its tables
	// to sessions that cannot read them
	auto connection = GetClientConnection(context);
	auto temp = connection->TempSchemas();
	if (!temp) {
		QuackLoadCatalogData load_data;
		load_data.schemas = ExecuteCommandInternal(context, QuackSchemaSet::GetTempLoadQuery());
		load_data.tables = ExecuteCommandInternal(context, QuackTableSet::GetTempLoadQuery());
		temp = make_shared_ptr<QuackSchemaSet>(context, *this, load_data);
		connection->SetTempSchemas(temp);
	}
	auto &result = *temp;
	QuackTransaction::Get(context, *this).Retain(std::move(temp));
	return result;
}

void QuackCatalog::InvalidateIfChangedSince(idx_t version) {
	lock_guard<mutex> guard(snapshot_lock);
	if (invalidated_version != version) {
		invalidated_version++;
	}
}

idx_t QuackCatalog::InvalidatedVersion() {
	lock_guard<mutex> guard(snapshot_lock);
	return invalidated_version;
}

bool QuackCatalog::ServesOwnConnection(ClientContext &context) {
	auto session = QuackSessionState::Get(context);
	return session && connections->FindById(session->ConnectionId());
}

QuackSchemaSet &QuackCatalog::GetSchemas(ClientContext &context) {
	auto &transaction = QuackTransaction::Get(context, *this);
	auto current = AcquireSnapshot(context, transaction);
	auto &result = *current;
	transaction.Retain(std::move(current));
	return result;
}

shared_ptr<QuackSchemaSet> QuackCatalog::AcquireSnapshot(ClientContext &context, QuackTransaction &transaction) {
	if (transaction.CatalogChanged()) {
		// what this transaction created or dropped is visible only to its own server transaction - loading it
		// into the shared snapshot would hand it to every other connection, and another connection's reload
		// would take it away from this one
		auto own = transaction.OwnSnapshot();
		if (!own) {
			auto load_data = LoadCatalog(context);
			own = make_shared_ptr<QuackSchemaSet>(context, *this, load_data);
			transaction.SetOwnSnapshot(own);
		}
		return own;
	}
	auto uses_published = ServesOwnConnection(context);
	unique_lock<mutex> guard(snapshot_lock);
	while (loaded_version != invalidated_version && !uses_published && loading_context.get() != &context) {
		if (loading_context) {
			snapshot_loaded.wait(guard);
			continue;
		}
		auto target_version = invalidated_version;
		loading_context = &context;
		guard.unlock();
		shared_ptr<QuackSchemaSet> fresh;
		std::exception_ptr failure;
		try {
			auto load_data = LoadCatalog(context);
			fresh = make_shared_ptr<QuackSchemaSet>(context, *this, load_data);
		} catch (...) {
			failure = std::current_exception();
		}
		guard.lock();
		loading_context = nullptr;
		snapshot_loaded.notify_all();
		if (failure) {
			std::rethrow_exception(failure);
		}
		snapshot = std::move(fresh);
		loaded_version = target_version;
	}
	return snapshot;
}

string QuackCatalog::GetConnectionId(ClientContext &context) {
	return GetClientConnection(context)->ConnectionId();
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
	reference<QuackCatalogSet> target_set = GetSchemas(transaction.GetContext());
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
	Invalidate(transaction.GetContext());

	auto existing = target_set.get().GetEntry(info.SchemaName().GetIdentifierName());
	if (existing && info.on_conflict != OnCreateConflict::REPLACE_ON_CONFLICT) {
		return existing;
	}
	return quack_transaction.Retain(make_uniq<QuackSchemaCatalogEntry>(*this, info, parent.get()));
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
	for (auto &schema : GetSchemas(context).GetAllCatalogEntries()) {
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
	// the data is stored on the server and takes no local disk space
	return DatabaseSize();
}

unique_ptr<TableRef> QuackCatalog::RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) {
	// attached-catalog path: a read of this catalog, so it joins the local transaction
	return CreateRemoteQueryRef(node->ToString(), false, true);
}

unique_ptr<TableRef> QuackCatalog::RemoteExecute(ClientContext &context, unique_ptr<SQLStatement> statement) {
	// a statement pushed down as a whole is DDL - it changes the catalog on the server, so the local
	// snapshot of the remote catalog has to be reloaded once the statement has run
	if (statement->type == StatementType::CREATE_STATEMENT) {
		auto &info = *statement->Cast<CreateStatement>().info;
		auto &path = info.GetQualifiedName().Path();
		// the server takes a table or view in its temp catalog only as a TEMP one
		if ((info.type == CatalogType::TABLE_ENTRY || info.type == CatalogType::VIEW_ENTRY) && path.size() > 2 &&
		    StringUtil::CIEquals(path[0].GetIdentifierName(), TEMP_CATALOG)) {
			info.temporary = true;
		}
	}
	return CreateRemoteQueryRef(statement->ToString(), true);
}

unique_ptr<TableRef> QuackCatalog::RemoteExecute(ClientContext &context, const string &sql) {
	// CONNECT forwards statements verbatim - no synthesized transaction, or a forwarded BEGIN would
	// land inside it and COMMIT/ROLLBACK arrive after it has closed
	return CreateRemoteQueryRef(sql, false, false);
}

unique_ptr<TableRef> QuackCatalog::CreateRemoteQueryRef(const string &sql, bool refresh_catalog,
                                                        bool use_transaction_p) {
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(ConstantExpression::FromValue(Value(GetName())));
	args.push_back(ConstantExpression::FromValue(Value(sql)));
	auto use_transaction = ConstantExpression::FromValue(Value::BOOLEAN(use_transaction_p));
	use_transaction->SetAlias("use_transaction");
	args.push_back(std::move(use_transaction));
	if (refresh_catalog) {
		auto refresh = ConstantExpression::FromValue(Value::BOOLEAN(true));
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
	// knows it by - the local schema path is exactly the remote qualification, only the ATTACH alias is local
	auto remote_name = info.GetQualifiedName();
	remote_name.StripCatalog();
	auto &schema_path = remote_name.Path();
	if (schema_path.empty()) {
		throw InternalException("DropSchema called without a schema name");
	}
	auto drop_info = info.Copy();
	drop_info->SetQualifiedName(remote_name);
	auto &transaction = QuackTransaction::Get(context, *this);
	transaction.Query(drop_info->ToString());
	Invalidate(context);
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
