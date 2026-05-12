#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"

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
                           const string &token)
    : Catalog(db_p) {
	// connect to the server
	client_connection = QuackClient::ConnectToServer(context, server_uri, token);

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

optional_ptr<SchemaCatalogEntry> QuackCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	auto schema_entry = schemas->GetEntry(schema_name);
	if (schema_entry) {
		return schema_entry->Cast<SchemaCatalogEntry>();
	}
	switch (if_not_found) {
	case OnEntryNotFound::THROW_EXCEPTION:
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	case OnEntryNotFound::RETURN_NULL:
	default:
		return nullptr;
	}
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
	    client.Request<PrepareResponseMessage>(context, make_uniq<PrepareRequestMessage>(GetConnectionId(), query));
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

optional_ptr<CatalogEntry> QuackCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &quack_transaction = QuackTransaction::Get(transaction);
	// create schema remotely
	quack_transaction.Query(info.ToString());
	// register schema locally
	auto schema_entry = make_uniq<QuackSchemaCatalogEntry>(*this, info);
	return schemas->CreateEntry(std::move(schema_entry), info.on_conflict);
}

void QuackCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	for (auto &schema : schemas->GetAllCatalogEntries()) {
		callback(schema.get().Cast<SchemaCatalogEntry>());
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

//! Whether or not this is an in-memory SQLite database
bool QuackCatalog::InMemory() {
	throw NotImplementedException("InMemory not implemented yet");
}
string QuackCatalog::GetDBPath() {
	throw NotImplementedException("GetDBPath not implemented yet");
}

void QuackCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO should we just send over the drop info in a dropmessage???
	throw NotImplementedException("DropSchema not implemented yet");
}

} // namespace duckdb
