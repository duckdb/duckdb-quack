//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "storage/quack_schema.hpp"
#include "quack_uri.hpp"

namespace duckdb {

class QuackCatalog;
class QuackClient;
class QuackClientConnection;

class QuackCatalog : public Catalog {
public:
	explicit QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri_p, ClientContext &context,
	                      const string &token, string client_id = {});
	~QuackCatalog() override;

public:
	string GetCatalogType() override {
		return "quack";
	}
	static QuackCatalog &GetQuackCatalog(ClientContext &context, Value &catalog_name);
	static bool IsQuackScan(const string &name);
	bool SupportsPushdown(const TableRef &ref) override;
	void Initialize(bool load_builtin) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	bool Supports(RemoteCapability capability) const override {
		switch (capability) {
		case RemoteCapability::IS_REMOTE:
		case RemoteCapability::EXECUTE_QUERY_NODE:
		case RemoteCapability::CONNECT:
			return true;
		default:
			return false;
		}
	}
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	string GetConnectDisplay() override {
		return GetDBPath();
	}

	unique_ptr<ColumnDataCollection> ExecuteCommandInternal(ClientContext &context, const string &query);
	const QuackUri &GetServerUri();
	const string &GetConnectionId();

	shared_ptr<QuackClientConnection> GetClientConnection();
	//! Open a FRESH server session (its own connection_id) using this catalog's server URI + credentials.
	//! Each concurrent scan must get its own session: the server keeps a single result per connection_id,
	//! so sharing GetClientConnection() across concurrent scans lets one scan's PREPARE clobber another's
	//! in-flight result ("Result has been closed" / short reads).  The extra sessions are reclaimed by the
	//! server's socket-close reaper once the scan's client sockets go away.
	shared_ptr<QuackClientConnection> CreateClientConnection(ClientContext &context);

	void Refresh(ClientContext &context);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	QuackLoadCatalogData LoadCatalog(ClientContext &context);

private:
	shared_ptr<QuackClientConnection> client_connection;
	//! Server URI + credentials retained so CreateClientConnection() can mint fresh per-scan sessions.
	QuackUri server_uri;
	string token;
	string client_id;
	unique_ptr<QuackSchemaSet> schemas;
};

} // namespace duckdb
