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
	                      const string &token);
	~QuackCatalog() override;

public:
	string GetCatalogType() override {
		return "quack";
	}
	static bool IsQuackScan(const string &name);
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

	unique_ptr<ColumnDataCollection> ExecuteCommandInternal(ClientContext &context, const string &query);
	const QuackUri &GetServerUri();
	const string &GetConnectionId();

	shared_ptr<QuackClientConnection> GetClientConnection();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	QuackLoadCatalogData LoadCatalog(ClientContext &context);

private:
	shared_ptr<QuackClientConnection> client_connection;
	unique_ptr<QuackSchemaSet> schemas;
};

} // namespace duckdb
