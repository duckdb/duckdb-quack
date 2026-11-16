//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "storage/quack_schema.hpp"

namespace duckdb {

class QuackCatalog;

enum class QuackTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class QuackTransaction : public Transaction {
public:
	QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p, ClientContext &context_p);
	~QuackTransaction() override;

	//! Lazily start a transaction - this won't actually do anything until the first query is fired
	void Start();
	//! Forcibly start a transaction - ensure we actually start a transaction server-side
	void ForceStart();
	void Commit();
	void Rollback();

	static QuackTransaction &Get(ClientContext &context, Catalog &catalog);
	static QuackTransaction &Get(CatalogTransaction transaction);

	unique_ptr<ColumnDataCollection> Query(const string &query);

	idx_t StartVersion() const {
		return start_version;
	}
	void Retain(shared_ptr<QuackSchemaSet> snapshot);
	optional_ptr<CatalogEntry> Retain(unique_ptr<CatalogEntry> entry);

	//! This transaction changed the remote catalog: until it ends it reads the catalog as its own server
	//! transaction sees it, and the shared snapshot is invalidated only once the change is committed
	void MarkCatalogChanged();
	bool CatalogChanged() const {
		return catalog_changed;
	}
	shared_ptr<QuackSchemaSet> OwnSnapshot();
	void SetOwnSnapshot(shared_ptr<QuackSchemaSet> snapshot);

private:
	QuackCatalog &quack_catalog;
	QuackTransactionState transaction_state;
	idx_t start_version;
	mutex retained_lock;
	vector<shared_ptr<QuackSchemaSet>> retained_snapshots;
	vector<unique_ptr<CatalogEntry>> retained_entries;
	bool catalog_changed = false;
	//! The catalog as this transaction sees it after its own change - empty until loaded
	shared_ptr<QuackSchemaSet> own_snapshot;
};

} // namespace duckdb
