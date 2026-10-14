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
	//! This transaction changed the cached remote catalog; unless it commits, the cache is reloaded on next use
	void MarkCatalogChanged();

	static QuackTransaction &Get(ClientContext &context, Catalog &catalog);
	static QuackTransaction &Get(CatalogTransaction transaction);

	unique_ptr<ColumnDataCollection> Query(const string &query);

private:
	void MarkCatalogStaleIfChanged();

	QuackCatalog &quack_catalog;
	QuackTransactionState transaction_state;
	bool catalog_changed = false;
};

} // namespace duckdb
