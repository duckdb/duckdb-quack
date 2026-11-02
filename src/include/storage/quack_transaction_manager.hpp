//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/quack_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class QuackCatalog;

class QuackTransactionManager : public TransactionManager {
public:
	QuackTransactionManager(AttachedDatabase &db_p, QuackCatalog &sqlite_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	void DestroyTransaction(Transaction &transaction);

private:
	QuackCatalog &quack_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<QuackTransaction>> transactions;
};

} // namespace duckdb
