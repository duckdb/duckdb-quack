#include "storage/quack_transaction.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/quack_catalog.hpp"

namespace duckdb {

QuackTransaction::QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p,
                                   ClientContext &context_p)
    : Transaction(manager_p, context_p), quack_catalog(quack_catalog_p),
      transaction_state(QuackTransactionState::TRANSACTION_NOT_YET_STARTED) {
}

QuackTransaction::~QuackTransaction() {
}

void QuackTransaction::Start() {
	transaction_state = QuackTransactionState::TRANSACTION_NOT_YET_STARTED;
}

void QuackTransaction::ForceStart() {
	if (transaction_state == QuackTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_STARTED;
		Query("BEGIN TRANSACTION");
	}
}

void QuackTransaction::Commit() {
	if (transaction_state == QuackTransactionState::TRANSACTION_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_FINISHED;
		try {
			Query("COMMIT");
		} catch (...) {
			// the server rolled the transaction back
			MarkCatalogStaleIfChanged();
			throw;
		}
	}
}

void QuackTransaction::Rollback() {
	if (transaction_state == QuackTransactionState::TRANSACTION_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_FINISHED;
		try {
			Query("ROLLBACK");
		} catch (...) {
			MarkCatalogStaleIfChanged();
			throw;
		}
		// only after the ROLLBACK: a reload before it would load this transaction's DDL again
		MarkCatalogStaleIfChanged();
	}
}

void QuackTransaction::MarkCatalogChanged() {
	catalog_changed = true;
}

void QuackTransaction::MarkCatalogStaleIfChanged() {
	if (catalog_changed) {
		quack_catalog.MarkStale();
	}
}

QuackTransaction &QuackTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<QuackTransaction>();
}

QuackTransaction &QuackTransaction::Get(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<QuackTransaction>();
}

unique_ptr<ColumnDataCollection> QuackTransaction::Query(const string &query) {
	ForceStart();
	auto context_ref = context.lock();
	if (!context_ref) {
		// context has been destroyed - silently ignore the query
		return nullptr;
	}
	return quack_catalog.ExecuteCommandInternal(*context_ref, query);
}

} // namespace duckdb
