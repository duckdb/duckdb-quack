#include "storage/quack_transaction_manager.hpp"
#include "storage/quack_catalog.hpp"

#include <exception>

namespace duckdb {

QuackTransactionManager::QuackTransactionManager(AttachedDatabase &db_p, QuackCatalog &quack_catalog_p)
    : TransactionManager(db_p), quack_catalog(quack_catalog_p) {
}

Transaction &QuackTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<QuackTransaction>(quack_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}
ErrorData QuackTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &quack_transaction = transaction.Cast<QuackTransaction>();
	ErrorData error;
	try {
		quack_transaction.Commit();
	} catch (const std::exception &ex) {
		error = ErrorData(ex);
	} catch (...) {
		error = ErrorData("Server COMMIT failed");
	}
	if (error.HasError()) {
		quack_catalog.InvalidateIfChangedSince(quack_transaction.StartVersion());
	} else if (quack_transaction.CatalogChanged()) {
		// the change is everyone's now
		quack_catalog.Invalidate();
	}
	DestroyTransaction(transaction);
	return error;
}

void QuackTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &quack_transaction = transaction.Cast<QuackTransaction>();
	std::exception_ptr failure;
	try {
		quack_transaction.Rollback();
	} catch (...) {
		failure = std::current_exception();
	}
	quack_catalog.InvalidateIfChangedSince(quack_transaction.StartVersion());
	DestroyTransaction(transaction);
	if (failure) {
		std::rethrow_exception(failure);
	}
}

void QuackTransactionManager::DestroyTransaction(Transaction &transaction) {
	unique_ptr<QuackTransaction> removed_transaction;
	{
		lock_guard<mutex> guard(transaction_lock);
		auto entry = transactions.find(transaction);
		D_ASSERT(entry != transactions.end());
		removed_transaction = std::move(entry->second);
		transactions.erase(entry);
	}
}

void QuackTransactionManager::Checkpoint(ClientContext &context, bool force) {
	throw NotImplementedException("Checkpoint not implemented yet");
}

} // namespace duckdb
