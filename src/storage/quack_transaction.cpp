#include "storage/quack_transaction.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/quack_catalog.hpp"

namespace duckdb {

QuackTransaction::QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p,
                                   ClientContext &context_p)
    : Transaction(manager_p, context_p), quack_catalog(quack_catalog_p),
      transaction_state(QuackTransactionState::TRANSACTION_NOT_YET_STARTED),
      start_version(quack_catalog_p.InvalidatedVersion()) {
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
		Query("COMMIT");
	}
}

void QuackTransaction::Rollback() {
	if (transaction_state == QuackTransactionState::TRANSACTION_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_FINISHED;
		Query("ROLLBACK");
	}
}

void QuackTransaction::Retain(shared_ptr<QuackSchemaSet> snapshot) {
	lock_guard<mutex> guard(retained_lock);
	if (retained_snapshots.empty() || retained_snapshots.back() != snapshot) {
		retained_snapshots.push_back(std::move(snapshot));
	}
}

optional_ptr<CatalogEntry> QuackTransaction::Retain(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> guard(retained_lock);
	retained_entries.push_back(std::move(entry));
	return retained_entries.back().get();
}

void QuackTransaction::MarkCatalogChanged() {
	lock_guard<mutex> guard(retained_lock);
	catalog_changed = true;
	own_snapshot.reset();
}

shared_ptr<QuackSchemaSet> QuackTransaction::OwnSnapshot() {
	lock_guard<mutex> guard(retained_lock);
	return own_snapshot;
}

void QuackTransaction::SetOwnSnapshot(shared_ptr<QuackSchemaSet> snapshot) {
	lock_guard<mutex> guard(retained_lock);
	own_snapshot = std::move(snapshot);
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
