#pragma once

#include "duckdb/main/secret/secret_storage.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#include <atomic>

namespace duckdb {
class ClientContext;
class QuackClientConnection;

//! A write-through secret storage bound to an attached quack database. Its name is exactly the attach alias, so
//! `CREATE SECRET <name> (...) IN <alias>` ships the secret to the quack server (where federated queries actually
//! execute and therefore need the credentials). It is *not* consulted during local secret lookups
//! (IncludeInLookups() == false): the client never reads e.g. s3:// itself, the server does.
//!
//! Transport is the regular query channel - StoreSecret/DropSecretByName reconstruct CREATE/DROP SECRET SQL and send
//! it as a normal PrepareRequest. The server lands it in its own (instance-wide, in-memory) secret manager.
class QuackSecretStorage : public SecretStorage {
public:
	QuackSecretStorage(const string &storage_name, shared_ptr<QuackClientConnection> connection);

public:
	//! Register a quack secret storage named after the attach alias, or rebind an existing one (on re-ATTACH of the
	//! same alias) to the new connection. There is no SecretManager unregister API, so storages outlive DETACH; we
	//! reconnect them here instead. Collisions with a foreign storage of the same name are logged and skipped so they
	//! never break ATTACH.
	static void Register(ClientContext &context, const string &storage_name,
	                     shared_ptr<QuackClientConnection> connection);

	//! Point this storage at a (new) live connection.
	void Rebind(shared_ptr<QuackClientConnection> connection);

	//! SecretStorage API
	unique_ptr<SecretEntry> StoreSecret(unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
	                                    optional_ptr<CatalogTransaction> transaction = nullptr) override;
	vector<SecretEntry> AllSecrets(optional_ptr<CatalogTransaction> transaction = nullptr) override;
	void DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
	                      optional_ptr<CatalogTransaction> transaction = nullptr) override;
	SecretMatch LookupSecret(const string &path, const string &type,
	                         optional_ptr<CatalogTransaction> transaction = nullptr) override;
	unique_ptr<SecretEntry> GetSecretByName(const string &name,
	                                        optional_ptr<CatalogTransaction> transaction = nullptr) override;

	//! The server executes the federated query, so the client must not consult this storage during its own lookups.
	bool IncludeInLookups() override {
		return false;
	}

private:
	//! Resolve the live connection or throw if the database has been detached.
	shared_ptr<QuackClientConnection> GetConnection();
	//! Send a statement to the server over the query channel.
	void RunOnServer(ClientContext &context, const string &sql);

private:
	mutex lock;
	//! Weak so DETACH still tears the connection down (we never keep the server alive). Re-ATTACH calls Rebind().
	weak_ptr<QuackClientConnection> connection;

	//! Tie-break offsets must be unique across all storages; hand them out sequentially (postgres uses the same
	//! trick, starting at 25 just past the built-in local-file storage at 20).
	static std::atomic<int64_t> next_tie_break_offset;
	static int64_t GetNextTieBreakOffset() {
		return next_tie_break_offset++;
	}
};

} // namespace duckdb
