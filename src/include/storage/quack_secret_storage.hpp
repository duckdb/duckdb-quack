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
//! The storage holds no connection pointer - it carries only the alias and resolves the live connection on demand by
//! looking up the attached catalog of that name. So it never dangles across DETACH/ATTACH or across DatabaseInstances:
//! after DETACH the lookup fails (clean error), and a re-ATTACH transparently resolves the fresh connection.
//!
//! Transport is the regular query channel - StoreSecret/DropSecretByName reconstruct CREATE/DROP SECRET SQL and send
//! it as a normal PrepareRequest. The server lands it in its connection-scoped storage (QuackConnectionSecretStorage).
class QuackSecretStorage : public SecretStorage {
public:
	explicit QuackSecretStorage(const string &storage_name);

public:
	//! Register a quack secret storage named after the attach alias. Idempotent: if one is already registered for this
	//! alias on this instance's SecretManager (re-ATTACH, or a name collision), this is a no-op - the existing storage
	//! resolves the current connection dynamically, so there is nothing to rebind.
	static void Register(ClientContext &context, const string &storage_name);

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
	//! Resolve the live connection for this alias, or throw if the database isn't currently attached (detached).
	shared_ptr<QuackClientConnection> ResolveConnection(ClientContext &context);
	//! Send a statement to the server over the query channel.
	void RunOnServer(ClientContext &context, const string &sql);

private:
	//! Tie-break offsets must be unique across all storages; hand them out sequentially (postgres uses the same
	//! trick, starting at 25 just past the built-in local-file storage at 20).
	static std::atomic<int64_t> next_tie_break_offset;
	static int64_t GetNextTieBreakOffset() {
		return next_tie_break_offset++;
	}
};

} // namespace duckdb
