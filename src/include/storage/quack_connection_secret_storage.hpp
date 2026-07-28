#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/secret/secret_storage.hpp"

namespace duckdb {
class DatabaseInstance;

//! Name of the connection-scoped secret storage. The quack server registers it; the client targets it by emitting
//! `CREATE OR REPLACE TEMPORARY SECRET <name> IN quack_connection_storage (...)` so shipped secrets land here.
//! Namespaced under `quack_` so it won't collide with any future generic upstream `connection_storage`.
static constexpr const char *QUACK_CONNECTION_SECRET_STORAGE = "quack_connection_storage";
//! Key under which the per-connection secret container is registered on a ClientContext.
static constexpr const char *QUACK_CONNECTION_SECRET_STATE_KEY = "quack_connection_secrets";

//! Per-connection secret container. Lives on the ClientContext's RegisteredStateManager, so it is destroyed exactly
//! when the connection (its ClientContext) goes away - giving automatic, crash-robust cleanup with no client
//! cooperation.
struct QuackConnectionSecretState : public ClientContextState {
	mutex lock;
	case_insensitive_map_t<unique_ptr<SecretEntry>> secrets;
};

//! A secret storage whose secrets are scoped to the calling connection. Registered once on a DatabaseInstance's
//! SecretManager, it presents a per-ClientContext view: StoreSecret/LookupSecret read the calling connection's context
//! out of the passed CatalogTransaction and operate only on that connection's secrets. A contextless lookup (no
//! ClientContext on the transaction) simply returns no match - so it never leaks across connections and never
//! resolves for system/background callsites.
class QuackConnectionSecretStorage : public SecretStorage {
public:
	QuackConnectionSecretStorage();

	//! Register the connection secret storage on `db`'s SecretManager (idempotent - safe to call once per server).
	static void Register(DatabaseInstance &db);

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

	//! Must be consulted during lookups - this is the server side where the federated read actually runs.
	bool IncludeInLookups() override {
		return true;
	}

private:
	//! Fetch the calling connection's secret container. With create=false returns nullptr when there is no context or
	//! no container yet; with create=true it allocates the container on the context (used by StoreSecret).
	static optional_ptr<QuackConnectionSecretState> GetState(optional_ptr<CatalogTransaction> transaction, bool create);
};

} // namespace duckdb
