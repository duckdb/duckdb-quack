#include "storage/quack_secret_storage.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include "quack_client.hpp"
#include "quack_message.hpp"
#include "storage/quack_catalog.hpp"
#include "storage/quack_connection_secret_storage.hpp"

namespace duckdb {

std::atomic<int64_t> QuackSecretStorage::next_tie_break_offset(25);

QuackSecretStorage::QuackSecretStorage(const string &storage_name_p)
    : SecretStorage(storage_name_p, GetNextTieBreakOffset()) {
	// The secret physically lives on the server (beyond this local statement), so from DuckDB's point of view this is
	// a persistent backend - this also makes the ergonomic `CREATE SECRET ... IN <alias>` resolve correctly (a named,
	// non-"memory" storage is treated as PERSISTENT, and persistent secrets may only go into a persistent backend).
	persistent = true;
}

void QuackSecretStorage::Register(ClientContext &context, const string &storage_name) {
	auto &secret_manager = SecretManager::Get(context);
	try {
		secret_manager.LoadSecretStorage(make_uniq<QuackSecretStorage>(storage_name));
	} catch (std::exception &) {
		// Either a storage for this alias is already registered (re-ATTACH on the same instance) or the name collides
		// with a pre-existing non-quack storage. Either way there's nothing to do: the storage carries no connection
		// state (it resolves the live one by alias), so re-ATTACH needs no rebind, and a collision just means you
		// can't `CREATE SECRET ... IN <alias>` into it. Never fail the ATTACH over this.
	}
}

shared_ptr<QuackClientConnection> QuackSecretStorage::ResolveConnection(ClientContext &context) {
	// storage_name is the attach alias. Look it up among the currently-attached catalogs; if it's gone the database
	// has been detached.
	auto catalog = Catalog::GetCatalogEntry(context, storage_name);
	if (!catalog || catalog->GetCatalogType() != "quack") {
		throw InvalidInputException(
		    "quack secret storage '%s' is not connected - no quack database is attached as '%s'. ATTACH it to use "
		    "`CREATE SECRET ... IN %s`.",
		    storage_name, storage_name, storage_name);
	}
	return catalog->Cast<QuackCatalog>().GetClientConnection();
}

void QuackSecretStorage::RunOnServer(ClientContext &context, const string &sql) {
	auto conn = ResolveConnection(context);
	auto client_wrapper = conn->GetClient(context);
	auto &client = client_wrapper->GetClient();
	// Request<> throws the server-side error (e.g. unknown secret type) back to the client on failure.
	client.Request<PrepareResponseMessage>(context, make_uniq<PrepareRequestMessage>(conn->ConnectionId(), sql));
}

static string BuildCreateSecretSQL(const KeyValueSecret &secret, OnCreateConflict on_conflict) {
	// Always TEMPORARY and routed into the server's connection-scoped storage, so the shipped secret is isolated to
	// this connection and torn down with it (see QuackConnectionSecretStorage).
	string verb;
	switch (on_conflict) {
	case OnCreateConflict::REPLACE_ON_CONFLICT:
		verb = "CREATE OR REPLACE TEMPORARY SECRET ";
		break;
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		verb = "CREATE TEMPORARY SECRET IF NOT EXISTS ";
		break;
	default: // ERROR_ON_CONFLICT
		verb = "CREATE TEMPORARY SECRET ";
		break;
	}

	string sql = verb + KeywordHelper::WriteOptionallyQuoted(secret.GetName());
	sql += " IN " + KeywordHelper::WriteOptionallyQuoted(QUACK_CONNECTION_SECRET_STORAGE);
	sql += " (TYPE " + KeywordHelper::WriteOptionallyQuoted(secret.GetType());
	// Re-emit as the manual 'config' provider: the KeyValueSecret already holds the fully-resolved key/values
	// regardless of how it was originally created (e.g. credential_chain), so config reproduces it faithfully.
	sql += ", PROVIDER config";

	auto &scope = secret.GetScope();
	if (!scope.empty()) {
		sql += ", SCOPE ";
		if (scope.size() == 1) {
			sql += Value(scope[0]).ToSQLString();
		} else {
			sql += "[";
			for (idx_t i = 0; i < scope.size(); i++) {
				if (i > 0) {
					sql += ", ";
				}
				sql += Value(scope[i]).ToSQLString();
			}
			sql += "]";
		}
	}

	for (auto &entry : secret.secret_map) {
		sql += ", " + KeywordHelper::WriteOptionallyQuoted(entry.first) + " " + entry.second.ToSQLString();
	}
	sql += ")";
	return sql;
}

unique_ptr<SecretEntry> QuackSecretStorage::StoreSecret(unique_ptr<const BaseSecret> secret,
                                                        OnCreateConflict on_conflict,
                                                        optional_ptr<CatalogTransaction> transaction) {
	auto kv_secret = dynamic_cast<const KeyValueSecret *>(secret.get());
	if (!kv_secret) {
		throw NotImplementedException(
		    "quack secret storage only supports key-value secrets, cannot ship a secret of type '%s'",
		    secret->GetType());
	}
	if (!transaction || !transaction->HasContext()) {
		throw InvalidInputException("`CREATE SECRET ... IN %s` requires an active client context", storage_name);
	}
	auto sql = BuildCreateSecretSQL(*kv_secret, on_conflict);
	RunOnServer(transaction->GetContext(), sql);
	return make_uniq<SecretEntry>(std::move(secret));
}

void QuackSecretStorage::DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
                                          optional_ptr<CatalogTransaction> transaction) {
	if (!transaction || !transaction->HasContext()) {
		throw InvalidInputException("`DROP SECRET ... FROM %s` requires an active client context", storage_name);
	}
	string sql = "DROP SECRET ";
	if (on_entry_not_found == OnEntryNotFound::RETURN_NULL) {
		sql += "IF EXISTS ";
	}
	sql += KeywordHelper::WriteOptionallyQuoted(name);
	sql += " FROM " + KeywordHelper::WriteOptionallyQuoted(QUACK_CONNECTION_SECRET_STORAGE);
	RunOnServer(transaction->GetContext(), sql);
}

SecretMatch QuackSecretStorage::LookupSecret(const string &path, const string &type,
                                             optional_ptr<CatalogTransaction> transaction) {
	// The server, not the client, executes the federated read - so this storage never wins a local lookup. See also
	// IncludeInLookups() == false, which already keeps the SecretManager from consulting us.
	return SecretMatch();
}

unique_ptr<SecretEntry> QuackSecretStorage::GetSecretByName(const string &name,
                                                            optional_ptr<CatalogTransaction> transaction) {
	// Introspection of server-side secrets (SHOW SECRETS round-trip) is not proxied yet.
	return nullptr;
}

vector<SecretEntry> QuackSecretStorage::AllSecrets(optional_ptr<CatalogTransaction> transaction) {
	return {};
}

} // namespace duckdb
