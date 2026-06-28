#include "storage/quack_secret_storage.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include "quack_client.hpp"
#include "quack_message.hpp"
#include "storage/quack_connection_secret_storage.hpp"

namespace duckdb {

std::atomic<int64_t> QuackSecretStorage::next_tie_break_offset(25);

QuackSecretStorage::QuackSecretStorage(const string &storage_name_p, shared_ptr<QuackClientConnection> connection_p)
    : SecretStorage(storage_name_p, GetNextTieBreakOffset()), connection(connection_p) {
	// The secret physically lives on the server (beyond this local statement), so from DuckDB's point of view this is
	// a persistent backend - this also makes the ergonomic `CREATE SECRET ... IN <alias>` resolve correctly (a named,
	// non-"memory" storage is treated as PERSISTENT, and persistent secrets may only go into a persistent backend).
	persistent = true;
}

namespace {
//! Process-wide registry of quack secret storages by (lower-cased) name. There is no SecretManager unregister API, so
//! a storage outlives DETACH; on re-ATTACH of the same alias we rebind the existing instance to the new connection
//! instead of registering a duplicate (which would throw). The raw pointers are owned by the SecretManager and, since
//! storages are never removed, stay valid for the lifetime of the process.
mutex &RegistryLock() {
	static mutex lock;
	return lock;
}
unordered_map<string, QuackSecretStorage *> &Registry() {
	static unordered_map<string, QuackSecretStorage *> registry;
	return registry;
}
} // namespace

void QuackSecretStorage::Register(ClientContext &context, const string &storage_name,
                                  shared_ptr<QuackClientConnection> connection) {
	lock_guard<mutex> registry_guard(RegistryLock());
	auto &registry = Registry();
	auto key = StringUtil::Lower(storage_name);

	auto it = registry.find(key);
	if (it != registry.end()) {
		// Re-ATTACH of the same alias: reconnect the existing storage.
		it->second->Rebind(std::move(connection));
		return;
	}

	auto &secret_manager = SecretManager::Get(context);
	auto storage = make_uniq<QuackSecretStorage>(storage_name, std::move(connection));
	auto storage_ptr = storage.get();
	try {
		secret_manager.LoadSecretStorage(std::move(storage));
	} catch (std::exception &) {
		// Name collides with a pre-existing (non-quack) secret storage. Don't fail the ATTACH over it - the database
		// is still usable, you just can't `CREATE SECRET ... IN <alias>` into it.
		return;
	}
	registry[key] = storage_ptr;
}

void QuackSecretStorage::Rebind(shared_ptr<QuackClientConnection> connection_p) {
	lock_guard<mutex> guard(lock);
	connection = connection_p;
}

shared_ptr<QuackClientConnection> QuackSecretStorage::GetConnection() {
	lock_guard<mutex> guard(lock);
	auto conn = connection.lock();
	if (!conn) {
		throw InvalidInputException(
		    "quack secret storage '%s' is not connected - the database has been detached. Re-ATTACH it to use "
		    "`CREATE SECRET ... IN %s` again.",
		    storage_name, storage_name);
	}
	return conn;
}

void QuackSecretStorage::RunOnServer(ClientContext &context, const string &sql) {
	auto conn = GetConnection();
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
