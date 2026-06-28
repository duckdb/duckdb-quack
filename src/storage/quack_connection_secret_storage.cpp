#include "storage/quack_connection_secret_storage.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

// Tie-break offset: only matters between secrets with an *equal* path-match score. We sit below the built-ins
// (temporary=10, local_file=20) so that, on an otherwise-equal match, a connection-scoped secret wins over a global
// one - the connection-scoped one is the more specific intent.
static constexpr int64_t QUACK_CONNECTION_SECRET_TIE_BREAK = 5;

QuackConnectionSecretStorage::QuackConnectionSecretStorage()
    : SecretStorage(QUACK_CONNECTION_SECRET_STORAGE, QUACK_CONNECTION_SECRET_TIE_BREAK) {
	// persistent stays false (base default): these secrets are connection-lifetime only.
}

void QuackConnectionSecretStorage::Register(DatabaseInstance &db) {
	auto &secret_manager = db.GetSecretManager();
	try {
		secret_manager.LoadSecretStorage(make_uniq<QuackConnectionSecretStorage>());
	} catch (std::exception &) {
		// Already registered on this instance (e.g. a second server / repeated load) - nothing to do.
	}
}

optional_ptr<QuackConnectionSecretState>
QuackConnectionSecretStorage::GetState(optional_ptr<CatalogTransaction> transaction, bool create) {
	if (!transaction || !transaction->HasContext()) {
		return nullptr;
	}
	auto &context = transaction->GetContext();
	if (create) {
		return context.registered_state->GetOrCreate<QuackConnectionSecretState>(QUACK_CONNECTION_SECRET_STATE_KEY)
		    .get();
	}
	return context.registered_state->Get<QuackConnectionSecretState>(QUACK_CONNECTION_SECRET_STATE_KEY).get();
}

unique_ptr<SecretEntry> QuackConnectionSecretStorage::StoreSecret(unique_ptr<const BaseSecret> secret,
                                                                  OnCreateConflict on_conflict,
                                                                  optional_ptr<CatalogTransaction> transaction) {
	auto state = GetState(transaction, true);
	if (!state) {
		throw InvalidInputException("Cannot create a connection-scoped secret without an active client context");
	}
	lock_guard<mutex> guard(state->lock);
	// Copy the name: we std::move(secret) below, after which a reference into it would dangle and we'd insert under
	// a garbage key (breaking later name-keyed lookups like DROP / GetSecretByName).
	auto name = secret->GetName();

	auto existing = state->secrets.find(name);
	if (existing != state->secrets.end()) {
		switch (on_conflict) {
		case OnCreateConflict::ERROR_ON_CONFLICT:
			throw InvalidInputException("Connection secret with name '%s' already exists", name);
		case OnCreateConflict::IGNORE_ON_CONFLICT:
			return make_uniq<SecretEntry>(*existing->second);
		default: // REPLACE_ON_CONFLICT
			break;
		}
	}

	auto entry = make_uniq<SecretEntry>(std::move(secret));
	entry->persist_type = SecretPersistType::TEMPORARY;
	entry->storage_mode = QUACK_CONNECTION_SECRET_STORAGE;
	auto result = make_uniq<SecretEntry>(*entry);
	state->secrets[name] = std::move(entry);
	return result;
}

SecretMatch QuackConnectionSecretStorage::LookupSecret(const string &path, const string &type,
                                                       optional_ptr<CatalogTransaction> transaction) {
	auto state = GetState(transaction, false);
	if (!state) {
		// No connection context (or nothing stored yet) - decline, so global storages serve this lookup.
		return SecretMatch();
	}
	lock_guard<mutex> guard(state->lock);
	auto best_match = SecretMatch();
	for (auto &entry : state->secrets) {
		if (StringUtil::CIEquals(entry.second->secret->GetType(), type)) {
			best_match = SelectBestMatch(*entry.second, path, tie_break_offset, best_match);
		}
	}
	return best_match;
}

unique_ptr<SecretEntry> QuackConnectionSecretStorage::GetSecretByName(const string &name,
                                                                      optional_ptr<CatalogTransaction> transaction) {
	auto state = GetState(transaction, false);
	if (!state) {
		return nullptr;
	}
	lock_guard<mutex> guard(state->lock);
	auto entry = state->secrets.find(name);
	if (entry == state->secrets.end()) {
		return nullptr;
	}
	return make_uniq<SecretEntry>(*entry->second);
}

void QuackConnectionSecretStorage::DropSecretByName(const string &name, OnEntryNotFound on_entry_not_found,
                                                    optional_ptr<CatalogTransaction> transaction) {
	auto state = GetState(transaction, false);
	idx_t erased = 0;
	if (state) {
		lock_guard<mutex> guard(state->lock);
		erased = state->secrets.erase(name);
	}
	if (erased == 0 && on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw InvalidInputException("Connection secret with name '%s' not found", name);
	}
}

vector<SecretEntry> QuackConnectionSecretStorage::AllSecrets(optional_ptr<CatalogTransaction> transaction) {
	vector<SecretEntry> result;
	auto state = GetState(transaction, false);
	if (!state) {
		return result;
	}
	lock_guard<mutex> guard(state->lock);
	for (auto &entry : state->secrets) {
		result.push_back(*entry.second);
	}
	return result;
}

} // namespace duckdb
