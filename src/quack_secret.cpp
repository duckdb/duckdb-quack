#include "duckdb/main/client_context.hpp"

#include "quack_secret.hpp"
#include "quack_uri.hpp"

namespace duckdb {

//! The only quack secret in the catalog, if there is exactly one - with nothing else to go on, that is
//! the default secret. Ambiguity is not an error: the caller then falls back to its own defaults.
static unique_ptr<SecretEntry> FindSingleSecret(ClientContext &context, CatalogTransaction transaction) {
	unique_ptr<SecretEntry> result;
	for (auto &entry : SecretManager::Get(context).AllSecrets(transaction)) {
		if (entry.secret->GetType() != QuackSecret::TYPE) {
			continue;
		}
		if (result) {
			return nullptr;
		}
		result = make_uniq<SecretEntry>(entry);
	}
	return result;
}

unique_ptr<SecretEntry> QuackSecret::Find(ClientContext &context, optional_ptr<const Value> secret_name,
                                          const string &path, DefaultLookup default_lookup) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	if (!secret_name) {
		auto match = secret_manager.LookupSecret(transaction, path, TYPE);
		if (match.HasMatch()) {
			return std::move(match.secret_entry);
		}
		if (default_lookup == DefaultLookup::ALLOW_SINGLE_SECRET) {
			return FindSingleSecret(context, transaction);
		}
		return nullptr;
	}
	if (secret_name->IsNull()) {
		throw InvalidInputException("secret cannot be NULL");
	}
	auto name = secret_name->GetValue<string>();
	if (name.empty()) {
		throw InvalidInputException("secret cannot be empty");
	}
	auto entry = secret_manager.GetSecretByName(transaction, name);
	if (!entry) {
		throw InvalidInputException("Secret with name \"%s\" not found", name);
	}
	if (entry->secret->GetType() != TYPE) {
		throw InvalidInputException("Secret \"%s\" has type \"%s\" - a secret of type \"quack\" is required", name,
		                            entry->secret->GetType().GetIdentifierName());
	}
	return entry;
}

bool QuackSecret::TryGetEndpoint(const SecretEntry &entry, string &result) {
	bool found = false;
	QuackUri endpoint;
	for (auto &scope : entry.secret->GetScope()) {
		auto trimmed = scope;
		StringUtil::Trim(trimmed);
		if (trimmed.empty() || trimmed == "quack:" || trimmed == "quack://") {
			// catch-all scope: it matches every server, so it points at none
			continue;
		}
		QuackUri scope_uri(trimmed, /* endpoints are compared without SSL, the caller decides */ false);
		if (found && scope_uri.CanonicalUri() != endpoint.CanonicalUri()) {
			throw InvalidInputException("Secret \"%s\" is scoped to multiple endpoints (%s and %s) - specify the URI "
			                            "explicitly to choose one",
			                            entry.secret->GetName().GetIdentifierName(), endpoint.Uri(), scope_uri.Uri());
		}
		endpoint = scope_uri;
		found = true;
	}
	if (found) {
		result = endpoint.Uri();
	}
	return found;
}

void QuackSecret::CreateDefault(ClientContext &context, const string &token) {
	CreateSecretInput input;
	input.type = Identifier(TYPE);
	input.name = Identifier(DEFAULT_NAME);
	// an empty scope leaves the catch-all `quack:` default in place, exactly like an unnamed CREATE SECRET
	input.options["token"] = Value(token);
	input.persist_type = SecretPersistType::PERSISTENT;
	// the secret is a convenience, so a name that is already taken is not worth failing the server over
	input.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	SecretManager::Get(context).CreateSecret(context, input);
}

string QuackSecret::GetToken(const SecretEntry &entry) {
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*entry.secret);
	Value token_value;
	if (!kv_secret.TryGetValue("token", token_value) || token_value.IsNull()) {
		throw InvalidInputException("Quack secret \"%s\" does not contain a token",
		                            entry.secret->GetName().GetIdentifierName());
	}
	return token_value.ToString();
}

} // namespace duckdb
