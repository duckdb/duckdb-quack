//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_secret.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

//! Resolution of `quack` secrets, shared by the server side (quack_serve) and the client side (ATTACH).
class QuackSecret {
public:
	//! The name of the quack secret type
	static constexpr const char *TYPE = "quack";
	//! The name DuckDB gives an unnamed `CREATE SECRET (TYPE quack, ...)`, i.e. the default quack secret
	static constexpr const char *DEFAULT_NAME = "__default_quack";

	//! How far an unnamed lookup may go. A secret scope is a match prefix, so a path that carries no endpoint
	//! of its own (`quack_serve()`, `ATTACH 'quack:'`) only ever matches the catch-all `quack:` scope. There
	//! the secret is what tells us the endpoint, so a lone quack secret is taken to be the default one;
	//! ALLOW_SINGLE_SECRET asks for that fallback. A path with a real endpoint must honor scoping instead.
	enum class DefaultLookup { SCOPE_MATCH_ONLY, ALLOW_SINGLE_SECRET };

	//! The secret to use: an explicit name is looked up by name, otherwise we take the best scope match for
	//! `path`, i.e. the default secret. Returns nullptr when no secret matches (only possible without a name).
	static unique_ptr<SecretEntry> Find(ClientContext &context, optional_ptr<const Value> secret_name,
	                                    const string &path,
	                                    DefaultLookup default_lookup = DefaultLookup::SCOPE_MATCH_ONLY);
	//! A secret scope is a match prefix, but a fully-qualified one (`quack:localhost:9000`) doubles as an
	//! endpoint: that is the endpoint we use when no URI was given. Returns false when the secret names no
	//! endpoint, i.e. when it only carries the catch-all `quack:` scope.
	static bool TryGetEndpoint(const SecretEntry &entry, string &result);
	//! The token stored in the secret - throws when the secret does not carry one
	static string GetToken(const SecretEntry &entry);
	//! Persist `token` as the default quack secret, so later sessions and clients pick it up without being
	//! told the token. Does nothing when a secret of that name already exists.
	static void CreateDefault(ClientContext &context, const string &token);
};

} // namespace duckdb
