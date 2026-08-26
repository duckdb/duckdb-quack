#include <thread>

#include "duckdb/main/database.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

#include "quack_storage.hpp"
#include "quack_client.hpp"
#include "quack_secret.hpp"
#include "quack_server.hpp"
#include "storage/quack_catalog.hpp"
#include "storage/quack_transaction_manager.hpp"

using namespace duckdb;

QuackStorageExtensionInfo &QuackStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto &config = instance.config;
	auto ext = StorageExtension::Find(config, STORAGE_EXTENSION_KEY);
	if (!ext) {
		throw std::runtime_error("Fatal error: couldn't find rpc extension state.");
	}
	return *static_cast<QuackStorageExtensionInfo *>(ext->storage_info.get());
}

QuackServer &QuackStorageExtensionInfo::CreateServer(ClientContext &context, const QuackUri &listen_uri,
                                                     const string &token) {
	auto server = make_uniq<HttpQuackServer>(context, listen_uri, token);

	auto &actual_uri = server->ListenUri();
	auto key = actual_uri.CanonicalUri();
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto it = servers.find(key);
	if (it != servers.end()) {
		throw InvalidInputException("Server already exists for %s", key);
	}
	servers.emplace(key, std::move(server));
	return *servers[key];
}

vector<QuackStorageExtensionInfo::ServerSnapshot> QuackStorageExtensionInfo::ListServers() {
	vector<ServerSnapshot> result;
	std::lock_guard<std::mutex> lock(servers_mutex);
	result.reserve(servers.size());
	for (auto &kv : servers) {
		auto &uri = kv.second->ListenUri();
		ServerSnapshot snap;
		snap.listen_uri = uri.Uri();
		snap.listen_url = uri.Http();
		snap.host = uri.Host();
		snap.port = uri.Port();
		snap.active_connections = kv.second->ActiveConnectionCount();
		snap.info.emplace_back("ipv6", uri.IPv6() ? "true" : "false");
		result.push_back(std::move(snap));
	}
	return result;
}

vector<QuackConnectionSnapshot> QuackStorageExtensionInfo::GetActiveConnectionSnaps() {
	vector<QuackConnectionSnapshot> result;
	std::lock_guard<std::mutex> lock(servers_mutex);
	for (auto &[uri, server] : servers) {
		for (auto &snapshot : server->GetActiveConnectionSnap()) {
			snapshot.server_id = uri;
			result.push_back(std::move(snapshot));
		}
	}
	return result;
}

bool QuackStorageExtensionInfo::StopServer(ClientContext &context, const QuackUri &listen_uri) {
	unique_ptr<QuackServer> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		const auto it = servers.find(listen_uri.CanonicalUri());
		if (it == servers.end()) {
			return false;
		}
		to_destroy = std::move(it->second);
		servers.erase(it);
	}
	// Synchronously free the listening port
	to_destroy->StopAccepting();
	// Full destruction (httplib worker-pool join) runs off-thread
	std::thread([srv = std::move(to_destroy)]() mutable { srv.reset(); }).detach();
	return true;
}

//! A path is bare when it carries no host: `ATTACH 'quack:'` (or an empty path, once the "quack:" prefix
//! has been stripped by the ATTACH binder). The endpoint then comes from the secret.
static bool IsBareQuackPath(const string &uri) {
	auto trimmed = uri;
	StringUtil::Trim(trimmed);
	return trimmed == "quack:" || trimmed == "quack://";
}

static unique_ptr<Catalog> QuackAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &attach_options) {
	// info.path may or may not already carry the "quack:" prefix.
	auto uri = StringUtil::StartsWith(info.path, "quack:") ? info.path : "quack:" + info.path;

	auto token_entry = attach_options.options.find("token");
	auto secret_entry = attach_options.options.find("secret");
	auto has_token = token_entry != attach_options.options.end();
	auto has_secret_name = secret_entry != attach_options.options.end();
	if (has_token && has_secret_name) {
		throw InvalidInputException("Cannot specify both token and secret - the secret supplies the token");
	}

	// A named secret always decides the token; a bare `quack:` additionally needs a secret to tell us
	// where to connect to. Otherwise the token is resolved further down, when the connection is made.
	auto bare_path = IsBareQuackPath(uri);
	unique_ptr<SecretEntry> secret;
	if (!has_token && (has_secret_name || bare_path)) {
		secret = QuackSecret::Find(
		    context, has_secret_name ? &secret_entry->second : nullptr, bare_path ? "quack:" : uri,
		    bare_path ? QuackSecret::DefaultLookup::ALLOW_SINGLE_SECRET : QuackSecret::DefaultLookup::SCOPE_MATCH_ONLY);
	}
	if (bare_path) {
		// no host given: take the endpoint from the secret scope, falling back to the default host
		string secret_endpoint;
		uri = secret && QuackSecret::TryGetEndpoint(*secret, secret_endpoint) ? secret_endpoint : "quack:localhost";
	}
	auto initial_uri = QuackUri(uri);

	// no ssl on local by default
	auto enable_ssl = !initial_uri.IsLocal();
	if (attach_options.options.find("disable_ssl") != attach_options.options.end()) {
		enable_ssl = !attach_options.options["disable_ssl"].GetValue<bool>();
	}
	string token;
	if (has_token) {
		token = token_entry->second.GetValue<string>();
	} else if (secret) {
		token = QuackSecret::GetToken(*secret);
	}
	auto client_id_entry = attach_options.options.find("client_id");
	auto client_id = QuackClient::ResolveClientId(
	    context, client_id_entry != attach_options.options.end() ? &client_id_entry->second : nullptr);
	auto heartbeat_timeout_entry = attach_options.options.find("heartbeat_timeout");
	auto heartbeat_timeout = QuackClient::ResolveHeartbeatTimeout(
	    context, heartbeat_timeout_entry != attach_options.options.end() ? &heartbeat_timeout_entry->second : nullptr);
	return make_uniq<QuackCatalog>(db, QuackUri(uri, enable_ssl), context, token, std::move(client_id),
	                               heartbeat_timeout);
}

static unique_ptr<TransactionManager> QuackCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog) {
	auto &quack_catalog = catalog.Cast<QuackCatalog>();
	return make_uniq<QuackTransactionManager>(db, quack_catalog);
}

QuackStorageExtension::QuackStorageExtension() {
	attach = QuackAttach;
	create_transaction_manager = QuackCreateTransactionManager;
}
