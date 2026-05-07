#include <thread>

#include "duckdb/main/database.hpp"

#include "quack_storage.hpp"
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
	auto key = listen_uri.CanonicalUri();
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto it = servers.find(key);
	if (it != servers.end()) {
		throw InvalidInputException("Server already exists for %s", key);
	}
	unique_ptr<QuackServer> server;
	server = make_uniq<HttpQuackServer>(context, listen_uri, token);
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

static unique_ptr<Catalog> QuackAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &attach_options) {
	// info.path may or may not already carry the "quack:" prefix.
	auto uri = StringUtil::StartsWith(info.path, "quack:") ? info.path : "quack:" + info.path;
	auto initial_uri = QuackUri(uri);

	// no ssl on local by default
	auto enable_ssl = !initial_uri.IsLocal();
	if (attach_options.options.find("disable_ssl") != attach_options.options.end()) {
		enable_ssl = !attach_options.options["disable_ssl"].GetValue<bool>();
	}
	string token;
	if (attach_options.options.find("token") != attach_options.options.end()) {
		token = attach_options.options["token"].GetValue<string>();
	}
	return make_uniq<QuackCatalog>(db, QuackUri(uri, enable_ssl), context, token);
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
