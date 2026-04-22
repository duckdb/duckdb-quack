#include "rpc_storage_extension.hpp"
#include "duckdb/main/database.hpp"
#include "rpc_server.hpp"

#include <thread>

using namespace duckdb;

RpcStorageExtensionInfo &RpcStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto &config = instance.config;
	auto ext = StorageExtension::Find(config, STORAGE_EXTENSION_KEY);
	if (!ext) {
		throw std::runtime_error("Fatal error: couldn't find rpc extension state.");
	}
	return *static_cast<RpcStorageExtensionInfo *>(ext->storage_info.get());
}

RpcServer &RpcStorageExtensionInfo::FindOrCreateServer(ClientContext &context, const RpcUri &listen_uri) {
	std::lock_guard<std::mutex> lock(servers_mutex);
	auto it = servers.find(listen_uri.Uri());
	if (it != servers.end()) {
		return *it->second;
	}
	unique_ptr<RpcServer> server;
	server = make_uniq<HttpRpcServer>(context);
	server->Listen(listen_uri);
	servers.emplace(listen_uri.Uri(), std::move(server));
	return *servers[listen_uri.Uri()];
}

bool RpcStorageExtensionInfo::StopServer(ClientContext &context, const RpcUri &listen_uri) {
	unique_ptr<RpcServer> to_destroy;
	{
		std::lock_guard<std::mutex> lock(servers_mutex);
		const auto it = servers.find(listen_uri.Uri());
		if (it == servers.end()) {
			return false;
		}
		to_destroy = std::move(it->second);
		servers.erase(it);
	}
	// Synchronously free the listening port so that clients racing a subsequent
	// connect() after rpc_stop observe a real refusal rather than a stale socket.
	to_destroy->Close();
	// Full destruction (httplib worker-pool join) runs off-thread so that when
	// rpc_stop is invoked from inside one of the server's own worker threads
	std::thread([srv = std::move(to_destroy)]() mutable { srv.reset(); }).detach();
	return true;
}
