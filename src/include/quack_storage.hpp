#pragma once

#include "duckdb/storage/storage_extension.hpp"

#include "quack_server.hpp"

namespace duckdb {

class DatabaseInstance;

class QuackStorageExtension : public StorageExtension {
public:
	QuackStorageExtension();
};

class QuackStorageExtensionInfo : public StorageExtensionInfo {
public:
	static QuackStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	QuackServer &CreateServer(ClientContext &context, const QuackUri &listen_uri, const string &token);
	bool StopServer(ClientContext &context, const QuackUri &listen_uri);

	struct ServerSnapshot {
		string listen_uri;
		string listen_url;
		string host;
		uint16_t port;
		idx_t active_connections;
		vector<std::pair<string, string>> info;
	};
	vector<ServerSnapshot> ListServers();

	static constexpr const char *STORAGE_EXTENSION_KEY = "quack";

private:
	std::mutex servers_mutex;
	unordered_map<string, unique_ptr<QuackServer>> servers;
};
} // namespace duckdb
