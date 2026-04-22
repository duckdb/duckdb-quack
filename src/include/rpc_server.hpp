#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "rpc_uri.hpp"

#include <thread>

#include "httplib.hpp"

namespace duckdb {

class ClientContext;
class ProtocolMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
class PreparedStatement;

struct RpcConnection {
	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	//	unordered_map<string, std::pair<unique_ptr<PreparedStatement>, unique_ptr<QueryResult>>> duckdb_statements;
	unique_ptr<QueryResult> duckdb_query_result;
	//! Monotonic counter assigned per FETCH batch — enables order-preserving parallel scans on
	idx_t next_batch_index = 0;
};

class RpcServer {
public:
	explicit RpcServer(ClientContext &context_p);
	// TODO should listen be part of the constructor?
	virtual void Listen(const RpcUri &uri) {};

	//! Synchronously stop accepting connections and join the listener threads.
	virtual void Close() {};

	optional_ptr<RpcConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id);
	// TODO need something to destroy connections

	static string GenerateSessionId();

	virtual ~RpcServer();

protected:
	unique_ptr<ProtocolMessage> HandleMessage(ProtocolMessage &received_message);
	unique_ptr<ProtocolMessage> HandleMessageInternal(ProtocolMessage &received_message);

protected:
	std::vector<std::thread> listen_threads;

	shared_ptr<DatabaseInstance> db;
	mutex active_connections_mutex;
	unordered_map<string, unique_ptr<RpcConnection>> active_connections;
};

class HttpRpcServer : public RpcServer {
public:
	HttpRpcServer(ClientContext &context_p) : RpcServer(context_p) {
	}
	void Listen(const RpcUri &uri) override;
	void Close() override;

	~HttpRpcServer() override;

private:
	static void ListenThread(HttpRpcServer *rpc_server, const string &listen_host, int listen_port);

	unique_ptr<duckdb_httplib::Server> server;
};
;

} // namespace duckdb
