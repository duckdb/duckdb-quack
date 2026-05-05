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
class EncryptionState;

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

	string GenerateSessionId();

	bool EvaluateAuthn(const Value &v1, const Value &v2);
	bool EvaluateAuthz(const Value &v1, const Value &v2);

	virtual ~RpcServer();

protected:
	unique_ptr<ProtocolMessage> HandleMessage(ProtocolMessage &received_message);
	unique_ptr<ProtocolMessage> HandleMessageInternal(ProtocolMessage &received_message);

protected:
	std::vector<std::thread> listen_threads;

	shared_ptr<DatabaseInstance> db;
	mutex active_connections_mutex;
	unordered_map<string, unique_ptr<RpcConnection>> active_connections;

	mutex session_id_rng_mutex;
	shared_ptr<EncryptionState> session_id_rng;

	//! Separate Connection + cached PreparedStatement for each callback —
	//! authn and authz are isolated so a misbehaving authn callback (e.g.
	//! one that leaves transaction state behind) can't poison the authz
	//! path. SQL string is stashed alongside the prepared statement so a
	//! setting change (`rpc_authentication_function` etc.) triggers a
	//! re-prepare on the next call. Each lock serializes its own callback
	//! across worker threads but doesn't block the other.
	mutex authn_mutex;
	unique_ptr<Connection> authn_connection;
	unique_ptr<PreparedStatement> authn_stmt;
	string authn_stmt_sql;

	mutex authz_mutex;
	unique_ptr<Connection> authz_connection;
	unique_ptr<PreparedStatement> authz_stmt;
	string authz_stmt_sql;
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
