#pragma once

#include <atomic>
#include <chrono>
#include <thread>

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include "quack_uri.hpp"

#include "httplib.hpp" // TODO forward declare

namespace duckdb {

class ClientContext;
class QuackMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
class PreparedStatement;
class EncryptionState;
enum class MessageType : uint8_t;

struct QuackConnection {
	explicit QuackConnection(string session_id_p);
	~QuackConnection();

	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	unique_ptr<QueryResult> duckdb_query_result;
	//! Monotonic counter assigned per FETCH batch — enables order-preserving parallel scans on
	idx_t next_batch_index = 1;
	//! Current result UUID
	hugeint_t result_uuid;
	string session_id;
};

class QuackServer {
public:
	static constexpr const idx_t QUACK_VERSION = 1;

public:
	explicit QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);
	virtual ~QuackServer();

	//! Stop accepting new connections (close the listener socket) without
	//! joining listener threads. Safe to call from a request-handler thread —
	//! does not wait on httplib's task-queue, which would deadlock when the
	//! caller is itself a worker.
	virtual void StopAccepting() {};

	//! Synchronously stop accepting connections and join the listener threads.
	//! Must NOT be called from a worker / request-handler thread; httplib's
	//! listen-loop teardown joins all workers, which would deadlock.
	virtual void Close() {};

	shared_ptr<QuackConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id);
	bool DisconnectConnection(const string &session_id);
	// TODO need something to destroy connections

	string GenerateSessionId();

	//! Generate a fresh CSPRNG-backed 128-bit token, hex-encoded (32 chars).
	static string GenerateRandomToken(DatabaseInstance &db);

	//! Throw InvalidInputException if `token` doesn't meet requirements(currently, length >= 4)
	static void ValidateToken(const string &token);

	const string &Token() {
		return token;
	}

	const QuackUri &ListenUri() const {
		return uri;
	}

	idx_t ActiveConnectionCount() {
		std::lock_guard<std::mutex> lock(active_connections_mutex);
		return active_connections.size();
	}

	//! Returns the wall-clock epoch seconds of the most recent non-STATUS message handled by this server.
	//! STATUS_REQUEST handling does NOT bump this — the timestamp reflects actual work, not health-checks.
	int64_t LastActivityAtEpochSeconds() const {
		return last_activity_at_epoch_seconds.load();
	}

	//! Bumps last_activity_at_epoch_seconds if `type` is anything other than STATUS_REQUEST.
	//! Called from HandleMessage on every incoming message.
	void OnMessageReceived(MessageType type);

protected:
	unique_ptr<QuackMessage> HandleMessage(MemoryStream &read_stream);
	unique_ptr<QuackMessage> HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
	                                               optional_ptr<QuackConnection> connection);

	//! Reads `quack_idle_timeout_seconds` + `quack_idle` settings and computes the directive
	//! ("OK" or "IDLE") for STATUS_RESPONSE. Read fresh on every STATUS request so SET changes
	//! take effect immediately. The server never rejects traffic based on this — directive is
	//! purely a signal for orchestrators that decide externally what to do with idle instances.
	string EvaluateDirective(DatabaseInstance &db) const;

protected:
	std::vector<std::thread> listen_threads;

	weak_ptr<DatabaseInstance> db_ptr;
	mutex active_connections_mutex;
	unordered_map<string, shared_ptr<QuackConnection>> active_connections;

	mutex session_id_rng_mutex;
	shared_ptr<EncryptionState> session_id_rng;

	//! Wall-clock epoch seconds of the most recent non-STATUS message. Initialized to server-start time;
	//! bumped by OnMessageReceived. system_clock-derived so it's serializable to clients.
	std::atomic<int64_t> last_activity_at_epoch_seconds;

private:
	QuackUri uri;
	string token;
};

class HttpQuackServer : public QuackServer {
public:
	HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);

	void StopAccepting() override;
	void Close() override;

	~HttpQuackServer() override;

private:
	static void ListenThread(HttpQuackServer *server, const string &listen_host, int listen_port);

	unique_ptr<QuackMessage> ReadMessage(MemoryStream &read_stream);

	unique_ptr<duckdb_httplib::Server> server;
	bool is_running = false;
};

} // namespace duckdb
