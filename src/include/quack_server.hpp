#pragma once

#include <queue>
#include <thread>

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "quack_result_cache.hpp"
#include "quack_uri.hpp"

#include "httplib.hpp" // TODO forward declare

namespace duckdb {

class ClientContext;
class QuackMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
struct QuackFetchStream;
class PreparedStatement;
class EncryptionState;
class QuackDataStream;
class ErrorData;

enum class QuackQueryState : uint8_t { IDLE, ACTIVE, FINISHED, CANCELLED, QUACK_ERROR };

//! An insert stream + its driver thread, taken off a connection so finish/join can run unlocked.
struct DetachedInsertStream {
	shared_ptr<QuackDataStream> stream;
	std::thread thread;
	string id;
	//! Finish + join + deregister; returns any INSERT error. Call WITHOUT a lock held.
	ErrorData FinishAndJoin();
	//! Roll the INSERT back (SetError) + join + deregister. Call WITHOUT a lock held.
	void AbortAndJoin(const string &reason);
};

//! Server-side state for the INSERT a client is currently driving via a SEND_DATA stream on this
//! connection (one at a time). `lock` is held only briefly — never across a Push or a join.
struct QuackInsertState {
	annotated_mutex lock;
	shared_ptr<QuackDataStream> stream DUCKDB_GUARDED_BY(lock);
	std::thread thread DUCKDB_GUARDED_BY(lock);
	string stream_id DUCKDB_GUARDED_BY(lock);
	//! Dead-range markers that arrived before the stream-creating data message, tagged with the stream
	//! they belong to; applied once that stream is created.
	string pending_marker_stream_id DUCKDB_GUARDED_BY(lock);
	vector<std::pair<idx_t, idx_t>> pending_dead_ranges DUCKDB_GUARDED_BY(lock);

	//! Take the active stream/thread/id off (briefly under the lock) for an unlocked finish/abort.
	DetachedInsertStream Detach();
	//! Detach only if the active stream is unrelated to `msg_stream_id` (else return an empty detach).
	DetachedInsertStream DetachIfUnrelated(const string &msg_stream_id);
	//! Detach + drain (`watermark`) + finish + join; returns any INSERT error. Empty if none active.
	ErrorData Finalize(optional_idx watermark = optional_idx());
	//! Active stream if it matches `sid` (caller pushes the range), else buffer the range and return null.
	shared_ptr<QuackDataStream> StreamForDeadRangeOrBuffer(const string &sid, idx_t lo, idx_t hi);
};

//! The stream the connection's active query fills, one at a time. The query runs on a background
//! thread with a rebalancing result collector, and the FETCH handlers drain the stream's buffer.
struct QuackFetchState {
	mutex lock;
	shared_ptr<QuackFetchStream> stream;
	std::thread thread;
	hugeint_t uuid = 0;
	//! A late FETCH for this uuid gets this error. The stream and its payloads can then go away.
	ErrorData abort_error;
};

struct QuackConnection {
	explicit QuackConnection(string session_id_p, idx_t heartbeat_timeout_seconds_p);
	~QuackConnection();

	//! Renew unless the timeout has already elapsed. Once expired, a lease cannot be revived.
	bool TryRenewLease();
	//! True if the lease timeout has elapsed, latching the expiry ("cannot be revived").
	bool LeaseExpiredLocked(time_point<steady_clock> now) DUCKDB_REQUIRES(lease_lock);

	//! Guards the session state below and the result cache. Never held across a statement.
	mutex lock;
	//! Held for a whole statement, because `duckdb_connection` runs one at a time. The fetch
	//! producer and the insert driver take it. Request handlers must not.
	mutex statement_lock;
	unique_ptr<Connection> duckdb_connection;
	//! Replay cache of the last client query's result stream, null unless quack_enable_reconnects.
	unique_ptr<QuackResultCache> result_cache;
	//! The owning server's live-cache counter, handed to every cache this connection creates.
	shared_ptr<atomic<idx_t>> live_caches;
	//! Rows held by result_cache
	atomic<idx_t> cached_rows {DConstants::INVALID_INDEX};
	//! Current query UUID
	hugeint_t query_uuid;
	string session_id;

	void SyncCachedRows() {
		cached_rows = result_cache ? result_cache->retained_rows : DConstants::INVALID_INDEX;
	}

	//! The only way to drop the cache, keeps the lock-free cached_rows mirror in sync with the drop
	void ClearResultCache() {
		result_cache.reset();
		SyncCachedRows();
	}

	//! True while this connection holds its slot in the server's cache-expiry queue (guarded by `lock`).
	//! The slot outlives any one cache: the sweep re-queues it while a cache exists and releases it otherwise.
	bool cache_in_expiry_queue = false;

	//! Stable per-client reconnect key: HMAC-SHA256(server_hmac_key, client_id). Intentionally excludes
	//! session_id so it stays identical across (re)connections for the same client_id. Empty if no client_id.
	string client_id_hash;

	//! Heartbeat and lease variables
	const idx_t heartbeat_timeout_seconds;
	annotated_mutex lease_lock;
	time_point<steady_clock> lease_last_renewed_at DUCKDB_GUARDED_BY(lease_lock);
	bool lease_expired DUCKDB_GUARDED_BY(lease_lock) = false;

	string sql_query;
	//! Read unlocked by the cache sweep and the connection listing, so it must be atomic.
	atomic<QuackQueryState> query_state {QuackQueryState::IDLE};
	timestamp_t query_started_at {0};

	//! The INSERT this connection is currently driving via a client SEND_DATA stream (one at a time).
	QuackInsertState insert;
	QuackFetchState fetch;
};

struct QuackConnectionSnapshot {
	string server_id;
	string session_id;
	string client_id_hash;
	string sql_query;
	QuackQueryState query_state = QuackQueryState::IDLE;
	timestamp_t query_started_at {0};
	//! Rows in the connection's result cache
	optional_idx cached_rows;
};

enum class QuackServerState { UNINITIALIZED, WAITING_TO_START, RUNNING, CLOSED };

struct CacheExpiryEntry {
	timestamp_t served_at;
	string session_id;
};

struct CacheExpiresLater {
	bool operator()(const CacheExpiryEntry &lhs, const CacheExpiryEntry &rhs) const {
		return lhs.served_at > rhs.served_at;
	}
};

class QuackServer {
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
	string CreateNewConnection(const string &session_id, const string &client_id_hash, idx_t heartbeat_timeout_seconds);
	bool DisconnectConnection(const string &session_id);
	// TODO need something to destroy connections

	string GenerateSessionId();

	//! Generate a fresh CSPRNG-backed 128-bit token, hex-encoded (32 chars).
	static string GenerateRandomToken(DatabaseInstance &db);

	//! Throw InvalidInputException if `token` doesn't meet requirements(currently, length >= 4)
	static void ValidateToken(const string &token);

	vector<QuackConnectionSnapshot> GetActiveConnectionSnap();

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

protected:
	unique_ptr<QuackMessage> HandleMessage(MemoryStream &read_stream);
	//! Drops caches idle past quack_result_ttl
	void SweepExpiredCaches(DatabaseInstance &db);
	//! Give `connection` its expiry-queue slot when its cache is created; caller must hold connection.lock.
	void RegisterCacheForExpiry(QuackConnection &connection);
	unique_ptr<QuackMessage> HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
	                                               optional_ptr<QuackConnection> connection);

protected:
	std::vector<std::thread> listen_threads;

	weak_ptr<DatabaseInstance> db_ptr;
	mutex active_connections_mutex;
	unordered_map<string, shared_ptr<QuackConnection>> active_connections;
	shared_ptr<atomic<idx_t>> live_caches = make_shared_ptr<atomic<idx_t>>(0);
	//! Min-heap of the expiry-queue slots, at most one per connection with a cache
	mutex cache_expiry_mutex;
	std::priority_queue<CacheExpiryEntry, vector<CacheExpiryEntry>, CacheExpiresLater> cache_expiry_queue;

	mutex session_id_rng_mutex;
	shared_ptr<EncryptionState> session_id_rng;

	QuackUri uri;

private:
	bool RenewConnectionLease(const string &connection_id, const shared_ptr<QuackConnection> &connection);
	static void CleanupExpiredConnection(QuackConnection &connection);
	//! Destroys reaped caches on a one-shot detached thread so no response waits on their teardown
	void DestroyCachesDetached(vector<unique_ptr<QuackResultCache>> doomed);

	string token;
	//! Per-server random key that seeds the HMAC for client_id_hash.
	string server_hmac_key;
};

class HttpQuackServer : public QuackServer {
public:
	HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);

	void StopAccepting() override;
	void Close() override;

	~HttpQuackServer() override;

private:
	static void ListenThread(HttpQuackServer *server, const string &listen_host, uint16_t listen_port);

	unique_ptr<QuackMessage> ReadMessage(MemoryStream &read_stream);

	unique_ptr<duckdb_httplib::Server> server;
	mutex state_lock;
	atomic<QuackServerState> server_state {QuackServerState::UNINITIALIZED};
};

} // namespace duckdb
