#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_uri.hpp"

namespace duckdb {
class QuackClientConnection;
struct QuackClientWrapper;

class QuackClient {
public:
	explicit QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p);
	virtual ~QuackClient();

	template <class TARGET>
	unique_ptr<TARGET> Request(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message) {
		auto response_message = RequestInternal(context, std::move(request_message));
		if (response_message->Type() != TARGET::TYPE) {
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				// if we get an error throw it immediately
				response_message->Cast<ErrorResponse>().Error().Throw();
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr_cast<QuackMessage, TARGET>(std::move(response_message));
	}

	//! POST already-serialized request bytes and return the raw response body, throwing on transport failure.
	//! Lets an async sender serialize on a producer thread and perform the blocking POST from the ASYNC pool;
	//! pass context=nullptr when called off the execution thread (parameters fall back to the database).
	virtual string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size) = 0;

	//! Encode a request (inject client_query_id when a query is active, then serialize) into `out`.
	//! Protocol-level and lock-free; the caller owns `out` and any locking around it.
	static void EncodeRequest(optional_ptr<ClientContext> context, QuackMessage &message, MemoryStream &out);

	//! Decode a response body (owned bytes) into a message. Protocol-level and lock-free.
	static unique_ptr<QuackMessage> DecodeResponse(const string &response_body);

	//! Emit a Quack request log entry through `logger`. Pass the query's context logger for per-query
	//! attribution (connection/transaction/query id columns); the db logger only fits context-less
	//! teardown. Safe from an async pool thread when handed a captured context logger.
	void LogRequest(Logger &logger, MessageType request_type, const string &connection_id, optional_idx client_query_id,
	                const string &query, int64_t duration_ms, MessageType response_type, const string &error);

	//! Stamp the logger for this client's HTTP transport-log entries. Set at checkout so a pooled client
	//! (incl. an async send whose pool thread has no ClientContext) logs under the checking-out query.
	void SetRequestLogger(shared_ptr<Logger> logger);

	static unique_ptr<QuackClient> GetClient(DatabaseInstance &db, const QuackUri &uri);
	static unique_ptr<QuackClient> GetClient(ClientContext &context, const QuackUri &uri);

	static shared_ptr<QuackClientConnection> ConnectToServer(ClientContext &context, const QuackUri &uri, string token,
	                                                         string client_id = {}, string remote_catalog = {});

	//! Resolve the effective client_id for a new connection
	static string ResolveClientId(ClientContext &context, optional_ptr<const Value> explicit_value);

	//! Throw unless `client_id` is either empty ("no client_id") or >= 4 characters
	static void ValidateClientId(const string &client_id);

protected:
	//! Resolve the logger for a request: the context (per-query) logger when available, else the db logger.
	Logger &GetRequestLogger(optional_ptr<ClientContext> context);

	mutex request_mutex;
	MemoryStream write_stream;
	DatabaseInstance &db;
	QuackUri uri;
	//! HTTP transport-log logger, stamped at checkout (see SetRequestLogger).
	shared_ptr<Logger> request_logger;

private:
	virtual unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                                 unique_ptr<QuackMessage> request_message) = 0;
};

class QuackClientConnection : public enable_shared_from_this<QuackClientConnection> {
public:
	explicit QuackClientConnection(unique_ptr<QuackClient> client_p, QuackUri uri_p, string connection_id_p,
	                               idx_t max_connections_cached = 1, string remote_catalog_p = {});
	~QuackClientConnection();

	void CancelQuery(hugeint_t query_uuid);

	const string &ConnectionId() const {
		return connection_id;
	}
	const QuackUri &ServerURI() const {
		return uri;
	}
	const string &RemoteCatalog() const {
		return remote_catalog;
	}

	//! Get a client (either a cached one, or open a new one if required)
	unique_ptr<QuackClientWrapper> GetClient(ClientContext &context) const;
	//! Return a client back to the cache
	void StoreClient(unique_ptr<QuackClient> client_p) const;

private:
	QuackUri uri;
	string connection_id;
	string remote_catalog;
	mutable mutex lock;
	//! Bounds cached_clients: each cached client holds a persistent socket that pins a server
	//! connection slot, so an unbounded cache would let one attach starve the server's budget.
	idx_t max_connections_cached;
	mutable vector<unique_ptr<QuackClient>> cached_clients;
};

struct QuackClientWrapper {
	QuackClientWrapper(unique_ptr<QuackClient> client, shared_ptr<const QuackClientConnection> client_connection);
	~QuackClientWrapper();

	QuackClient &GetClient();

private:
	unique_ptr<QuackClient> client;
	shared_ptr<const QuackClientConnection> client_connection;
};

class HttpsQuackClient : public QuackClient {
public:
	HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p);
	~HttpsQuackClient() override;

	string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size) override;

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                         unique_ptr<QuackMessage> request_message) override;
	//! POST bytes assuming request_mutex is already held.
	string PostRawLocked(const_data_ptr_t data, idx_t size);
	//! Lazily initialise http_params (context-aware) and attach request_logger; assumes request_mutex is held.
	void EnsureHttpParams(optional_ptr<ClientContext> context);

private:
	unique_ptr<HTTPParams> http_params;
	//! Persistent keep-alive HTTP client: reused across requests so the TCP connection (and its
	//! warm congestion window) survives between POSTs; replaced by the retry path on dead sockets.
	unique_ptr<HTTPClient> http_client;
};

} // namespace duckdb
