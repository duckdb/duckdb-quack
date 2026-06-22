#pragma once

#include "duckdb/common/http_util.hpp"
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

	static unique_ptr<QuackClient> GetClient(DatabaseInstance &db, const QuackUri &uri);
	static unique_ptr<QuackClient> GetClient(ClientContext &context, const QuackUri &uri);

	static shared_ptr<QuackClientConnection> ConnectToServer(ClientContext &context, const QuackUri &uri, string token);

protected:
	mutex request_mutex;
	MemoryStream read_stream, write_stream;
	DatabaseInstance &db;
	QuackUri uri;

private:
	virtual unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                                 unique_ptr<QuackMessage> request_message) = 0;
};

class QuackClientConnection : public enable_shared_from_this<QuackClientConnection> {
public:
	explicit QuackClientConnection(unique_ptr<QuackClient> client_p, QuackUri uri_p, string connection_id_p,
	                               idx_t max_connections_cached = 1);
	~QuackClientConnection();

	void CancelQuery(hugeint_t query_uuid);

	const string &ConnectionId() const {
		return connection_id;
	}
	const QuackUri &ServerURI() const {
		return uri;
	}

	//! Get a client (either a cached one, or open a new one if required)
	unique_ptr<QuackClientWrapper> GetClient(ClientContext &context) const;
	//! Return a client back to the cache
	void StoreClient(unique_ptr<QuackClient> client_p) const;

private:
	QuackUri uri;
	string connection_id;
	mutable mutex lock;
	mutable vector<unique_ptr<QuackClient>> cached_clients;
	idx_t max_connections_cached;
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

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                         unique_ptr<QuackMessage> request_message) override;

private:
	unique_ptr<HTTPParams> http_params;
};

} // namespace duckdb
