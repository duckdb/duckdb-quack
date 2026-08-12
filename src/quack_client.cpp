#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include "quack_client.hpp"
#include "quack_uri.hpp"

namespace duckdb {

template <class T>
string GetUriPart(T ele) {
	if (ele.afterLast - ele.first < 1) {
		throw InvalidInputException("Invalid URI");
	}
	return string(ele.first, ele.afterLast - ele.first);
}

void QuackClientConnection::CancelQuery(hugeint_t query_uuid) {
	lock_guard<mutex> guard(lock);
	if (cached_clients.empty()) {
		return;
	}
	// get a cached client, if any
	auto &client = cached_clients.back();
	client->Request<SuccessResponse>(nullptr, make_uniq<CancelRequestMessage>(connection_id, query_uuid));
}

QuackClient::QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p) : db(db_p), uri(uri_p) {
}

QuackClient::~QuackClient() {
}

void QuackClient::EncodeRequest(optional_ptr<ClientContext> context, QuackMessage &message, MemoryStream &out) {
	// Inject client_query_id from the active query so client and server logs correlate. Guard against
	// transaction start (e.g. BEGIN via QuackCatalog::ExecuteCommand), where the transaction isn't yet
	// installed on the TransactionContext and there is no active query to read.
	if (context && context->transaction.HasActiveTransaction()) {
		auto raw_query_id = context->transaction.GetActiveQuery();
		if (raw_query_id != DConstants::INVALID_INDEX) {
			message.SetClientQueryId(raw_query_id);
		}
	}
	message.ToMemoryStream(out);
}

unique_ptr<QuackMessage> QuackClient::DecodeResponse(const string &response_body) {
	MemoryStream read_stream((data_ptr_t)response_body.data(), response_body.size());
	return QuackMessage::FromMemoryStream(read_stream);
}

Logger &QuackClient::GetRequestLogger(optional_ptr<ClientContext> context) {
	return context ? Logger::Get(*context) : Logger::Get(db);
}

void QuackClient::SetRequestLogger(shared_ptr<Logger> logger) {
	request_logger = std::move(logger);
}

void QuackClient::LogRequest(Logger &logger, MessageType request_type, const string &connection_id,
                             optional_idx client_query_id, const string &query, int64_t duration_ms,
                             MessageType response_type, const string &error) {
	if (!logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL)) {
		return;
	}
	// client_id_hash is a server-side identifier (never sent back to the client), so it is empty here.
	auto msg = QuackLogType::ConstructLogMessage(request_type, connection_id, string(), client_query_id, query,
	                                             uri.Http(), duration_ms, response_type, error);
	logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
}

HttpsQuackClient::HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p) : QuackClient(db, uri_p) {};

HttpsQuackClient::~HttpsQuackClient() {
}

string HttpsQuackClient::PostRawLocked(const_data_ptr_t data, idx_t size) {
	D_ASSERT(http_params);
	auto &http_util = HTTPUtil::Get(db);
	auto request_url = uri.Http() + "/quack";
	HTTPHeaders headers;
	PostRequestInfo post_request(request_url, headers, *http_params, data, size);
	unique_ptr<HTTPResponse> response;
	try {
		response = http_util.Request(post_request, http_client);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw IOException("Failed to send message: %s", error.Message());
	}
	if (!response || !response->Success()) {
		string error = response ? response->GetError() : "no response";
		throw IOException("Failed to send message: %s", error);
	}
	return std::move(post_request.buffer_out);
}

void HttpsQuackClient::EnsureHttpParams(optional_ptr<ClientContext> context) {
	if (!http_params) {
		auto &http_util = HTTPUtil::Get(db);
		auto request_url = uri.Http() + "/quack";
		if (context && context->transaction.HasActiveTransaction()) {
			http_params = http_util.InitializeParameters(*context, request_url);
		} else {
			http_params = http_util.InitializeParameters(db, request_url);
		}
	}
	// http_params is cached across checkouts; re-scope its logger each request so a context-less
	// teardown on a pooled client never logs under a prior query's scope.
	if (request_logger) {
		if (http_params->logger != request_logger) {
			http_params->logger = request_logger;
		}
	} else if (!context) {
		http_params->logger.reset();
	}
}

string HttpsQuackClient::PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size) {
	lock_guard<mutex> guard(request_mutex);
	EnsureHttpParams(context);
	return PostRawLocked(data, size);
}

unique_ptr<QuackMessage> HttpsQuackClient::RequestInternal(optional_ptr<ClientContext> context,
                                                           unique_ptr<QuackMessage> request_message) {
	D_ASSERT(request_message);

	lock_guard<mutex> guard(request_mutex);
	EnsureHttpParams(context);

	auto start_time = QuackNowMillis();
	EncodeRequest(context, *request_message, write_stream);
	auto response_body = PostRawLocked(write_stream.GetData(), write_stream.GetPosition());
	auto response_message = DecodeResponse(response_body);
	auto duration_ms = QuackNowMillis() - start_time;

	string error;
	if (response_message->Type() == MessageType::ERROR_RESPONSE) {
		error = response_message->Cast<ErrorResponse>().ErrorMessage();
	}
	LogRequest(GetRequestLogger(context), request_message->Type(), request_message->ConnectionId(),
	           request_message->ClientQueryId(), request_message->LoggableQuery(), duration_ms,
	           response_message->Type(), error);

	return response_message;
}

unique_ptr<QuackClient> QuackClient::GetClient(DatabaseInstance &db, const QuackUri &uri) {
	ExtensionHelper::AutoLoadExtension(db, "httpfs");
	if (!db.ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("The rpc extension requires the httpfs extension to be loaded!");
	}

	return make_uniq<HttpsQuackClient>(db, uri);
}

unique_ptr<QuackClient> QuackClient::GetClient(ClientContext &context, const QuackUri &uri) {
	return GetClient(*context.db, uri);
}

QuackClientConnection::QuackClientConnection(unique_ptr<QuackClient> client_p, QuackUri uri_p, string connection_id_p,
                                             idx_t heartbeat_timeout_seconds_p, idx_t max_connections_cached_p)
    : uri(std::move(uri_p)), connection_id(std::move(connection_id_p)),
      heartbeat_timeout_seconds(heartbeat_timeout_seconds_p), max_connections_cached(max_connections_cached_p) {
	if (client_p) {
		StoreClient(std::move(client_p));
	}
}

QuackClientConnection::~QuackClientConnection() {
	if (!cached_clients.empty()) {
		try {
			auto &client = cached_clients.back();
			client->Request<SuccessResponse>(nullptr, make_uniq<DisconnectMessage>(connection_id));
		} catch (...) {
		}
	}
}

void QuackClient::ValidateClientId(const string &client_id) {
	// An empty client_id means "no client_id" (opt-out); anything non-empty must carry a little entropy,
	// mirroring the >= 4 minimum QuackServer::ValidateToken enforces on the token.
	if (!client_id.empty() && client_id.size() < 4) {
		throw InvalidInputException("client_id must be at least 4 characters long");
	}
}

string QuackClient::ResolveClientId(ClientContext &context, optional_ptr<const Value> explicit_value) {
	if (explicit_value) {
		if (explicit_value->IsNull()) {
			throw InvalidInputException("client_id cannot be null");
		}
		return explicit_value->GetValue<string>();
	}
	// No explicit value -> fall back to the precomputed default (set from $QUACK_CLIENT_ID or a random
	// per-instance id at load time). An empty setting means the deployment opted out of a default.
	Value setting_val;
	if (context.TryGetCurrentSetting("quack_default_client_id", setting_val) && !setting_val.IsNull()) {
		return setting_val.GetValue<string>();
	}
	return string();
}

void QuackClient::ValidateHeartbeatTimeout(idx_t heartbeat_timeout_seconds) {
	if (heartbeat_timeout_seconds == 0) {
		throw InvalidInputException("heartbeat_timeout must be greater than zero");
	}
}

idx_t QuackClient::ResolveHeartbeatTimeout(ClientContext &context, optional_ptr<const Value> explicit_value) {
	Value timeout_value;
	if (explicit_value) {
		if (explicit_value->IsNull()) {
			throw InvalidInputException("heartbeat_timeout cannot be null");
		}
		timeout_value = *explicit_value;
	} else if (!context.TryGetCurrentSetting("quack_default_heartbeat_timeout", timeout_value) ||
	           timeout_value.IsNull()) {
		throw InternalException("quack_default_heartbeat_timeout is not registered");
	}
	auto timeout_seconds = timeout_value.GetValue<idx_t>();
	ValidateHeartbeatTimeout(timeout_seconds);
	return timeout_seconds;
}

shared_ptr<QuackClientConnection> QuackClient::ConnectToServer(ClientContext &context, const QuackUri &uri,
                                                               string token, string client_id,
                                                               idx_t heartbeat_timeout_seconds) {
	// Single choke point for every connection path (ATTACH + quack_query), so a malformed client_id is
	// rejected here regardless of where it came from.
	ValidateClientId(client_id);
	ValidateHeartbeatTimeout(heartbeat_timeout_seconds);
	// if no token is provided fetch it from the secret manager
	if (token.empty()) {
		auto &secret_manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		auto match = secret_manager.LookupSecret(transaction, uri.Uri(), "quack");
		if (match.HasMatch()) {
			const auto &kv = dynamic_cast<const KeyValueSecret &>(*match.secret_entry->secret);
			token = kv.TryGetValue("token", true).ToString();
		}
	}
	if (token.empty()) {
		throw InvalidInputException("Could not find a Quack authentication token");
	}

	// open a HTTP client to the server
	auto client = QuackClient::GetClient(context, uri);

	// submit the connection request
	auto connection_request_response = client->Request<ConnectionResponseMessage>(
	    context, make_uniq<ConnectionRequestMessage>(token, std::move(client_id)));
	// Validate the server's selected protocol version before trusting the connection (client speaks QUACK_VERSION).
	if (connection_request_response->QuackVersion() != QUACK_VERSION) {
		throw IOException("Incompatible Quack protocol version: server uses %llu, client supports %llu",
		                  connection_request_response->QuackVersion(), QUACK_VERSION);
	}
	// success! we got a connection id
	auto connection_id = connection_request_response->ConnectionId();
	// Cache at most one client per async send slot: pending SEND_DATA tasks can check out far more
	// clients than ever POST concurrently, and each cached client pins a server connection slot.
	idx_t pool_size = MaxValue<idx_t>(1, (idx_t)TaskScheduler::GetScheduler(context).NumberOfAsyncThreads());
	return make_shared_ptr<QuackClientConnection>(std::move(client), uri, std::move(connection_id),
	                                              heartbeat_timeout_seconds, pool_size);
}

unique_ptr<QuackClientWrapper> QuackClientConnection::GetClient(ClientContext &context) const {
	lock_guard<mutex> guard(lock);
	unique_ptr<QuackClient> result;
	if (!cached_clients.empty()) {
		// use client from the cache
		result = std::move(cached_clients.back());
		cached_clients.pop_back();
	} else {
		// instantiate a new client
		result = QuackClient::GetClient(context, uri);
	}
	// Stamp the checking-out query's logger so this client's POSTs (incl. off-thread async sends) are logged.
	result->SetRequestLogger(context.logger);
	return make_uniq<QuackClientWrapper>(std::move(result), shared_from_this());
}

void QuackClientConnection::StoreClient(unique_ptr<QuackClient> client_p) const {
	lock_guard<mutex> guard(lock);
	if (cached_clients.size() >= max_connections_cached) {
		// Beyond the cap, drop the client: destroying it closes its persistent socket, freeing the
		// server-side connection slot instead of retaining an idle keep-alive the server must carry.
		return;
	}
	// A pooled client has no owning query; drop the stamp so later teardown doesn't log under it.
	client_p->SetRequestLogger(nullptr);
	cached_clients.push_back(std::move(client_p));
}

QuackClientWrapper::QuackClientWrapper(unique_ptr<QuackClient> client_p,
                                       shared_ptr<const QuackClientConnection> client_connection_p)
    : client(std::move(client_p)), client_connection(std::move(client_connection_p)) {
}

QuackClientWrapper::~QuackClientWrapper() {
	client_connection->StoreClient(std::move(client));
}

QuackClient &QuackClientWrapper::GetClient() {
	return *client;
}

} // namespace duckdb
