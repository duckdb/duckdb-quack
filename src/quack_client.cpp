#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include "quack_client.hpp"
#include "quack_uri.hpp"

#include <chrono>

namespace duckdb {

static int64_t NowMillis() {
	return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	    .time_since_epoch()
	    .count();
}

static void MergeHeaders(QuackHTTPHeaders &target, const QuackHTTPHeaders &source) {
	for (auto &header : source) {
		target[header.first] = header.second;
	}
}

QuackHTTPHeaders QuackClient::ParseHTTPHeaders(const Value &value) {
	if (value.IsNull()) {
		throw InvalidInputException("headers cannot be NULL");
	}
	if (value.type().id() != LogicalTypeId::MAP) {
		throw InvalidInputException("headers must be a MAP(VARCHAR, VARCHAR), got %s", value.type().ToString());
	}
	auto cast_value = value.DefaultCastAs(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	QuackHTTPHeaders result;
	for (auto &entry : MapValue::GetChildren(cast_value)) {
		auto &key_value = StructValue::GetChildren(entry);
		if (key_value[0].IsNull()) {
			throw InvalidInputException("headers cannot contain NULL header names");
		}
		if (key_value[1].IsNull()) {
			throw InvalidInputException("headers cannot contain NULL header values");
		}
		auto name = key_value[0].GetValue<string>();
		auto trimmed_name = name;
		StringUtil::Trim(trimmed_name);
		if (trimmed_name.empty()) {
			throw InvalidInputException("headers cannot contain empty header names");
		}
		result[name] = key_value[1].GetValue<string>();
	}
	return result;
}

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

QuackClient::QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p, QuackHTTPHeaders headers_p)
    : db(db_p), uri(uri_p), headers(std::move(headers_p)) {
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
	auto msg = QuackLogType::ConstructLogMessage(request_type, connection_id, client_query_id, query, uri.Http(),
	                                             duration_ms, response_type, error);
	logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
}

HttpsQuackClient::HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p, QuackHTTPHeaders headers_p)
    : QuackClient(db, uri_p, std::move(headers_p)) {};

HttpsQuackClient::~HttpsQuackClient() {
}

string HttpsQuackClient::PostRawLocked(const_data_ptr_t data, idx_t size) {
	D_ASSERT(http_params);
	auto &http_util = HTTPUtil::Get(db);
	auto request_url = uri.Http() + "/quack";
	HTTPHeaders headers;
	for (auto &header : this->headers) {
		headers.Insert(header.first, header.second);
	}
	PostRequestInfo post_request(request_url, headers, *http_params, data, size);
	unique_ptr<HTTPResponse> response;
	try {
		response = http_util.Request(post_request);
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
	// http_params is cached across checkouts, so mirror the checkout's logger in each request; the guard
	// skips the refcount churn when it is unchanged.
	if (request_logger && http_params->logger != request_logger) {
		http_params->logger = request_logger;
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

	int64_t start_time = NowMillis();
	EncodeRequest(context, *request_message, write_stream);
	auto response_body = PostRawLocked(write_stream.GetData(), write_stream.GetPosition());
	auto response_message = DecodeResponse(response_body);
	int64_t duration_ms = NowMillis() - start_time;

	string error;
	if (response_message->Type() == MessageType::ERROR_RESPONSE) {
		error = response_message->Cast<ErrorResponse>().ErrorMessage();
	}
	LogRequest(GetRequestLogger(context), request_message->Type(), request_message->ConnectionId(),
	           request_message->ClientQueryId(), request_message->LoggableQuery(), duration_ms,
	           response_message->Type(), error);

	return response_message;
}

unique_ptr<QuackClient> QuackClient::GetClient(DatabaseInstance &db, const QuackUri &uri, QuackHTTPHeaders headers) {
	ExtensionHelper::AutoLoadExtension(db, "httpfs");
	if (!db.ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("The rpc extension requires the httpfs extension to be loaded!");
	}

	return make_uniq<HttpsQuackClient>(db, uri, std::move(headers));
}

unique_ptr<QuackClient> QuackClient::GetClient(ClientContext &context, const QuackUri &uri, QuackHTTPHeaders headers) {
	return GetClient(*context.db, uri, std::move(headers));
}

QuackClientConnection::QuackClientConnection(unique_ptr<QuackClient> client_p, QuackUri uri_p, string connection_id_p,
                                             QuackHTTPHeaders headers_p, idx_t max_connections_cached)
    : uri(std::move(uri_p)), headers(std::move(headers_p)), connection_id(std::move(connection_id_p)),
      max_connections_cached(max_connections_cached) {
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

shared_ptr<QuackClientConnection> QuackClient::ConnectToServer(ClientContext &context, const QuackUri &uri,
                                                               string token, QuackHTTPHeaders headers) {
	QuackHTTPHeaders secret_headers;
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto match = secret_manager.LookupSecret(transaction, uri.Uri(), "quack");
	if (match.HasMatch()) {
		const auto &kv = dynamic_cast<const KeyValueSecret &>(*match.secret_entry->secret);
		if (token.empty()) {
			token = kv.TryGetValue("token", true).ToString();
		}
		Value secret_header_value;
		if (kv.TryGetValue("headers", secret_header_value)) {
			secret_headers = ParseHTTPHeaders(secret_header_value);
		}
	}
	if (token.empty()) {
		throw InvalidInputException("Could not find a Quack authentication token");
	}
	MergeHeaders(secret_headers, headers);
	headers = std::move(secret_headers);

	// open a HTTP client to the server
	auto client = QuackClient::GetClient(context, uri, headers);

	// submit the connection request
	auto connection_request_response =
	    client->Request<ConnectionResponseMessage>(context, make_uniq<ConnectionRequestMessage>(token));
	// Validate the server's selected protocol version before trusting the connection (client speaks QUACK_VERSION).
	if (connection_request_response->QuackVersion() != QUACK_VERSION) {
		throw IOException("Incompatible Quack protocol version: server uses %llu, client supports %llu",
		                  connection_request_response->QuackVersion(), QUACK_VERSION);
	}
	// success! we got a connection id
	auto connection_id = connection_request_response->ConnectionId();
	idx_t pool_size = MaxValue<idx_t>(1, (idx_t)TaskScheduler::GetScheduler(context).NumberOfAsyncThreads());
	return make_shared_ptr<QuackClientConnection>(std::move(client), uri, std::move(connection_id), std::move(headers),
	                                              pool_size);
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
		result = QuackClient::GetClient(context, uri, headers);
	}
	// Stamp the checking-out query's logger so this client's POSTs (incl. off-thread async sends) are logged.
	result->SetRequestLogger(context.logger);
	return make_uniq<QuackClientWrapper>(std::move(result), shared_from_this());
}

void QuackClientConnection::StoreClient(unique_ptr<QuackClient> client_p) const {
	lock_guard<mutex> guard(lock);
	if (cached_clients.size() >= max_connections_cached) {
		// already exceeded max cache size
		return;
	}
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
