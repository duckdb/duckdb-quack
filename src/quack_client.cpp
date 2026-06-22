#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

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

HttpsQuackClient::HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p) : QuackClient(db, uri_p) {};

HttpsQuackClient::~HttpsQuackClient() {
}

unique_ptr<QuackMessage> HttpsQuackClient::RequestInternal(optional_ptr<ClientContext> context,
                                                           unique_ptr<QuackMessage> request_message) {
	D_ASSERT(request_message);

	lock_guard<mutex> guard(request_mutex);

	auto &http_util = HTTPUtil::Get(db);
	auto request_url = uri.Http() + "/quack";
	if (!http_params) {
		if (context && context->transaction.HasActiveTransaction()) {
			http_params = http_util.InitializeParameters(*context, request_url);
		} else {
			http_params = http_util.InitializeParameters(db, request_url);
		}
	}

	HTTPHeaders headers;

	// Inject client_query_id from context into the message before sending.
	// Guard against reading the active query during transaction start itself
	// (e.g. BEGIN TRANSACTION via QuackCatalog::ExecuteCommand), where the
	// transaction isn't yet installed on the TransactionContext.
	optional_idx client_query_id;
	if (context && context->transaction.HasActiveTransaction()) {
		auto raw_query_id = context->transaction.GetActiveQuery();
		if (raw_query_id != DConstants::INVALID_INDEX) {
			client_query_id = raw_query_id;
			request_message->SetClientQueryId(client_query_id);
		}
	}

	request_message->ToMemoryStream(write_stream);
	PostRequestInfo post_request(request_url, headers, *http_params, write_stream.GetData(),
	                             write_stream.GetPosition());
	unique_ptr<HTTPResponse> response;

	// Time the request
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();

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

	MemoryStream non_owning_read_stream((data_ptr_t)post_request.buffer_out.data(), post_request.buffer_out.size());
	auto response_message = QuackMessage::FromMemoryStream(non_owning_read_stream);

	// logging stuff, own scope
	{
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();

		auto request_type = request_message->Type();
		string connection_id;
		string query;
		optional_idx client_query_id;
		switch (request_type) {
		case MessageType::PREPARE_REQUEST: {
			auto &msg = request_message->Cast<PrepareRequestMessage>();
			connection_id = msg.ConnectionId();
			query = msg.Query();
			break;
		}
		case MessageType::FETCH_REQUEST:
			connection_id = request_message->Cast<FetchRequestMessage>().ConnectionId();
			break;
		case MessageType::APPEND_REQUEST:
			connection_id = request_message->Cast<AppendRequestMessage>().ConnectionId();
			break;
		default:
			break;
		}

		// Log RPC message
		auto &logger = context ? Logger::Get(*context) : Logger::Get(db);
		if (logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL)) {
			string error;
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				error = response_message->Cast<ErrorResponse>().ErrorMessage();
			}
			auto msg =
			    QuackLogType::ConstructLogMessage(request_type, connection_id, client_query_id, query, uri.Http(),
			                                      end_time - start_time, response_message->Type(), error);
			logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
		}
	}

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
                                             idx_t max_connections_cached)
    : uri(std::move(uri_p)), connection_id(std::move(connection_id_p)), max_connections_cached(max_connections_cached) {
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
                                                               string token) {
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
	auto connection_request_response =
	    client->Request<ConnectionResponseMessage>(context, make_uniq<ConnectionRequestMessage>(token));
	// success! we got a connection id
	// construct the client connection and return it
	auto connection_id = connection_request_response->ConnectionId();
	return make_shared_ptr<QuackClientConnection>(std::move(client), uri, std::move(connection_id));
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
