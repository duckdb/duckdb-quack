#include "client.hpp"
#include "rpc_uri.hpp"

#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

using namespace duckdb;

template <class T>
string GetUriPart(T ele) {
	if (ele.afterLast - ele.first < 1) {
		throw InvalidInputException("Invalid URI");
	}
	return string(ele.first, ele.afterLast - ele.first);
}

HttpsRpcClient::HttpsRpcClient(const RpcUri &uri_p) : RpcClient(uri_p) {};

HttpsRpcClient::~HttpsRpcClient() {
	http_client.reset();
}

unique_ptr<ProtocolMessage> HttpsRpcClient::RequestInternal(unique_ptr<ProtocolMessage> request_message) {
	D_ASSERT(request_message);
	if (!context) {
		throw InvalidInputException("RpcClient requires a ClientContext to make requests");
	}

	lock_guard<mutex> guard(request_mutex);

	auto &db = *context->db;
	ExtensionHelper::AutoLoadExtension(db, "httpfs");
	if (!db.ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("The rpc extension requires the httpfs extension to be loaded!");
	}

	auto &http_util = HTTPUtil::Get(db);
	auto request_url = uri.Http() + "/rpc";
	if (!http_params) {
		http_params = http_util.InitializeParameters(*context, request_url);
	}
	if (http_client) {
		http_client->Initialize(*http_params);
	}

	HTTPHeaders headers;
	headers.Insert("Content-Type", "application/duckdb");

	request_message->ToMemoryStream(write_stream);
	PostRequestInfo post_request(request_url, headers, *http_params, write_stream.GetData(),
	                             write_stream.GetPosition());
	unique_ptr<HTTPResponse> response;

	// Time the request
	int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
	                         .time_since_epoch()
	                         .count();

	try {
		// funny side-effect: Request will create (and populate) http_client if nullptr is passed
		response = http_util.Request(post_request, http_client);
	} catch (std::exception &e) {
		throw IOException("Failed to send message: %s", e.what());
	}
	if (!response || !response->Success()) {
		string error = response ? response->GetError() : "no response";
		throw IOException("Failed to send message: %s", error);
	}

	MemoryStream non_owning_read_stream((data_ptr_t)post_request.buffer_out.data(), post_request.buffer_out.size());
	auto response_message = ProtocolMessage::FromMemoryStream(non_owning_read_stream);

	// logging stuff, own scope
	{
		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();

		auto request_type = request_message->Type();
		string rpc_connection_id;
		string query;
		optional_idx client_query_id;
		switch (request_type) {
		case MessageType::PREPARE_REQUEST: {
			auto &msg = request_message->Cast<PrepareRequestMessage>();
			rpc_connection_id = msg.ConnectionId();
			query = msg.Query();
			break;
		}
		case MessageType::FETCH_REQUEST:
			rpc_connection_id = request_message->Cast<FetchRequestMessage>().ConnectionId();
			break;
		case MessageType::CATALOG_REQUEST:
			rpc_connection_id = request_message->Cast<CatalogRequestMessage>().ConnectionId();
			break;
		case MessageType::APPEND_REQUEST:
			rpc_connection_id = request_message->Cast<AppendRequestMessage>().ConnectionId();
			break;
		default:
			break;
		}

		// Inject client_query_id from context into the message before sending.
		// Guard against reading the active query during transactiopn start itself
		// (e.g. BEGIN TRANSACTION via RpcCatalog::ExecuteCommand), where the
		// transaction isn't yet installed on the TransactionContext.
		if (context->transaction.HasActiveTransaction()) {
			client_query_id = context->transaction.GetActiveQuery();
			request_message->SetClientQueryId(client_query_id);
		}

		// Log RPC message
		auto &logger = Logger::Get(*context);
		if (logger.ShouldLog(RPCLogType::NAME, RPCLogType::LEVEL)) {
			string error;
			if (response_message->Type() == MessageType::ERROR) {
				error = response_message->Cast<ErrorMessage>().Error();
			}
			auto msg =
			    RPCLogType::ConstructLogMessage(request_type, rpc_connection_id, client_query_id, query, uri.Http(),
			                                    end_time - start_time, response_message->Type(), error);
			logger.WriteLog(RPCLogType::NAME, RPCLogType::LEVEL, msg);
		}
	}

	return response_message;
}

unique_ptr<RpcClient> RpcClient::GetClient(const RpcUri &uri) {
	return make_uniq<HttpsRpcClient>(uri);
}
