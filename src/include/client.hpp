#pragma once

#include "message.hpp"
#include "rpc_log_type.hpp"
#include "rpc_uri.hpp"

#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

class RpcClient {
public:
	explicit RpcClient(const RpcUri &uri_p) : uri(uri_p) {};

	void SetContext(optional_ptr<ClientContext> context_p) {
		context = context_p;
	}

	template <class TARGET>
	unique_ptr<TARGET> Request(unique_ptr<ProtocolMessage> request_message) {
		lock_guard<mutex> guard(request_mutex);

		// Extract metadata before move
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
		// Guard against reading the active query during transaction start itself
		// (e.g. BEGIN TRANSACTION via RpcCatalog::ExecuteCommand), where the
		// transaction isn't yet installed on the TransactionContext.
		if (context && context->transaction.HasActiveTransaction()) {
			client_query_id = context->transaction.GetActiveQuery();
			request_message->SetClientQueryId(client_query_id);
		}

		// Time the request
		int64_t start_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                         .time_since_epoch()
		                         .count();

		auto response_message = RequestInternal(std::move(request_message)).release();

		int64_t end_time = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
		                       .time_since_epoch()
		                       .count();

		// Log RPC message
		if (context) {
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

		if (response_message->Type() != TARGET::TYPE) {
			if (response_message->Type() == MessageType::ERROR) {
				throw IOException("Expected %s message, got error message instead: %s",
				                  MessageTypeToString(TARGET::TYPE),
				                  response_message->Cast<ErrorMessage>().Error().c_str());
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr<TARGET>(reinterpret_cast<TARGET *>(response_message));
	}

	static unique_ptr<RpcClient> GetClient(const RpcUri &uri);

	virtual ~RpcClient() {};

protected:
	mutex request_mutex;
	MemoryStream read_stream, write_stream;
	RpcUri uri;
	optional_ptr<ClientContext> context;

private:
	virtual unique_ptr<ProtocolMessage> RequestInternal(unique_ptr<ProtocolMessage> request_message) = 0;
};

class HttpsRpcClient : public RpcClient {
public:
	HttpsRpcClient(const RpcUri &uri_p);
	~HttpsRpcClient() override;

private:
	unique_ptr<ProtocolMessage> RequestInternal(unique_ptr<ProtocolMessage> request_message) override;

private:
	void EnsureClient();

private:
	unique_ptr<HTTPParams> http_params;
	unique_ptr<HTTPClient> http_client;
};

} // namespace duckdb
