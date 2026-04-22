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
		auto response_message = RequestInternal(std::move(request_message)).release();
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
	unique_ptr<HTTPClient> http_client;
	unique_ptr<HTTPParams> http_params;
};

} // namespace duckdb
