#include "client.hpp"
#include "rpc_uri.hpp"

#include "duckdb/main/database.hpp"

using namespace duckdb;

template <class T>
string GetUriPart(T ele) {
	if (ele.afterLast - ele.first < 1) {
		throw InvalidInputException("Invalid URI");
	}
	return string(ele.first, ele.afterLast - ele.first);
}

HttpsRpcClient::HttpsRpcClient(const RpcUri &uri_p) : RpcClient(uri_p) {
}

HttpsRpcClient::~HttpsRpcClient() {
}

void HttpsRpcClient::EnsureClient() {
	if (http_client) {
		return;
	}
	if (!context) {
		throw InternalException("HttpsRpcClient::EnsureClient called without a ClientContext");
	}
	auto &http_util = HTTPUtil::Get(*context->db);
	http_params = http_util.InitializeParameters(*context, uri.Http());
	if (uri.Ssl()) {
		http_params->override_verify_ssl = true;
		http_params->verify_ssl = false;
	}
	http_params->extra_headers["Content-Type"] = "application/duckdb";
	http_client = http_util.InitializeClient(*http_params, uri.Http());
}

unique_ptr<ProtocolMessage> HttpsRpcClient::RequestInternal(unique_ptr<ProtocolMessage> request_message) {
	D_ASSERT(request_message);
	request_message->ToMemoryStream(write_stream);

	EnsureClient();

	HTTPHeaders headers;
	string url = uri.Http() + "/rpc";
	PostRequestInfo post(url, headers, *http_params, write_stream.GetData(), write_stream.GetPosition());

	auto &http_util = HTTPUtil::Get(*context->db);
	auto response = http_util.Request(post, http_client);
	if (!response) {
		throw IOException("RPC request failed: no response");
	}
	if (!response->Success()) {
		const auto &err = response->GetError();
		throw IOException("RPC request failed: %s", err.empty() ? response->reason : err);
	}

	MemoryStream non_owning_read_stream((data_ptr_t)post.buffer_out.data(), post.buffer_out.size());
	return ProtocolMessage::FromMemoryStream(non_owning_read_stream);
}

unique_ptr<RpcClient> RpcClient::GetClient(const RpcUri &uri) {
	return make_uniq<HttpsRpcClient>(uri);
}
