#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb_static_extension.h"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "quack_client.hpp"
#include "quack_server.hpp"

using namespace duckdb;

extern "C" int32_t duckdb_extension_httpfs_describe(duckdb_extension_descriptor *descriptor);
extern "C" int32_t duckdb_extension_quack_describe(duckdb_extension_descriptor *descriptor);

namespace {

class LoopbackQuackServer : public QuackServer {
public:
	using QuackServer::HandleMessage;
	using QuackServer::QuackServer;
};

enum class CancelRequest { COMMIT, PREPARE_STREAM, SEND, TERMINAL, FETCH };

class LoopbackHTTPUtil : public HTTPUtil {
public:
	explicit LoopbackHTTPUtil(LoopbackQuackServer &server_p) : server(server_p) {
	}

	unique_ptr<HTTPResponse> SendRequest(BaseRequest &request, unique_ptr<HTTPClient> &) override {
		auto &post = request.Cast<PostRequestInfo>();
		MemoryStream input(Allocator::DefaultAllocator());
		input.WriteData(post.buffer_in, post.buffer_in_len);
		input.Rewind();
		BinaryDeserializer deserializer(input);
		auto header = QuackMessage::DeserializeHeader(deserializer);
		auto message = QuackMessage::DeserializeMessage(deserializer, std::move(header));
		if (message->Type() == MessageType::PREPARE_REQUEST &&
		    message->Cast<PrepareRequestMessage>().Query() == "ROLLBACK") {
			rollbacks++;
		}
		input.Rewind();
		auto response = server.HandleMessage(input);
		if (armed.load() && Matches(*message) && armed.exchange(false)) {
			triggered = true;
			had_caller = bool(request.interrupt_context);
			caller->Interrupt();
			request.InterruptCheck();
		}
		auto raw = response->RawPayload();
		if (raw) {
			auto start = response->RawPayloadStart();
			post.buffer_out.assign(const_char_ptr_cast(raw->GetData()) + start, raw->GetPosition() - start);
		} else {
			MemoryStream output(Allocator::DefaultAllocator());
			response->ToMemoryStream(output);
			post.buffer_out.assign(const_char_ptr_cast(output.GetData()), output.GetPosition());
		}
		return make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
	}

	bool Matches(QuackMessage &message) const {
		switch (target) {
		case CancelRequest::COMMIT:
			return message.Type() == MessageType::PREPARE_REQUEST &&
			       message.Cast<PrepareRequestMessage>().Query() == "COMMIT";
		case CancelRequest::PREPARE_STREAM:
			return message.Type() == MessageType::PREPARE_REQUEST &&
			       StringUtil::Contains(message.Cast<PrepareRequestMessage>().Query(), "scan_data_from_quack_client");
		case CancelRequest::SEND:
			return message.Type() == MessageType::SEND_DATA_REQUEST &&
			       !message.Cast<SendDataRequestMessage>().TotalBatches().IsValid();
		case CancelRequest::TERMINAL:
			return message.Type() == MessageType::SEND_DATA_REQUEST &&
			       message.Cast<SendDataRequestMessage>().TotalBatches().IsValid();
		case CancelRequest::FETCH:
			return message.Type() == MessageType::FETCH_REQUEST &&
			       message.Cast<FetchRequestMessage>().QueryUUID() != hugeint_t(0);
		default:
			return false;
		}
	}

	LoopbackQuackServer &server;
	optional_ptr<ClientContext> caller;
	CancelRequest target = CancelRequest::COMMIT;
	atomic<bool> armed {false};
	atomic<bool> triggered {false};
	atomic<bool> had_caller {false};
	atomic<idx_t> rollbacks {0};
};

unique_ptr<QueryResult> QueryOk(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	INFO((result->HasError() ? result->GetError() : string()));
	REQUIRE_FALSE(result->HasError());
	return result;
}

int64_t Scalar(Connection &con, const string &sql) {
	auto result = QueryOk(con, sql);
	REQUIRE(result->RowCount() == 1);
	for (auto &row : *result) {
		return row.GetValue<int64_t>(0);
	}
	throw InternalException("Expected one result row");
}

} // namespace

TEST_CASE("Quack cancellation preserves transaction finalization and connection reuse", "[quack][intraquery]") {
	DuckDB server_db(nullptr);
	server_db.LoadStaticExtension(duckdb_extension_quack_describe);
	Connection server_con(server_db);
	LoopbackQuackServer server(*server_con.context, QuackUri("quack://localhost:1"), "test-token");
	DuckDB client_db(nullptr);
	client_db.LoadStaticExtension(duckdb_extension_httpfs_describe);
	client_db.LoadStaticExtension(duckdb_extension_quack_describe);
	Connection con(client_db);
	auto http = make_shared_ptr<LoopbackHTTPUtil>(server);
	http->caller = con.context.get();
	client_db.instance->config.SetHTTPUtil(http);
	QueryOk(con, "ATTACH 'quack:localhost:1' AS remote (TOKEN 'test-token')");
	QueryOk(con, "CREATE TABLE remote.items(i INTEGER)");
	QueryOk(con, "CREATE TABLE source AS SELECT i FROM range(10000) t(i)");
	string statement;
	bool interrupted = false;
	idx_t expected_rows = 0;
	SECTION("explicit commit") {
		QueryOk(con, "BEGIN");
		QueryOk(con, "INSERT INTO remote.items VALUES (42)");
		http->target = CancelRequest::COMMIT;
		statement = "COMMIT";
		expected_rows = 1;
	}
	SECTION("terminal upload") {
		http->target = CancelRequest::TERMINAL;
		statement = "INSERT INTO remote.items SELECT i FROM source";
		expected_rows = 10000;
	}
	SECTION("autocommit CTAS rolls back after cancellation") {
		http->target = CancelRequest::PREPARE_STREAM;
		statement = "CREATE TABLE remote.aborted AS SELECT i FROM source";
		interrupted = true;
	}
	SECTION("async upload") {
		http->target = CancelRequest::SEND;
		statement = "INSERT INTO remote.items SELECT i FROM source";
		interrupted = true;
	}
	SECTION("async fetch") {
		QueryOk(server_con, "INSERT INTO items SELECT i FROM range(10000) t(i)");
		QueryOk(server_con, "SET quack_prepare_inline_rows=0");
		http->target = CancelRequest::FETCH;
		statement = "SELECT * FROM remote.items";
		expected_rows = 10000;
		interrupted = true;
	}
	INFO(statement);
	http->armed = true;
	auto result = con.Query(statement);
	CHECK(http->triggered);
	CHECK(http->had_caller);
	if (interrupted) {
		REQUIRE(result->HasError());
		CHECK(result->GetErrorObject().Type() == ExceptionType::INTERRUPT);
	} else {
		INFO((result->HasError() ? result->GetError() : string()));
		REQUIRE_FALSE(result->HasError());
	}
	CHECK(Scalar(con, "SELECT count(*) FROM remote.items") == NumericCast<int64_t>(expected_rows));
	if (http->target == CancelRequest::PREPARE_STREAM) {
		CHECK(http->rollbacks == 1);
		CHECK(Scalar(server_con, "SELECT count(*) FROM duckdb_tables() WHERE table_name='aborted'") == 0);
	}
}
