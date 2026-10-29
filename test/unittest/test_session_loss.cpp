#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb_static_extension.h"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "quack_client.hpp"
#include "quack_server.hpp"

#include <condition_variable>
#include <thread>

using namespace duckdb;

extern "C" int32_t duckdb_extension_quack_describe(duckdb_extension_descriptor *descriptor);
extern "C" int32_t duckdb_extension_httpfs_describe(duckdb_extension_descriptor *descriptor);

namespace {

class SessionTestServer : public QuackServer {
public:
	using QuackServer::HandleMessage;
	using QuackServer::QuackServer;

	void Expire(const string &id) {
		auto connection = GetConnection(id);
		REQUIRE(connection);
		annotated_lock_guard<annotated_mutex> guard(connection->lease_lock);
		connection->lease_last_renewed_at =
		    steady_clock::now() - std::chrono::seconds(connection->heartbeat_timeout_seconds + 1);
	}
};

class SessionHTTPUtil : public HTTPUtil {
public:
	explicit SessionHTTPUtil(SessionTestServer &server_p) : server(server_p) {
	}

	unique_ptr<HTTPResponse> SendRequest(BaseRequest &request, unique_ptr<HTTPClient> &) override {
		auto &post = request.Cast<PostRequestInfo>();
		MemoryStream input(Allocator::DefaultAllocator());
		input.WriteData(post.buffer_in, post.buffer_in_len);
		input.Rewind();
		BinaryDeserializer deserializer(input);
		auto header = QuackMessage::DeserializeHeader(deserializer);
		{
			lock_guard<mutex> guard(lock);
			requests++;
			if (header.type == MessageType::HEARTBEAT_REQUEST) {
				heartbeats++;
			}
			cv.notify_all();
		}
		input.Rewind();
		auto response = server.HandleMessage(input);
		if (fail_next.exchange(false)) {
			// Lose the response after the server renewed the lease. This tests transport retry without
			// racing two heartbeat intervals against the short lease used by the expiry test.
			throw IOException("temporary transport failure");
		}
		MemoryStream output(Allocator::DefaultAllocator());
		response->ToMemoryStream(output);
		post.buffer_out.assign(const_char_ptr_cast(output.GetData()), output.GetPosition());
		return make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
	}

	bool WaitForHeartbeats(idx_t count, milliseconds timeout) {
		std::unique_lock<mutex> guard(lock);
		return cv.wait_for(guard, timeout, [&] { return heartbeats >= count; });
	}

	idx_t HeartbeatCount() {
		lock_guard<mutex> guard(lock);
		return heartbeats;
	}

	SessionTestServer &server;
	atomic<idx_t> requests {0};
	atomic<bool> fail_next {false};

private:
	mutex lock;
	std::condition_variable cv;
	idx_t heartbeats = 0;
};

struct SessionFixture {
	SessionFixture() : server_db(nullptr), server_con(server_db), client_db(nullptr), con(client_db) {
		server_db.LoadStaticExtension(duckdb_extension_httpfs_describe);
		server_db.LoadStaticExtension(duckdb_extension_quack_describe);
		server = make_uniq<SessionTestServer>(*server_con.context, uri, "test-token");
		client_db.LoadStaticExtension(duckdb_extension_httpfs_describe);
		client_db.LoadStaticExtension(duckdb_extension_quack_describe);
		http = make_shared_ptr<SessionHTTPUtil>(*server);
		client_db.instance->config.SetHTTPUtil(http);
		con.BeginTransaction();
	}

	shared_ptr<QuackClientConnection> Connect(idx_t timeout = 60) {
		return QuackClient::ConnectToServer(*con.context, uri, "test-token", "", timeout);
	}

	void Heartbeat(const shared_ptr<QuackClientConnection> &connection) {
		auto client = connection->GetClient(*con.context);
		client->GetClient().Request<SuccessResponse>(nullptr,
		                                             make_uniq<HeartbeatRequestMessage>(connection->ConnectionId()));
	}

	DuckDB server_db;
	Connection server_con;
	QuackUri uri {"quack://localhost:1"};
	unique_ptr<SessionTestServer> server;
	DuckDB client_db;
	Connection con;
	shared_ptr<SessionHTTPUtil> http;
};

ErrorData CheckoutError(const shared_ptr<QuackClientConnection> &connection, ClientContext &context) {
	try {
		auto client = connection->GetClient(context);
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	return ErrorData();
}

} // namespace

TEST_CASE("Lost Quack sessions fail locally until explicitly reconnected", "[quack][session]") {
	SessionFixture fixture;
	auto connection = fixture.Connect();
	string expected;
	SECTION("expired session") {
		fixture.server->Expire(connection->ConnectionId());
		expected = "Quack session expired";
	}
	SECTION("unknown session") {
		REQUIRE(fixture.server->DisconnectConnection(connection->ConnectionId()));
		expected = "Unknown Quack session";
	}
	SECTION("cancel discovers the loss") {
		fixture.server->Expire(connection->ConnectionId());
		expected = "Quack session expired";
		CHECK_THROWS_WITH(connection->CancelQuery(hugeint_t(42)), Catch::Matchers::Contains(expected));
		// Prove CANCEL itself latched the loss, before any wrapper-based request can do it.
		REQUIRE(CheckoutError(connection, *fixture.con.context).HasError());
	}
	CHECK_THROWS_WITH(fixture.Heartbeat(connection), Catch::Matchers::Contains(expected));
	auto requests = fixture.http->requests.load();
	auto error = CheckoutError(connection, *fixture.con.context);
	REQUIRE(error.HasError());
	CHECK(error.Type() == ExceptionType::CONNECTION);
	CHECK(StringUtil::Contains(error.Message(), expected));
	CHECK(StringUtil::Contains(error.Message(), "DETACH and ATTACH"));
	CHECK_FALSE(StringUtil::Contains(error.Message(), connection->ConnectionId()));
	CHECK_THROWS(connection->CancelQuery(hugeint_t(43)));
	CHECK(fixture.http->requests == requests);
	connection.reset();
	CHECK(fixture.http->requests == requests);
	// A new connection is explicit; losing the old one never sends CONNECT or replays its request.
	auto replacement = fixture.Connect();
	CHECK_NOTHROW(fixture.Heartbeat(replacement));
}

TEST_CASE("Lost-session metadata is compatible with the existing error wire layout", "[quack][session]") {
	SessionFixture fixture;
	const string id = "session-bearer-token";
	HeartbeatRequestMessage request(id);
	MemoryStream input(Allocator::DefaultAllocator());
	request.ToMemoryStream(input);
	auto response = fixture.server->HandleMessage(input);
	REQUIRE(response->Type() == MessageType::ERROR_RESPONSE);
	MemoryStream output(Allocator::DefaultAllocator());
	response->ToMemoryStream(output);
	output.Rewind();
	BinaryDeserializer legacy(output);
	QuackMessage::DeserializeHeader(legacy);
	// Read exactly the four fields understood by existing protocol-v3 clients.
	legacy.Begin();
	auto message = legacy.ReadPropertyWithDefault<string>(1, "message");
	auto type = legacy.ReadPropertyWithDefault<string>(2, "exception_type");
	auto info = legacy.ReadPropertyWithDefault<unordered_map<string, string>>(3, "extra_info");
	auto must_invalidate = legacy.ReadPropertyWithDefault<bool>(4, "must_invalidate");
	CHECK_NOTHROW(legacy.End());
	CHECK(type == "Connection");
	CHECK_FALSE(must_invalidate);
	CHECK_FALSE(StringUtil::Contains(message, id));
	REQUIRE(info.count("quack_lost_session_id_hash") == 1);
	CHECK(info.at("quack_lost_session_id_hash").size() == 64);
	for (auto &entry : info) {
		CHECK_FALSE(StringUtil::Contains(entry.second, id));
	}
	// Old servers have no marker. The new reader must keep their errors usable, without guessing from text.
	auto old_error = make_uniq<ErrorResponse>("Invalid connection id");
	old_error->ToMemoryStream(output);
	auto decoded = QuackMessage::FromMemoryStream(output);
	CHECK(decoded->Cast<ErrorResponse>().Error().ExtraInfo().empty());
	CHECK(StringUtil::Contains(decoded->Cast<ErrorResponse>().ErrorMessage(), "Invalid connection id"));
}

TEST_CASE("A relayed inner session loss leaves the outer Quack session usable", "[quack][session]") {
	SessionFixture fixture;
	auto outer = fixture.Connect();
	auto inner = fixture.Connect();
	fixture.server->Expire(inner->ConnectionId());
	CHECK_THROWS(fixture.Heartbeat(inner));
	auto error = CheckoutError(inner, *fixture.con.context);
	REQUIRE(error.HasError());
	// A server catches the inner exception and constructs a new response from its ErrorData.
	ErrorResponse relayed(error);
	MemoryStream output(Allocator::DefaultAllocator());
	relayed.ToMemoryStream(output);
	auto decoded = QuackMessage::FromMemoryStream(output);
	{
		auto client = outer->GetClient(*fixture.con.context);
		client->GetClient().NoteError(decoded->Cast<ErrorResponse>());
	}
	CHECK_NOTHROW(fixture.Heartbeat(outer));
	CHECK_FALSE(CheckoutError(outer, *fixture.con.context).HasError());
	// quack_cancel can target another session through a healthy caller's client.
	{
		auto client = outer->GetClient(*fixture.con.context);
		CHECK_THROWS_WITH(client->GetClient().Request<SuccessResponse>(
		                      nullptr, make_uniq<CancelRequestMessage>("not-a-session", hugeint_t(0))),
		                  Catch::Matchers::Contains("Unknown Quack session"));
	}
	CHECK_NOTHROW(fixture.Heartbeat(outer));
	// A generic connection error, including an old peer's text-only session error, is not evidence of loss.
	{
		auto client = outer->GetClient(*fixture.con.context);
		client->GetClient().NoteError(ErrorResponse(ErrorData(ExceptionType::CONNECTION, "Invalid connection id")));
	}
	CHECK_NOTHROW(fixture.Heartbeat(outer));
}

TEST_CASE("The heartbeat worker stops only for confirmed session loss", "[quack][session][intraquery]") {
	SessionFixture fixture;
	auto connection = fixture.Connect(1);
	SECTION("heartbeat is the first request to discover expiry") {
		fixture.server->Expire(connection->ConnectionId());
		REQUIRE(fixture.http->WaitForHeartbeats(1, milliseconds(3000)));
		// SendRequest finishes before NoteError runs, so wait for the client to consume the response.
		auto deadline = steady_clock::now() + std::chrono::seconds(1);
		ErrorData error;
		do {
			error = CheckoutError(connection, *fixture.con.context);
			if (error.HasError()) {
				break;
			}
			std::this_thread::sleep_for(milliseconds(1));
		} while (steady_clock::now() < deadline);
		REQUIRE(error.HasError());
		CHECK(StringUtil::Contains(error.Message(), "Quack session expired"));
		// More than two maximum heartbeat intervals: no further heartbeat should reach the network.
		auto heartbeats = fixture.http->HeartbeatCount();
		CHECK_FALSE(fixture.http->WaitForHeartbeats(heartbeats + 1, milliseconds(850)));
		auto requests = fixture.http->requests.load();
		connection.reset();
		CHECK(fixture.http->requests == requests);
	}
	SECTION("a transient transport failure is retried") {
		fixture.http->fail_next = true;
		REQUIRE(fixture.http->WaitForHeartbeats(2, milliseconds(3000)));
		CHECK_FALSE(CheckoutError(connection, *fixture.con.context).HasError());
		CHECK_NOTHROW(fixture.Heartbeat(connection));
	}
}
