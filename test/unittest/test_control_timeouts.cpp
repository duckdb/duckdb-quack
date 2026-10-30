#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb_static_extension.h"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "quack_client.hpp"

#include <condition_variable>
#include <future>
#include <thread>

using namespace duckdb;

extern "C" int32_t duckdb_extension_httpfs_describe(duckdb_extension_descriptor *descriptor);
extern "C" int32_t duckdb_extension_quack_describe(duckdb_extension_descriptor *descriptor);

namespace {

class ControlServer {
public:
	explicit ControlServer(bool tls, bool legacy = false) : db(nullptr), con(db) {
		db.LoadStaticExtension(duckdb_extension_httpfs_describe);
		db.LoadStaticExtension(duckdb_extension_quack_describe);
		REQUIRE_FALSE(
		    con.Query(legacy ? "SET httpfs_client_implementation='httplib'" : "SET httpfs_client_implementation='curl'")
		        ->HasError());
		con.BeginTransaction();
		if (tls) {
			server = make_uniq<duckdb_httplib_openssl::SSLServer>(QUACK_TEST_CERT_DIR "/server.pem",
			                                                      QUACK_TEST_CERT_DIR "/private_key.pem");
		} else {
			server = make_uniq<duckdb_httplib_openssl::Server>();
		}
		REQUIRE(server->is_valid());
		server->new_task_queue = [] {
			return new duckdb_httplib_openssl::ThreadPool(2);
		};
		server->Post("/quack", [this](const duckdb_httplib_openssl::Request &request,
		                              duckdb_httplib_openssl::Response &response) {
			MemoryStream input(Allocator::DefaultAllocator());
			input.WriteData(reinterpret_cast<const_data_ptr_t>(request.body.data()), request.body.size());
			input.Rewind();
			BinaryDeserializer deserializer(input);
			const auto header = QuackMessage::DeserializeHeader(deserializer);
			milliseconds delay;
			bool delay_this;
			{
				std::unique_lock<std::mutex> guard(lock);
				delay = data_delay;
				delay_this = header.type == delayed_type;
				if (header.type == MessageType::DISCONNECT_MESSAGE) {
					disconnects++;
				}
				if (stall && header.type == target) {
					started = true;
					cv.notify_all();
					if (stream_body) {
						response.set_chunked_content_provider(
						    "application/vnd.duckdb", [this](size_t, duckdb_httplib_openssl::DataSink &sink) {
							    std::unique_lock<std::mutex> guard(lock);
							    if (cv.wait_for(guard, milliseconds(50), [&] { return released; })) {
								    return false;
							    }
							    guard.unlock();
							    static const string chunk(1024, 'x');
							    return sink.write(chunk.data(), chunk.size());
						    });
						return;
					}
					cv.wait(guard, [&] { return released; });
				}
			}
			if (delay_this && delay.count()) {
				std::this_thread::sleep_for(delay);
			}
			MemoryStream output(Allocator::DefaultAllocator());
			if (header.type == MessageType::CONNECTION_REQUEST) {
				ConnectionResponseMessage message("control-test-session", 1);
				message.ToMemoryStream(output);
			} else {
				SuccessResponse message;
				message.ToMemoryStream(output);
			}
			response.set_content(string(const_char_ptr_cast(output.GetData()), output.GetPosition()),
			                     "application/vnd.duckdb");
		});
		const auto port = server->bind_to_any_port("127.0.0.1");
		REQUIRE(port > 0);
		uri = make_uniq<QuackUri>("quack://127.0.0.1:" + std::to_string(port));
		uri->SetSsl(tls);
		if (tls) {
			uri->SetSslFingerprint(
			    "2B:31:F0:0A:44:D3:E8:D0:5B:82:C4:0A:31:3D:EC:F2:31:01:1E:0A:3A:37:73:6B:50:99:BC:99:84:37:BC:EA");
		}
		worker = std::thread([this] { server->listen_after_bind(); });
	}

	~ControlServer() {
		Release();
		server->stop();
		worker.join();
	}

	void Stall(MessageType type, bool body = false) {
		std::lock_guard<std::mutex> guard(lock);
		target = type;
		stall = true;
		stream_body = body;
	}

	bool WaitUntilStarted() {
		std::unique_lock<std::mutex> guard(lock);
		return cv.wait_for(guard, std::chrono::seconds(5), [&] { return started; });
	}

	void Release() {
		std::lock_guard<std::mutex> guard(lock);
		released = true;
		cv.notify_all();
	}

	void DelayData(milliseconds duration, MessageType type = MessageType::PREPARE_REQUEST) {
		std::lock_guard<std::mutex> guard(lock);
		data_delay = duration;
		delayed_type = type;
	}

	idx_t Disconnects() {
		std::lock_guard<std::mutex> guard(lock);
		return disconnects;
	}

	DuckDB db;
	Connection con;
	unique_ptr<QuackUri> uri;

private:
	unique_ptr<duckdb_httplib_openssl::Server> server;
	std::thread worker;
	std::mutex lock;
	std::condition_variable cv;
	MessageType target = MessageType::HEARTBEAT_REQUEST;
	milliseconds data_delay {0};
	MessageType delayed_type = MessageType::PREPARE_REQUEST;
	bool stall = false;
	bool stream_body = false;
	bool started = false;
	bool released = false;
	idx_t disconnects = 0;
};

unique_ptr<QuackMessage> ControlMessage(MessageType type) {
	switch (type) {
	case MessageType::CANCEL_REQUEST:
		return make_uniq<CancelRequestMessage>("control-test-session", hugeint_t(42));
	case MessageType::HEARTBEAT_REQUEST:
		return make_uniq<HeartbeatRequestMessage>("control-test-session");
	case MessageType::DISCONNECT_MESSAGE:
		return make_uniq<DisconnectMessage>("control-test-session");
	default:
		throw InternalException("not a control request");
	}
}

class DatabaseOwnedConnection : public ObjectCacheEntry {
public:
	explicit DatabaseOwnedConnection(shared_ptr<QuackClientConnection> connection_p)
	    : connection(std::move(connection_p)) {
	}
	string GetObjectType() override {
		return "quack_control_test";
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return {};
	}

private:
	shared_ptr<QuackClientConnection> connection;
};

} // namespace

TEST_CASE("Quack control requests time out independently of their caller", "[quack][control_timeout]") {
	const auto transport = GENERATE(0, 1, 2);
	ControlServer server(transport == 1, transport == 2);
	auto client = QuackClient::GetClient(*server.db.instance, *server.uri);
	auto type = MessageType::CANCEL_REQUEST;
	bool body = false;
	SECTION("cancel before headers") {
	}
	SECTION("heartbeat before headers") {
		type = MessageType::HEARTBEAT_REQUEST;
	}
	SECTION("disconnect before headers") {
		type = MessageType::DISCONNECT_MESSAGE;
	}
	SECTION("heartbeat while response keeps making progress") {
		type = MessageType::HEARTBEAT_REQUEST;
		body = true;
	}
	server.Stall(type, body);
	server.con.Interrupt();
	auto result = std::async(std::launch::async, [&] {
		return client->Request<SuccessResponse>(*server.con.context, ControlMessage(type));
	});
	const auto started = server.WaitUntilStarted();
	const auto completed = result.wait_for(milliseconds(3500)) == std::future_status::ready;
	server.Release();
	CHECK_THROWS_AS(result.get(), IOException);
	CHECK(started);
	CHECK(completed);
	CHECK(server.con.context->IsInterrupted());
}

TEST_CASE("Quack shutdown joins a stalled heartbeat within its control budget", "[quack][control_timeout]") {
	const auto transport = GENERATE(0, 1, 2);
	ControlServer server(transport == 1, transport == 2);
	server.Stall(MessageType::HEARTBEAT_REQUEST);
	auto connection = QuackClient::ConnectToServer(*server.con.context, *server.uri, "test-token", "test-client", 1);
	const auto started = server.WaitUntilStarted();
	auto stopped = std::async(std::launch::async, [&] { connection.reset(); });
	const auto completed = stopped.wait_for(milliseconds(3500)) == std::future_status::ready;
	server.Release();
	stopped.get();
	CHECK(started);
	CHECK(completed);
}

TEST_CASE("Quack control timeouts do not shorten later data requests", "[quack][control_timeout]") {
	const auto transport = GENERATE(0, 1, 2);
	ControlServer server(transport == 1, transport == 2);
	server.Stall(MessageType::CANCEL_REQUEST);
	auto client = QuackClient::GetClient(*server.db.instance, *server.uri);
	auto result = std::async(std::launch::async, [&] {
		return client->Request<SuccessResponse>(nullptr, ControlMessage(MessageType::CANCEL_REQUEST));
	});
	const auto started = server.WaitUntilStarted();
	const auto completed = result.wait_for(milliseconds(3500)) == std::future_status::ready;
	server.Release();
	CHECK_THROWS_AS(result.get(), IOException);
	REQUIRE(started);
	REQUIRE(completed);
	server.DelayData(milliseconds(2500));
	CHECK(client->Request<SuccessResponse>(
	    *server.con.context, make_uniq<PrepareRequestMessage>("control-test-session", "SELECT 1", hugeint_t(43))));
}

TEST_CASE("Quack control budget includes waiting for its client lock", "[quack][control_timeout]") {
	ControlServer server(false);
	server.Stall(MessageType::PREPARE_REQUEST);
	auto client = QuackClient::GetClient(*server.db.instance, *server.uri);
	auto data = std::async(std::launch::async, [&] {
		return client->Request<SuccessResponse>(
		    nullptr, make_uniq<PrepareRequestMessage>("control-test-session", "SELECT 1", hugeint_t(43)));
	});
	const auto started = server.WaitUntilStarted();
	auto control = std::async(std::launch::async, [&] {
		return client->Request<SuccessResponse>(nullptr, ControlMessage(MessageType::CANCEL_REQUEST));
	});
	const auto completed = control.wait_for(milliseconds(3500)) == std::future_status::ready;
	server.Release();
	CHECK(data.get());
	CHECK_THROWS_AS(control.get(), IOException);
	CHECK(started);
	CHECK(completed);
}

TEST_CASE("Quack control requests respect a shorter configured HTTP timeout", "[quack][control_timeout]") {
	const auto transport = GENERATE(0, 1, 2);
	ControlServer server(transport == 1, transport == 2);
	REQUIRE_FALSE(server.con.Query("SET quack_http_timeout=1")->HasError());
	server.Stall(MessageType::HEARTBEAT_REQUEST);
	auto client = QuackClient::GetClient(*server.db.instance, *server.uri);
	// An expired caller deadline must not prevent the maintenance request from being sent.
	server.con.context->query_deadline = 1;
	auto result = std::async(std::launch::async, [&] {
		return client->Request<SuccessResponse>(*server.con.context, ControlMessage(MessageType::HEARTBEAT_REQUEST));
	});
	const auto started = server.WaitUntilStarted();
	const auto completed = result.wait_for(milliseconds(1750)) == std::future_status::ready;
	server.Release();
	CHECK_THROWS_AS(result.get(), IOException);
	CHECK(started);
	CHECK(completed);
}

TEST_CASE("A successful heartbeat does not shorten later data requests", "[quack][control_timeout]") {
	const auto transport = GENERATE(0, 1, 2);
	ControlServer server(transport == 1, transport == 2);
	auto client = QuackClient::GetClient(*server.db.instance, *server.uri);
	REQUIRE(client->Request<SuccessResponse>(nullptr, ControlMessage(MessageType::HEARTBEAT_REQUEST)));
	server.DelayData(milliseconds(2500));
	CHECK(client->Request<SuccessResponse>(
	    *server.con.context, make_uniq<PrepareRequestMessage>("control-test-session", "SELECT 1", hugeint_t(43))));
}

TEST_CASE("Closing a database drains its heartbeat without extending database lifetime", "[quack][control_timeout]") {
	const auto transport = GENERATE(0, 1, 2);
	bool stalled = false;
	SECTION("idle connection sends disconnect during database teardown") {
	}
	SECTION("database closes with a heartbeat in flight") {
		stalled = true;
	}
	ControlServer server(transport == 1, transport == 2);
	if (stalled) {
		server.Stall(MessageType::HEARTBEAT_REQUEST);
	}
	auto client_db = make_uniq<DuckDB>(nullptr);
	client_db->LoadStaticExtension(duckdb_extension_httpfs_describe);
	client_db->LoadStaticExtension(duckdb_extension_quack_describe);
	{
		Connection con(*client_db);
		REQUIRE_FALSE(con.Query(transport == 2 ? "SET httpfs_client_implementation='httplib'"
		                                       : "SET httpfs_client_implementation='curl'")
		                  ->HasError());
		con.BeginTransaction();
		auto connection = QuackClient::ConnectToServer(*con.context, *server.uri, "test-token", "test-client", 1);
		client_db->instance->GetObjectCache().Put("quack_control_test",
		                                          make_shared_ptr<DatabaseOwnedConnection>(std::move(connection)));
	}
	weak_ptr<DatabaseInstance> lifetime = client_db->instance;
	const auto started = !stalled || server.WaitUntilStarted();
	auto stopped = std::async(std::launch::async, [&] { client_db.reset(); });
	const auto completed = stopped.wait_for(milliseconds(3500)) == std::future_status::ready;
	server.Release();
	stopped.get();
	CHECK(started);
	CHECK(completed);
	CHECK(lifetime.expired());
	if (!stalled) {
		CHECK(server.Disconnects() == 1);
	}
}

TEST_CASE("Quack control timeout can be increased for a slow peer", "[quack][control_timeout]") {
	const auto transport = GENERATE(0, 1, 2);
	ControlServer server(transport == 1, transport == 2);
	REQUIRE_FALSE(server.con.Query("SET quack_control_timeout=4")->HasError());
	server.DelayData(milliseconds(2500), MessageType::HEARTBEAT_REQUEST);
	auto client = QuackClient::GetClient(*server.db.instance, *server.uri);
	CHECK(client->Request<SuccessResponse>(nullptr, ControlMessage(MessageType::HEARTBEAT_REQUEST)));
}
