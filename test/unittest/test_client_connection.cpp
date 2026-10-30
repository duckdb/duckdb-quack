#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "quack_client.hpp"

#include <future>
#include <thread>

using namespace duckdb;

namespace {

struct CancelState {
	std::promise<void> started;
	std::promise<void> release;
	std::shared_future<void> released = release.get_future().share();
	atomic<idx_t> heartbeats {0};
};

class TestQuackClient : public QuackClient {
public:
	TestQuackClient(DatabaseInstance &db, const QuackUri &uri, CancelState &state_p)
	    : QuackClient(db, uri), state(state_p) {
	}

	string PostRaw(optional_ptr<ClientContext>, const_data_ptr_t, idx_t) override {
		throw InternalException("Test client only supports decoded requests");
	}

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext>, unique_ptr<QuackMessage> message,
	                                         optional_idx) override {
		if (message->Type() == MessageType::CANCEL_REQUEST) {
			state.started.set_value();
			state.released.wait();
		} else if (message->Type() == MessageType::HEARTBEAT_REQUEST) {
			state.heartbeats++;
		}
		return make_uniq<SuccessResponse>();
	}

	CancelState &state;
};

class InterruptHTTPUtil : public HTTPUtil {
public:
	unique_ptr<HTTPResponse> SendRequest(BaseRequest &request, unique_ptr<HTTPClient> &) override {
		requests++;
		caller = request.interrupt_context;
		deadline = request.deadline;
		if (caller && interrupt_requests) {
			caller->Interrupt();
			request.InterruptCheck();
		}
		MemoryStream stream(Allocator::DefaultAllocator());
		SuccessResponse response;
		response.ToMemoryStream(stream);
		request.Cast<PostRequestInfo>().buffer_out.assign(const_char_ptr_cast(stream.GetData()), stream.GetPosition());
		return make_uniq<HTTPResponse>(HTTPStatusCode::OK_200);
	}

	bool interrupt_requests = true;
	idx_t requests = 0;
	optional_ptr<ClientContext> caller;
	optional_idx deadline;
};

struct InitializeState {
	vector<shared_ptr<Logger>> loggers;
	idx_t clients_created = 0;
	bool throw_on_initialize = false;
};

class RecordingHTTPClient : public HTTPClient {
public:
	explicit RecordingHTTPClient(InitializeState &state_p) : state(state_p) {
		state.clients_created++;
	}

	void Initialize(HTTPParams &params) override {
		state.loggers.push_back(params.logger);
		if (state.throw_on_initialize) {
			throw IOException("Test HTTP client initialization failed");
		}
	}

	unique_ptr<HTTPResponse> Get(GetRequestInfo &) override {
		throw InternalException("Unexpected HTTP client request");
	}
	unique_ptr<HTTPResponse> Put(PutRequestInfo &) override {
		throw InternalException("Unexpected HTTP client request");
	}
	unique_ptr<HTTPResponse> Head(HeadRequestInfo &) override {
		throw InternalException("Unexpected HTTP client request");
	}
	unique_ptr<HTTPResponse> Delete(DeleteRequestInfo &) override {
		throw InternalException("Unexpected HTTP client request");
	}
	unique_ptr<HTTPResponse> Post(PostRequestInfo &) override {
		throw InternalException("Unexpected HTTP client request");
	}
	unique_ptr<HTTPResponse> Options(OptionsRequestInfo &) override {
		throw InternalException("Unexpected HTTP client request");
	}

private:
	InitializeState &state;
};

class ReinitializingHTTPUtil : public InterruptHTTPUtil {
public:
	unique_ptr<HTTPResponse> SendRequest(BaseRequest &request, unique_ptr<HTTPClient> &client) override {
		if (!client) {
			client = make_uniq<RecordingHTTPClient>(state);
			client->Initialize(request.params);
		}
		return InterruptHTTPUtil::SendRequest(request, client);
	}

	InitializeState state;
};

} // namespace

TEST_CASE("Quack control requests reinitialize the transport with the current logger", "[quack]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto http = make_shared_ptr<ReinitializingHTTPUtil>();
	http->interrupt_requests = false;
	db.instance->config.SetHTTPUtil(http);
	HttpsQuackClient client(*db.instance, QuackUri("quack://localhost:1"));

	client.SetRequestLogger(con.context->logger);
	REQUIRE(client.Request<SuccessResponse>(
	    *con.context, make_uniq<PrepareRequestMessage>("test-session", "SELECT 1", hugeint_t(1))));
	REQUIRE(http->state.loggers.size() == 1);
	CHECK(http->state.loggers.back() == con.context->logger);
	CHECK(http->state.clients_created == 1);

	client.SetRequestLogger(nullptr);
	REQUIRE(client.Request<SuccessResponse>(nullptr, make_uniq<HeartbeatRequestMessage>("test-session")));
	REQUIRE(http->state.loggers.size() == 2);
	CHECK_FALSE(http->state.loggers.back());
	CHECK(http->state.clients_created == 1);

	client.SetRequestLogger(con.context->logger);
	http->state.throw_on_initialize = true;
	CHECK_THROWS_AS(client.Request<SuccessResponse>(
	                    *con.context, make_uniq<PrepareRequestMessage>("test-session", "SELECT 1", hugeint_t(2))),
	                IOException);
	http->state.throw_on_initialize = false;
	REQUIRE(client.Request<SuccessResponse>(
	    *con.context, make_uniq<PrepareRequestMessage>("test-session", "SELECT 1", hugeint_t(3))));
	CHECK(http->state.clients_created == 2);
	REQUIRE(http->state.loggers.size() == 4);
	CHECK(http->state.loggers.back() == con.context->logger);
}

TEST_CASE("Quack HTTP requests preserve interruption and leave cleanup runnable", "[quack]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.BeginTransaction();
	auto http = make_shared_ptr<InterruptHTTPUtil>();
	db.instance->config.SetHTTPUtil(http);
	HttpsQuackClient client(*db.instance, QuackUri("quack://localhost:1"));
	SECTION("decoded request") {
		CHECK_THROWS_AS(client.Request<SuccessResponse>(
		                    *con.context, make_uniq<PrepareRequestMessage>("test-session", "SELECT 1", hugeint_t(1))),
		                InterruptException);
	}
	SECTION("serialized async request") {
		CHECK_THROWS_AS(client.PostRaw(*con.context, nullptr, 0), InterruptException);
	}
	CHECK(http->caller.get() == con.context.get());
	CHECK_FALSE(http->deadline.IsValid());
	CHECK(http->requests == 1);
	CHECK(con.context->IsInterrupted());
	http->interrupt_requests = false;
	CHECK(client.Request<SuccessResponse>(*con.context, make_uniq<CancelRequestMessage>("test-session", hugeint_t(1))));
	CHECK_FALSE(http->caller);
	CHECK(http->deadline.IsValid());
	CHECK(client.Request<SuccessResponse>(*con.context, make_uniq<HeartbeatRequestMessage>("test-session")));
	CHECK_FALSE(http->caller);
	CHECK(http->deadline.IsValid());
	CHECK(client.Request<SuccessResponse>(*con.context, make_uniq<DisconnectMessage>("test-session")));
	CHECK_FALSE(http->caller);
	CHECK(http->deadline.IsValid());
	CHECK(http->requests == 4);
}

TEST_CASE("A pending cancel does not block the client cache", "[quack][intraquery]") {
	DuckDB db(nullptr);
	Connection context(db);
	QuackUri uri("quack://localhost:1");
	CancelState state;
	auto started = state.started.get_future();
	auto connection = make_shared_ptr<QuackClientConnection>(
	    *db.instance, make_uniq<TestQuackClient>(*db.instance, uri, state), uri, "test-connection", 60);
	std::exception_ptr cancel_error;
	std::thread cancel([&] {
		try {
			connection->CancelQuery(hugeint_t(42));
		} catch (...) {
			cancel_error = std::current_exception();
		}
	});

	const auto cancel_started = started.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
	std::promise<void> heartbeat_done;
	auto heartbeat_result = heartbeat_done.get_future();
	std::exception_ptr heartbeat_error;
	std::thread heartbeat([&] {
		try {
			// Return another client, then check it out as the heartbeat worker does.
			connection->StoreClient(make_uniq<TestQuackClient>(*db.instance, uri, state));
			auto client = connection->GetClient(*context.context);
			client->GetClient().Request<SuccessResponse>(
			    nullptr, make_uniq<HeartbeatRequestMessage>(connection->ConnectionId()));
		} catch (...) {
			heartbeat_error = std::current_exception();
		}
		heartbeat_done.set_value();
	});

	const auto heartbeat_completed = heartbeat_result.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
	// Always unblock cancellation and join both threads before assertions, including on the old implementation.
	state.release.set_value();
	cancel.join();
	heartbeat.join();

	REQUIRE(cancel_started);
	REQUIRE_FALSE(cancel_error);
	REQUIRE_FALSE(heartbeat_error);
	REQUIRE(heartbeat_completed);
	REQUIRE(state.heartbeats == 1);
}
