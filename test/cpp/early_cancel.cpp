#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "quack_fetch_collector.hpp"
#include "quack_message.hpp"
#include "quack_server.hpp"
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>

using namespace duckdb;

class TestServer : public QuackServer {
public:
	using QuackServer::HandleMessageInternal;
	using QuackServer::QuackServer;
};

static void Require(bool condition, const char *message) {
	if (!condition) {
		throw std::runtime_error(message);
	}
}

template <class Predicate>
static void WaitFor(Predicate predicate) {
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (!predicate()) {
		if (std::chrono::steady_clock::now() >= deadline) {
			// A stuck asynchronous handler must not hang the test's future destructors.
			std::cerr << "Timed out waiting for query cancellation" << std::endl;
			std::_Exit(1);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}

// QueryEnd runs after the first INSERT commits and before the next statement
// starts. Its one-shot gate also stays inert when the connection is reused.
class PauseAfterFirstStatement : public ClientContextState {
public:
	void QueryEnd() override {
		if (!paused.exchange(true)) {
			WaitFor([&] { return resume.load(); });
		}
	}

	std::atomic<bool> paused {false};
	std::atomic<bool> resume {false};
};

static void TestCancellation(bool cancel_between_statements) {
	DuckDB db(nullptr);
	ExtensionHelper::LoadExtension(db, "core_functions");
	ExtensionHelper::LoadExtension(db, "httpfs");
	ExtensionHelper::LoadExtension(db, "quack");
	Connection observer(db);
	Require(!observer.Query("CREATE TABLE sink(i INTEGER)")->HasError(), "create table failed");
	TestServer server(*observer.context, QuackUri("quack:localhost:0", false), "test-token");
	auto id = server.CreateNewConnection("cancel-test", "", 60);
	auto connection = server.GetConnection(id);

	// Pause either before Query(), or after its first SQL statement commits. Both
	// synchronization points belong to the test; no production hook is needed.
	unique_lock<mutex> pause(connection->statement_lock, std::defer_lock);
	auto between_statements = make_shared_ptr<PauseAfterFirstStatement>();
	if (cancel_between_statements) {
		connection->duckdb_connection->context->registered_state->Insert("test_cancel_pause", between_statements);
	} else {
		pause.lock();
	}
	auto uuid = UUID::GenerateRandomUUID();
	PrepareRequestMessage prepare(id, "INSERT INTO sink VALUES (1); INSERT INTO sink VALUES (2)", uuid);
	auto query = std::async(std::launch::async,
	                        [&] { return server.HandleMessageInternal(*db.instance, prepare, connection.get()); });
	shared_ptr<QuackResultStream> stream;
	WaitFor([&] {
		lock_guard<mutex> guard(connection->statement.lock);
		stream = connection->statement.stream;
		return bool(stream);
	});
	if (cancel_between_statements) {
		WaitFor([&] { return between_statements->paused.load(); });
	}
	CancelRequestMessage cancel(id, uuid);
	auto cancellation = std::async(
	    std::launch::async, [&] { return server.HandleMessageInternal(*db.instance, cancel, connection.get()); });
	WaitFor([&] { return stream->buffer.HasError(); });
	// The buffer error precedes Interrupt(). Observe both effects before releasing
	// the producer, so cancellation cannot race past the intended boundary.
	WaitFor([&] { return connection->duckdb_connection->context->IsInterrupted(); });
	if (cancel_between_statements) {
		between_statements->resume = true;
	} else {
		pause.unlock();
	}
	WaitFor([&] {
		return query.wait_for(std::chrono::seconds(0)) == std::future_status::ready &&
		       cancellation.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
	});
	Require(cancellation.get()->Type() == MessageType::SUCCESS_RESPONSE, "CANCEL failed");
	auto response = query.get();
	Require(response->Type() == MessageType::ERROR_RESPONSE, "PREPARE did not report cancellation");
	Require(response->Cast<ErrorResponse>().Error().Type() == ExceptionType::INTERRUPT,
	        "PREPARE returned an unrelated error");
	auto result = observer.Query("SELECT count(*) FROM sink");
	Require(!result->HasError(), "read failed");
	auto expected_rows = cancel_between_statements ? 1 : 0;
	Require(result->Fetch()->GetValue(0, 0).GetValue<int64_t>() == expected_rows,
	        "cancelled query executed an unexpected number of INSERTs");

	// A cancelled stream must not poison the next request on the same connection.
	PrepareRequestMessage next(id, "INSERT INTO sink VALUES (42)", UUID::GenerateRandomUUID());
	auto next_query = std::async(std::launch::async,
	                             [&] { return server.HandleMessageInternal(*db.instance, next, connection.get()); });
	WaitFor([&] { return next_query.wait_for(std::chrono::seconds(0)) == std::future_status::ready; });
	Require(next_query.get()->Type() == MessageType::PREPARE_RESPONSE, "connection could not execute the next query");
	result = observer.Query("SELECT count(*) FROM sink");
	Require(!result->HasError(), "read after connection reuse failed");
	Require(result->Fetch()->GetValue(0, 0).GetValue<int64_t>() == expected_rows + 1, "next query did not execute");
}

int main() {
	try {
		TestCancellation(false);
		TestCancellation(true);
		std::cout << "Cancellation before startup, between statements, and connection reuse passed" << std::endl;
		return 0;
	} catch (std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
