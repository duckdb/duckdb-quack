#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "quack_fetch_collector.hpp"
#include "quack_message.hpp"
#include "quack_server.hpp"
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

int main() {
	try {
		DuckDB db(nullptr);
		ExtensionHelper::LoadExtension(db, "core_functions");
		ExtensionHelper::LoadExtension(db, "httpfs");
		ExtensionHelper::LoadExtension(db, "quack");
		Connection observer(db);
		Require(!observer.Query("CREATE TABLE sink(i INTEGER)")->HasError(), "create table failed");
		TestServer server(*observer.context, QuackUri("quack:localhost:0", false), "test-token");
		auto id = server.CreateNewConnection("cancel-test", "", 60);
		auto connection = server.GetConnection(id);

		// Freeze the producer before Query() without adding a production test hook. CANCEL
		// must reach its buffer error and Interrupt() before the producer is released.
		unique_lock<mutex> pause(connection->statement_lock);
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
		CancelRequestMessage cancel(id, uuid);
		auto cancellation = std::async(
		    std::launch::async, [&] { return server.HandleMessageInternal(*db.instance, cancel, connection.get()); });
		WaitFor([&] { return stream->buffer.HasError(); });
		// The buffer error precedes Interrupt(). Observe the engine flag as well so the
		// test cannot accidentally exercise cancellation after Query() has already begun.
		WaitFor([&] { return connection->duckdb_connection->context->IsInterrupted(); });
		pause.unlock();
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
		Require(result->Fetch()->GetValue(0, 0).GetValue<int64_t>() == 0, "cancelled query executed INSERTs");

		// A cancelled stream must not poison the next request on the same connection.
		PrepareRequestMessage next(id, "INSERT INTO sink VALUES (42)", UUID::GenerateRandomUUID());
		auto next_query = std::async(
		    std::launch::async, [&] { return server.HandleMessageInternal(*db.instance, next, connection.get()); });
		WaitFor([&] { return next_query.wait_for(std::chrono::seconds(0)) == std::future_status::ready; });
		Require(next_query.get()->Type() == MessageType::PREPARE_RESPONSE,
		        "connection could not execute the next query");
		result = observer.Query("SELECT count(*) FROM sink");
		Require(!result->HasError(), "read after connection reuse failed");
		Require(result->Fetch()->GetValue(0, 0).GetValue<int64_t>() == 1, "next query did not execute");
		std::cout << "Early cancellation and connection reuse passed" << std::endl;
		return 0;
	} catch (std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
