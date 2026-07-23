#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_uri.hpp"

#include "httplib.hpp"

namespace duckdb {

void HttpQuackServer::StopAccepting() {
	// Closes the listening socket only. Idempotent. Safe to call from a
	// request-handler thread.
	if (is_running) {
		server->stop();
		is_running = false;
	}
}

void HttpQuackServer::Close() {
	// Stops accepting new connections AND joins the listener threads (NOT the
	// httplib worker pool). Must not be called from a worker thread — the
	// listener's exit path inside httplib joins all workers, so a worker
	// joining the listener would deadlock through that chain.
	StopAccepting();
	for (auto &thread : listen_threads) {
		if (thread.joinable()) {
			thread.join();
		}
	}
}

HttpQuackServer::~HttpQuackServer() {
	try {
		HttpQuackServer::Close();
	} catch (std::exception &) {
	}
}

void HttpQuackServer::ListenThread(HttpQuackServer *server, const string &listen_host, int listen_port) {
	D_ASSERT(server->server);
	// Ports 1 and 65535 are both valid — the privileged-port rule is a bind()
	// policy, not a range limit (and does not apply on every platform).
	D_ASSERT(listen_port >= 1 && listen_port <= 65535);
	// Socket is already bound (synchronously, in the constructor).
	// Catch everything so the listener thread never lets an exception escape — that
	// would call std::terminate and abort the host process.
	try {
		server->server->listen_after_bind();
	} catch (...) {
		server->is_running = false;
	}
}

HttpQuackServer::HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p,
                                 std::chrono::milliseconds connection_idle_timeout_p)
    : QuackServer(context_p, uri_p, token_p, connection_idle_timeout_p) {
	server = make_uniq<duckdb_httplib::Server>();

	// Each keep-alive connection holds a server thread for its lifetime.
	// We need enough threads to handle all concurrent keep-alive connections
	// (catalog clients + scan thread clients) simultaneously, otherwise requests
	// from scan thread clients can deadlock waiting for threads held by the
	// catalog clients that are in turn waiting for the scan to complete.
	server->new_task_queue = [] {
		return new duckdb_httplib::ThreadPool(128);
	};
	server->set_keep_alive_max_count(128);
	server->set_keep_alive_timeout(10);
	server->set_tcp_nodelay(true);

	server->Get("/", [=](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		res.set_content("This is a DuckDB Quack RPC endpoint. Use ATTACH 'quack:...' to connect here.\n", "text/plain");
	});

	// TODO: this is very liberal, and there might be reasonable cases to restrict to trusted domains (note, this is
	// only relevant from within a Web browser, since other actors can just ignore the CORS convention
	server->Options("/quack", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		res.set_header("Access-Control-Allow-Origin", "*");
		res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
		res.set_header("Access-Control-Allow-Headers", "*");
		res.status = 204;
	});

	server->Post("/quack", [&](const duckdb_httplib::Request &, duckdb_httplib::Response &res,
	                           const duckdb_httplib::ContentReader &content_reader) {
		res.set_header("Access-Control-Allow-Origin", "*");
		MemoryStream stream;
		content_reader([&](const char *data, size_t data_length) {
			stream.WriteData((data_ptr_t)data, data_length);
			return true;
		});
		auto response = HandleMessage(stream);
		response->ToMemoryStream(stream);
		res.set_content((const char *)stream.GetData(), stream.GetPosition(), "application/vnd.duckdb");
	});

	if (!server->is_valid()) {
		throw IOException("Failed to instantiate DuckDB server at %s / %s", uri_p.Uri(), uri_p.Http());
	}
	is_running = true;

	// Bind synchronously here so that bind() failures (e.g. EADDRINUSE)
	// propagate to the caller of quack_serve()
	if (!server->bind_to_port(uri_p.Host(), uri_p.Port())) {
		throw IOException("Failed to bind DuckDB Quack RPC server to %s (address in use, permission denied, "
		                  "or invalid host/port)",
		                  uri_p.Http());
	}

	StartConnectionReaper();
	listen_threads.push_back(std::thread(ListenThread, this, uri_p.Host(), uri_p.Port()));
}

} // namespace duckdb
