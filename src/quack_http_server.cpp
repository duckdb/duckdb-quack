#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_uri.hpp"

#include "httplib.hpp"

using namespace duckdb;

void HttpQuackServer::Close() {
	// Stops accepting new connections and joins the listener threads (NOT the
	// httplib worker pool)
	server->stop();
	try {
		for (auto &thread : listen_threads) {
			if (thread.joinable()) {
				thread.join();
			}
		}
	} catch (std::exception &) {
	}
}

HttpQuackServer::~HttpQuackServer() {
	Close();
}

void HttpQuackServer::ListenThread(HttpQuackServer *server, const string &listen_host, int listen_port) {
	D_ASSERT(connection_id);
	D_ASSERT(server->server);
	D_ASSERT(listen_port > 1 && listen_port < 65535);
	server->server->listen(listen_host, listen_port);
}

void HttpQuackServer::Listen(const QuackUri &uri) {
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
		HandleMessage(*QuackMessage::FromMemoryStream(stream))->ToMemoryStream(stream);
		res.set_content((const char *)stream.GetData(), stream.GetPosition(), "application/duckdb");
	});

	if (!server->is_valid()) {
		throw IOException("Failed to instantiate DuckDB server at %s / %s", uri.Uri(), uri.Http());
	}

	listen_threads.push_back(std::thread(ListenThread, this, uri.Host(), uri.Port()));
}
