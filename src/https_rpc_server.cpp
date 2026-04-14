#include "rpc_server.hpp"
#include "message.hpp"
#include "rpc_uri.hpp"

#include "ssl_key_generator.hpp"

#include "duckdb/common/serializer/memory_stream.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

using namespace duckdb;

HttpsRpcServer::~HttpsRpcServer() {
	server->stop();
	try {
		for (auto &thread : listen_threads) {
			thread.join();
		}
	} catch (std::exception &) {
	}
}

void HttpsRpcServer::ListenThread(HttpsRpcServer *rpc_server, const string &listen_host, int listen_port) {
	D_ASSERT(rpc_server);
	D_ASSERT(rpc_server->server);
	D_ASSERT(listen_port > 1 && listen_port < 65535);
	rpc_server->server->listen(listen_host, listen_port);
}

void HttpsRpcServer::Listen(const RpcUri &uri) {
	if (uri.Ssl()) {
		auto &fs = FileSystem::GetFileSystem(*db);
		// TODO make this configurable
		auto certificate_directory = SslKeyGenerator::GetDefaultCertificateDirectory(fs);
		auto server_key_file = fs.JoinPath(certificate_directory, "server.pem");
		auto private_key_file = fs.JoinPath(certificate_directory, "private_key.pem");
		if (!fs.FileExists(server_key_file) || !fs.FileExists(private_key_file)) {
			SslKeyGenerator::GenerateSslKeys(server_key_file, private_key_file, "", 3650);
		}
		// auto dh_param_file = fs.JoinPath(certificate_directory, "dh.pem");
		server = make_uniq<duckdb_httplib_openssl::SSLServer>(server_key_file.c_str(), private_key_file.c_str());
	} else {
		server = make_uniq<duckdb_httplib_openssl::Server>();
	}

	// Each keep-alive connection holds a server thread for its lifetime.
	// We need enough threads to handle all concurrent keep-alive connections
	// (catalog clients + scan thread clients) simultaneously, otherwise requests
	// from scan thread clients can deadlock waiting for threads held by the
	// catalog clients that are in turn waiting for the scan to complete.
	server->new_task_queue = [] {
		return new duckdb_httplib_openssl::ThreadPool(128);
	};

	server->Get("/", [=](const duckdb_httplib_openssl::Request &req, duckdb_httplib_openssl::Response &res) {
		res.set_content("This is a DuckDB Quack RPC endpoint. Use ATTACH 'quack:...' to connect here.\n", "text/plain");
	});

	// TODO: this is very liberal, and there might be reasonable cases to restrict to trusted domains (note, this is only
	// relevant from within a Web browser, since other actors can just ignore the CORS convention
	server->Options("/rpc", [](const duckdb_httplib_openssl::Request &, duckdb_httplib_openssl::Response &res) {
		res.set_header("Access-Control-Allow-Origin", "*");
		res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
		res.set_header("Access-Control-Allow-Headers", "*");
		res.status = 204;
	});

	server->Post("/rpc", [&](const duckdb_httplib_openssl::Request &req, duckdb_httplib_openssl::Response &res,
	                         const duckdb_httplib_openssl::ContentReader &content_reader) {
		MemoryStream stream;
		content_reader([&](const char *data, size_t data_length) {
			stream.WriteData((data_ptr_t)data, data_length);
			return true;
		});
		HandleMessage(*ProtocolMessage::FromMemoryStream(stream))->ToMemoryStream(stream);
		res.set_content((const char *)stream.GetData(), stream.GetPosition(), "application/duckdb");
		res.set_header("Access-Control-Allow-Origin", "*");
	});

	if (!server->is_valid()) {
		throw IOException("Failed to instantiate DuckDB server at %s / %s", uri.Uri(), uri.Http());
	}

	listen_threads.push_back(std::thread(ListenThread, this, uri.Host(), uri.Port()));
}
