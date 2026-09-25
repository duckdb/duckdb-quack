#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"

#include "quack_random.hpp"
#include "quack_secret.hpp"
#include "quack_ssl_key_generator.hpp"
#include "quack_startstop.hpp"
#include "quack_storage.hpp"

using namespace duckdb;

struct QuackStartStopFunctionData : public TableFunctionData {
	QuackStartStopFunctionData() {
	}

	bool finished = false;
	QuackUri listen_uri;
	string token;
	//! Persist the token as the default quack secret once the server is up
	bool create_secret = false;
	//! PEM files for HTTPS; both empty means the default self-signed pair, generated on first use
	string ssl_cert_file;
	string ssl_key_file;
};

static unique_ptr<FunctionData> QuackServeBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
#ifdef __EMSCRIPTEN__
	throw NotImplementedException("quack_serve is currently not implemented for the wasm platform, consider connecting "
	                              "to already available endpoint");
#endif

	auto bind_data = make_uniq<QuackStartStopFunctionData>();
	bool explicit_uri = !input.inputs.empty();
	string listen_uri = "quack:localhost";
	if (explicit_uri) {
		auto &uri_value = input.inputs[0];
		if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
			throw InvalidInputException("Invalid listen string specified");
		}
		listen_uri = uri_value.GetValue<string>();
	}

	auto allow_other_hostname = input.named_parameters.find("allow_other_hostname") != input.named_parameters.end() &&
	                            input.named_parameters["allow_other_hostname"].GetValue<bool>();

	// Every server has a token: either user-supplied, taken from a secret, or auto-generated. The
	// authn callback (default token-check or a user-defined function) decides what to do with it;
	// the server itself doesn't care which path is in use.
	auto token_entry = input.named_parameters.find("token");
	auto secret_entry = input.named_parameters.find("secret");
	bool has_token = token_entry != input.named_parameters.end();
	bool has_secret_name = secret_entry != input.named_parameters.end();
	if (has_token && has_secret_name) {
		throw InvalidInputException("Cannot specify both token and secret - the secret supplies the token");
	}

	// An explicit name selects the secret; without one we use the default secret for this endpoint (if any).
	// Without a URI the secret decides where we listen, so a lone secret counts as the default one.
	auto secret = QuackSecret::Find(context, has_secret_name ? &secret_entry->second : nullptr, listen_uri,
	                                explicit_uri ? QuackSecret::DefaultLookup::SCOPE_MATCH_ONLY
	                                             : QuackSecret::DefaultLookup::ALLOW_SINGLE_SECRET);

	// Nothing to fall back on next time: persist the token we are about to use, if we are asked to.
	auto create_secret_entry = input.named_parameters.find("create_secret_if_not_exists");
	bind_data->create_secret = create_secret_entry != input.named_parameters.end() &&
	                           !create_secret_entry->second.IsNull() && create_secret_entry->second.GetValue<bool>() &&
	                           !has_secret_name && !secret;
	if (secret && !explicit_uri) {
		// No URI was given: a fully-qualified secret scope tells us where to listen.
		string secret_endpoint;
		if (QuackSecret::TryGetEndpoint(*secret, secret_endpoint)) {
			listen_uri = secret_endpoint;
		}
	}

	// Mirrors the client default: plain HTTP on localhost, HTTPS elsewhere, disable_ssl overrides either way
	auto initial_uri = QuackUri(listen_uri);
	auto enable_ssl = !initial_uri.IsLocal();
	auto disable_ssl_entry = input.named_parameters.find("disable_ssl");
	if (disable_ssl_entry != input.named_parameters.end() && !disable_ssl_entry->second.IsNull()) {
		enable_ssl = !disable_ssl_entry->second.GetValue<bool>();
	}
	bind_data->listen_uri = QuackUri(listen_uri, enable_ssl);
	if (!allow_other_hostname && !bind_data->listen_uri.IsLocal()) {
		throw InvalidInputException(
		    "Only localhost is allowed as a Quack RPC hostname by default, set allow_other_hostname=true to override. "
		    "We strongly recommend reverse-proxying the Quack RPC when making it publicly available.");
	}

	auto cert_entry = input.named_parameters.find("ssl_cert_file");
	auto key_entry = input.named_parameters.find("ssl_key_file");
	bool has_cert = cert_entry != input.named_parameters.end() && !cert_entry->second.IsNull();
	bool has_key = key_entry != input.named_parameters.end() && !key_entry->second.IsNull();
	if (has_cert != has_key) {
		throw InvalidInputException("ssl_cert_file and ssl_key_file must be specified together");
	}
	if (has_cert) {
		if (!enable_ssl) {
			throw InvalidInputException("ssl_cert_file and ssl_key_file only apply when serving over HTTPS");
		}
		bind_data->ssl_cert_file = cert_entry->second.GetValue<string>();
		bind_data->ssl_key_file = key_entry->second.GetValue<string>();
		auto &fs = FileSystem::GetFileSystem(context);
		for (auto &file : {bind_data->ssl_cert_file, bind_data->ssl_key_file}) {
			if (file.empty() || !fs.FileExists(file)) {
				throw InvalidInputException("TLS file \"%s\" does not exist", file);
			}
		}
	}

	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("listen_uri");
	names.emplace_back("listen_url");
	names.emplace_back("auth_token");

	if (has_token) {
		bind_data->token = token_entry->second.GetValue<string>();
	} else if (secret) {
		bind_data->token = QuackSecret::GetToken(*secret);
	} else {
		bind_data->token = QuackRandomToken(*context.db);
	}
	// Validate at bind-time: a length error here fails before the listener
	// thread is spawned, instead of leaving a half-built server behind.
	QuackServer::ValidateToken(bind_data->token);

	return std::move(bind_data);
}

static void QuackServe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<QuackStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}

	auto &server = QuackStorageExtensionInfo::GetState(*context.db)
	                   .CreateServer(context, bind_data.listen_uri, bind_data.token, bind_data.ssl_cert_file,
	                                 bind_data.ssl_key_file);
	if (bind_data.create_secret) {
		// only once the server is actually up: a token that never made it onto a socket is not worth persisting
		QuackSecret::CreateDefault(context, bind_data.token);
	}
	auto &actual_uri = server.ListenUri();
	output.data[0].SetValue(0, actual_uri.Uri());
	output.data[1].SetValue(0, actual_uri.Http());
	output.data[2].SetValue(0, bind_data.token);

	output.SetChildCardinality(1);
	bind_data.finished = true;
}

TableFunctionSet QuackServeFunction::GetFunction() {
	TableFunctionSet set("quack_serve");
	auto fun = TableFunction("quack_serve", {LogicalType::VARCHAR}, QuackServe, QuackServeBind);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.named_parameters["ssl_cert_file"] = LogicalType::VARCHAR;
	fun.named_parameters["ssl_key_file"] = LogicalType::VARCHAR;
	fun.named_parameters["allow_other_hostname"] = LogicalType::BOOLEAN;
	fun.named_parameters["token"] = LogicalType::VARCHAR;
	fun.named_parameters["secret"] = LogicalType::VARCHAR;
	fun.named_parameters["create_secret_if_not_exists"] = LogicalType::BOOLEAN;
	set.AddFunction(fun);
	fun.arguments.clear();
	set.AddFunction(fun);

	return set;
}

static unique_ptr<FunctionData> QuackStopBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<QuackStartStopFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid listen string specified");
	}
	bind_data->listen_uri =
	    QuackUri(uri_value.GetValue<string>(), /* not really, but we don't want to ask the user again */ true);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");

	return std::move(bind_data);
}

static void QuackStop(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<QuackStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}
	auto &state = QuackStorageExtensionInfo::GetState(*context.db);
	if (state.StopServer(context, bind_data.listen_uri)) {
		output.data[0].SetValue(0, StringUtil::Format("Stopped listening on %s", bind_data.listen_uri.Uri()));
	} else {
		output.data[0].SetValue(0, StringUtil::Format("No server found listening on %s", bind_data.listen_uri.Uri()));
	}
	output.SetChildCardinality(1);
	bind_data.finished = true;
}

TableFunction QuackStopFunction::GetFunction() {
	return TableFunction("quack_stop", {LogicalType::VARCHAR}, QuackStop, QuackStopBind);
}

struct QuackServerListFunctionData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> QuackServerListBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("listen_uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("listen_url");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("host");
	return_types.emplace_back(LogicalType::USMALLINT);
	names.emplace_back("port");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("active_connections");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("info");
	return make_uniq<QuackServerListFunctionData>();
}

static void QuackServerList(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<QuackServerListFunctionData>();
	if (bind_data.finished) {
		return;
	}
	auto snapshots = QuackStorageExtensionInfo::GetState(*context.db).ListServers();
	idx_t row = 0;
	for (auto &s : snapshots) {
		output.data[0].SetValue(row, Value(s.listen_uri));
		output.data[1].SetValue(row, Value(s.listen_url));
		output.data[2].SetValue(row, Value(s.host));
		output.data[3].SetValue(row, Value::USMALLINT(s.port));
		output.data[4].SetValue(row, Value::UBIGINT(s.active_connections));
		vector<Value> keys;
		vector<Value> values;
		keys.reserve(s.info.size());
		values.reserve(s.info.size());
		for (auto &kv : s.info) {
			keys.emplace_back(Value(kv.first));
			values.emplace_back(Value(kv.second));
		}
		output.data[5].SetValue(
		    row, Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values)));
		row++;
	}
	output.SetChildCardinality(row);
	bind_data.finished = true;
}

TableFunction QuackServerListFunction::GetFunction() {
	return TableFunction("quack_server_list", {}, QuackServerList, QuackServerListBind);
}

struct QuackGenerateKeysFunctionData : public TableFunctionData {
	QuackGenerateKeysFunctionData() {
	}

	bool finished = false;
	string certificate_directory;
};

static unique_ptr<FunctionData> QuackGenerateKeysBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<QuackGenerateKeysFunctionData>();
	auto directory_entry = input.named_parameters.find("directory");
	if (directory_entry != input.named_parameters.end() && !directory_entry->second.IsNull()) {
		result->certificate_directory = directory_entry->second.GetValue<string>();
		if (result->certificate_directory.empty()) {
			throw InvalidInputException("Invalid certificate directory specified");
		}
	}
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("cert_file");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("key_file");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("fingerprint");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return std::move(result);
}

static void QuackGenerateKeysFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<QuackGenerateKeysFunctionData>();
	if (bind_data.finished) {
		return;
	}
	output.SetCardinality(1);
	bind_data.finished = true;

	auto &fs = FileSystem::GetFileSystem(context);
	auto certificate_directory = bind_data.certificate_directory;
	if (certificate_directory.empty()) {
		certificate_directory =
		    SslKeyGenerator::GetDefaultCertificateDirectory(fs, ClientData::Get(context).file_opener.get());
	} else if (!fs.DirectoryExists(certificate_directory)) {
		fs.CreateDirectoriesRecursive(certificate_directory);
	}

	auto server_key_file = SslKeyGenerator::GetDefaultCertificateFile(fs, certificate_directory);
	auto private_key_file = SslKeyGenerator::GetDefaultPrivateKeyFile(fs, certificate_directory);
	output.data[0].SetValue(0, server_key_file);
	output.data[1].SetValue(0, private_key_file);

	if (fs.FileExists(server_key_file) || fs.FileExists(private_key_file)) {
		output.data[2].SetValue(0, fs.FileExists(server_key_file)
		                               ? Value(SslKeyGenerator::CertificateFingerprint(server_key_file))
		                               : Value());
		output.data[3].SetValue(
		    0, StringUtil::Format("Key file(s) exist in %s - remove to recreate them", certificate_directory));
		return;
	}
	SslKeyGenerator::GenerateSslKeys(server_key_file, private_key_file, "", SslKeyGenerator::DEFAULT_DAYS_VALID);
	SslKeyGenerator::RestrictPermissions(private_key_file);

	output.data[2].SetValue(0, SslKeyGenerator::CertificateFingerprint(server_key_file));
	output.data[3].SetValue(0, StringUtil::Format("Key files generated in %s", certificate_directory));
}

TableFunction QuackGenerateKeysFunction::GetFunction() {
	auto fun = TableFunction("quack_generate_keys", {}, QuackGenerateKeysFun, QuackGenerateKeysBind);
	fun.named_parameters["directory"] = LogicalType::VARCHAR;
	return fun;
}
