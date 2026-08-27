#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

#include "quack_random.hpp"
#include "quack_secret.hpp"
#include "quack_startstop.hpp"
#include "quack_storage.hpp"

using namespace duckdb;

struct QuackStartStopFunctionData : public TableFunctionData {
	QuackStartStopFunctionData() {
	}

	bool finished = false;
	QuackUri listen_uri;
	string token;
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

	unique_ptr<SecretEntry> secret;
	if (!has_token) {
		// An explicit name selects the secret; without one we use the default secret for this endpoint (if any).
		// Without a URI the secret decides where we listen, so a lone secret counts as the default one.
		secret = QuackSecret::Find(context, has_secret_name ? &secret_entry->second : nullptr, listen_uri,
		                           explicit_uri ? QuackSecret::DefaultLookup::SCOPE_MATCH_ONLY
		                                        : QuackSecret::DefaultLookup::ALLOW_SINGLE_SECRET);
	}
	if (secret && !explicit_uri) {
		// No URI was given: a fully-qualified secret scope tells us where to listen.
		string secret_endpoint;
		if (QuackSecret::TryGetEndpoint(*secret, secret_endpoint)) {
			listen_uri = secret_endpoint;
		}
	}

	bind_data->listen_uri = QuackUri(listen_uri, /* the server will always listen without SSL */ false);
	if (!allow_other_hostname && !bind_data->listen_uri.IsLocal()) {
		throw InvalidInputException(
		    "Only localhost is allowed as a Quack RPC hostname by default, set allow_other_hostname=true to override. "
		    "We strongly recommend reverse-proxying the Quack RPC when making it publicly available.");
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

	auto &server =
	    QuackStorageExtensionInfo::GetState(*context.db).CreateServer(context, bind_data.listen_uri, bind_data.token);
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
	fun.named_parameters["allow_other_hostname"] = LogicalType::BOOLEAN;
	fun.named_parameters["token"] = LogicalType::VARCHAR;
	fun.named_parameters["secret"] = LogicalType::VARCHAR;
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
