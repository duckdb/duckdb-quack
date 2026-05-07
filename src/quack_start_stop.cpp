#include "duckdb/main/database.hpp"

#include "quack_startstop.hpp"
#include "quack_storage.hpp"

using namespace duckdb;

struct QuackStartStopFunctionData : public TableFunctionData {
	QuackStartStopFunctionData() {
	}

	bool finished = false;
	bool auth_is_default = false;
	QuackUri listen_uri;
	string token;
};

static unique_ptr<FunctionData> QuackServeBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<QuackStartStopFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid listen string specified");
	}

	auto allow_other_hostname = input.named_parameters.find("allow_other_hostname") != input.named_parameters.end() &&
	                            input.named_parameters["allow_other_hostname"].GetValue<bool>();

	bind_data->listen_uri =
	    QuackUri(uri_value.GetValue<string>(), /* the server will always listen without SSL */ false);
	if (!allow_other_hostname && !bind_data->listen_uri.IsLocal()) {
		throw InvalidInputException(
		    "Only localhost is allowed as a Quack RPC hostname by default, set allow_other_hostname=true to override. "
		    "We strongly recommend reverse-proxying the Quack RPC when making it publicly available.");
	}

	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("listen_uri");
	names.emplace_back("listen_url");

	auto &config = DBConfig::GetConfig(context);
	Value default_auth_val;
	auto lookup_result_token = config.TryGetCurrentSetting("quack_authentication_function", default_auth_val);
	bind_data->auth_is_default =
	    lookup_result_token && !default_auth_val.IsNull() && default_auth_val.GetValue<string>() == "quack_check_token";

	if (bind_data->auth_is_default) {
		if (input.named_parameters.find("token") != input.named_parameters.end()) {
			bind_data->token = input.named_parameters["token"].GetValue<string>();
		} else {
			bind_data->token = QuackServer::GenerateRandomToken(*context.db);
		}
		QuackServer::ValidateToken(bind_data->token);
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("auth_token");
	}

	return std::move(bind_data);
}

static void QuackServe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<QuackStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}

	auto &server =
	    QuackStorageExtensionInfo::GetState(*context.db).CreateServer(context, bind_data.listen_uri, bind_data.token);
	output.SetValue(0, 0, bind_data.listen_uri.Uri());
	output.SetValue(1, 0, bind_data.listen_uri.Http());
	if (bind_data.auth_is_default) {
		output.SetValue(2, 0, bind_data.token);
	}

	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction QuackServeFunction::GetFunction() {
	auto fun = TableFunction("quack_serve", {LogicalType::VARCHAR}, QuackServe, QuackServeBind);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	fun.named_parameters["allow_other_hostname"] = LogicalType::BOOLEAN;
	fun.named_parameters["token"] = LogicalType::VARCHAR;

	return fun;
}

static unique_ptr<FunctionData> QuackStopBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
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
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction QuackStopFunction::GetFunction() {
	return TableFunction("quack_stop", {LogicalType::VARCHAR}, QuackStop, QuackStopBind);
}

struct QuackStopEachFunctionData : public TableFunctionData {
	idx_t listen_uri_col = DConstants::INVALID_INDEX;
};

static unique_ptr<FunctionData> QuackStopEachBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<QuackStopEachFunctionData>();
	for (idx_t i = 0; i < input.input_table_names.size(); i++) {
		if (StringUtil::Lower(input.input_table_names[i]) == "listen_uri") {
			if (input.input_table_types[i].id() != LogicalTypeId::VARCHAR) {
				throw BinderException("quack_stop_each: input column 'listen_uri' must be VARCHAR");
			}
			bind_data->listen_uri_col = i;
			break;
		}
	}
	if (bind_data->listen_uri_col == DConstants::INVALID_INDEX) {
		throw BinderException("quack_stop_each: input table must contain a 'listen_uri' VARCHAR column");
	}
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("listen_uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return std::move(bind_data);
}

static OperatorResultType QuackStopEach(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                       DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<QuackStopEachFunctionData>();
	auto &state = QuackStorageExtensionInfo::GetState(*context.client.db);

	output.SetCardinality(input.size());
	for (idx_t row = 0; row < input.size(); row++) {
		auto uri_val = input.GetValue(bind_data.listen_uri_col, row);
		if (uri_val.IsNull()) {
			throw InvalidInputException("quack_stop_each: listen_uri must not be NULL");
		}
		auto uri_str = uri_val.GetValue<string>();
		QuackUri listen_uri(uri_str, /* not really */ true);
		string status = state.StopServer(context.client, listen_uri)
		                    ? StringUtil::Format("Stopped listening on %s", listen_uri.Uri())
		                    : StringUtil::Format("No server found listening on %s", listen_uri.Uri());
		output.SetValue(0, row, Value(uri_str));
		output.SetValue(1, row, Value(status));
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

TableFunction QuackStopEachFunction::GetFunction() {
	TableFunction fun("quack_stop_each", {LogicalType::TABLE}, nullptr, QuackStopEachBind);
	fun.in_out_function = QuackStopEach;
	return fun;
}

struct QuackServerListFunctionData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> QuackServerListBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
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
		output.SetValue(0, row, Value(s.listen_uri));
		output.SetValue(1, row, Value(s.listen_url));
		output.SetValue(2, row, Value(s.host));
		output.SetValue(3, row, Value::USMALLINT(s.port));
		output.SetValue(4, row, Value::UBIGINT(s.active_connections));
		vector<Value> keys;
		vector<Value> values;
		keys.reserve(s.info.size());
		values.reserve(s.info.size());
		for (auto &kv : s.info) {
			keys.emplace_back(Value(kv.first));
			values.emplace_back(Value(kv.second));
		}
		output.SetValue(5, row,
		                Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values)));
		row++;
	}
	output.SetCardinality(row);
	bind_data.finished = true;
}

TableFunction QuackServerListFunction::GetFunction() {
	return TableFunction("quack_server_list", {}, QuackServerList, QuackServerListBind);
}
