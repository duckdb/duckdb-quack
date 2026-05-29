#include "quack_cancel.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "quack_startstop.hpp"
#include "quack_storage.hpp"
#include "quack_client.hpp"
#include "quack_message.hpp"
#include "quack_uri.hpp"
#include "duckdb.hpp"

namespace duckdb {

struct QuackCancelBindData : FunctionData {
	string target_connection_id;
	string server_uri;
	bool finished = false;

	explicit QuackCancelBindData(string connection_id_p) : target_connection_id(std::move(connection_id_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackCancelBindData>(target_connection_id);
		result->finished = finished;
		return result;
	}
	bool Equals(const FunctionData &other_p) const override {
		return target_connection_id == other_p.Cast<QuackCancelBindData>().target_connection_id;
	}
};

static unique_ptr<FunctionData> QuackCancelBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull() || input.inputs[2].IsNull()) {
		throw BinderException("quack_cancel arguments cannot be NULL");
	}
	auto server_uri_str = input.inputs[0].GetValue<string>();
	auto target_connection_id = input.inputs[1].GetValue<string>();
	auto token = input.inputs[2].GetValue<string>();

	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN};
	names = {"connection_id", "cancelled"};

	auto initial_uri = QuackUri(server_uri_str);
	auto enable_ssl = !initial_uri.IsLocal();
	if (input.named_parameters.find("disable_ssl") != input.named_parameters.end()) {
		enable_ssl = !input.named_parameters["disable_ssl"].GetValue<bool>();
	}
	auto server_uri = QuackUri(server_uri_str, enable_ssl);

	// Connect to server — validates token via CONNECTION_REQUEST auth function
	auto client_connection = QuackClient::ConnectToServer(context, server_uri, token);
	auto client_wrapper = client_connection->GetClient(context);
	auto &client = client_wrapper->GetClient();

	// Target connection goes in the header; zero UUID = cancel whatever is running
	client.Request<CancelResponseMessage>(context,
	                                      make_uniq<CancelRequestMessage>(target_connection_id, hugeint_t {0, 0}));

	return make_uniq<QuackCancelBindData>(std::move(target_connection_id));
}

static void QuackCancelScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<QuackCancelBindData>();
	if (data.finished) {
		return;
	}
	data.finished = true;
	output.SetValue(0, 0, data.target_connection_id);
	output.SetValue(1, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
}

TableFunction QuackCancelFunction::GetFunction() {
	auto fun = TableFunction("quack_cancel", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         QuackCancelScan, QuackCancelBind);
	fun.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	return fun;
}

} // namespace duckdb
