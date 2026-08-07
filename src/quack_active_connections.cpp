#include "quack_active_connections.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"

#include "quack_startstop.hpp"
#include "quack_storage.hpp"

namespace duckdb {

static string QueryStateToString(QuackQueryState state) {
	switch (state) {
	case QuackQueryState::IDLE:
		return "idle";
	case QuackQueryState::ACTIVE:
		return "active";
	case QuackQueryState::FINISHED:
		return "finished";
	case QuackQueryState::CANCELLED:
		return "cancelled";
	case QuackQueryState::QUACK_ERROR:
		return "error";
	default:
		return "unknown";
	}
}

struct QuackActiveConnectionsData : FunctionData {
	bool finished = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackActiveConnectionsData>();
		result->finished = finished;
		return result;
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

static unique_ptr<FunctionData> QuackActiveConnectionsBind(ClientContext &, TableFunctionBindInput &,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP};
	names = {"server_id", "connection_id", "query", "state", "client_id_hash", "query_started_at"};
	return make_uniq<QuackActiveConnectionsData>();
}

static void QuackActiveConnectionsScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<QuackActiveConnectionsData>();
	if (data.finished) {
		return;
	}

	auto snapshots = QuackStorageExtensionInfo::GetState(*context.db).GetActiveConnectionSnaps();

	idx_t row = 0;
	for (auto &snap : snapshots) {
		output.data[0].SetValue(row, snap.server_id);
		output.data[1].SetValue(row, snap.session_id);
		output.data[2].SetValue(row, snap.sql_query);
		output.data[3].SetValue(row, Value(QueryStateToString(snap.query_state)));
		output.data[4].SetValue(row,
		                        snap.client_id_hash.empty() ? Value(LogicalType::VARCHAR) : Value(snap.client_id_hash));
		if (snap.query_state == QuackQueryState::IDLE) {
			output.data[5].SetValue(row, Value(LogicalType::TIMESTAMP));
		} else {
			output.data[5].SetValue(row, Value::TIMESTAMP(snap.query_started_at));
		}
		row++;
	}
	output.SetChildCardinality(row);
	data.finished = true;
}

TableFunction QuacktivityFunction::GetFunction() {
	return TableFunction("quack_active_connections", {}, QuackActiveConnectionsScan, QuackActiveConnectionsBind);
}

} // namespace duckdb
