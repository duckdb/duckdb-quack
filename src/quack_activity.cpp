#include "quack_activity.hpp"
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
	default:
		return "unknown";
	}
}

struct QuacktivityData : FunctionData {
	bool finished = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuacktivityData>();
		result->finished = finished;
		return result;
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

static unique_ptr<FunctionData> QuacktivityBind(ClientContext &, TableFunctionBindInput &,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP};
	names = {"connection_id", "query", "state", "query_started_at"};
	return make_uniq<QuacktivityData>();
}

static void QuacktivityScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<QuacktivityData>();
	if (data.finished) {
		return;
	}

	auto snapshots = QuackStorageExtensionInfo::GetState(*context.db).GetActiveConnectionSnaps();

	idx_t row = 0;
	for (auto &snap : snapshots) {
		output.SetValue(0, row, snap.session_id);
		output.SetValue(1, row, snap.sql_query);
		output.SetValue(2, row, Value(QueryStateToString(snap.query_state)));
		if (snap.query_state == QuackQueryState::IDLE) {
			output.SetValue(3, row, Value(LogicalType::TIMESTAMP));
		} else {
			output.SetValue(3, row, Value::TIMESTAMP(snap.query_started_at));
		}
		row++;
	}
	output.SetCardinality(row);
	data.finished = true;
}

TableFunction QuacktivityFunction::GetFunction() {
	return TableFunction("quacktivity", {}, QuacktivityScan, QuacktivityBind);
}

} // namespace duckdb
