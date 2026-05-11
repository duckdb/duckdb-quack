#include "quack_activity.hpp"
#include "duckdb.hpp"
#include "duckdb/main/database.hpp"

#include "quack_startstop.hpp"
#include "quack_storage.hpp"

namespace duckdb {

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
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"connection_id", "query"};
	return make_uniq<QuacktivityData>();
}

static void QuacktivityScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<QuacktivityData>();
	if (data.finished) {
		return;
	}
	auto snapshots = QuackStorageExtensionInfo::GetState(*context.db).GetActiveConnectionSnaps();
	idx_t count = 0;
	for (auto &snap : snapshots) {
		output.SetValue(0, count, snap.session_id);
		output.SetValue(1, count, snap.sql_query);
		count++;
	}
	output.SetCardinality(count);
	data.finished = true;
}

TableFunction QuacktivityFunction::GetFunction() {
	return TableFunction("quacktivity", {}, QuacktivityScan, QuacktivityBind);
}

} // namespace duckdb
