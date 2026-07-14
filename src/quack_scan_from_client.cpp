#include "quack_scan_from_client.hpp"

#include "duckdb/common/enums/order_preservation_type.hpp"
#include "duckdb/common/enums/task_scheduler_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/partition_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include "quack_data_stream.hpp"

namespace duckdb {

class QuackWaitForChunkTask : public AsyncTask {
public:
	explicit QuackWaitForChunkTask(shared_ptr<QuackDataStream> stream_p) : stream(std::move(stream_p)) {
	}
	void Execute() override {
		stream->WaitForData();
	}

private:
	shared_ptr<QuackDataStream> stream;
};

struct QuackScanFromClientBindData : public FunctionData {
	string stream_id;
	shared_ptr<QuackDataStream> stream;
	vector<LogicalType> types;

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackScanFromClientBindData>();
		return other.stream_id == stream_id;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackScanFromClientBindData>();
		result->stream_id = stream_id;
		result->stream = stream;
		result->types = types;
		return std::move(result);
	}
};

struct QuackScanFromClientGlobalState : public GlobalTableFunctionState {
	idx_t num_threads = 1;
	idx_t MaxThreads() const override {
		return num_threads;
	}
};

struct QuackScanFromClientLocalState : public LocalTableFunctionState {
	idx_t current_batch_index = 0;
	vector<unique_ptr<DataChunk>> batch_buffer;
	size_t batch_pos = 0;
	unique_ptr<DataChunk> current_chunk; // keeps the referenced chunk alive between scan calls
};

static unique_ptr<FunctionData> QuackScanFromClientBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto stream_id = input.inputs[0].GetValue<string>();
	auto stream = QuackStreamRegistry::Get().Find(stream_id);
	if (!stream) {
		throw InvalidInputException("scan_data_from_quack_client: no active stream '%s' (this is an internal "
		                            "function driven by the quack server)",
		                            stream_id);
	}

	// Unordered streams signal NO_ORDER so the planner picks PhysicalInsert(parallel=true) instead of
	// PhysicalBatchInsert. Mutates a query-scoped by-value copy — never touches the global catalog entry.
	if (!stream->IsOrdered()) {
		input.table_function.order_preservation_type = OrderPreservationType::NO_ORDER;
	}

	auto bind_data = make_uniq<QuackScanFromClientBindData>();
	bind_data->stream_id = std::move(stream_id);
	bind_data->types = stream->Types();
	bind_data->stream = std::move(stream);

	for (idx_t i = 0; i < bind_data->types.size(); i++) {
		return_types.push_back(bind_data->types[i]);
		names.push_back("col" + to_string(i));
	}
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> QuackScanFromClientInitGlobal(ClientContext &context,
                                                                          TableFunctionInitInput &input) {
	auto state = make_uniq<QuackScanFromClientGlobalState>();
	state->num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	return std::move(state);
}

static unique_ptr<LocalTableFunctionState>
QuackScanFromClientInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *) {
	return make_uniq<QuackScanFromClientLocalState>();
}

static void QuackScanFromClient(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<QuackScanFromClientBindData>();
	auto &local_state = input.local_state->Cast<QuackScanFromClientLocalState>();
	auto &stream = *bind_data.stream;

	if (context.IsInterrupted()) {
		throw InterruptException();
	}

	while (true) {
		if (local_state.batch_pos < local_state.batch_buffer.size()) {
			local_state.current_chunk = std::move(local_state.batch_buffer[local_state.batch_pos++]);
			output.Reference(*local_state.current_chunk);
			return;
		}
		local_state.batch_buffer.clear();
		local_state.batch_pos = 0;
		idx_t batch_idx;
		switch (stream.TryPopBatch(batch_idx, local_state.batch_buffer)) {
		case QuackDataStream::PopBatchStatus::BATCH:
			local_state.current_batch_index = batch_idx;
			continue;
		case QuackDataStream::PopBatchStatus::FINISHED:
			output.SetCardinality(0);
			return;
		case QuackDataStream::PopBatchStatus::ERRORED:
			stream.GetError().Throw();
			return;
		case QuackDataStream::PopBatchStatus::EMPTY: {
			vector<unique_ptr<AsyncTask>> tasks;
			tasks.push_back(make_uniq<QuackWaitForChunkTask>(bind_data.stream));
			AsyncResult res(std::move(tasks), TaskSchedulerType::ASYNC);
			if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				input.async_result = std::move(res);
				return;
			}
			// Synchronous fallback (async_threads=0): run the wait inline, then retry.
			res.ExecuteTasksSynchronously();
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			break;
		}
		}
	}
}

static OperatorPartitionData QuackScanFromClientGetPartitionData(ClientContext &,
                                                                 TableFunctionGetPartitionInput &input) {
	auto &local_state = input.local_state->Cast<QuackScanFromClientLocalState>();
	return OperatorPartitionData(local_state.current_batch_index);
}

TableFunction QuackScanFromClientFunction::GetFunction() {
	TableFunction fun("scan_data_from_quack_client", {LogicalType::VARCHAR}, QuackScanFromClient,
	                  QuackScanFromClientBind, QuackScanFromClientInitGlobal, QuackScanFromClientInitLocal);
	fun.get_partition_data = QuackScanFromClientGetPartitionData;
	return fun;
}

} // namespace duckdb
