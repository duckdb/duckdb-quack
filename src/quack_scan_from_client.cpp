#include "quack_scan_from_client.hpp"

#include "duckdb/common/enums/task_scheduler_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/partition_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/async_result.hpp"

#include "quack_data_stream.hpp"

namespace duckdb {

//! Async wait used when the stream has no chunk yet but is not finished: the scan returns BLOCKED
//! with this task, the executor parks the pipeline thread and runs the task on the ASYNC pool, and
//! reschedules the scan once the task completes (a chunk arrived, the stream finished, or the bounded
//! wait timed out).
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
	//! Single consumer: this matches the synchronous client and avoids multi-consumer coordination.
	//! PhysicalBatchInsert is still selected because use_batch_index only needs the global scheduler
	//! to have >1 thread plus a source that advertises batch-index partitioning (get_partition_data).
	idx_t MaxThreads() const override {
		return 1;
	}
};

struct QuackScanFromClientLocalState : public LocalTableFunctionState {
	idx_t current_batch_index = 0;
	//! Holds the chunk currently referenced by the scan output, keeping it alive until the next call.
	QuackStreamChunk current;
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
	return make_uniq<QuackScanFromClientGlobalState>();
}

static unique_ptr<LocalTableFunctionState>
QuackScanFromClientInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *) {
	return make_uniq<QuackScanFromClientLocalState>();
}

static void QuackScanFromClient(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<QuackScanFromClientBindData>();
	auto &local_state = input.local_state->Cast<QuackScanFromClientLocalState>();
	auto &stream = *bind_data.stream;

	// Re-entry point after a BLOCKED reschedule (or each synchronous retry): observe cancellation.
	if (context.IsInterrupted()) {
		throw InterruptException();
	}

	while (true) {
		QuackStreamChunk entry;
		switch (stream.TryPop(entry)) {
		case QuackDataStream::PopStatus::CHUNK:
			// Use the wire batch index if the (future async) client supplied one; otherwise a constant
			// index keeps all chunks in one PhysicalBatchInsert collection.
			local_state.current_batch_index = entry.batch_index.IsValid() ? entry.batch_index.GetIndex() : 0;
			// Keep the chunk alive while `output` references it (until the next scan call).
			local_state.current = std::move(entry);
			output.Reference(*local_state.current.chunk);
			return;
		case QuackDataStream::PopStatus::FINISHED:
			output.SetCardinality(0);
			return;
		case QuackDataStream::PopStatus::ERRORED:
			stream.GetError().Throw();
			return; // unreachable
		case QuackDataStream::PopStatus::EMPTY: {
			// No data yet but the stream isn't finished: hand off a wait task on the ASYNC pool.
			vector<unique_ptr<AsyncTask>> tasks;
			tasks.push_back(make_uniq<QuackWaitForChunkTask>(bind_data.stream));
			AsyncResult res(std::move(tasks), TaskSchedulerType::ASYNC);
			if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				// Park the pipeline thread; the executor reschedules this scan when the task completes.
				input.async_result = std::move(res);
				return;
			}
			// SYNCHRONOUS mode (e.g. async_threads=0): run the wait inline, then retry.
			res.ExecuteTasksSynchronously();
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			break; // retry TryPop
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
	// Advertise batch-index partitioning so the planner selects PhysicalBatchInsert.
	fun.get_partition_data = QuackScanFromClientGetPartitionData;
	return fun;
}

} // namespace duckdb
