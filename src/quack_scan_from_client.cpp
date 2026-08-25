#include "quack_scan_from_client.hpp"

#include "duckdb/common/enums/order_preservation_type.hpp"
#include "duckdb/common/enums/task_scheduler_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/partition_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include "quack_fetch_collector.hpp"
#include "quack_insert_stream.hpp"
#include "quack_storage.hpp"

namespace duckdb {

//! The wait on the unordered path. Any batch, or the end of the stream, wakes it.
class QuackWaitForAnyBatchTask : public AsyncTask {
public:
	explicit QuackWaitForAnyBatchTask(shared_ptr<QuackInsertStream> stream_p) : stream(std::move(stream_p)) {
	}
	void Execute() override {
		stream->buffer.WaitForAny();
	}

private:
	shared_ptr<QuackInsertStream> stream;
};

struct QuackScanFromClientBindData : public FunctionData {
	string stream_id;
	shared_ptr<QuackInsertStream> stream;
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
	//! The ordered path: this thread's claimed dense index. It survives a BLOCKED yield.
	optional_idx claim;
	idx_t current_batch_index = 0;
	vector<unique_ptr<DataChunk>> batch_buffer;
	size_t batch_pos = 0;
	unique_ptr<DataChunk> current_chunk; // keeps the referenced chunk alive between scan calls
};

static unique_ptr<FunctionData> QuackScanFromClientBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto session_state = QuackSessionState::Get(context);
	if (!session_state) {
		throw InvalidInputException("scan_data_from_quack_client is an internal function driven by the quack server");
	}
	auto stream_id = input.inputs[0].GetValue<string>();

	// The second argument is a prototype value, for example NULL::STRUCT(a INTEGER, b VARCHAR). Its
	// TYPE carries the schema, so the statement is planned before the client sends a batch.
	auto &prototype = input.inputs[1].type();
	if (prototype.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException(
		    "scan_data_from_quack_client: the second argument must be a STRUCT prototype, for example "
		    "NULL::STRUCT(a INTEGER)");
	}
	bool ordered = true;
	auto named = input.named_parameters.find("ordered");
	if (named != input.named_parameters.end()) {
		ordered = named->second.GetValue<bool>();
	}

	vector<LogicalType> types;
	for (auto &child : StructType::GetChildTypes(prototype)) {
		names.push_back(Identifier(child.first));
		return_types.push_back(child.second);
		types.push_back(child.second);
	}
	if (types.empty()) {
		throw InvalidInputException("scan_data_from_quack_client: the STRUCT prototype has no columns");
	}

	// Unordered streams signal NO_ORDER so the planner picks PhysicalInsert(parallel=true) instead of
	// PhysicalBatchInsert. Mutates a query-scoped by-value copy — never touches the global catalog entry.
	if (!ordered) {
		input.table_function.order_preservation_type = OrderPreservationType::NO_ORDER;
	}

	auto bind_data = make_uniq<QuackScanFromClientBindData>();
	bind_data->types = types;
	bind_data->stream = QuackStorageExtensionInfo::GetState(*context.db)
	                        .InsertStreams()
	                        .Create(stream_id, std::move(types), ordered, session_state->session_id);
	bind_data->stream_id = std::move(stream_id);
	// The stream exists now, so the client may send. PREPARE waits for this.
	if (auto statement = session_state->Statement()) {
		bind_data->stream->result = statement;
		statement->SignalClientDataPending();
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

enum class QuackScanBatchResult : uint8_t {
	//! A batch was popped into local_state.batch_buffer.
	BATCH,
	//! The stream ended, so this thread is done.
	FINISHED,
	//! No batch is ready. The scan yielded, and it runs again later.
	BLOCKED
};

// The ordered path: a thread claims the next dense index, and waits for that batch only.
static QuackScanBatchResult QuackScanOrderedBatch(ClientContext &context, TableFunctionInput &input,
                                                  QuackScanFromClientLocalState &local_state,
                                                  QuackChunkClaimBuffer &buffer) {
	if (!local_state.claim.IsValid()) {
		local_state.claim = optional_idx(buffer.ClaimBatch());
	}
	while (true) {
		switch (buffer.TryPopClaimed(local_state.claim.GetIndex(), local_state.batch_buffer)) {
		case QuackClaimPopStatus::BATCH:
			local_state.current_batch_index = local_state.claim.GetIndex();
			local_state.claim = optional_idx();
			return QuackScanBatchResult::BATCH;
		case QuackClaimPopStatus::FINISHED:
			return QuackScanBatchResult::FINISHED;
		case QuackClaimPopStatus::ERRORED:
			buffer.GetError().Throw();
			return QuackScanBatchResult::FINISHED;
		case QuackClaimPopStatus::EMPTY: {
			if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR && input.interrupt_state) {
				auto completion = buffer.RegisterWaiter(local_state.claim.GetIndex());
				if (!completion || !completion->TryPark(*input.interrupt_state)) {
					// the batch arrived, or the stream ended, in the meantime: pop again
					continue;
				}
				// parked: the push of this claim's batch wakes exactly this scan task
				input.async_result = AsyncResultType::BLOCKED;
				return QuackScanBatchResult::BLOCKED;
			}
			// The synchronous fallback (async_threads=0): wait inline, then retry.
			buffer.WaitForBatch(local_state.claim.GetIndex());
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			continue;
		}
		}
	}
}

// The unordered path: pop any batch that is ready.
static QuackScanBatchResult QuackScanAnyBatch(ClientContext &context, TableFunctionInput &input,
                                              QuackScanFromClientLocalState &local_state,
                                              const shared_ptr<QuackInsertStream> &stream) {
	while (true) {
		idx_t batch_index;
		switch (stream->buffer.TryPopAny(batch_index, local_state.batch_buffer)) {
		case QuackClaimPopStatus::BATCH:
			local_state.current_batch_index = batch_index;
			return QuackScanBatchResult::BATCH;
		case QuackClaimPopStatus::FINISHED:
			return QuackScanBatchResult::FINISHED;
		case QuackClaimPopStatus::ERRORED:
			stream->buffer.GetError().Throw();
			return QuackScanBatchResult::FINISHED;
		case QuackClaimPopStatus::EMPTY: {
			vector<unique_ptr<AsyncTask>> tasks;
			tasks.push_back(make_uniq<QuackWaitForAnyBatchTask>(stream));
			AsyncResult res(std::move(tasks), TaskSchedulerType::ASYNC);
			if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				input.async_result = std::move(res);
				return QuackScanBatchResult::BLOCKED;
			}
			// The synchronous fallback (async_threads=0): wait inline, then retry.
			res.ExecuteTasksSynchronously();
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			continue;
		}
		}
	}
}

static void QuackScanFromClient(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<QuackScanFromClientBindData>();
	auto &local_state = input.local_state->Cast<QuackScanFromClientLocalState>();
	auto &stream = bind_data.stream;

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
		auto result = stream->ordered ? QuackScanOrderedBatch(context, input, local_state, stream->buffer)
		                              : QuackScanAnyBatch(context, input, local_state, stream);
		switch (result) {
		case QuackScanBatchResult::BATCH:
			continue;
		case QuackScanBatchResult::FINISHED:
			output.SetChildCardinality(0);
			return;
		case QuackScanBatchResult::BLOCKED:
			// yielded through input.async_result; the executor calls the scan again when it wakes
			return;
		}
	}
}

static OperatorPartitionData QuackScanFromClientGetPartitionData(ClientContext &,
                                                                 TableFunctionGetPartitionInput &input) {
	auto &local_state = input.local_state->Cast<QuackScanFromClientLocalState>();
	return OperatorPartitionData(local_state.current_batch_index);
}

TableFunction QuackScanFromClientFunction::GetFunction() {
	TableFunction fun("scan_data_from_quack_client", {LogicalType::VARCHAR, LogicalType::ANY}, QuackScanFromClient,
	                  QuackScanFromClientBind, QuackScanFromClientInitGlobal, QuackScanFromClientInitLocal);
	fun.get_partition_data = QuackScanFromClientGetPartitionData;
	fun.named_parameters["ordered"] = LogicalType::BOOLEAN;
	return fun;
}

} // namespace duckdb
