#include "quack_rebalancer_sink.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

idx_t QuackGetUBigintSetting(ClientContext &context, const char *name, idx_t default_value) {
	Value val;
	if (context.TryGetCurrentSetting(name, val) && !val.IsNull()) {
		return val.GetValue<uint64_t>();
	}
	return default_value;
}

idx_t QuackGetUBigintSetting(DatabaseInstance &db, const char *name, idx_t default_value) {
	Value val;
	if (DBConfig::GetConfig(db).TryGetCurrentSetting(name, val) && !val.IsNull()) {
		return val.GetValue<uint64_t>();
	}
	return default_value;
}

idx_t QuackRebalancerGlobalState::MaxThreads(idx_t source_max_threads) {
	if (order_mode != AppendOrderMode::PARALLEL_ORDERED) {
		return source_max_threads;
	}
	auto &memory_manager = core->MemoryManager();
	memory_manager.SetMemorySize(source_max_threads * minimum_memory_per_thread);
	// cap the concurrent threads working on this sink by the amount of available memory
	return MinValue<idx_t>(source_max_threads, memory_manager.AvailableMemory() / minimum_memory_per_thread + 1);
}

unique_ptr<QuackRebalancerGlobalState> MakeQuackRebalancerGlobalState(ClientContext &context,
                                                                      const vector<LogicalType> &types,
                                                                      AppendOrderMode order_mode,
                                                                      unique_ptr<QuackBatchEmitter> emitter) {
	auto target_bytes = MaxValue<idx_t>(
	    1, QuackGetUBigintSetting(context, "quack_target_batch_bytes", QUACK_TARGET_BATCH_BYTES_DEFAULT));
	auto buffer_bytes_override =
	    QuackGetUBigintSetting(context, "quack_rebalance_buffer_bytes", QUACK_REBALANCE_BUFFER_BYTES_DEFAULT);
	// same heuristic as core's batch operators: 4MB of buffer space per column per thread
	auto minimum_memory_per_thread = MaxValue<idx_t>(types.size(), 1) * 4ULL * 1024ULL * 1024ULL;

	auto global_state = make_uniq<QuackRebalancerGlobalState>(order_mode, target_bytes, minimum_memory_per_thread);
	global_state->core =
	    make_uniq<QuackRebalancerCore>(context, buffer_bytes_override, minimum_memory_per_thread, std::move(emitter));
	return global_state;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// Seal this thread's current builder and shelve it as one fragment of its executor batch.
static void PushLocalFragment(ClientContext &context, QuackRebalancerGlobalState &gstate,
                              QuackRebalancerLocalState &lstate) {
	if (!lstate.builder) {
		return;
	}
	auto min_batch_index = lstate.partition_info.min_batch_index.GetIndex();
	auto prepared = lstate.builder->Seal(context);
	lstate.size_hint = lstate.builder->SizeBytes();
	lstate.builder.reset();
	gstate.core->AddPendingFragment(context, lstate.batch_index.GetIndex(), min_batch_index, lstate.local_memory_usage,
	                                std::move(prepared));
	lstate.local_memory_usage = 0;
	lstate.fragment_bytes = 0;
	lstate.last_chunk_bytes = 0;
}

// Seal and emit immediately (SERIAL_ORDERED / UNORDERED — everything is settled on arrival).
static void EmitLocalFragment(ClientContext &context, QuackRebalancerGlobalState &gstate,
                              QuackRebalancerLocalState &lstate) {
	if (!lstate.builder) {
		return;
	}
	auto prepared = lstate.builder->Seal(context);
	lstate.size_hint = lstate.builder->SizeBytes();
	lstate.builder.reset();
	gstate.core->AddSettledData(context, std::move(prepared));
}

SinkResultType QuackRebalancerSink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) {
	auto &gstate = input.global_state.Cast<QuackRebalancerGlobalState>();
	auto &lstate = input.local_state.Cast<QuackRebalancerLocalState>();
	auto &core = *gstate.core;

	if (gstate.order_mode != AppendOrderMode::PARALLEL_ORDERED) {
		// SERIAL_ORDERED / UNORDERED: everything is stampable on arrival — cut at the target and emit.
		if (chunk.size() == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}
		if (!lstate.builder) {
			lstate.builder = core.OpenFragment(context.client, lstate.size_hint);
		}
		lstate.builder->Append(context.client, chunk);
		lstate.local_count += chunk.size();
		if (lstate.builder->SizeBytes() >= gstate.target_bytes) {
			EmitLocalFragment(context.client, gstate, lstate);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto &memory_manager = core.MemoryManager();
	auto batch_index = lstate.partition_info.batch_index.GetIndex();
	if (lstate.processing_tasks) {
		core.ExecuteTasks(context.client);
		if (!memory_manager.IsMinimumBatchIndex(batch_index) && core.OutOfMemory(batch_index)) {
			annotated_lock_guard<annotated_mutex> guard(memory_manager.lock);
			if (!memory_manager.IsMinimumBatchIndex(batch_index)) {
				// still over budget and not the minimum batch: park this producer until the prefix drains
				return memory_manager.BlockSink(input.interrupt_state);
			}
		}
		lstate.processing_tasks = false;
	}
	if (!memory_manager.IsMinimumBatchIndex(batch_index)) {
		memory_manager.UpdateMinBatchIndex(lstate.partition_info.min_batch_index.GetIndex());
		if (core.OutOfMemory(batch_index)) {
			// over budget: stop sinking and help emit batches for the minimum batch index instead
			lstate.processing_tasks = true;
			return QuackRebalancerSink(context, chunk, input);
		}
	}
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	// Cut BEFORE the append that would cross the grain, so the settled frontier advances continuously
	// mid-batch and the min batch's fragments stream out immediately.
	if (lstate.builder && lstate.fragment_bytes > 0 &&
	    lstate.fragment_bytes + lstate.last_chunk_bytes > gstate.FragmentGrain()) {
		PushLocalFragment(context.client, gstate, lstate);
	}
	if (!lstate.builder) {
		lstate.builder = core.OpenFragment(context.client, lstate.size_hint);
		lstate.batch_index = batch_index;
	}
	lstate.builder->Append(context.client, chunk);
	lstate.local_count += chunk.size();
	auto new_bytes = lstate.builder->SizeBytes();
	lstate.last_chunk_bytes = new_bytes - lstate.fragment_bytes;
	lstate.fragment_bytes = new_bytes;
	auto new_memory_usage = lstate.builder->AllocatedBytes();
	if (new_memory_usage > lstate.local_memory_usage) {
		memory_manager.IncreaseUnflushedMemory(new_memory_usage - lstate.local_memory_usage);
		lstate.local_memory_usage = new_memory_usage;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// NextBatch (PARALLEL_ORDERED path)
//===--------------------------------------------------------------------===//
// Push the finished batch's remaining fragment, then advance. NextBatch fires BEFORE the new batch's
// first Sink, so lstate.batch_index is still the OLD batch here.
SinkNextBatchType QuackRebalancerNextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) {
	auto &gstate = input.global_state.Cast<QuackRebalancerGlobalState>();
	auto &lstate = input.local_state.Cast<QuackRebalancerLocalState>();
	if (gstate.order_mode != AppendOrderMode::PARALLEL_ORDERED) {
		return SinkNextBatchType::READY;
	}

	if (lstate.batch_index.IsValid()) {
		PushLocalFragment(context.client, gstate, lstate);
	}
	gstate.core->MemoryManager().UpdateMinBatchIndex(lstate.partition_info.min_batch_index.GetIndex());
	lstate.batch_index = lstate.partition_info.batch_index;
	return SinkNextBatchType::READY;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType QuackRebalancerCombine(ExecutionContext &context, OperatorSinkCombineInput &input) {
	auto &gstate = input.global_state.Cast<QuackRebalancerGlobalState>();
	auto &lstate = input.local_state.Cast<QuackRebalancerLocalState>();
	auto &core = *gstate.core;

	if (gstate.order_mode == AppendOrderMode::PARALLEL_ORDERED) {
		if (lstate.batch_index.IsValid()) {
			PushLocalFragment(context.client, gstate, lstate);
		}
		core.MemoryManager().UpdateMinBatchIndex(lstate.partition_info.min_batch_index.GetIndex());
		core.ExecuteTasks(context.client);
	} else {
		EmitLocalFragment(context.client, gstate, lstate);
	}
	gstate.row_count += lstate.local_count;
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
// The final cut can leave several stamped batches queued (every producer's tail settles at once);
// drain them across threads instead of on the one Finalize thread, then FinishEvent closes the stream.
class QuackEmitRemainingTask : public ExecutorTask {
public:
	QuackEmitRemainingTask(Executor &executor, shared_ptr<Event> event_p, QuackRebalancerGlobalState &gstate_p,
	                       ClientContext &context_p)
	    : ExecutorTask(executor, std::move(event_p)), gstate(gstate_p), context(context_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		gstate.core->ExecuteTasks(context);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "QuackEmitRemainingTask";
	}

private:
	QuackRebalancerGlobalState &gstate;
	ClientContext &context;
};

class QuackEmitRemainingEvent : public BasePipelineEvent {
public:
	QuackEmitRemainingEvent(QuackRebalancerGlobalState &gstate_p, Pipeline &pipeline_p, ClientContext &context_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p), context(context_p) {
	}

	void Schedule() override {
		vector<shared_ptr<Task>> tasks;
		auto num_threads =
		    MinValue<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads(), gstate.core->TaskCount());
		for (idx_t i = 0; i < num_threads; i++) {
			tasks.push_back(make_uniq<QuackEmitRemainingTask>(pipeline->executor, shared_from_this(), gstate, context));
		}
		D_ASSERT(!tasks.empty());
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		gstate.core->FinalizeFinish(context);
	}

private:
	QuackRebalancerGlobalState &gstate;
	ClientContext &context;
};

SinkFinalizeType QuackRebalancerFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         QuackRebalancerGlobalState &gstate) {
	auto &core = *gstate.core;
	core.FinalizeCut(context);
	if (core.TaskCount() <= 1) {
		core.ExecuteTasks(context);
		core.FinalizeFinish(context);
	} else {
		// several stamped batches remain: emit them in parallel before closing the stream
		event.InsertEvent(make_shared_ptr<QuackEmitRemainingEvent>(gstate, pipeline, context));
	}
	return SinkFinalizeType::READY;
}

} // namespace duckdb
