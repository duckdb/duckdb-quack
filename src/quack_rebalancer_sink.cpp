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
	// core's batch operators use the same heuristic: 4MB for each column on each thread
	auto minimum_memory_per_thread = MaxValue<idx_t>(types.size(), 1) * 4ULL * 1024ULL * 1024ULL;

	auto global_state = make_uniq<QuackRebalancerGlobalState>(order_mode, target_bytes, minimum_memory_per_thread);
	global_state->core =
	    make_uniq<QuackRebalancerCore>(context, buffer_bytes_override, minimum_memory_per_thread, std::move(emitter));
	return global_state;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
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

// Emission stays separate from the stamp, so a parked batch can be retried without a second stamp.
static void StampLocalFragment(ClientContext &context, QuackRebalancerGlobalState &gstate,
                               QuackRebalancerLocalState &lstate) {
	if (!lstate.builder) {
		return;
	}
	D_ASSERT(!lstate.pending_emit);
	auto prepared = lstate.builder->Seal(context);
	lstate.size_hint = lstate.builder->SizeBytes();
	lstate.builder.reset();
	lstate.pending_emit = gstate.core->StampSettled(std::move(prepared));
}

SinkResultType QuackRebalancerSink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) {
	auto &gstate = input.global_state.Cast<QuackRebalancerGlobalState>();
	auto &lstate = input.local_state.Cast<QuackRebalancerLocalState>();
	auto &core = *gstate.core;

	if (gstate.order_mode != AppendOrderMode::PARALLEL_ORDERED) {
		// The retry runs BEFORE the chunk is consumed, so BLOCKED is safe here: the executor calls
		// Sink again with the same chunk after the wake.
		if (lstate.pending_emit) {
			if (!core.TryEmitStamped(context.client, *lstate.pending_emit, input.interrupt_state)) {
				return SinkResultType::BLOCKED;
			}
			lstate.pending_emit.reset();
		}
		if (chunk.size() == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}
		if (!lstate.builder) {
			lstate.builder = core.OpenFragment(context.client, lstate.size_hint);
		}
		lstate.builder->Append(context.client, chunk);
		lstate.local_count += chunk.size();
		if (lstate.builder->SizeBytes() >= gstate.target_bytes) {
			// The chunk is consumed, so this call cannot yield: probe only. A full buffer leaves the
			// batch pending, and the next call blocks before it takes its chunk.
			StampLocalFragment(context.client, gstate, lstate);
			if (core.TryEmitStamped(context.client, *lstate.pending_emit, nullptr)) {
				lstate.pending_emit.reset();
			}
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto &memory_manager = core.MemoryManager();
	auto batch_index = lstate.partition_info.batch_index.GetIndex();
	// Retry parked emits first, while the chunk is untouched and a yield is safe.
	if (core.HasParkedEmits()) {
		if (core.ExecuteTasks(context.client, input.interrupt_state) == QuackEmitProgress::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
	}
	if (lstate.processing_tasks) {
		if (core.ExecuteTasks(context.client, input.interrupt_state) == QuackEmitProgress::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
		if (!memory_manager.IsMinimumBatchIndex(batch_index) && core.OutOfMemory(batch_index)) {
			annotated_lock_guard<annotated_mutex> guard(memory_manager.lock);
			if (!memory_manager.IsMinimumBatchIndex(batch_index)) {
				// still over budget and not the minimum batch: wait for the prefix to drain
				return memory_manager.BlockSink(input.interrupt_state);
			}
		}
		lstate.processing_tasks = false;
	}
	if (!memory_manager.IsMinimumBatchIndex(batch_index)) {
		memory_manager.UpdateMinBatchIndex(lstate.partition_info.min_batch_index.GetIndex());
		if (core.OutOfMemory(batch_index)) {
			// over budget: stop sinking and help to emit the minimum batch instead
			lstate.processing_tasks = true;
			return QuackRebalancerSink(context, chunk, input);
		}
	}
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	// Cut BEFORE the append that would cross the grain. The settled frontier then moves forward
	// inside a batch, and the min batch's fragments go out at once.
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
// NextBatch runs BEFORE the new batch's first Sink, so lstate.batch_index is still the old batch.
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
// A parked emit makes this return BLOCKED, and the executor calls it again. Each step below does
// nothing on the second call, because the first call cleared its state.
SinkCombineResultType QuackRebalancerCombine(ExecutionContext &context, OperatorSinkCombineInput &input) {
	auto &gstate = input.global_state.Cast<QuackRebalancerGlobalState>();
	auto &lstate = input.local_state.Cast<QuackRebalancerLocalState>();
	auto &core = *gstate.core;

	if (gstate.order_mode == AppendOrderMode::PARALLEL_ORDERED) {
		if (lstate.batch_index.IsValid()) {
			PushLocalFragment(context.client, gstate, lstate);
		}
		core.MemoryManager().UpdateMinBatchIndex(lstate.partition_info.min_batch_index.GetIndex());
		if (core.ExecuteTasks(context.client, input.interrupt_state) == QuackEmitProgress::BLOCKED) {
			return SinkCombineResultType::BLOCKED;
		}
	} else {
		if (lstate.pending_emit) {
			if (!core.TryEmitStamped(context.client, *lstate.pending_emit, input.interrupt_state)) {
				return SinkCombineResultType::BLOCKED;
			}
			lstate.pending_emit.reset();
		}
		if (lstate.builder) {
			StampLocalFragment(context.client, gstate, lstate);
			if (!core.TryEmitStamped(context.client, *lstate.pending_emit, input.interrupt_state)) {
				return SinkCombineResultType::BLOCKED;
			}
			lstate.pending_emit.reset();
		}
	}
	gstate.row_count += lstate.local_count;
	lstate.local_count = 0;
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
// The final cut can leave several stamped batches queued, because every producer's tail settles at
// the same time. These tasks drain them in parallel, then FinishEvent closes the stream.
class QuackEmitRemainingTask : public ExecutorTask {
public:
	QuackEmitRemainingTask(Executor &executor, shared_ptr<Event> event_p, QuackRebalancerGlobalState &gstate_p,
	                       ClientContext &context_p)
	    : ExecutorTask(executor, std::move(event_p)), gstate(gstate_p), context(context_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// A slow client must not stop a scheduler thread: yield instead. The buffer's wake puts this
		// task back on the queue.
		InterruptState interrupt(shared_from_this());
		if (gstate.core->ExecuteTasks(context, interrupt) == QuackEmitProgress::BLOCKED) {
			return TaskExecutionResult::TASK_BLOCKED;
		}
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
		auto reported = gstate.core->FinalizeFinish(context);
		if (reported.IsValid()) {
			gstate.row_count = reported.GetIndex();
		}
	}

private:
	QuackRebalancerGlobalState &gstate;
	ClientContext &context;
};

SinkFinalizeType QuackRebalancerFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         QuackRebalancerGlobalState &gstate) {
	auto &core = *gstate.core;
	core.FinalizeCut(context);
	// A single batch emits here. Several batches, or one that parks on capacity, go to the event
	// tasks: those emit in parallel, and they can yield when the delivery buffer is full.
	if (core.TaskCount() <= 1 && core.ExecuteTasks(context) == QuackEmitProgress::DONE) {
		auto reported = core.FinalizeFinish(context);
		if (reported.IsValid()) {
			gstate.row_count = reported.GetIndex();
		}
	} else {
		event.InsertEvent(make_shared_ptr<QuackEmitRemainingEvent>(gstate, pipeline, context));
	}
	return SinkFinalizeType::READY;
}

} // namespace duckdb
