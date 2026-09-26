#include "quack_rebalancer_core.hpp"

namespace duckdb {

QuackRebalancerCore::QuackRebalancerCore(ClientContext &context, idx_t buffer_bytes_override_p,
                                         idx_t initial_memory_request, unique_ptr<QuackBatchEmitter> emitter_p)
    : memory_manager(context, initial_memory_request), buffer_bytes_override(buffer_bytes_override_p),
      emitter(std::move(emitter_p)) {
}

void QuackRebalancerCore::AddPendingFragment(ClientContext &context, idx_t executor_batch, idx_t min_batch_index,
                                             idx_t memory_usage, unique_ptr<QuackPreparedBatch> prepared) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		raw_batches[executor_batch].emplace_back(memory_usage, std::move(prepared));
	}
	// The min batch's pushed fragments are a settled prefix: later fragments only get later indices.
	// The frontier therefore always includes them, whichever thread calls.
	ReleaseSettled(min_batch_index + 1);
	// blocked producers can help to emit
	{
		annotated_lock_guard<annotated_mutex> guard(memory_manager.lock);
		memory_manager.UnblockTasks();
	}
	// A probe only: the chunk is already consumed, so this call cannot yield. A parked batch waits
	// for the next drain that carries an interrupt (the top of Sink, Combine, or Finalize).
	ExecuteTasks(context);
}

bool QuackRebalancerCore::TryEmitStamped(ClientContext &context, QuackEmitTask &task,
                                         optional_ptr<const InterruptState> interrupt) {
	return emitter->TryEmitPrepared(context, task.dense_index, task.prepared, interrupt);
}

void QuackRebalancerCore::ReleaseSettled(idx_t min_exclusive) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	for (auto entry = raw_batches.begin(); entry != raw_batches.end();) {
		if (entry->first >= min_exclusive) {
			break;
		}
		for (auto &fragment : entry->second) {
			task_manager.AddTask(
			    make_uniq<QuackEmitTask>(next_dense++, fragment.memory_usage, std::move(fragment.prepared)));
		}
		entry = raw_batches.erase(entry);
	}
}

QuackEmitProgress QuackRebalancerCore::ExecuteTasks(ClientContext &context,
                                                    optional_ptr<const InterruptState> interrupt) {
	while (true) {
		// Parked retries first, lowest index first. The head of the stream always gets in, so index
		// order makes progress certain after capacity frees.
		unique_ptr<QuackEmitTask> task;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (!parked_tasks.empty()) {
				task = std::move(parked_tasks.begin()->second);
				parked_tasks.erase(parked_tasks.begin());
			}
		}
		if (!task) {
			task = task_manager.GetTask();
		}
		if (!task) {
			return QuackEmitProgress::DONE;
		}
		if (!TryEmitStamped(context, *task, interrupt)) {
			annotated_lock_guard<annotated_mutex> guard(lock);
			parked_tasks.emplace(task->dense_index, std::move(task));
			return QuackEmitProgress::BLOCKED;
		}
		auto memory_usage = task->memory_usage;
		if (memory_usage > 0) {
			memory_manager.ReduceUnflushedMemory(memory_usage);
			annotated_lock_guard<annotated_mutex> guard(memory_manager.lock);
			memory_manager.UnblockTasks();
		}
	}
}

bool QuackRebalancerCore::OutOfMemory(idx_t batch_index) {
	if (buffer_bytes_override > 0 && memory_manager.GetUnflushedMemory() > buffer_bytes_override &&
	    !memory_manager.IsMinimumBatchIndex(batch_index)) {
		return true;
	}
	return memory_manager.OutOfMemory(batch_index);
}

void QuackRebalancerCore::FinalizeCut(ClientContext &context) {
	ReleaseSettled(NumericLimits<idx_t>::Maximum());
}

optional_idx QuackRebalancerCore::FinalizeFinish(ClientContext &context) {
	if (TaskCount() != 0) {
		throw InternalException("Unemitted batches remain in QuackRebalancerCore::FinalizeFinish");
	}
	memory_manager.FinalCheck();
	return emitter->Finish(context, TotalBatches());
}

} // namespace duckdb
