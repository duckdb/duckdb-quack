#include "quack_rebalancer_core.hpp"

namespace duckdb {

QuackRebalancerCore::QuackRebalancerCore(ClientContext &context, idx_t buffer_bytes_override_p,
                                         idx_t initial_memory_request, unique_ptr<QuackBatchEmitter> emitter_p)
    : memory_manager(context, initial_memory_request), buffer_bytes_override(buffer_bytes_override_p),
      emitter(std::move(emitter_p)) {
}

void QuackRebalancerCore::AddPendingFragment(ClientContext &context, idx_t executor_batch, idx_t min_batch_index,
                                             idx_t memory_usage, unique_ptr<QuackPreparedBatch> prepared) {
	// The fragment arrives already emit-ready (built on the producing thread as chunks arrived) —
	// settling releases ready bytes, not work.
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		raw_batches[executor_batch].emplace_back(memory_usage, std::move(prepared));
	}
	// The min batch's already-pushed fragments are a settled prefix of the dense stream (future
	// fragments simply get later indices), so the frontier always includes them — regardless of caller.
	ReleaseSettled(min_batch_index + 1);
	// Blocked producers can help emit; also drain here so stamped batches hit the wire immediately.
	{
		annotated_lock_guard<annotated_mutex> guard(memory_manager.lock);
		memory_manager.UnblockTasks();
	}
	ExecuteTasks(context);
}

void QuackRebalancerCore::AddSettledData(ClientContext &context, unique_ptr<QuackPreparedBatch> prepared) {
	emitter->EmitPrepared(context, next_dense++, std::move(prepared));
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

bool QuackRebalancerCore::ExecuteTask(ClientContext &context) {
	auto task = task_manager.GetTask();
	if (!task) {
		return false;
	}
	auto memory_usage = task->memory_usage;
	emitter->EmitPrepared(context, task->dense_index, std::move(task->prepared));
	if (memory_usage > 0) {
		memory_manager.ReduceUnflushedMemory(memory_usage);
		annotated_lock_guard<annotated_mutex> guard(memory_manager.lock);
		memory_manager.UnblockTasks();
	}
	return true;
}

void QuackRebalancerCore::ExecuteTasks(ClientContext &context) {
	while (ExecuteTask(context)) {
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

void QuackRebalancerCore::FinalizeFinish(ClientContext &context) {
	if (task_manager.TaskCount() != 0) {
		throw InternalException("Unemitted batches remain in QuackRebalancerCore::FinalizeFinish");
	}
	memory_manager.FinalCheck();
	emitter->Finish(context, TotalBatches());
}

} // namespace duckdb
