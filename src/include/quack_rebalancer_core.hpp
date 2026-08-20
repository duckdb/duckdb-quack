#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/operator/persistent/batch_memory_manager.hpp"
#include "duckdb/execution/operator/persistent/batch_task_manager.hpp"
#include "duckdb/parallel/interrupt.hpp"

namespace duckdb {

//! A wire-ready batch that has no dense index yet.
struct QuackPreparedBatch {
	virtual ~QuackPreparedBatch() = default;
};

//! Accumulates one fragment on its producing thread, in the emitter's final form. The buffer IS the
//! prepared batch, so a cut costs no copy and no second serialization.
class QuackFragmentBuilder {
public:
	virtual ~QuackFragmentBuilder() = default;
	virtual void Append(ClientContext &context, DataChunk &chunk) = 0;
	//! The cut measure. After Seal: the final fragment size.
	virtual idx_t SizeBytes() const = 0;
	//! The memory-accounting measure.
	virtual idx_t AllocatedBytes() const = 0;
	virtual unique_ptr<QuackPreparedBatch> Seal(ClientContext &context) = 0;
};

//! Merges small chunks into full ones, so a sparse source does not pay the framing cost of each
//! chunk. Large chunks go through without a copy.
class QuackChunkStager {
public:
	template <class FLUSH>
	void Append(DataChunk &chunk, FLUSH &&flush) {
		if (chunk.size() * 2 >= STANDARD_VECTOR_SIZE) {
			Flush(flush);
			flush(chunk);
			return;
		}
		if (staged && staged->size() + chunk.size() > STANDARD_VECTOR_SIZE) {
			Flush(flush);
		}
		if (!staged) {
			staged = make_uniq<DataChunk>();
			staged->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
		}
		staged->Append(chunk);
		if (staged->size() == STANDARD_VECTOR_SIZE) {
			Flush(flush);
		}
	}

	template <class FLUSH>
	void Flush(FLUSH &&flush) {
		if (staged && staged->size() > 0) {
			flush(*staged);
			staged->Reset();
		}
	}

private:
	unique_ptr<DataChunk> staged;
};

//! OpenFragment accumulates chunks on the producing thread, before the dense index is known.
//! TryEmitPrepared stamps and sends at release time, in parallel and out of dense order: the
//! receiver puts them back in order by index. Finish runs once, after all emits.
class QuackBatchEmitter {
public:
	virtual ~QuackBatchEmitter() = default;
	//! size_hint is the previous sealed fragment's size, 0 for a thread's first fragment. It lets an
	//! implementation reserve capacity and prevent growth copies.
	virtual unique_ptr<QuackFragmentBuilder> OpenFragment(ClientContext &context, idx_t size_hint) = 0;
	//! false = the delivery target is full and `batch` was NOT consumed: retry the same batch later.
	//! With `interrupt` set, capacity freeing fires it. An emitter that cannot fill up returns true.
	virtual bool TryEmitPrepared(ClientContext &context, idx_t dense_index, unique_ptr<QuackPreparedBatch> &batch,
	                             optional_ptr<const InterruptState> interrupt) = 0;
	virtual void Finish(ClientContext &context, idx_t total_batches) = 0;
};

//! BLOCKED = a batch is parked on delivery capacity and stays queued. The caller must yield with a
//! registered interrupt, or make sure a later drain carries one.
enum class QuackEmitProgress : uint8_t { DONE, BLOCKED };

//! A stamped batch that waits for emission. Emission runs outside the stamping lock, on any thread.
struct QuackEmitTask {
	QuackEmitTask(idx_t dense_index_p, idx_t memory_usage_p, unique_ptr<QuackPreparedBatch> prepared_p)
	    : dense_index(dense_index_p), memory_usage(memory_usage_p), prepared(std::move(prepared_p)) {
	}

	idx_t dense_index;
	idx_t memory_usage;
	unique_ptr<QuackPreparedBatch> prepared;
};

//! Maps sparse executor batches onto dense indices 1,2,3,... One fragment becomes one dense batch.
//! Stamping is ordered: the lock releases only the settled prefix, up to the current minimum batch.
//! Emission is parallel and out of order.
class QuackRebalancerCore {
public:
	QuackRebalancerCore(ClientContext &context, idx_t buffer_bytes_override_p, idx_t initial_memory_request,
	                    unique_ptr<QuackBatchEmitter> emitter_p);

	unique_ptr<QuackFragmentBuilder> OpenFragment(ClientContext &context, idx_t size_hint) {
		return emitter->OpenFragment(context, size_hint);
	}

	//! PARALLEL_ORDERED: shelve a sealed fragment, then stamp and emit the settled prefix. All
	//! fragments of one executor batch come from its owning thread, in order.
	void AddPendingFragment(ClientContext &context, idx_t executor_batch, idx_t min_batch_index, idx_t memory_usage,
	                        unique_ptr<QuackPreparedBatch> prepared);
	//! SERIAL_ORDERED / UNORDERED: stamp a sealed fragment. The caller emits it, so a parked batch
	//! can be retried without a second stamp.
	unique_ptr<QuackEmitTask> StampSettled(unique_ptr<QuackPreparedBatch> prepared) {
		return make_uniq<QuackEmitTask>(next_dense++, 0, std::move(prepared));
	}
	//! false = parked on delivery capacity. The task keeps the batch.
	bool TryEmitStamped(ClientContext &context, QuackEmitTask &task, optional_ptr<const InterruptState> interrupt);

	//! Drain the emit tasks: parked retries first, then the settled queue. A null interrupt only
	//! probes, and it shelves a parked batch for a later drain that carries one.
	QuackEmitProgress ExecuteTasks(ClientContext &context, optional_ptr<const InterruptState> interrupt = nullptr);
	bool HasParkedEmits() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return !parked_tasks.empty();
	}

	//! Over the byte budget and not the minimum batch: stop sinking, help to emit, and maybe block.
	bool OutOfMemory(idx_t batch_index);

	void FinalizeCut(ClientContext &context);
	//! Lets Finalize choose between an inline drain and a parallel event.
	idx_t TaskCount() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return task_manager.TaskCount() + parked_tasks.size();
	}
	//! Verify the accounting, then let the emitter close the stream.
	void FinalizeFinish(ClientContext &context);

	BatchMemoryManager &MemoryManager() {
		return memory_manager;
	}
	idx_t TotalBatches() const {
		return next_dense.load() - 1;
	}

private:
	struct Fragment {
		Fragment(idx_t memory_usage_p, unique_ptr<QuackPreparedBatch> prepared_p)
		    : memory_usage(memory_usage_p), prepared(std::move(prepared_p)) {
		}

		idx_t memory_usage;
		unique_ptr<QuackPreparedBatch> prepared;
	};

	//! Stamp every shelved fragment below min_exclusive and queue it. This is a pointer walk under
	//! the lock: the payloads were prepared at cut time.
	void ReleaseSettled(idx_t min_exclusive);

private:
	BatchMemoryManager memory_manager;
	BatchTaskManager<QuackEmitTask> task_manager;
	annotated_mutex lock;
	std::map<idx_t, vector<Fragment>> raw_batches DUCKDB_GUARDED_BY(lock);
	//! Stamping order defines the stream order.
	atomic<idx_t> next_dense {1};
	//! Retried lowest index first. The head of the stream always gets in, so index order makes
	//! progress certain.
	std::map<idx_t, unique_ptr<QuackEmitTask>> parked_tasks DUCKDB_GUARDED_BY(lock);
	//! Test override: a value above 0 also counts this many pending bytes as out of memory.
	idx_t buffer_bytes_override;
	unique_ptr<QuackBatchEmitter> emitter;
};

} // namespace duckdb
