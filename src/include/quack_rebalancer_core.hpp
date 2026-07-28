#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/operator/persistent/batch_memory_manager.hpp"
#include "duckdb/execution/operator/persistent/batch_task_manager.hpp"

namespace duckdb {

//! A batch that has been made wire-ready (e.g. serialized) but not yet stamped with its dense index.
struct QuackPreparedBatch {
	virtual ~QuackPreparedBatch() = default;
};

//! Accumulates one fragment on its producing thread, directly in the emitter's final form — the
//! accumulation buffer IS the prepared batch, so cutting costs no extra copy or serialization pass.
class QuackFragmentBuilder {
public:
	virtual ~QuackFragmentBuilder() = default;
	//! Append one chunk; the chunk may be reused by the caller afterwards.
	virtual void Append(ClientContext &context, DataChunk &chunk) = 0;
	//! Bytes accumulated so far — the cut measure. After Seal: the final fragment size.
	virtual idx_t SizeBytes() const = 0;
	//! Bytes allocated — the memory-accounting measure.
	virtual idx_t AllocatedBytes() const = 0;
	//! Close the fragment into a stampable batch.
	virtual unique_ptr<QuackPreparedBatch> Seal(ClientContext &context) = 0;
};

//! Compacts sub-half-vector chunks into staged ~STANDARD_VECTOR_SIZE chunks so sparse sources
//! (filtered scans) don't pay per-chunk framing overhead; larger chunks pass straight through.
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

//! Emits dense batches: OpenFragment accumulates chunks in emit-ready form on the producing thread
//! (before the dense index is known); EmitPrepared stamps + sends at release time, concurrently and
//! out of dense order (receivers reassemble by index). Finish runs once, after all EmitPrepared.
class QuackBatchEmitter {
public:
	virtual ~QuackBatchEmitter() = default;
	//! size_hint: the previous sealed fragment's size (0 for a thread's first fragment), so
	//! implementations can pre-reserve capacity and avoid growth copies while accumulating.
	virtual unique_ptr<QuackFragmentBuilder> OpenFragment(ClientContext &context, idx_t size_hint) = 0;
	virtual void EmitPrepared(ClientContext &context, idx_t dense_index, unique_ptr<QuackPreparedBatch> batch) = 0;
	virtual void Finish(ClientContext &context, idx_t total_batches) = 0;
};

//! A stamped batch waiting to be emitted; emission runs outside the stamping lock, on any thread.
struct QuackEmitTask {
	QuackEmitTask(idx_t dense_index_p, idx_t memory_usage_p, unique_ptr<QuackPreparedBatch> prepared_p)
	    : dense_index(dense_index_p), memory_usage(memory_usage_p), prepared(std::move(prepared_p)) {
	}

	idx_t dense_index;
	idx_t memory_usage;
	unique_ptr<QuackPreparedBatch> prepared;
};

//! Maps sparse executor batches onto dense (1,2,3,…) indices, one fragment = one dense batch.
//! Stamping is ordered — only the settled prefix (up to and including the current minimum batch,
//! whose dense prefix is known) is released under the lock — but emission is parallel and out of order.
class QuackRebalancerCore {
public:
	QuackRebalancerCore(ClientContext &context, idx_t buffer_bytes_override_p, idx_t initial_memory_request,
	                    unique_ptr<QuackBatchEmitter> emitter_p);

	//! Open a fragment accumulator on the calling thread (delegates to the emitter).
	unique_ptr<QuackFragmentBuilder> OpenFragment(ClientContext &context, idx_t size_hint) {
		return emitter->OpenFragment(context, size_hint);
	}

	//! PARALLEL_ORDERED: shelve a sealed fragment of an executor batch (fragments of one batch only
	//! ever come from its owning thread, in order), then stamp + emit the settled prefix.
	void AddPendingFragment(ClientContext &context, idx_t executor_batch, idx_t min_batch_index, idx_t memory_usage,
	                        unique_ptr<QuackPreparedBatch> prepared);
	//! SERIAL_ORDERED / UNORDERED: a sealed fragment that is stampable immediately; stamped and emitted inline.
	void AddSettledData(ClientContext &context, unique_ptr<QuackPreparedBatch> prepared);

	//! Emit one stamped batch if any is queued; returns false when the queue is empty.
	bool ExecuteTask(ClientContext &context);
	void ExecuteTasks(ClientContext &context);

	//! Over the pending-bytes budget and not the minimum batch: stop sinking, help emit, maybe block.
	bool OutOfMemory(idx_t batch_index);

	//! Final release: stamp everything still shelved (Finalize).
	void FinalizeCut(ClientContext &context);
	//! Stamped batches still queued for emission — lets Finalize decide inline drain vs parallel event.
	idx_t TaskCount() {
		return task_manager.TaskCount();
	}
	//! After the last EmitBatch returned: verify accounting and let the emitter close the stream.
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

	//! Stamp every shelved fragment of batches below min_exclusive with the next dense indices and queue
	//! them for emission — a pointer walk under the lock; the payloads were prepared at cut time.
	void ReleaseSettled(idx_t min_exclusive);

private:
	BatchMemoryManager memory_manager;
	BatchTaskManager<QuackEmitTask> task_manager;
	annotated_mutex lock;
	//! Prepared fragments of not-yet-settled executor batches, keyed by executor batch index.
	std::map<idx_t, vector<Fragment>> raw_batches DUCKDB_GUARDED_BY(lock);
	//! The next dense index to hand out; stamping order defines the stream order.
	atomic<idx_t> next_dense {1};
	//! Test/ops override: >0 also treats this many pending bytes as out-of-memory.
	idx_t buffer_bytes_override;
	unique_ptr<QuackBatchEmitter> emitter;
};

} // namespace duckdb
