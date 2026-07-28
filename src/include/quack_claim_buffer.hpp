#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include <chrono>
#include <condition_variable>
#include <map>
#include <set>

namespace duckdb {

enum class QuackClaimPopStatus : uint8_t {
	BATCH,
	EMPTY,
	FINISHED,
	ERRORED,
};

//! Dense-index membership tracked as a contiguous prefix + sparse overflow; the sparse set stays
//! bounded by the producer's in-flight window. Caller provides the locking.
struct QuackDenseIndexSet {
	bool Contains(idx_t index) const {
		return index <= contiguous || sparse.count(index) > 0;
	}
	void Insert(idx_t index) {
		sparse.insert(index);
		while (sparse.count(contiguous + 1) > 0) {
			sparse.erase(contiguous + 1);
			++contiguous;
		}
	}
	//! Highest index with all of 1..index present.
	idx_t ContiguousMax() const {
		return contiguous;
	}

private:
	idx_t contiguous = 0;
	std::set<idx_t> sparse;
};

//! Claim-based delivery for dense batch streams (indices from 1): each consumer claims the next index
//! and waits for exactly that batch; unordered consumers pop any. PAYLOAD is one batch's storage.
template <class PAYLOAD>
class QuackClaimBuffer {
public:
	using PopStatus = QuackClaimPopStatus;

	//! Claim the next batch index to consume; each index gets exactly one claimant.
	idx_t ClaimBatch() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return next_claim++;
	}

	//! Bound the buffered bytes; PushBatch blocks while full (producer backpressure). 0 = unbounded.
	void SetCapacity(idx_t bytes) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		capacity = bytes;
	}

	//! Publish a batch under its dense index. With a capacity set, `bytes` is the batch's weight and
	//! the call blocks until it fits (at least one batch is always admitted — no self-deadlock).
	void PushBatch(idx_t batch_index, PAYLOAD payload, idx_t bytes = 0) {
		shared_ptr<ReadAheadJobCompletion> waiter;
		{
			annotated_unique_lock<annotated_mutex> guard(lock);
			if (Seen(batch_index)) {
				// duplicate delivery (transport-level retry) — the first copy won
				return;
			}
			// producer backpressure: wait until the batch fits. An empty buffer always admits one, and
			// the head of the stream is always admitted so later batches can't starve the next pop.
			while (capacity > 0 && buffered_bytes > 0 && buffered_bytes + bytes > capacity && !finished && !errored &&
			       !HeadOfStream(batch_index)) {
				cv.wait(guard);
			}
			if (finished || errored) {
				return;
			}
			if (Seen(batch_index)) {
				// a concurrent duplicate got in while this push waited on capacity
				return;
			}
			RecordSeen(batch_index);
			batches[batch_index] = Entry {std::move(payload), bytes};
			buffered_bytes += bytes;
			pushed_count++;
			auto entry = waiters.find(batch_index);
			if (entry != waiters.end()) {
				waiter = std::move(entry->second);
				waiters.erase(entry);
			}
		}
		cv.notify_all();
		if (waiter) {
			// wakes exactly the scan task parked on this batch index
			waiter->FinishIOTask();
		}
	}

	//! Register a per-claim wake event; nullptr when the claim is already poppable (caller retries).
	shared_ptr<ReadAheadJobCompletion> RegisterWaiter(idx_t claim) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		// Checked under the same lock PushBatch inserts with, so a concurrent publish either
		// makes us retry the pop here or finds the registered waiter — no lost wakeup.
		if (batches.find(claim) != batches.end() || finished || errored) {
			return nullptr;
		}
		auto entry = waiters.find(claim);
		if (entry != waiters.end()) {
			return entry->second;
		}
		auto completion = make_shared_ptr<ReadAheadJobCompletion>(1);
		waiters.emplace(claim, completion);
		return completion;
	}

	//! Pop the claimed batch if present. FINISHED means the stream ended and the claim can never arrive.
	PopStatus TryPopClaimed(idx_t claim, PAYLOAD &payload_out) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (errored) {
			return PopStatus::ERRORED;
		}
		auto entry = batches.find(claim);
		if (entry != batches.end()) {
			payload_out = std::move(entry->second.payload);
			buffered_bytes -= entry->second.bytes;
			batches.erase(entry);
			cv.notify_all(); // pushers may be waiting on capacity
			return PopStatus::BATCH;
		}
		// Indices are dense and Finish() runs after the last push, so an absent claim can never arrive.
		if (finished) {
			return PopStatus::FINISHED;
		}
		return PopStatus::EMPTY;
	}

	//! Pop the lowest available batch regardless of claims (unordered consumption).
	PopStatus TryPopAny(idx_t &batch_index_out, PAYLOAD &payload_out) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (errored) {
			return PopStatus::ERRORED;
		}
		if (!batches.empty()) {
			auto entry = batches.begin();
			batch_index_out = entry->first;
			payload_out = std::move(entry->second.payload);
			buffered_bytes -= entry->second.bytes;
			batches.erase(entry);
			cv.notify_all(); // pushers may be waiting on capacity
			return PopStatus::BATCH;
		}
		if (finished) {
			return PopStatus::FINISHED;
		}
		return PopStatus::EMPTY;
	}

	//! Block until the claimed batch arrives, the stream ends, or a short timeout elapses.
	void WaitForBatch(idx_t claim) {
		annotated_unique_lock<annotated_mutex> guard(lock);
		if (batches.find(claim) != batches.end() || finished || errored) {
			return;
		}
		// Bounded wait so the consumer can re-check cancellation even if the batch never arrives.
		cv.wait_for(guard, std::chrono::milliseconds(200));
	}

	//! Block until any batch is available, the stream ends, or a short timeout elapses.
	void WaitForAny() {
		annotated_unique_lock<annotated_mutex> guard(lock);
		if (!batches.empty() || finished || errored) {
			return;
		}
		// Bounded wait so the consumer can re-check cancellation even if no batch ever arrives.
		cv.wait_for(guard, std::chrono::milliseconds(200));
	}

	//! End the stream. When expected_total is set, error out unless exactly that many batches were
	//! pushed — validates that a dense stream arrived completely.
	void Finish(optional_idx expected_total = optional_idx()) {
		std::map<idx_t, shared_ptr<ReadAheadJobCompletion>> drained;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (expected_total.IsValid() && !errored && pushed_count != expected_total.GetIndex()) {
				errored = true;
				error = ErrorData(ExceptionType::IO,
				                  StringUtil::Format("Quack stream ended with %llu of %llu batches received",
				                                     pushed_count, expected_total.GetIndex()));
			}
			finished = true;
			drained = std::move(waiters);
			waiters.clear();
		}
		cv.notify_all();
		// wake every parked claimant so it observes FINISHED (or the count-mismatch error)
		for (auto &entry : drained) {
			entry.second->FinishIOTask();
		}
	}

	void SetError(ErrorData error_p) {
		std::map<idx_t, shared_ptr<ReadAheadJobCompletion>> drained;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (!errored) {
				errored = true;
				error = std::move(error_p);
			}
			finished = true;
			drained = std::move(waiters);
			waiters.clear();
		}
		cv.notify_all();
		// wake every parked claimant so it observes ERRORED
		for (auto &entry : drained) {
			entry.second->FinishIOTask();
		}
	}

	bool HasError() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return errored;
	}

	//! The producer closed the stream (Finish or SetError ran).
	bool Finished() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return finished;
	}

	ErrorData GetError() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return error;
	}

	//! The stream ended and every published batch has been consumed.
	bool Exhausted() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return finished && batches.empty();
	}

private:
	//! No pending batch precedes this one, so the consumer's next pop is waiting on exactly this index;
	//! admitting it over capacity is the only way forward, and overshoots the cap by at most one batch.
	bool HeadOfStream(idx_t batch_index) const DUCKDB_REQUIRES(lock) {
		return batches.empty() || batches.begin()->first > batch_index;
	}

	//! True when this dense index was already pushed once (whether or not it was popped since).
	bool Seen(idx_t batch_index) const DUCKDB_REQUIRES(lock) {
		return seen.Contains(batch_index);
	}

	void RecordSeen(idx_t batch_index) DUCKDB_REQUIRES(lock) {
		seen.Insert(batch_index);
	}

private:
	struct Entry {
		PAYLOAD payload;
		idx_t bytes;
	};

	annotated_mutex lock;
	std::condition_variable cv;
	idx_t next_claim DUCKDB_GUARDED_BY(lock) = 1;
	//! batch_index -> published batch, awaiting a consumer.
	std::map<idx_t, Entry> batches DUCKDB_GUARDED_BY(lock);
	idx_t buffered_bytes DUCKDB_GUARDED_BY(lock) = 0;
	idx_t capacity DUCKDB_GUARDED_BY(lock) = 0;
	//! batch_index -> wake event for the parked claimant; publishing that index fires exactly this one.
	std::map<idx_t, shared_ptr<ReadAheadJobCompletion>> waiters DUCKDB_GUARDED_BY(lock);
	//! Batches ever pushed, checked against Finish(expected_total); dedupe keeps retries from inflating it.
	idx_t pushed_count DUCKDB_GUARDED_BY(lock) = 0;
	//! Push dedupe.
	QuackDenseIndexSet seen DUCKDB_GUARDED_BY(lock);
	bool finished DUCKDB_GUARDED_BY(lock) = false;
	bool errored DUCKDB_GUARDED_BY(lock) = false;
	ErrorData error DUCKDB_GUARDED_BY(lock);
};

//! One batch stored as owned chunks — the client fetch buffer and the server insert stream.
using QuackChunkBatch = vector<unique_ptr<DataChunk>>;
using QuackChunkClaimBuffer = QuackClaimBuffer<QuackChunkBatch>;
//! The client fetch path reads more naturally with its historical name.
using QuackFetchBuffer = QuackChunkClaimBuffer;

} // namespace duckdb
