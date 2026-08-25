#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/multi_file/multi_file_read_ahead.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/interrupt.hpp"

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

enum class QuackPushStatus : uint8_t {
	PUSHED,      //! inserted, and the waiter for this index is awake
	DROPPED,     //! a duplicate, or the stream is closed: the batch is consumed, do not retry
	NO_CAPACITY, //! not inserted: retry the same batch after capacity frees
};

//! Dense-index membership: a contiguous prefix plus a sparse overflow set. The caller locks.
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
	//! The highest index for which all of 1..index are present.
	idx_t ContiguousMax() const {
		return contiguous;
	}

private:
	idx_t contiguous = 0;
	std::set<idx_t> sparse;
};

//! Delivery for dense batch streams, indexed from 1. Each consumer claims the next index and waits
//! for that batch only. Unordered consumers pop any batch. PAYLOAD is the storage of one batch.
template <class PAYLOAD>
class QuackClaimBuffer {
public:
	using PopStatus = QuackClaimPopStatus;

	idx_t ClaimBatch() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return next_claim++;
	}

	//! Limit the buffered bytes. A full buffer holds the producer back. 0 = no limit.
	void SetCapacity(idx_t bytes) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		capacity = bytes;
	}

	//! Publish a batch under its dense index. The call waits until the batch fits. An empty buffer
	//! and the head of the stream always admit one, so the producer cannot deadlock.
	void PushBatch(idx_t batch_index, PAYLOAD payload, idx_t bytes = 0) {
		shared_ptr<ReadAheadJobCompletion> waiter;
		{
			annotated_unique_lock<annotated_mutex> guard(lock);
			if (Seen(batch_index)) {
				// a transport retry delivered this batch again
				return;
			}
			// The head of the stream always gets in. Later batches cannot starve the next pop.
			while (capacity > 0 && buffered_bytes > 0 && buffered_bytes + bytes > capacity && !finished && !errored &&
			       !HeadOfStream(batch_index)) {
				cv.wait(guard);
			}
			if (finished || errored) {
				return;
			}
			if (Seen(batch_index)) {
				// a duplicate got in while this push waited
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
			waiter->FinishIOTask();
		}
	}

	//! PushBatch that does not wait. NO_CAPACITY does not touch `payload`, so the caller can park and
	//! retry the same batch. With `interrupt` set, the next pop or the stream close fires it.
	QuackPushStatus TryPushBatch(idx_t batch_index, PAYLOAD &payload, idx_t bytes,
	                             optional_ptr<const InterruptState> interrupt) {
		shared_ptr<ReadAheadJobCompletion> waiter;
		{
			annotated_unique_lock<annotated_mutex> guard(lock);
			if (finished || errored) {
				return QuackPushStatus::DROPPED;
			}
			if (Seen(batch_index)) {
				// a transport retry delivered this batch again
				return QuackPushStatus::DROPPED;
			}
			if (capacity > 0 && buffered_bytes > 0 && buffered_bytes + bytes > capacity && !HeadOfStream(batch_index)) {
				if (interrupt) {
					capacity_waiters.push_back(*interrupt);
				}
				return QuackPushStatus::NO_CAPACITY;
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
			waiter->FinishIOTask();
		}
		return QuackPushStatus::PUSHED;
	}

	//! Returns nullptr if the claim is poppable now: the caller retries the pop.
	shared_ptr<ReadAheadJobCompletion> RegisterWaiter(idx_t claim) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		// One lock guards this check and the insert. A publish either makes the caller retry the pop,
		// or it finds this waiter. No wakeup is lost.
		if (batches.find(claim) != batches.end() || finished || errored) {
			return nullptr;
		}
		auto entry = waiters.find(claim);
		if (entry != waiters.end()) {
			return entry->second;
		}
		auto completion = make_shared_ptr<ReadAheadJobCompletion>(nullptr, 1);
		waiters.emplace(claim, completion);
		return completion;
	}

	//! FINISHED means the stream ended and the claim cannot arrive.
	PopStatus TryPopClaimed(idx_t claim, PAYLOAD &payload_out) {
		vector<InterruptState> capacity_wakes;
		PopStatus status;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (errored) {
				return PopStatus::ERRORED;
			}
			auto entry = batches.find(claim);
			if (entry != batches.end()) {
				payload_out = std::move(entry->second.payload);
				buffered_bytes -= entry->second.bytes;
				batches.erase(entry);
				capacity_wakes = TakeCapacityWaiters();
				status = PopStatus::BATCH;
			} else if (finished) {
				// Indices are dense and Finish() runs after the last push.
				return PopStatus::FINISHED;
			} else {
				return PopStatus::EMPTY;
			}
		}
		cv.notify_all(); // pushers may be waiting on capacity
		WakeCapacity(capacity_wakes);
		return status;
	}

	PopStatus TryPopAny(idx_t &batch_index_out, PAYLOAD &payload_out) {
		vector<InterruptState> capacity_wakes;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (errored) {
				return PopStatus::ERRORED;
			}
			if (batches.empty()) {
				return finished ? PopStatus::FINISHED : PopStatus::EMPTY;
			}
			auto entry = batches.begin();
			batch_index_out = entry->first;
			payload_out = std::move(entry->second.payload);
			buffered_bytes -= entry->second.bytes;
			batches.erase(entry);
			capacity_wakes = TakeCapacityWaiters();
		}
		cv.notify_all(); // pushers may be waiting on capacity
		WakeCapacity(capacity_wakes);
		return PopStatus::BATCH;
	}

	//! True when `claim` can be popped, or the stream ended. A consumer that only waits for the end
	//! of a statement uses it, because it must not take the batch away from the real consumer.
	bool BatchReadyOrEnd(idx_t claim) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return batches.find(claim) != batches.end() || finished || errored;
	}

	void WaitForBatch(idx_t claim) {
		annotated_unique_lock<annotated_mutex> guard(lock);
		if (batches.find(claim) != batches.end() || finished || errored) {
			return;
		}
		// The wait has a limit, so the consumer can look for a cancel again.
		cv.wait_for(guard, std::chrono::milliseconds(200));
	}

	void WaitForAny() {
		annotated_unique_lock<annotated_mutex> guard(lock);
		if (!batches.empty() || finished || errored) {
			return;
		}
		// The wait has a limit, so the consumer can look for a cancel again.
		cv.wait_for(guard, std::chrono::milliseconds(200));
	}

	//! End the stream. With expected_total set, a different push count becomes an error.
	void Finish(optional_idx expected_total = optional_idx()) {
		std::map<idx_t, shared_ptr<ReadAheadJobCompletion>> drained;
		vector<InterruptState> capacity_wakes;
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
			capacity_wakes = TakeCapacityWaiters();
		}
		cv.notify_all();
		for (auto &entry : drained) {
			entry.second->FinishIOTask();
		}
		WakeCapacity(capacity_wakes);
	}

	void SetError(ErrorData error_p) {
		std::map<idx_t, shared_ptr<ReadAheadJobCompletion>> drained;
		vector<InterruptState> capacity_wakes;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			if (!errored) {
				errored = true;
				error = std::move(error_p);
			}
			finished = true;
			drained = std::move(waiters);
			waiters.clear();
			capacity_wakes = TakeCapacityWaiters();
		}
		cv.notify_all();
		for (auto &entry : drained) {
			entry.second->FinishIOTask();
		}
		WakeCapacity(capacity_wakes);
	}

	bool HasError() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return errored;
	}

	//! Finish() or SetError() ran.
	bool Finished() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return finished;
	}

	ErrorData GetError() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return error;
	}

	//! The stream ended and every batch left the buffer.
	bool Exhausted() {
		annotated_lock_guard<annotated_mutex> guard(lock);
		return finished && batches.empty();
	}

private:
	//! No pending batch is before this one, so the next pop waits for this index. Admit it over the
	//! limit: that is the only way to make progress.
	bool HeadOfStream(idx_t batch_index) const DUCKDB_REQUIRES(lock) {
		return batches.empty() || batches.begin()->first > batch_index;
	}

	//! This index was pushed before, even if a pop removed it since.
	bool Seen(idx_t batch_index) const DUCKDB_REQUIRES(lock) {
		return seen.Contains(batch_index);
	}

	void RecordSeen(idx_t batch_index) DUCKDB_REQUIRES(lock) {
		seen.Insert(batch_index);
	}

	vector<InterruptState> TakeCapacityWaiters() DUCKDB_REQUIRES(lock) {
		auto taken = std::move(capacity_waiters);
		capacity_waiters.clear();
		return taken;
	}

	//! Fired outside the lock. An unnecessary wake only makes the producer retry.
	static void WakeCapacity(vector<InterruptState> &wakes) {
		for (auto &state : wakes) {
			state.Callback();
		}
	}

private:
	struct Entry {
		PAYLOAD payload;
		idx_t bytes;
	};

	annotated_mutex lock;
	std::condition_variable cv;
	idx_t next_claim DUCKDB_GUARDED_BY(lock) = 1;
	std::map<idx_t, Entry> batches DUCKDB_GUARDED_BY(lock);
	idx_t buffered_bytes DUCKDB_GUARDED_BY(lock) = 0;
	idx_t capacity DUCKDB_GUARDED_BY(lock) = 0;
	//! A publish of one index fires the waiter for that index only.
	std::map<idx_t, shared_ptr<ReadAheadJobCompletion>> waiters DUCKDB_GUARDED_BY(lock);
	//! Parked by TryPushBatch. A pop that frees bytes, or a stream close, fires all of them.
	vector<InterruptState> capacity_waiters DUCKDB_GUARDED_BY(lock);
	//! Checked against Finish(expected_total). Dedupe keeps retries out of this count.
	idx_t pushed_count DUCKDB_GUARDED_BY(lock) = 0;
	QuackDenseIndexSet seen DUCKDB_GUARDED_BY(lock);
	bool finished DUCKDB_GUARDED_BY(lock) = false;
	bool errored DUCKDB_GUARDED_BY(lock) = false;
	ErrorData error DUCKDB_GUARDED_BY(lock);
};

//! One batch as owned chunks: the client fetch buffer and the server insert stream.
using QuackChunkBatch = vector<unique_ptr<DataChunk>>;
using QuackChunkClaimBuffer = QuackClaimBuffer<QuackChunkBatch>;
using QuackFetchBuffer = QuackChunkClaimBuffer;

} // namespace duckdb
