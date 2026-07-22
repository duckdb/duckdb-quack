#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/serializer/async_task_queue.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/async_result.hpp"

#include <condition_variable>
#include <map>

#include "quack_client.hpp"

namespace duckdb {

struct TableFunctionInput;
class ReadAheadJobCompletion;

//! Result of QuackFetcher::GetBatch, from the consuming scan thread's perspective.
enum class QuackFetchResult : uint8_t {
	//! The claimed batch was popped.
	BATCH,
	//! The stream ended below this thread's claim; this thread is done.
	FINISHED,
	//! Batch not ready; the scan yielded via input.async_result and will be rescheduled.
	BLOCKED
};

//! Claim-based delivery: FETCH batch indices are dense, so each scan thread claims the next index and
//! waits for exactly that batch — per-thread order is monotone; downstream sinks reassemble global order.
class QuackFetchBuffer {
public:
	enum class PopStatus : uint8_t {
		BATCH,
		EMPTY,
		FINISHED,
		ERRORED,
	};

	//! Claim the next batch index to consume; each index gets exactly one claimant.
	idx_t ClaimBatch();
	//! Publish a decoded batch under its server-assigned index.
	void PushBatch(idx_t batch_index, vector<unique_ptr<DataChunk>> chunks);
	//! Pop the claimed batch if present. FINISHED means the stream ended and the claim can never arrive.
	PopStatus TryPopClaimed(idx_t claim, vector<unique_ptr<DataChunk>> &chunks_out);
	//! Register a per-claim wake event; nullptr when the claim is already poppable (caller retries).
	shared_ptr<ReadAheadJobCompletion> RegisterWaiter(idx_t claim);
	//! Block until the claimed batch arrives, the stream ends, or a short timeout elapses.
	void WaitForBatch(idx_t claim);

	void Finish();
	void SetError(ErrorData error_p);
	ErrorData GetError();

private:
	annotated_mutex lock;
	std::condition_variable cv;
	idx_t next_claim DUCKDB_GUARDED_BY(lock) = 1;
	//! batch_index -> decoded chunks, awaiting its claimant.
	std::map<idx_t, vector<unique_ptr<DataChunk>>> batches DUCKDB_GUARDED_BY(lock);
	//! batch_index -> wake event for the parked claimant; publishing that index fires exactly this one.
	std::map<idx_t, shared_ptr<ReadAheadJobCompletion>> waiters DUCKDB_GUARDED_BY(lock);
	bool finished DUCKDB_GUARDED_BY(lock) = false;
	bool errored DUCKDB_GUARDED_BY(lock) = false;
	ErrorData error DUCKDB_GUARDED_BY(lock);
};

//! Client-side FETCH read-ahead: keeps up to `depth` fetches in flight on the ASYNC pool so scan
//! threads never block on the wire. Top-up is consumer-driven only: tasks never register new tasks.
class QuackFetcher {
	friend class QuackFetchDataTask;

public:
	QuackFetcher(ClientContext &context, QuackClientConnection &connection, hugeint_t query_uuid, idx_t depth_p);
	~QuackFetcher();

public:
	//! Resolve the read-ahead depth from the quack_fetch_read_ahead setting (0 = async thread count).
	static idx_t GetReadAheadDepth(ClientContext &context);

	//! Pop this thread's claimed batch, claiming a fresh index when `claim` is empty; the claim
	//! survives BLOCKED yields. Yields via input.async_result when supported, else waits inline.
	QuackFetchResult GetBatch(ClientContext &context, TableFunctionInput &input, optional_idx &claim,
	                          idx_t &batch_index, vector<unique_ptr<DataChunk>> &chunks);

	//! Stop topping up and drain all in-flight fetches; errors were already surfaced through the buffer.
	void StopAndDrain();

private:
	//! Register fetch tasks until in-flight + buffered batches reach `depth`.
	void TopUp();
	//! Account one popped batch and refill the pipeline.
	void BatchConsumed();
	//! Return a checked-out client to the idle stack.
	void ReturnClient(unique_ptr<QuackClientWrapper> client);
	//! Task epilogue: recycle the client, settle counters, finish the buffer after the last fetch.
	void FinishTask(unique_ptr<QuackClientWrapper> client, bool pushed_batch);

private:
	unique_ptr<ManagedAsyncTaskQueue> queue;
	shared_ptr<QuackFetchBuffer> buffer;
	//! The FETCH request bytes, encoded once; identical for every fetch of the query.
	unique_ptr<MemoryStream> payload;
	idx_t payload_size = 0;
	string connection_id;
	optional_idx client_query_id;
	//! Context logger captured on the creating thread; pool threads have no ClientContext.
	shared_ptr<Logger> logger;
	idx_t depth;
	//! DEBUG (quack_debug_fetch_delay_ms): max random delay before publishing a fetched batch.
	idx_t debug_delay_ms = 0;

	mutex client_lock;
	vector<unique_ptr<QuackClientWrapper>> idle_clients;

	//! In-flight fetches + buffered batches not yet popped; bounded by depth.
	atomic<idx_t> outstanding {0};
	atomic<idx_t> in_flight {0};
	atomic<bool> stop {false};
	//! Set on the first empty FETCH response; no further fetches are issued.
	atomic<bool> no_more_fetches {false};
};

} // namespace duckdb
