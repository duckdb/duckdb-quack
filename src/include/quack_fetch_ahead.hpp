#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/serializer/async_task_queue.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/async_result.hpp"

#include "quack_claim_buffer.hpp"
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
	//! Register fetch tasks until in-flight + buffered batches reach `depth`. Each task names its batch
	//! and carries its own encoded payload, so a transport retry re-asks for the SAME batch (idempotent).
	void TopUp(ClientContext &context);
	//! Account one popped batch and refill the pipeline.
	void BatchConsumed(ClientContext &context);
	//! Record a received index and advance the contiguous-received watermark (the ack).
	void RecordReceived(idx_t index);
	//! Highest index with all of 1..ack received; the server may drop retained batches <= it.
	idx_t CurrentAck();
	//! Return a checked-out client to the idle stack.
	void ReturnClient(unique_ptr<QuackClientWrapper> client);
	//! Task epilogue: recycle the client, settle counters, finish the buffer after the last fetch.
	void FinishTask(unique_ptr<QuackClientWrapper> client, bool pushed_batch);

private:
	unique_ptr<ManagedAsyncTaskQueue> queue;
	shared_ptr<QuackFetchBuffer> buffer;
	hugeint_t query_uuid;
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
	//! Next client-visible batch index to request; requests are issued in dense order, matching the
	//! scan threads' claim sequence.
	atomic<idx_t> next_request {1};
	//! Received indices; the contiguous prefix is the ack watermark sent to the server.
	mutex ack_lock;
	QuackDenseIndexSet acked;
	atomic<bool> stop {false};
	//! Set on the first empty FETCH response; no further fetches are issued.
	atomic<bool> no_more_fetches {false};
	//! Client-visible total announced by the terminal FETCH response (INVALID_INDEX until seen);
	//! passed to the buffer's Finish so a short stream errors instead of truncating silently.
	atomic<uint64_t> expected_total {DConstants::INVALID_INDEX};
};

} // namespace duckdb
