#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/serializer/async_task_queue.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_client.hpp"
#include "quack_data_stream.hpp"

namespace duckdb {

struct TableFunctionInput;

//! Result of QuackFetcher::GetBatch, from the consuming scan thread's perspective.
enum class QuackFetchResult : uint8_t {
	//! A batch was popped; its chunks and batch index are set.
	BATCH,
	//! The server cursor is exhausted and the buffer fully drained.
	FINISHED,
	//! No batch ready; the scan yielded via input.async_result and will be rescheduled.
	BLOCKED
};

//! Client-side FETCH read-ahead: keeps up to `depth` FETCH requests in flight on the ASYNC pool so
//! scan threads consume decoded batches from a buffer instead of blocking on the wire. Mirrors the
//! async SEND_DATA path. Top-up is consumer-driven only: tasks never register new tasks.
class QuackFetcher {
	friend class QuackFetchDataTask;

public:
	QuackFetcher(ClientContext &context, QuackClientConnection &connection, hugeint_t query_uuid, idx_t depth_p);
	~QuackFetcher();

public:
	//! Resolve the read-ahead depth from the quack_fetch_read_ahead setting (0 = async thread count).
	static idx_t GetReadAheadDepth(ClientContext &context);

	//! Whether more batches may still arrive; false once GetBatch has returned FINISHED.
	bool HasMore() const {
		return !exhausted;
	}

	//! Pop the next batch, refilling the fetch pipeline as slots free up. When no batch is ready:
	//! yields BLOCKED through input.async_result if the executor supports it, otherwise waits inline.
	QuackFetchResult GetBatch(ClientContext &context, TableFunctionInput &input, idx_t &batch_index,
	                          vector<unique_ptr<DataChunk>> &chunks);

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
	shared_ptr<QuackDataStream> buffer;
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
	//! Consumer-side end-of-stream: the buffer reported FINISHED to GetBatch.
	atomic<bool> exhausted {false};
};

} // namespace duckdb
