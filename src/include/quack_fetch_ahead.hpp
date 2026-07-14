#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/serializer/async_task_queue.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_client.hpp"
#include "quack_data_stream.hpp"

namespace duckdb {

//! Client-side FETCH read-ahead: keeps up to `depth` FETCH requests in flight on the ASYNC pool so
//! scan threads consume decoded batches from a buffer instead of blocking on the wire. Mirrors the
//! async SEND_DATA path. Top-up is consumer-driven only: tasks never register new tasks.
class QuackFetchAhead {
	friend class QuackFetchDataTask;

public:
	QuackFetchAhead(ClientContext &context, QuackClientConnection &connection, hugeint_t query_uuid, idx_t depth_p);
	~QuackFetchAhead();

public:
	//! Resolve the read-ahead depth from the quack_fetch_read_ahead setting (0 = async thread count).
	static idx_t GetReadAheadDepth(ClientContext &context);

	QuackDataStream &Buffer() {
		return *buffer;
	}
	shared_ptr<QuackDataStream> BufferPtr() {
		return buffer;
	}

	//! Register fetch tasks until in-flight + buffered batches reach `depth`.
	void TopUp();
	//! Account one popped batch and refill the pipeline. Called by scan threads after a successful pop.
	void BatchConsumed();
	//! Stop topping up and drain all in-flight fetches; errors were already surfaced through the buffer.
	void StopAndDrain();

private:
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
