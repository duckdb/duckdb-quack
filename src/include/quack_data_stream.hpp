#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <condition_variable>
#include <deque>

namespace duckdb {

struct QuackStreamChunk {
	unique_ptr<DataChunk> chunk;
	//! Wire batch index (set by the future async/ordered client). Invalid for the current client,
	//! in which case the scan assigns a constant batch index.
	optional_idx batch_index;
};

//! Bounded, thread-safe queue feeding one server-side scan_data_from_quack_client INSERT. SEND_DATA
//! handlers push chunks; the table function pops them; the bound backpressures the client.
class QuackDataStream {
public:
	enum class PopStatus : uint8_t {
		CHUNK,    //! a chunk was dequeued into `out`
		EMPTY,    //! queue empty, more chunks may still arrive
		FINISHED, //! queue empty and the stream is finished
		ERRORED   //! the stream recorded a fatal error
	};

	explicit QuackDataStream(vector<LogicalType> types_p, idx_t capacity_p = 64)
	    : types(std::move(types_p)), capacity(capacity_p) {
	}

	const vector<LogicalType> &Types() const {
		return types;
	}

	//! Producer: enqueue a chunk, blocking while the queue is full (until the consumer drains or the
	//! stream is finished/errored).
	void Push(unique_ptr<DataChunk> chunk, optional_idx batch_index);
	//! Producer: signal that no more chunks will arrive.
	void Finish();
	//! Record a fatal error (also unblocks producer and consumer).
	void SetError(ErrorData error_p);
	bool HasError();
	ErrorData GetError();

	//! Consumer: non-blocking dequeue. Fills `out` and returns CHUNK if one is ready; otherwise returns
	//! EMPTY / FINISHED / ERRORED. Never blocks.
	PopStatus TryPop(QuackStreamChunk &out);
	//! Consumer: bounded wait used by the async wait-task. Blocks until a chunk is available, the
	//! stream is finished/errored, or a short timeout elapses (so the caller can re-check cancellation).
	void WaitForData();

private:
	annotated_mutex lock;
	std::condition_variable cv_nonempty;
	std::condition_variable cv_nonfull;
	std::deque<QuackStreamChunk> queue DUCKDB_GUARDED_BY(lock);
	vector<LogicalType> types;
	idx_t capacity;
	bool finished DUCKDB_GUARDED_BY(lock) = false;
	bool errored DUCKDB_GUARDED_BY(lock) = false;
	ErrorData error DUCKDB_GUARDED_BY(lock);
};

//! Process-global registry mapping a stream id to its QuackDataStream, letting the
//! scan_data_from_quack_client table function find the stream the request handlers created.
class QuackStreamRegistry {
public:
	static QuackStreamRegistry &Get();

	static string MakeId(const string &connection_id, hugeint_t query_uuid);

	shared_ptr<QuackDataStream> Create(const string &id, vector<LogicalType> types);
	shared_ptr<QuackDataStream> Find(const string &id);
	void Erase(const string &id);

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<QuackDataStream>> streams DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
