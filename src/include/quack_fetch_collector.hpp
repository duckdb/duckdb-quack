//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_fetch_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_claim_buffer.hpp"

#include <condition_variable>

namespace duckdb {

class ClientContext;
class PhysicalOperator;
struct PreparedStatementData;

//! One sealed FETCH_RESPONSE wire payload awaiting delivery. The batch-index field is patched twice:
//! the dense index at emit time, the client-visible remap (dense − prepare_batches) at reply time.
struct QuackFetchPayload {
	unique_ptr<MemoryStream> payload;
	idx_t payload_size = 0;
	//! Offset of the fixed-width batch-index field (patch target).
	idx_t index_offset = 0;
	//! Rows in the batch (drives the PREPARE inline-drain loop).
	idx_t rows = 0;
};
using QuackFetchPayloadBuffer = QuackClaimBuffer<QuackFetchPayload>;

//! Server-side state for one client-facing query result: the claim buffer the fetch collector fills
//! (sealed wire payloads, out of order, byte-capacity bounded) and the FETCH handlers drain.
struct QuackFetchStream {
	QuackFetchPayloadBuffer buffer;

	void SignalBound(vector<LogicalType> types_p, vector<string> names_p) {
		{
			lock_guard<mutex> guard(bind_lock);
			types = std::move(types_p);
			names = std::move(names_p);
			bound = true;
		}
		bind_cv.notify_all();
	}

	//! Wait until the query is planned (true) or failed before planning (false — see buffer error).
	bool WaitBound() {
		unique_lock<mutex> guard(bind_lock);
		while (!bound && !buffer.HasError() && !buffer.Exhausted()) {
			bind_cv.wait_for(guard, std::chrono::milliseconds(50));
		}
		return bound;
	}

	bool Bound() {
		lock_guard<mutex> guard(bind_lock);
		return bound;
	}

	//! Valid after WaitBound() returned true.
	vector<LogicalType> types;
	vector<string> names;
	//! Dense batches the PREPARE response consumed inline; FETCH responses subtract this so the
	//! client-facing indices stay dense from 1. Written by PREPARE before any FETCH arrives.
	idx_t prepare_batches = 0;
	//! Total dense batches the producer emitted; shipped on the terminal FINISHED response so a short
	//! stream fails loudly instead of truncating.
	optional_idx announced_total;

	//! Served payloads retained until the client acks them, so a transport retry re-serves the SAME
	//! batch instead of losing it; bounded by the client's in-flight read-ahead window.
	mutex serve_lock;
	std::map<idx_t, shared_ptr<MemoryStream>> served;

private:
	mutex bind_lock;
	std::condition_variable bind_cv;
	bool bound = false;
};

//! Plugged into ClientConfig::get_result_collector so the query executes in parallel into `stream`
//! instead of a single lazily-driven cursor.
unique_ptr<PhysicalOperator> MakeQuackFetchCollector(ClientContext &context, PreparedStatementData &data,
                                                     shared_ptr<QuackFetchStream> stream);

} // namespace duckdb
