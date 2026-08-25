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

//! One sealed FETCH_RESPONSE payload. Two patches write its batch-index field: the dense index at
//! emit time, then the client-visible index (dense minus prepare_batches) at reply time.
struct QuackFetchPayload {
	unique_ptr<MemoryStream> payload;
	idx_t payload_size = 0;
	//! Where the fixed-width batch-index field starts: the patch target.
	idx_t index_offset = 0;
	//! The rows in the batch. The PREPARE inline drain counts them.
	idx_t rows = 0;
};
using QuackFetchPayloadBuffer = QuackClaimBuffer<QuackFetchPayload>;

//! Server state for one client-facing result. The fetch collector fills the claim buffer with sealed
//! payloads, out of order and under a byte limit. The FETCH handlers drain it.
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

	//! Raised when a scan of this statement registers a client data stream. The statement cannot
	//! reach a result until the client sends its batches, so PREPARE must answer now.
	void SignalClientDataPending() {
		{
			lock_guard<mutex> guard(bind_lock);
			client_data_pending = true;
		}
		bind_cv.notify_all();
	}

	//! true = the query is planned, or it waits for the client's data. false = it failed before
	//! that; the buffer holds the error.
	bool WaitBound() {
		unique_lock<mutex> guard(bind_lock);
		while (!bound && !client_data_pending && !buffer.HasError() && !buffer.Exhausted()) {
			bind_cv.wait_for(guard, std::chrono::milliseconds(50));
		}
		return bound || client_data_pending;
	}

	bool Bound() {
		lock_guard<mutex> guard(bind_lock);
		return bound;
	}

	vector<LogicalType> types;
	vector<string> names;
	bool client_data_pending = false;
	//! The dense batches PREPARE consumed inline. FETCH subtracts this, so the client-facing indices
	//! stay dense from 1. PREPARE writes it before the first FETCH arrives.
	idx_t prepare_batches = 0;
	//! The dense batches the producer emitted. The terminal response carries it, so a short stream
	//! fails instead of truncating.
	optional_idx announced_total;

	//! A payload the stream keeps, so it can serve the same bytes again.
	struct RetainedPayload {
		shared_ptr<MemoryStream> payload;
		idx_t rows = 0;
	};

	//! Kept until the client acks them, so a transport retry gets the SAME batch again. The client's
	//! read-ahead window bounds this map. With `retain_all` an ack releases nothing, and the map
	//! grows to the size of the result.
	mutex serve_lock;
	std::map<idx_t, RetainedPayload> served;
	//! Set while a result cache pins this stream. Guarded by serve_lock.
	bool retain_all = false;
	//! Rows in `served`. Guarded by serve_lock.
	idx_t retained_rows = 0;

	//! Keep every payload from now on. An ack then releases nothing.
	void RetainAll() {
		lock_guard<mutex> guard(serve_lock);
		retain_all = true;
	}

	//! Add a payload that the PREPARE inline drain served, outside the FETCH handler.
	void Retain(idx_t dense_index, shared_ptr<MemoryStream> payload, idx_t rows) {
		lock_guard<mutex> guard(serve_lock);
		if (!served.emplace(dense_index, RetainedPayload {std::move(payload), rows}).second) {
			return;
		}
		retained_rows += rows;
	}

	idx_t RetainedRows() {
		lock_guard<mutex> guard(serve_lock);
		return retained_rows;
	}

	//! Stop retaining and drop what is held. The result grew past the cache limit.
	void DropRetention() {
		lock_guard<mutex> guard(serve_lock);
		retain_all = false;
		served.clear();
		retained_rows = 0;
	}

private:
	mutex bind_lock;
	std::condition_variable bind_cv;
	bool bound = false;
};

//! Installed in ClientConfig::get_result_collector, so the query runs in parallel into `stream`.
unique_ptr<PhysicalOperator> MakeQuackFetchCollector(ClientContext &context, PreparedStatementData &data,
                                                     shared_ptr<QuackFetchStream> stream);

} // namespace duckdb
