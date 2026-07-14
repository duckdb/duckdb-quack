#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <condition_variable>
#include <deque>
#include <map>

namespace duckdb {

//! Thread-safe delivery queue for scan_data_from_quack_client. Producers push complete batches into
//! ready_batches_; scan tasks claim one batch at a time via TryPopBatch for exclusive ownership.
class QuackDataStream {
public:
	enum class PopBatchStatus : uint8_t {
		BATCH,
		EMPTY,
		FINISHED,
		ERRORED,
	};

	explicit QuackDataStream(vector<LogicalType> types_p, bool ordered_p = false)
	    : types(std::move(types_p)), ordered(ordered_p) {
	}

	const vector<LogicalType> &Types() const {
		return types;
	}

	bool IsOrdered() const {
		return ordered;
	}

	//! Publish all chunks from one unordered wire message as a single ready batch.
	void PushUnordered(vector<unique_ptr<DataChunk>> chunks);

	//! Buffer chunks for (batch, seq); drain complete batches to ready_batches_ in order.
	//! `watermark` seeds the delivery cursor; once started the cursor only advances.
	void PushOrdered(vector<unique_ptr<DataChunk>> chunks, idx_t batch, idx_t seq, bool is_last,
	                 optional_idx watermark);

	//! Record that batches [dead_start, dead_end) are dead (never produced), so the cursor can skip the gap.
	//! Safe under reordering: recorded, then applied when the cursor reaches it.
	void PushDeadRange(idx_t dead_start, idx_t dead_end);

	//! Seed the delivery cursor from FINALIZE if no per-message watermark arrived first.
	void SetWatermarkAndDrain(optional_idx watermark);

	void Finish();
	void SetError(ErrorData error_p);
	bool HasError();
	ErrorData GetError();

	//! Atomically claim the next ready batch; moves chunks into chunks_out.
	PopBatchStatus TryPopBatch(idx_t &batch_idx_out, vector<unique_ptr<DataChunk>> &chunks_out);
	//! Block until a batch is ready, the stream ends, or a short timeout elapses.
	void WaitForData();

private:
	void DrainOrderedBatches() DUCKDB_REQUIRES(lock);

	annotated_mutex lock;
	std::condition_variable cv_nonempty;
	std::deque<std::pair<idx_t, vector<unique_ptr<DataChunk>>>> ready_batches_ DUCKDB_GUARDED_BY(lock);
	//! Monotonic counter for unordered batch indices (ignored by PhysicalInsert, kept for uniformity).
	idx_t unordered_batch_seq_ DUCKDB_GUARDED_BY(lock) = 0;
	vector<LogicalType> types;
	bool ordered;
	bool finished DUCKDB_GUARDED_BY(lock) = false;
	bool errored DUCKDB_GUARDED_BY(lock) = false;
	ErrorData error DUCKDB_GUARDED_BY(lock);

	// Ordered assembly state — all guarded by lock.
	std::map<std::pair<idx_t, idx_t>, vector<unique_ptr<DataChunk>>> ordered_pending DUCKDB_GUARDED_BY(lock);
	std::map<idx_t, idx_t> last_seq_for_batch DUCKDB_GUARDED_BY(lock);
	optional_idx next_expected_batch_ DUCKDB_GUARDED_BY(lock);
	bool any_batch_delivered_ DUCKDB_GUARDED_BY(lock) = false;
	//! Dead batch ranges [lo, hi) reported by the client for filtered/pruned gaps the sink never crossed.
	//! The drain skips the cursor over a covering range instead of waiting for data that never comes.
	std::map<idx_t, idx_t> dead_ranges DUCKDB_GUARDED_BY(lock);
};

//! Process-global registry: maps stream id → QuackDataStream so the scan function can find it.
class QuackStreamRegistry {
public:
	static QuackStreamRegistry &Get();

	static string MakeId(const string &connection_id, hugeint_t query_uuid);

	shared_ptr<QuackDataStream> Create(const string &id, vector<LogicalType> types, bool ordered);
	shared_ptr<QuackDataStream> Find(const string &id);
	void Erase(const string &id);

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<QuackDataStream>> streams DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
