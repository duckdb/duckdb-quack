#include "quack_data_stream.hpp"

#include "duckdb/common/string_util.hpp"

#include <chrono>

namespace duckdb {

void QuackDataStream::PushUnordered(vector<unique_ptr<DataChunk>> chunks) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (finished || errored) {
			return;
		}
		ready_batches_.push_back({unordered_batch_seq_++, std::move(chunks)});
	}
	cv_nonempty.notify_one();
}

void QuackDataStream::PushOrdered(vector<unique_ptr<DataChunk>> chunks, idx_t batch, idx_t seq, bool is_last,
                                  optional_idx watermark) {
	annotated_unique_lock<annotated_mutex> guard(lock);
	// Only lower the cursor before delivery starts; once started it must never retreat.
	if (watermark.IsValid()) {
		if (!next_expected_batch_.IsValid()) {
			next_expected_batch_ = watermark;
		} else if (!any_batch_delivered_ && watermark.GetIndex() < next_expected_batch_.GetIndex()) {
			next_expected_batch_ = watermark;
		}
	}
	if (is_last) {
		last_seq_for_batch[batch] = seq;
	}
	// Always insert — zero-chunk is_last messages must be recorded so the drain loop can account for them.
	ordered_pending[{batch, seq}] = std::move(chunks);
	DrainOrderedBatches();
}

void QuackDataStream::SetWatermarkAndDrain(optional_idx watermark) {
	annotated_unique_lock<annotated_mutex> guard(lock);
	if (watermark.IsValid() && !next_expected_batch_.IsValid()) {
		next_expected_batch_ = watermark;
		DrainOrderedBatches();
	}
}

void QuackDataStream::PushDeadRange(idx_t dead_start, idx_t dead_end) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (finished || errored) {
		return;
	}
	if (dead_end > dead_start) {
		dead_ranges[dead_start] = dead_end;
	}
	DrainOrderedBatches();
}

void QuackDataStream::DrainOrderedBatches() {
	if (!next_expected_batch_.IsValid()) {
		return;
	}
	while (true) {
		idx_t expected = next_expected_batch_.GetIndex();

		auto last_seq_it = last_seq_for_batch.find(expected);
		if (last_seq_it != last_seq_for_batch.end()) {
			idx_t last_seq = last_seq_it->second;

			bool all_seqs_present = true;
			for (idx_t seq = 0; seq <= last_seq; seq++) {
				if (ordered_pending.find({expected, seq}) == ordered_pending.end()) {
					all_seqs_present = false;
					break;
				}
			}
			if (!all_seqs_present) {
				break;
			}

			vector<unique_ptr<DataChunk>> batch_chunks;
			for (idx_t seq = 0; seq <= last_seq; seq++) {
				auto it = ordered_pending.find({expected, seq});
				for (auto &chunk : it->second) {
					batch_chunks.push_back(std::move(chunk));
				}
				ordered_pending.erase(it);
			}
			last_seq_for_batch.erase(last_seq_it);
			any_batch_delivered_ = true;
			ready_batches_.push_back({expected, std::move(batch_chunks)});
			next_expected_batch_ = optional_idx(expected + 1);
			cv_nonempty.notify_all();
			continue;
		}

		// No data for `expected`. If it falls inside a client-reported dead range, skip the whole gap;
		// otherwise it may still be in flight, so wait.
		if (!dead_ranges.empty()) {
			auto dr = dead_ranges.upper_bound(expected); // first range starting after `expected`
			if (dr != dead_ranges.begin()) {
				--dr; // range with lo <= expected
				if (dr->second > expected) {
					any_batch_delivered_ = true; // cursor is committed; it must never retreat
					next_expected_batch_ = optional_idx(dr->second);
					dead_ranges.erase(dr);
					continue;
				}
			}
		}
		break;
	}
}

void QuackDataStream::Finish() {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		finished = true;
	}
	cv_nonempty.notify_all();
}

void QuackDataStream::SetError(ErrorData error_p) {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (!errored) {
			errored = true;
			error = std::move(error_p);
		}
		finished = true;
	}
	cv_nonempty.notify_all();
}

bool QuackDataStream::HasError() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return errored;
}

ErrorData QuackDataStream::GetError() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return error;
}

QuackDataStream::PopBatchStatus QuackDataStream::TryPopBatch(idx_t &batch_idx_out,
                                                             vector<unique_ptr<DataChunk>> &chunks_out) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (errored) {
		return PopBatchStatus::ERRORED;
	}
	if (!ready_batches_.empty()) {
		batch_idx_out = ready_batches_.front().first;
		chunks_out = std::move(ready_batches_.front().second);
		ready_batches_.pop_front();
		return PopBatchStatus::BATCH;
	}
	if (finished) {
		return PopBatchStatus::FINISHED;
	}
	return PopBatchStatus::EMPTY;
}

void QuackDataStream::WaitForData() {
	annotated_unique_lock<annotated_mutex> guard(lock);
	if (!ready_batches_.empty() || finished || errored) {
		return;
	}
	// Bounded wait so the scan can re-check cancellation even if no batch arrives.
	cv_nonempty.wait_for(guard, std::chrono::milliseconds(200));
}

QuackStreamRegistry &QuackStreamRegistry::Get() {
	static QuackStreamRegistry instance;
	return instance;
}

string QuackStreamRegistry::MakeId(const string &connection_id, hugeint_t query_uuid) {
	return connection_id + ":" + UUID::ToString(query_uuid);
}

shared_ptr<QuackDataStream> QuackStreamRegistry::Create(const string &id, vector<LogicalType> types, bool ordered) {
	auto stream = make_shared_ptr<QuackDataStream>(std::move(types), ordered);
	annotated_lock_guard<annotated_mutex> guard(lock);
	streams[id] = stream;
	return stream;
}

shared_ptr<QuackDataStream> QuackStreamRegistry::Find(const string &id) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto entry = streams.find(id);
	if (entry == streams.end()) {
		return nullptr;
	}
	return entry->second;
}

void QuackStreamRegistry::Erase(const string &id) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	streams.erase(id);
}

} // namespace duckdb
