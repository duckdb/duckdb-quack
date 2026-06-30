#include "quack_data_stream.hpp"

#include "duckdb/common/string_util.hpp"

#include <chrono>

namespace duckdb {

void QuackDataStream::Push(unique_ptr<DataChunk> chunk, optional_idx batch_index) {
	annotated_unique_lock<annotated_mutex> guard(lock);
	// Wait while the queue is full (unless the stream ended). Written as an explicit loop rather than a
	// cv predicate lambda so the guarded fields are read under `guard`, where the analysis can see it.
	while (queue.size() >= capacity && !finished && !errored) {
		cv_nonfull.wait(guard);
	}
	if (finished || errored) {
		// Consumer is gone or the statement failed - drop the chunk; the error is surfaced elsewhere.
		return;
	}
	QuackStreamChunk entry;
	entry.chunk = std::move(chunk);
	entry.batch_index = batch_index;
	queue.push_back(std::move(entry));
	cv_nonempty.notify_one();
}

void QuackDataStream::Finish() {
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		finished = true;
	}
	cv_nonempty.notify_all();
	cv_nonfull.notify_all();
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
	cv_nonfull.notify_all();
}

bool QuackDataStream::HasError() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return errored;
}

ErrorData QuackDataStream::GetError() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return error;
}

QuackDataStream::PopStatus QuackDataStream::TryPop(QuackStreamChunk &out) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (errored) {
		return PopStatus::ERRORED;
	}
	if (!queue.empty()) {
		out = std::move(queue.front());
		queue.pop_front();
		cv_nonfull.notify_one();
		return PopStatus::CHUNK;
	}
	if (finished) {
		return PopStatus::FINISHED;
	}
	return PopStatus::EMPTY;
}

void QuackDataStream::WaitForData() {
	annotated_unique_lock<annotated_mutex> guard(lock);
	if (!queue.empty() || finished || errored) {
		return;
	}
	// Bounded wait: woken by Push/Finish/SetError, else times out so the scan can re-check cancellation.
	cv_nonempty.wait_for(guard, std::chrono::milliseconds(200));
}

QuackStreamRegistry &QuackStreamRegistry::Get() {
	static QuackStreamRegistry instance;
	return instance;
}

string QuackStreamRegistry::MakeId(const string &connection_id, hugeint_t query_uuid) {
	return connection_id + ":" + UUID::ToString(query_uuid);
}

shared_ptr<QuackDataStream> QuackStreamRegistry::Create(const string &id, vector<LogicalType> types) {
	auto stream = make_shared_ptr<QuackDataStream>(std::move(types));
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
