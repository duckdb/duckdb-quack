//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_insert_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "quack_claim_buffer.hpp"

namespace duckdb {

//! Server state for one client INSERT stream. The SEND_DATA handler fills the buffer, and
//! scan_data_from_quack_client drains it. It is the mirror of QuackFetchStream.
struct QuackInsertStream {
	QuackInsertStream(vector<LogicalType> types_p, bool ordered_p) : types(std::move(types_p)), ordered(ordered_p) {
	}

	vector<LogicalType> types;
	//! True if the INSERT must keep the client's stream order.
	bool ordered;
	QuackChunkClaimBuffer buffer;
};

//! Maps a stream id to its stream. It lives on the database instance state, because the scan and
//! the request handler that fills the buffer run on different ClientContexts.
class QuackInsertStreamRegistry {
public:
	static string MakeId(const string &connection_id, hugeint_t query_uuid) {
		return connection_id + ":" + UUID::ToString(query_uuid);
	}

	shared_ptr<QuackInsertStream> Create(const string &id, vector<LogicalType> types, bool ordered) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto stream = make_shared_ptr<QuackInsertStream>(std::move(types), ordered);
		streams[id] = stream;
		return stream;
	}

	shared_ptr<QuackInsertStream> Find(const string &id) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto entry = streams.find(id);
		return entry == streams.end() ? nullptr : entry->second;
	}

	void Erase(const string &id) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		streams.erase(id);
	}

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<QuackInsertStream>> streams DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
