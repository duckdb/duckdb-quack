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
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

#include "quack_claim_buffer.hpp"

namespace duckdb {

struct QuackResultStream;

//! Server state for one client data stream. The SEND_DATA handler fills the buffer, and
//! scan_data_from_quack_client drains it. It is the mirror of QuackResultStream.
struct QuackInsertStream {
	QuackInsertStream(vector<LogicalType> types_p, bool ordered_p) : types(std::move(types_p)), ordered(ordered_p) {
	}

	vector<LogicalType> types;
	//! True if the INSERT must keep the client's stream order.
	bool ordered;
	QuackChunkClaimBuffer buffer;
};

//! The streams of one session, by id. The scan makes the entry when it binds, and an entry stays
//! until the session's next statement, so a repeated terminal message still finds its outcome.
class QuackInsertStreamRegistry {
public:
	shared_ptr<QuackInsertStream> Create(const string &id, vector<LogicalType> types, bool ordered) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (streams.find(id) != streams.end()) {
			throw InvalidInputException("scan_data_from_quack_client: a data stream '%s' already exists", id);
		}
		auto stream = make_shared_ptr<QuackInsertStream>(std::move(types), ordered);
		streams[id] = stream;
		return stream;
	}

	shared_ptr<QuackInsertStream> Find(const string &id) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto entry = streams.find(id);
		return entry == streams.end() ? nullptr : entry->second;
	}

	//! Drop every stream. A `reason` errors each buffer first, so a scan that waits for a batch wakes up.
	void Clear(const string &reason = string()) {
		unordered_map<string, shared_ptr<QuackInsertStream>> dropped;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			dropped = std::move(streams);
			streams.clear();
		}
		if (reason.empty()) {
			return;
		}
		for (auto &entry : dropped) {
			entry.second->buffer.SetError(ErrorData(ExceptionType::INVALID_INPUT, reason));
		}
	}

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<QuackInsertStream>> streams DUCKDB_GUARDED_BY(lock);
};

//! The quack session that owns a server-side ClientContext: the streams its SEND_DATA messages feed,
//! and the statement that drains them.
class QuackSessionState : public ClientContextState {
public:
	static constexpr const char *KEY = "quack_session";

	//! Null for any context that the quack server does not own.
	static shared_ptr<QuackSessionState> Get(ClientContext &context) {
		return context.registered_state->Get<QuackSessionState>(KEY);
	}

	//! PREPARE sets this before the statement runs.
	void SetStatement(shared_ptr<QuackResultStream> stream) {
		lock_guard<mutex> guard(lock);
		statement = std::move(stream);
	}
	shared_ptr<QuackResultStream> Statement() {
		lock_guard<mutex> guard(lock);
		return statement.lock();
	}

	QuackInsertStreamRegistry &Streams() {
		return streams;
	}

private:
	mutex lock;
	weak_ptr<QuackResultStream> statement;
	QuackInsertStreamRegistry streams;
};

} // namespace duckdb
