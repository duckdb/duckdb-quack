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

//! The quack session that owns a server-side ClientContext: which session may feed a stream, and
//! which statement drains it.
class QuackSessionState : public ClientContextState {
public:
	explicit QuackSessionState(string session_id_p) : session_id(std::move(session_id_p)) {
	}

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

	string session_id;

private:
	mutex lock;
	weak_ptr<QuackResultStream> statement;
};

//! Server state for one client data stream. The SEND_DATA handler fills the buffer, and
//! scan_data_from_quack_client drains it. It is the mirror of QuackResultStream.
struct QuackInsertStream {
	QuackInsertStream(vector<LogicalType> types_p, bool ordered_p, string session_id_p)
	    : types(std::move(types_p)), ordered(ordered_p), session_id(std::move(session_id_p)) {
	}

	vector<LogicalType> types;
	//! True if the INSERT must keep the client's stream order.
	bool ordered;
	//! Only this session may push to the stream.
	string session_id;
	//! The statement that drains this stream; the terminal SEND_DATA waits on it.
	weak_ptr<QuackResultStream> result;
	QuackChunkClaimBuffer buffer;
};

//! Maps a stream id to its stream. It lives on the database instance state, because the scan and the
//! request handler run on different ClientContexts. The scan makes the entry when it binds, so the
//! statement is planned before the client sends a batch. An entry stays until the session's next
//! statement, so a repeated terminal message still finds its outcome.
class QuackInsertStreamRegistry {
public:
	shared_ptr<QuackInsertStream> Create(const string &id, vector<LogicalType> types, bool ordered, string session_id) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (streams.find(id) != streams.end()) {
			throw InvalidInputException("scan_data_from_quack_client: a data stream '%s' already exists", id);
		}
		auto stream = make_shared_ptr<QuackInsertStream>(std::move(types), ordered, std::move(session_id));
		streams[id] = stream;
		return stream;
	}

	shared_ptr<QuackInsertStream> Find(const string &id) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto entry = streams.find(id);
		return entry == streams.end() ? nullptr : entry->second;
	}

	//! Drop every stream of a session. A `reason` errors each buffer first, so a scan that waits for a
	//! batch wakes up. Without it the statement stays open.
	void DropSession(const string &session_id, const string &reason = string()) {
		vector<shared_ptr<QuackInsertStream>> dropped;
		{
			annotated_lock_guard<annotated_mutex> guard(lock);
			for (auto entry = streams.begin(); entry != streams.end();) {
				if (entry->second->session_id != session_id) {
					++entry;
					continue;
				}
				dropped.push_back(entry->second);
				entry = streams.erase(entry);
			}
		}
		if (reason.empty()) {
			return;
		}
		for (auto &stream : dropped) {
			stream->buffer.SetError(ErrorData(ExceptionType::INVALID_INPUT, reason));
		}
	}

private:
	annotated_mutex lock;
	unordered_map<string, shared_ptr<QuackInsertStream>> streams DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
