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

//! The quack session a server-side ClientContext belongs to. The scan reads it, so a stream records
//! which session may feed it.
class QuackSessionState : public ClientContextState {
public:
	explicit QuackSessionState(string session_id_p) : session_id(std::move(session_id_p)) {
	}

	static constexpr const char *KEY = "quack_session";

	//! Null for any context that the quack server does not own.
	static shared_ptr<QuackSessionState> Get(ClientContext &context) {
		return context.registered_state->Get<QuackSessionState>(KEY);
	}

	//! PREPARE installs this before it runs the statement, and a scan that registers a client data
	//! stream calls it. It is how PREPARE learns that the statement now waits for the client.
	void SetClientDataHook(std::function<void()> hook) {
		lock_guard<mutex> guard(lock);
		on_client_data = std::move(hook);
	}
	void SignalClientData() {
		std::function<void()> hook;
		{
			lock_guard<mutex> guard(lock);
			hook = on_client_data;
		}
		if (hook) {
			hook();
		}
	}

	string session_id;

private:
	mutex lock;
	std::function<void()> on_client_data;
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
	QuackChunkClaimBuffer buffer;
};

//! Maps a stream id to its stream. It lives on the database instance state, because the scan and
//! the request handler that fills the buffer run on different ClientContexts. The scan creates the
//! entry when it binds, so the statement can be planned before the client sends a batch.
class QuackInsertStreamRegistry {
public:
	shared_ptr<QuackInsertStream> Create(const string &id, vector<LogicalType> types, bool ordered, string session_id) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto stream = make_shared_ptr<QuackInsertStream>(std::move(types), ordered, std::move(session_id));
		streams[id] = stream;
		return stream;
	}

	shared_ptr<QuackInsertStream> Find(const string &id) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto entry = streams.find(id);
		return entry == streams.end() ? nullptr : entry->second;
	}

	//! Drop every stream of a session. `reason`, when it is set, errors each buffer first, so a scan
	//! that waits for a batch wakes instead of holding the statement open.
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
