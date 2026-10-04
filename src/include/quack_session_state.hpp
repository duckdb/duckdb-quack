//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_session_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "quack_insert_stream.hpp"
#include "quack_rebalancer_sink.hpp"

namespace duckdb {

struct QuackResultStream;

//! The quack session that owns a server-side ClientContext: the streams its SEND_DATA messages feed,
//! and the statement that drains them.
class QuackSessionState : public ClientContextState {
public:
	static constexpr const char *KEY = "quack_session";

	explicit QuackSessionState(string connection_id_p) : connection_id(std::move(connection_id_p)) {
	}

	//! Null for any context that the quack server does not own.
	static shared_ptr<QuackSessionState> Get(ClientContext &context) {
		return context.registered_state->Get<QuackSessionState>(KEY);
	}

	static idx_t StreamBufferBytes(DatabaseInstance &db) {
		// One buffer exists for each connection, so a fixed size does not suit every server. Core
		// also sizes an optional buffer at a quarter of the memory limit. 0 still means no limit.
		auto buffer_bytes =
		    QuackGetUBigintSetting(db, "quack_fetch_producer_buffer_bytes", QUACK_FETCH_PRODUCER_BUFFER_BYTES_DEFAULT);
		auto memory_cap = BufferManager::GetBufferManager(db).GetOperatorMemoryLimit() / 4;
		if (buffer_bytes > 0 && memory_cap > 0) {
			buffer_bytes = MinValue<idx_t>(buffer_bytes, memory_cap);
		}
		return buffer_bytes;
	}

	//! The id the client uses to address this session, as returned to it by the connection handshake.
	const string &ConnectionId() const {
		return connection_id;
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
	string connection_id;
	mutex lock;
	weak_ptr<QuackResultStream> statement;
	QuackInsertStreamRegistry streams;
};

} // namespace duckdb
