#pragma once

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/log_type.hpp"
#include "quack_message.hpp"

namespace duckdb {

//! Wall-clock epoch millis, same clock domain as core's HTTP request logs (HTTPLogType), so Quack
//! and HTTP durations for the same request stay comparable.
inline int64_t QuackNowMillis() {
	return Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp());
}

class QuackLogType : public LogType {
public:
	static constexpr const char *NAME = "Quack";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;
	//! A logged connection id is cut to this length. The server logs the id a request carries before it checks
	//! it, so an unknown id can be any size; an id the server issued is 32 hex characters.
	static constexpr idx_t MAX_CONNECTION_ID_BYTES = 64;

	QuackLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(MessageType request_type, const string &connection_id,
	                                  const string &client_id_hash, optional_idx client_query_id, const string &query,
	                                  const string &server_uri, int64_t duration_ms, MessageType response_type,
	                                  const string &error, const string &client_address);
};

} // namespace duckdb
