#pragma once

#include "duckdb/logging/log_type.hpp"
#include "quack_message.hpp"

namespace duckdb {

class QuackLogType : public LogType {
public:
	static constexpr const char *NAME = "Quack";
	static constexpr LogLevel LEVEL = LogLevel::LOG_DEBUG;

	QuackLogType();

	static LogicalType GetLogType();
	static string ConstructLogMessage(MessageType request_type, const string &connection_id,
	                                  const string &client_id_hash, optional_idx client_query_id, const string &query,
	                                  const string &server_uri, int64_t duration_ms, MessageType response_type,
	                                  const string &error);
};

} // namespace duckdb
