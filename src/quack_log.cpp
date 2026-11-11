#include "quack_log.hpp"

#include "utf8proc_wrapper.hpp"

namespace duckdb {

//! A request's type, or UNKNOWN(<byte>) for a value the message enum does not name. The server logs requests
//! before it validates them, so the type byte can be anything.
static Value LogMessageType(MessageType type) {
	try {
		return Value(MessageTypeToString(type));
	} catch (std::exception &) {
		return Value(StringUtil::Format("UNKNOWN(%d)", static_cast<int>(type)));
	}
}

//! `text` as a log value: NULL when empty, cut to `max_bytes` (with the original length noted) when given, and
//! with invalid UTF-8 replaced. The server logs request fields before it authenticates or validates them.
static Value LogText(const string &text, idx_t max_bytes = 0) {
	if (text.empty()) {
		return Value();
	}
	string result = text;
	if (max_bytes > 0 && result.size() > max_bytes) {
		result = result.substr(0, max_bytes) + StringUtil::Format("...(%llu bytes)", text.size());
	}
	if (!Utf8Proc::IsValid(result.c_str(), result.size())) {
		Utf8Proc::MakeValid(&result[0], result.size());
	}
	return Value(result);
}

constexpr LogLevel QuackLogType::LEVEL;
constexpr idx_t QuackLogType::MAX_CONNECTION_ID_BYTES;

QuackLogType::QuackLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType QuackLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"message_type", LogicalType::VARCHAR},   {"quack_connection_id", LogicalType::VARCHAR},
	    {"client_id_hash", LogicalType::VARCHAR}, {"client_query_id", LogicalType::UBIGINT},
	    {"query", LogicalType::VARCHAR},          {"server", LogicalType::VARCHAR},
	    {"duration_ms", LogicalType::BIGINT},     {"response_type", LogicalType::VARCHAR},
	    {"error", LogicalType::VARCHAR},          {"client_address", LogicalType::VARCHAR},
	};
	return LogicalType::STRUCT(child_list);
}

string QuackLogType::ConstructLogMessage(MessageType request_type, const string &connection_id,
                                         const string &client_id_hash, optional_idx client_query_id,
                                         const string &query, const string &server_uri, int64_t duration_ms,
                                         MessageType response_type, const string &error, const string &client_address) {
	child_list_t<Value> child_list = {
	    {"message_type", LogMessageType(request_type)},
	    // Not LogText: an empty id is logged as '' rather than NULL, as it always was.
	    {"quack_connection_id", connection_id.empty() ? Value("") : LogText(connection_id, MAX_CONNECTION_ID_BYTES)},
	    {"client_id_hash", LogText(client_id_hash)},
	    {"client_query_id", client_query_id.IsValid() ? Value::UBIGINT(client_query_id.GetIndex()) : Value()},
	    {"query", LogText(query)},
	    {"server", LogText(server_uri)},
	    {"duration_ms", Value::BIGINT(duration_ms)},
	    {"response_type", LogMessageType(response_type)},
	    {"error", LogText(error)},
	    {"client_address", LogText(client_address)},
	};
	return Value::STRUCT(std::move(child_list)).ToString();
}

} // namespace duckdb
