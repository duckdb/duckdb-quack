#include "quack_log.hpp"

#include "utf8proc_wrapper.hpp"

namespace duckdb {

static Value LogMessageType(MessageType type) {
	try {
		return Value(MessageTypeToString(type));
	} catch (std::exception &) {
		return Value(StringUtil::Format("UNKNOWN(%d)", static_cast<int>(type)));
	}
}

static Value LogText(const string &text, idx_t max_bytes = 0) {
	if (text.empty()) {
		return Value();
	}
	bool truncated = max_bytes > 0 && text.size() > max_bytes;
	string result = truncated ? text.substr(0, max_bytes) : text;
	if (!Utf8Proc::IsValid(result.c_str(), result.size())) {
		Utf8Proc::MakeValid(&result[0], result.size());
	}
	// The logger takes the message as a C string, so a NUL would end it early.
	result = StringUtil::Replace(result, string(1, '\0'), "\\0");
	if (truncated) {
		result += StringUtil::Format("...(%llu bytes)", text.size());
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
	    {"quack_connection_id", LogText(connection_id, MAX_CONNECTION_ID_BYTES)},
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
