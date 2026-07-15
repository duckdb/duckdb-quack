#include "quack_log.hpp"

namespace duckdb {

constexpr LogLevel QuackLogType::LEVEL;

QuackLogType::QuackLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType QuackLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"message_type", LogicalType::VARCHAR},   {"quack_connection_id", LogicalType::VARCHAR},
	    {"client_id_hash", LogicalType::VARCHAR}, {"client_query_id", LogicalType::UBIGINT},
	    {"query", LogicalType::VARCHAR},          {"server", LogicalType::VARCHAR},
	    {"duration_ms", LogicalType::BIGINT},     {"response_type", LogicalType::VARCHAR},
	    {"error", LogicalType::VARCHAR},
	};
	return LogicalType::STRUCT(child_list);
}

string QuackLogType::ConstructLogMessage(MessageType request_type, const string &connection_id,
                                         const string &client_id_hash, optional_idx client_query_id,
                                         const string &query, const string &server_uri, int64_t duration_ms,
                                         MessageType response_type, const string &error) {
	child_list_t<Value> child_list = {
	    {"message_type", Value(MessageTypeToString(request_type))},
	    {"quack_connection_id", Value(connection_id)},
	    {"client_id_hash", client_id_hash.empty() ? Value() : Value(client_id_hash)},
	    {"client_query_id", client_query_id.IsValid() ? Value::UBIGINT(client_query_id.GetIndex()) : Value()},
	    {"query", query.empty() ? Value() : Value(query)},
	    {"server", server_uri.empty() ? Value() : Value(server_uri)},
	    {"duration_ms", Value::BIGINT(duration_ms)},
	    {"response_type", Value(MessageTypeToString(response_type))},
	    {"error", error.empty() ? Value() : Value(error)},
	};
	return Value::STRUCT(std::move(child_list)).ToString();
}

} // namespace duckdb
