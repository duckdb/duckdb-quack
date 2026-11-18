#include "quack_settings.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

idx_t QuackGetUBigintSetting(ClientContext &context, const char *name, idx_t default_value) {
	Value val;
	if (context.TryGetCurrentSetting(name, val) && !val.IsNull()) {
		return val.GetValue<uint64_t>();
	}
	return default_value;
}

idx_t QuackGetUBigintSetting(DatabaseInstance &db, const char *name, idx_t default_value) {
	Value val;
	if (DBConfig::GetConfig(db).TryGetCurrentSetting(name, val) && !val.IsNull()) {
		return val.GetValue<uint64_t>();
	}
	return default_value;
}

idx_t QuackHttpTimeout(DatabaseInstance &db) {
	return MaxValue<idx_t>(1, QuackGetUBigintSetting(db, "quack_http_timeout", QUACK_HTTP_TIMEOUT_DEFAULT));
}

idx_t QuackServerStreamBufferBytes(DatabaseInstance &db) {
	// The budget applies separately to each buffer; an INSERT also has a result buffer.
	auto bytes =
	    QuackGetUBigintSetting(db, "quack_server_stream_buffer_bytes", QUACK_SERVER_STREAM_BUFFER_BYTES_DEFAULT);
	// Match core's optional-buffer heuristic: at most a quarter of the operator memory limit.
	auto memory_cap = BufferManager::GetBufferManager(db).GetOperatorMemoryLimit() / 4;
	if (bytes > 0 && memory_cap > 0) {
		bytes = MinValue<idx_t>(bytes, memory_cap);
	}
	return bytes;
}

} // namespace duckdb
