#include "quack_result_cache.hpp"

#include "duckdb/common/types/interval.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

#include "quack_fetch_collector.hpp"
#include "quack_server.hpp"

namespace duckdb {

QuackResultCache::~QuackResultCache() {
	// Dropping the reference only releases the retained payloads. The query behind the stream is
	// owned by the connection's fetch state, so whoever drops a cache that is still serving must
	// abort that stream as well -- see ExpireCacheIfStale's `still_serving`.
	(*live_caches)--;
}

bool ServerCachingEnabled(DatabaseInstance &db) {
	Value val;
	DBConfig::GetConfig(db).TryGetCurrentSetting("quack_enable_reconnects", val);
	return !val.IsNull() && val.GetValue<bool>();
}

idx_t CacheMaxRows(DatabaseInstance &db) {
	Value val;
	DBConfig::GetConfig(db).TryGetCurrentSetting("quack_cache_max_rows", val);
	if (val.IsNull()) {
		return 0;
	}
	return val.GetValue<uint64_t>();
}

int64_t ResultTtlMicros(DatabaseInstance &db) {
	Value val;
	DBConfig::GetConfig(db).TryGetCurrentSetting("quack_result_ttl", val);
	if (val.IsNull()) {
		return 0;
	}
	auto ttl_seconds = val.GetValue<uint64_t>();
	auto max_ttl_seconds = static_cast<uint64_t>(NumericLimits<int64_t>::Maximum()) / Interval::MICROS_PER_SEC;
	if (ttl_seconds > max_ttl_seconds) {
		return NumericLimits<int64_t>::Maximum();
	}
	return NumericCast<int64_t>(ttl_seconds) * Interval::MICROS_PER_SEC;
}

unique_ptr<QuackResultCache> ExpireCacheIfStale(QuackConnection &connection, timestamp_t now, int64_t ttl_micros,
                                                bool &still_serving) {
	still_serving = false;
	auto &cache = connection.result_cache;
	if (!cache || ttl_micros == 0) {
		return nullptr;
	}
	if (now.value - cache->last_served_at.value <= ttl_micros) {
		return nullptr;
	}
	// an expired unfinished stream must fail loudly on later fetches instead of silently truncating
	if (cache->query_uuid == connection.query_uuid && cache->stream && !cache->stream->buffer.Exhausted()) {
		connection.query_state = QuackQueryState::CANCELLED;
		still_serving = true;
	}
	auto doomed = std::move(connection.result_cache);
	connection.ClearResultCache();
	return doomed;
}

} // namespace duckdb
