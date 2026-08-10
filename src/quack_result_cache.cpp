#include "quack_result_cache.hpp"

#include "duckdb/common/types/interval.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"

#include "quack_message.hpp"
#include "quack_server.hpp"

namespace duckdb {

static vector<unique_ptr<DataChunkWrapper>> CreateBatch(unique_ptr<QueryResult> &query_result, idx_t max_rows) {
	vector<unique_ptr<DataChunkWrapper>> results;
	idx_t rows = 0;
	while (rows < max_rows) {
		auto result_chunk = query_result->Fetch();
		if (!result_chunk && query_result->HasError()) {
			results.clear();
			return results;
		}
		if (!result_chunk || result_chunk->size() == 0) {
			query_result.reset();
			break;
		}
		rows += result_chunk->size();
		results.push_back(make_uniq<DataChunkWrapper>(*result_chunk));
	}
	return results;
}

QuackResultCache::~QuackResultCache() {
	if (tail && tail->GetResultType() == QueryResultType::STREAM_RESULT) {
		try {
			// If it's a stream we gotta close it
			tail->Cast<StreamQueryResult>().Close();
		} catch (...) {
		}
	}
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

unique_ptr<QuackResultCache> ExpireCacheIfStale(QuackConnection &connection, timestamp_t now, int64_t ttl_micros) {
	auto &cache = connection.result_cache;
	if (!cache || ttl_micros == 0) {
		return nullptr;
	}
	if (now.value - cache->last_served_at.value <= ttl_micros) {
		return nullptr;
	}
	// an expired unfinished stream must fail loudly on later fetches instead of silently truncating
	if (cache->query_uuid == connection.query_uuid && cache->tail) {
		connection.query_state = QuackQueryState::CANCELLED;
	}
	auto doomed = std::move(connection.result_cache);
	connection.ClearResultCache();
	return doomed;
}

bool HasMoreResults(QuackConnection &connection) {
	auto &cache = connection.result_cache;
	if (cache && cache->query_uuid == connection.query_uuid) {
		return cache->tail != nullptr;
	}
	return connection.duckdb_query_result != nullptr;
}

vector<unique_ptr<DataChunkWrapper>> ServeBatch(QuackConnection &connection, idx_t max_rows, idx_t max_cache_rows) {
	auto cache = connection.result_cache.get();
	if (!cache || cache->query_uuid != connection.query_uuid) {
		return CreateBatch(connection.duckdb_query_result, max_rows);
	}
	cache->last_served_at = Timestamp::GetCurrentTimestamp();
	vector<unique_ptr<DataChunkWrapper>> results;
	idx_t rows = 0;
	while (rows < max_rows && cache->tail) {
		auto result_chunk = cache->tail->Fetch();
		if (!result_chunk && cache->tail->HasError()) {
			connection.duckdb_query_result = std::move(cache->tail);
			connection.ClearResultCache();
			return vector<unique_ptr<DataChunkWrapper>>();
		}
		if (!result_chunk || result_chunk->size() == 0) {
			cache->tail.reset();
			break;
		}
		rows += result_chunk->size();
		// the single-argument Append keeps no append state between batches: a retained state pins the
		// current 256KB block in the buffer manager for the whole life of the cache
		cache->retained.Append(*result_chunk);
		results.push_back(make_uniq<DataChunkWrapper>(*result_chunk));
	}
	if (max_cache_rows != 0 && cache->retained.Count() > max_cache_rows) {
		// Too large to replay, hand the tail back to plain streaming and drop the cache
		connection.duckdb_query_result = std::move(cache->tail);
		connection.ClearResultCache();
	}
	return results;
}

} // namespace duckdb
