#include "quack_result_cache.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"

#include "quack_message.hpp"
#include "quack_server.hpp"

namespace duckdb {

// Accumulate whole chunks until `max_rows` is reached (row-based like the send path, so sparse
// filtered chunks don't shrink the batch). Resets query_result once the cursor is exhausted.
static vector<unique_ptr<DataChunkWrapper>> CreateBatch(unique_ptr<QueryResult> &query_result, idx_t max_rows) {
	vector<unique_ptr<DataChunkWrapper>> results;

	idx_t rows = 0;
	while (rows < max_rows) {
		auto result_chunk = query_result->Fetch();
		// error case
		if (!result_chunk && query_result->HasError()) {
			results.clear();
			return results;
		}
		// we are done case
		if (!result_chunk || result_chunk->size() == 0) {
			query_result.reset();
			break;
		}
		rows += result_chunk->size();
		results.push_back(make_uniq<DataChunkWrapper>(*result_chunk));
	}
	return results;
}

bool ServerCachingEnabled(DatabaseInstance &db) {
	Value val;
	DBConfig::GetConfig(db).TryGetCurrentSetting("quack_enable_reconnects", val);
	return !val.IsNull() && val.GetValue<bool>();
}

idx_t CacheMaxRows(DatabaseInstance &db) {
	Value val;
	DBConfig::GetConfig(db).TryGetCurrentSetting("quack_cache_max_rows", val);
	return val.GetValue<uint64_t>();
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
		cache->retained.Append(cache->append_state, *result_chunk);
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
