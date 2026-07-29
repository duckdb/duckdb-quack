#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

class DatabaseInstance;
class DataChunkWrapper;
struct QuackConnection;

//! Replayable server-side copy of the result stream of a connection's last client query
struct QuackResultCache {
	QuackResultCache(BufferManager &buffer_manager, string sql_p, hugeint_t query_uuid_p, vector<LogicalType> types,
	                 shared_ptr<atomic<idx_t>> live_caches_p)
	    : sql(std::move(sql_p)), query_uuid(query_uuid_p), retained(buffer_manager, std::move(types)),
	      last_served_at(Timestamp::GetCurrentTimestamp()), live_caches(std::move(live_caches_p)) {
		D_ASSERT(live_caches);
		(*live_caches)++;
	}
	~QuackResultCache() {
		(*live_caches)--;
	}

	// the live-cache count is maintained by this object's lifetime, so it must not be copied or moved
	QuackResultCache(const QuackResultCache &) = delete;
	QuackResultCache &operator=(const QuackResultCache &) = delete;

	//! The query text exactly as the client sent it.
	string sql;
	//! UUID the cached query is currently served under.
	hugeint_t query_uuid;
	//! Chunks retained as they are served, buffer managed so cached data counts against the memory limit
	ColumnDataCollection retained;
	//! Unproduced remainder of the live result
	unique_ptr<QueryResult> tail;
	//! Last time this cache served a batch, anchors the quack_result_ttl expiry clock
	timestamp_t last_served_at {0};
	//! Caches live on the owning server, counted so the TTL sweep can skip a server with nothing cached
	shared_ptr<atomic<idx_t>> live_caches;
};

//! Cache the result stream of each client query.
bool ServerCachingEnabled(DatabaseInstance &db);

//! Rows the server may retain per connection cache before it degrades to plain streaming (0 = unlimited)
idx_t CacheMaxRows(DatabaseInstance &db);

//! quack_result_ttl in microseconds (0 = caches never expire)
int64_t ResultTtlMicros(DatabaseInstance &db);

//! Drops the cache once idle past the TTL, failing an unfinished stream like a cancelled query.
void ExpireCacheIfStale(QuackConnection &connection, timestamp_t now, int64_t ttl_micros);

//! True while the connection's active query still has unserved chunks (cached or live).
bool HasMoreResults(QuackConnection &connection);

//! Serve up to max_rows of the active query.
vector<unique_ptr<DataChunkWrapper>> ServeBatch(QuackConnection &connection, idx_t max_rows, idx_t max_cache_rows);

} // namespace duckdb
