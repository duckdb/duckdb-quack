#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class DatabaseInstance;
struct QuackConnection;
struct QuackFetchStream;

//! Keeps a connection's last client query, so a client that reconnects can get the same bytes.
//! The fetch stream already holds each payload it served, for a transport retry. This cache holds
//! that retention open after the client acks, and counts what it holds.
struct QuackResultCache {
	QuackResultCache(string sql_p, hugeint_t query_uuid_p, shared_ptr<QuackFetchStream> stream_p,
	                 shared_ptr<atomic<idx_t>> live_caches_p)
	    : sql(std::move(sql_p)), query_uuid(query_uuid_p), stream(std::move(stream_p)),
	      last_served_at(Timestamp::GetCurrentTimestamp()), live_caches(std::move(live_caches_p)) {
		D_ASSERT(live_caches);
		(*live_caches)++;
	}
	~QuackResultCache();

	// the live-cache count is maintained by this object's lifetime, so it must not be copied or moved
	QuackResultCache(const QuackResultCache &) = delete;
	QuackResultCache &operator=(const QuackResultCache &) = delete;

	//! The query text exactly as the client sent it.
	string sql;
	//! UUID the cached query is currently served under.
	hugeint_t query_uuid;
	//! Held here too, because an internal query can replace the connection's stream.
	shared_ptr<QuackFetchStream> stream;
	//! Rows in the payloads the stream is holding for this cache.
	idx_t retained_rows = 0;
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

//! Detaches the cache once idle past the TTL, failing an unfinished stream like a cancelled query.
//! Returns the cache, so the caller can destroy it off the request path. If `still_serving` is
//! true, the caller must also abort the fetch stream. That releases the abandoned query.
unique_ptr<QuackResultCache> ExpireCacheIfStale(QuackConnection &connection, timestamp_t now, int64_t ttl_micros,
                                                bool &still_serving);

} // namespace duckdb
