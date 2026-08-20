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

//! Server-side retention of a connection's last client query, so a client that loses the
//! connection can be served the same bytes again.
//!
//! The fetch stream already keeps every payload it has served, keyed by dense batch index, so a
//! transport retry gets identical bytes. This cache is that retention held open: it pins the
//! stream past the point the client's acks would otherwise release it, and counts what it holds.
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
	//! Its own reference, so an internal query swapping the connection's stream cannot drop it.
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
//! Returns the doomed cache (null if kept) so the caller can destroy it off the request path. The
//! caller must abort the connection's fetch stream when `still_serving` comes back true, which
//! releases the abandoned query behind it.
unique_ptr<QuackResultCache> ExpireCacheIfStale(QuackConnection &connection, timestamp_t now, int64_t ttl_micros,
                                                bool &still_serving);

} // namespace duckdb
