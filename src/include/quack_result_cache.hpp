#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

class DatabaseInstance;
class DataChunkWrapper;
struct QuackConnection;

//! Replayable server-side copy of the result stream of a connection's last client query
struct QuackResultCache {
	QuackResultCache(BufferManager &buffer_manager, string sql_p, hugeint_t query_uuid_p, vector<LogicalType> types)
	    : sql(std::move(sql_p)), query_uuid(query_uuid_p), retained(buffer_manager, std::move(types)) {
		retained.InitializeAppend(append_state);
	}

	//! The query text exactly as the client sent it.
	string sql;
	//! UUID the cached query is currently served under.
	hugeint_t query_uuid;
	//! Chunks retained as they are served, buffer managed so cached data counts against the memory limit
	ColumnDataCollection retained;
	ColumnDataAppendState append_state;
	//! Unproduced remainder of the live result
	unique_ptr<QueryResult> tail;
};

//! Server-side half of quack_enable_reconnects: cache the result stream of each client query.
bool ServerCachingEnabled(DatabaseInstance &db);

//! Rows the server may retain per connection cache before it degrades to plain streaming (0 = unlimited)
idx_t CacheMaxRows(DatabaseInstance &db);

//! True while the connection's active query still has unserved chunks (cached or live).
bool HasMoreResults(QuackConnection &connection);

//! Serve up to max_rows of the active query. Producer errors land in duckdb_query_result like the uncached path
vector<unique_ptr<DataChunkWrapper>> ServeBatch(QuackConnection &connection, idx_t max_rows, idx_t max_cache_rows);

} // namespace duckdb
