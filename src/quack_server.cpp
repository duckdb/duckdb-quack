#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/temporary_file_manager.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "duckdb/main/client_config.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_random.hpp"
#include "quack_result_cache.hpp"
#include "quack_storage.hpp"
#include "quack_insert_stream.hpp"
#include "quack_fetch_collector.hpp"
#include "quack_rebalancer_sink.hpp"

#include "mbedtls_wrapper.hpp"

namespace duckdb {

static bool LeaseTimeoutElapsed(time_point<steady_clock> last_renewed_at, time_point<steady_clock> now,
                                idx_t timeout_seconds) {
	if (now <= last_renewed_at) {
		return false;
	}
	auto elapsed_seconds = duration_cast<std::chrono::seconds>(now - last_renewed_at).count();
	return static_cast<idx_t>(elapsed_seconds) >= timeout_seconds;
}

QuackConnection::QuackConnection(string session_id_p, idx_t heartbeat_timeout_seconds_p)
    : session_id(std::move(session_id_p)), heartbeat_timeout_seconds(heartbeat_timeout_seconds_p),
      lease_last_renewed_at(steady_clock::now()) {
}

bool QuackConnection::LeaseExpiredLocked(time_point<steady_clock> now) {
	if (!lease_expired && LeaseTimeoutElapsed(lease_last_renewed_at, now, heartbeat_timeout_seconds)) {
		lease_expired = true;
	}
	return lease_expired;
}

bool QuackConnection::TryRenewLease() {
	annotated_lock_guard<annotated_mutex> guard(lease_lock);
	auto now = steady_clock::now();
	if (LeaseExpiredLocked(now)) {
		return false;
	}
	lease_last_renewed_at = now;
	return true;
}

//! A fetch stream and its producer thread, taken off the connection so an abort can run unlocked.
struct DetachedResultStream {
	shared_ptr<QuackResultStream> stream;
	std::thread thread;
};

//! Error the buffer, so consumers and parked producers wake. Interrupt the query if it still runs,
//! then join the producer. Call this WITHOUT the statement lock.
static void AbortDetachedStatement(QuackConnection &connection, DetachedResultStream detached, const string &reason) {
	if (detached.stream) {
		auto was_finished = detached.stream->buffer.Finished();
		detached.stream->buffer.SetError(ErrorData(ExceptionType::INTERRUPT, reason));
		if (!was_finished && connection.duckdb_connection) {
			connection.duckdb_connection->Interrupt();
		}
	}
	if (detached.thread.joinable()) {
		detached.thread.join();
	}
}

//! Stop and join the connection's fetch collector. Call this WITHOUT the statement lock.
static void AbortStatement(QuackConnection &connection, const string &reason) {
	DetachedResultStream detached;
	{
		lock_guard<mutex> guard(connection.statement.lock);
		detached.stream = std::move(connection.statement.stream);
		detached.thread = std::move(connection.statement.thread);
		if (detached.stream) {
			// the abort reason must not hide a real failure
			connection.statement.abort_error = detached.stream->buffer.HasError()
			                                       ? detached.stream->buffer.GetError()
			                                       : ErrorData(ExceptionType::INTERRUPT, reason);
		}
	}
	// The statement can hold a scan that waits for a client batch. Error those streams too.
	if (connection.duckdb_connection) {
		if (auto session_state = QuackSessionState::Get(*connection.duckdb_connection->context)) {
			session_state->Streams().Clear(reason);
		}
	}
	AbortDetachedStatement(connection, std::move(detached), reason);
}

//! Update the cache with what its stream holds. Drops the cache when the result grows past
//! quack_cache_max_rows, because it is then too large to keep for a reconnect.
static void SyncResultCache(QuackConnection &connection, QuackResultStream &stream, DatabaseInstance &db) {
	std::unique_lock<std::mutex> lock(connection.lock);
	auto &cache = connection.result_cache;
	if (!cache || cache->stream.get() != &stream) {
		return;
	}
	cache->last_served_at = Timestamp::GetCurrentTimestamp();
	cache->retained_rows = stream.RetainedRows();
	auto max_cache_rows = CacheMaxRows(db);
	if (max_cache_rows != 0 && cache->retained_rows > max_cache_rows) {
		// the acks release the payloads again
		stream.DropRetention();
		connection.ClearResultCache();
		return;
	}
	connection.SyncCachedRows();
}

//! Runs one client statement with the result collector installed. It holds the statement lock for the
//! full duration. A statement that returns rows fills the claim buffer in parallel. A statement that
//! returns a count leaves the count in batch 1, through the fallback below.
static void DriveQuery(QuackConnection &connection, shared_ptr<QuackResultStream> stream, string sql) {
	// A failed statement must stop the client's sends, so its scan's streams get the error too.
	auto fail_streams = [&](const ErrorData &error) {
		if (auto session_state = QuackSessionState::Get(*connection.duckdb_connection->context)) {
			session_state->Streams().Fail(error);
		}
	};
	try {
		unique_lock<mutex> guard(connection.statement_lock);
		auto &context = *connection.duckdb_connection->context;

		// MakeQuackFetchCollector sends the FIRST statement that returns a result into the stream.
		// Every other statement keeps the default collector.
		auto &config = ClientConfig::GetConfig(context);
		config.get_result_collector = [stream](ClientContext &ctx, PreparedStatementData &data) {
			return MakeQuackFetchCollector(ctx, data, stream);
		};
		unique_ptr<QueryResult> result;
		try {
			result = connection.duckdb_connection->Query(sql);
		} catch (...) {
			// leave no collector hook on the connection's config
			config.get_result_collector = nullptr;
			throw;
		}
		config.get_result_collector = nullptr;
		if (result->HasError()) {
			stream->buffer.SetError(result->GetErrorObject());
			fail_streams(result->GetErrorObject());
		} else if (!stream->Bound()) {
			// No collector claimed the stream, because no statement returned a result. Send the last
			// statement's Success/Count result, as the protocol always has. No columns is an error.
			if (result->GetNames().empty()) {
				stream->buffer.SetError(ErrorData(ExceptionType::INVALID_INPUT, "Query did not return any columns"));
				stream->buffer.Finish();
				return;
			}
			// BaseQueryResult::names is a vector<Identifier>. The stream carries plain strings.
			vector<string> result_names;
			result_names.reserve(result->GetNames().size());
			for (auto &col_name : result->GetNames()) {
				result_names.push_back(col_name.GetIdentifierName());
			}
			stream->SignalBound(result->GetTypes(), std::move(result_names));
			unique_ptr<QuackChunkPayloadWriter> writer;
			idx_t rows = 0;
			while (auto chunk = result->Fetch()) {
				if (chunk->size() == 0) {
					continue;
				}
				if (!writer) {
					writer = make_uniq<QuackChunkPayloadWriter>(0);
				}
				writer->AppendChunk(*chunk);
				rows += chunk->size();
			}
			if (writer) {
				auto sealed = writer->Seal();
				QuackFetchPayload entry;
				entry.payload = std::move(sealed.payload);
				entry.payload_size = sealed.payload_size;
				entry.chunk_count = sealed.chunk_count;
				entry.rows = rows;
				auto bytes = entry.payload_size;
				stream->buffer.PushBatch(1, std::move(entry), bytes);
				stream->announced_total = 1;
			} else {
				stream->announced_total = 0;
			}
		}
	} catch (std::exception &ex) {
		stream->buffer.SetError(ErrorData(ex));
		fail_streams(ErrorData(ex));
	}
	// Close against the announced total, so a short stream errors instead of truncating.
	stream->buffer.Finish(stream->announced_total);
}

QuackConnection::~QuackConnection() {
	// Abort and join the query driver before the members go away.
	AbortStatement(*this, "connection closed during fetch");
}

void QuackServer::ValidateToken(const string &token) {
	if (token.size() < 4) {
		throw InvalidInputException("Quack server token must be at least 4 characters long");
	}
}

QuackServer::QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p)
    : db_ptr(context_p.db), uri(uri_p), token(token_p) {
	ValidateToken(token);
	server_hmac_key = QuackRandomToken(*context_p.db);
}

QuackServer::~QuackServer() {
}

void QuackServer::CleanupExpiredConnection(QuackConnection &connection) {
	if (connection.duckdb_connection) {
		connection.duckdb_connection->Interrupt();
	}
	// Interrupt() cannot wake a producer that is parked on the buffer capacity. Abort and join it
	// here, not in ~QuackConnection, so the connection is quiet before the handler continues.
	AbortStatement(connection, "connection heartbeat lease expired");
}

bool QuackServer::RenewConnectionLease(const string &connection_id, const shared_ptr<QuackConnection> &connection) {
	shared_ptr<QuackConnection> expired_connection;
	{
		std::lock_guard<std::mutex> guard(active_connections_mutex);
		auto entry = active_connections.find(connection_id);
		if (entry == active_connections.end() || entry->second.get() != connection.get()) {
			return false;
		}
		if (connection->TryRenewLease()) {
			return true;
		}
		expired_connection = std::move(entry->second);
		active_connections.erase(entry);
	}
	CleanupExpiredConnection(*expired_connection);
	return false;
}

void QuackServer::DestroyCachesDetached(vector<unique_ptr<QuackResultCache>> doomed) {
	if (doomed.empty()) {
		return;
	}
	auto db = db_ptr.lock();
	if (!db) {
		// database teardown, destroy inline as this scope ends
		return;
	}
	// the thread owns only the doomed caches (plus the db keeping their buffers valid), never the server
	std::thread([db = std::move(db), doomed = std::move(doomed)] {}).detach();
}

vector<QuackConnectionSnapshot> QuackServer::GetActiveConnectionSnap() {
	// sql_query and query_started_at are written under QuackConnection::lock, so reading them under
	// active_connections_mutex alone is a data race — and sql_query is a std::string, so a concurrent
	// write can free the storage we copy. (query_state is atomic and would be safe unlocked, but we
	// read it in the same critical section for a snapshot consistent with the other two.) Copy the
	// connection handles out under active_connections_mutex, release it, then read each connection's
	// fields under its own lock (never holding both mutex classes at once).
	vector<shared_ptr<QuackConnection>> connections;
	{
		std::lock_guard<std::mutex> lock(active_connections_mutex);
		connections.reserve(active_connections.size());
		for (auto &[id, conn] : active_connections) {
			connections.push_back(conn);
		}
	}
	vector<QuackConnectionSnapshot> result;
	result.reserve(connections.size());
	for (auto &conn : connections) {
		QuackConnectionSnapshot snapshot;
		// session_id and client_id_hash are set once when the connection is created, before it is
		// published to active_connections, and never rewritten — so they need no lock here.
		snapshot.session_id = conn->session_id;
		snapshot.client_id_hash = conn->client_id_hash;
		{
			std::lock_guard<std::mutex> lock(conn->lock);
			snapshot.sql_query = conn->sql_query;
			snapshot.query_state = conn->query_state;
			snapshot.query_started_at = conn->query_started_at;
		}
		auto cached_rows = conn->cached_rows.load();
		if (cached_rows != DConstants::INVALID_INDEX) {
			snapshot.cached_rows = cached_rows;
		}
		result.push_back(std::move(snapshot));
	}
	return result;
}

void QuackServer::RegisterCacheForExpiry(QuackConnection &connection) {
	if (connection.cache_in_expiry_queue) {
		// the sweep re-queues the existing slot under the new cache's timestamp once its old snapshot pops
		return;
	}
	connection.cache_in_expiry_queue = true;
	std::lock_guard<std::mutex> lock(cache_expiry_mutex);
	cache_expiry_queue.push({connection.result_cache->last_served_at, connection.session_id});
}

void QuackServer::SweepExpiredCaches(DatabaseInstance &db) {
	// nothing cached anywhere, we skip the settings lookup and the expiry queue
	if (live_caches->load(std::memory_order_relaxed) == 0) {
		return;
	}
	auto ttl_micros = ResultTtlMicros(db);
	if (ttl_micros == 0) {
		return;
	}
	auto now = Timestamp::GetCurrentTimestamp();
	vector<CacheExpiryEntry> candidates;
	{
		std::lock_guard<std::mutex> lock(cache_expiry_mutex);
		while (!cache_expiry_queue.empty() && now.value - cache_expiry_queue.top().served_at.value > ttl_micros) {
			candidates.push_back(cache_expiry_queue.top());
			cache_expiry_queue.pop();
		}
	}
	vector<CacheExpiryEntry> requeue;
	vector<unique_ptr<QuackResultCache>> doomed;
	// Their producer must be aborted, which joins a thread. Do that after the lock is released.
	vector<shared_ptr<QuackConnection>> abandoned;
	for (auto &entry : candidates) {
		auto connection = GetConnection(entry.session_id);
		if (!connection) {
			// disconnected, the cache died with the connection and the slot dies here
			continue;
		}
		{
			std::unique_lock<std::mutex> lock(connection->lock, std::try_to_lock);
			if (!lock.owns_lock()) {
				// another handler is mutating the session state; retry on the next sweep
				requeue.push_back(std::move(entry));
				continue;
			}
			bool still_serving = false;
			auto expired = ExpireCacheIfStale(*connection, now, ttl_micros, still_serving);
			if (expired) {
				doomed.push_back(std::move(expired));
				if (still_serving) {
					abandoned.push_back(connection);
				}
			}
			if (connection->result_cache) {
				// served (or replaced) since the snapshot was queued, keep the slot under the fresh timestamp
				entry.served_at = connection->result_cache->last_served_at;
				requeue.push_back(std::move(entry));
			} else {
				connection->cache_in_expiry_queue = false;
			}
		}
	}
	for (auto &connection : abandoned) {
		// Releases the query the client walked away from. A later FETCH finds this error.
		AbortStatement(*connection, "Query was interrupted");
	}
	if (!requeue.empty()) {
		std::lock_guard<std::mutex> lock(cache_expiry_mutex);
		for (auto &entry : requeue) {
			cache_expiry_queue.push(std::move(entry));
		}
	}
	DestroyCachesDetached(std::move(doomed));
}

shared_ptr<QuackConnection> QuackServer::GetConnection(const string &connection_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second;
	}
	return nullptr;
}

string QuackServer::CreateNewConnection(const string &session_id, const string &client_id_hash,
                                        idx_t heartbeat_timeout_seconds) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	D_ASSERT(active_connections.find(session_id) == active_connections.end());

	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	auto new_connection = make_shared_ptr<QuackConnection>(session_id, heartbeat_timeout_seconds);
	new_connection->client_id_hash = client_id_hash;
	new_connection->live_caches = live_caches;
	new_connection->duckdb_connection = make_uniq<Connection>(*db);
	auto &connection_context = *new_connection->duckdb_connection->context;
	// scan_data_from_quack_client registers its streams here, and SEND_DATA finds them by connection.
	connection_context.registered_state->Insert(QuackSessionState::KEY, make_shared_ptr<QuackSessionState>());
	connection_context.config.enable_progress_bar = false;
	// new_connection->duckdb_connection->context->config.streaming_buffer_size = 10 * 1000000; // 10 MB
	active_connections[session_id] = std::move(new_connection);
	return session_id;
}

bool QuackServer::DisconnectConnection(const string &session_id) {
	std::lock_guard<std::mutex> lock(active_connections_mutex);

	auto entry = active_connections.find(session_id);
	if (entry == active_connections.end()) {
		// unknown client
		return false;
	}
	active_connections.erase(entry);
	return true;
}

static string GetSettingString(DatabaseInstance &db, const string &setting_name) {
	Value setting_val;
	auto &config = DBConfig::GetConfig(db);

	auto lookup_result = config.TryGetCurrentSetting(Identifier(setting_name), setting_val);
	D_ASSERT(lookup_result);
	D_ASSERT(setting_val.type().id() == LogicalTypeId::VARCHAR);
	auto setting_str = setting_val.GetValue<string>();
	D_ASSERT(!setting_str.empty());
	return setting_str;
}

template <typename... ARGS>
static Value EvaluateAuthQuery(DatabaseInstance &db, const string &sql, ARGS... values) {
	Connection dummy_connection(db);
	auto auth_result = dummy_connection.Query(sql, values...);
	if (!auth_result || auth_result->HasError()) {
		return Value(false);
	}
	auto auth_result_chunk = auth_result->Fetch();
	if (!auth_result_chunk || auth_result_chunk->size() == 0) {
		return Value(false);
	}
	return auth_result_chunk->GetValue(0, 0);
}

// Derive a stable, per-client reconnect identifier as HMAC-SHA256(server_hmac_key, client_id)
static string ComputeClientHash(const string &server_hmac_key, const string &client_id) {
	unsigned char digest[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES];
	duckdb_mbedtls::MbedTlsWrapper::Hmac256(server_hmac_key.data(), server_hmac_key.size(), client_id.data(),
	                                        client_id.size(), reinterpret_cast<char *>(digest));
	return QuackHexEncode(digest, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
}

string QuackServer::GenerateSessionId() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	return QuackRandomToken(*db);
}

static string ExtractQuery(QuackMessage &msg) {
	if (msg.Type() == MessageType::PREPARE_REQUEST) {
		return msg.Cast<PrepareRequestMessage>().Query();
	}
	return "";
}

bool ServerSupportsMessage(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
	case MessageType::PREPARE_REQUEST:
	case MessageType::FETCH_REQUEST:
	case MessageType::SEND_DATA_REQUEST:
	case MessageType::DISCONNECT_MESSAGE:
	case MessageType::CANCEL_REQUEST:
	case MessageType::ACKNOWLEDGEMENT:
	case MessageType::HEARTBEAT_REQUEST:
		return true;
	default:
		return false;
	}
}

bool MessageRequiresConnection(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
		return false;
	default:
		return true;
	}
}

// main switcheroo happens here
unique_ptr<QuackMessage> QuackServer::HandleMessage(MemoryStream &read_stream) {
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
	}
	auto &logger = Logger::Get(*db);
	bool should_log = logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL);

	int64_t start_time = 0;
	if (should_log) {
		start_time = QuackNowMillis();
	}

	// start deserializing the message
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);

	// first read the header
	auto header = QuackMessage::DeserializeHeader(deserializer);

	// validate if the server can handle this type of message - the server cannot handle all message types
	if (!ServerSupportsMessage(header.type)) {
		return make_uniq<ErrorResponse>("Unsupported message type for server");
	}

	// if the message requires it, obtain a connection
	// these are basically all messages aside from connect request
	shared_ptr<QuackConnection> connection;
	if (MessageRequiresConnection(header.type)) {
		connection = GetConnection(header.connection_id);
		if (!connection) {
			return make_uniq<ErrorResponse>("Invalid connection id");
		}
	}

	SweepExpiredCaches(*db);

	// now deserialize the actual message, and any raw chunk blob that follows it
	auto received_message = QuackMessage::DeserializeMessage(deserializer, header);
	if (connection) {
		// Only supported, structurally valid messages for an existing session renew its lease.
		if (!RenewConnectionLease(header.connection_id, connection)) {
			return make_uniq<ErrorResponse>("Connection heartbeat lease expired");
		}
	}

	// process the message
	auto response = HandleMessageInternal(*db, *received_message, connection);

	if (should_log) {
		auto duration_ms = QuackNowMillis() - start_time;
		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		auto client_id_hash = connection ? connection->client_id_hash : string();
		auto msg = QuackLogType::ConstructLogMessage(header.type, header.connection_id, client_id_hash,
		                                             header.client_query_id, ExtractQuery(*received_message), "",
		                                             duration_ms, response->Type(), error);
		logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
	}

	return response;
}

unique_ptr<QuackMessage> QuackServer::HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
                                                            optional_ptr<QuackConnection> connection_p) {
	switch (received_message.Type()) {
	case MessageType::CONNECTION_REQUEST: {
		auto &connection_request_message = received_message.Cast<ConnectionRequestMessage>();
		// The server speaks exactly QUACK_VERSION; reject unless the client's [min, max] range includes it.
		if (connection_request_message.MinimumSupportedQuackVersion() > QUACK_VERSION ||
		    connection_request_message.MaximumSupportedQuackVersion() < QUACK_VERSION) {
			return make_uniq<ErrorResponse>(StringUtil::Format(
			    "Unsupported Quack version - server only supports version %llu of quack", QUACK_VERSION));
		}
		auto heartbeat_timeout_seconds = connection_request_message.HeartbeatTimeoutSeconds();
		if (heartbeat_timeout_seconds == 0 || heartbeat_timeout_seconds > MAX_HEARTBEAT_TIMEOUT_SECONDS) {
			return make_uniq<ErrorResponse>(StringUtil::Format(
			    "heartbeat_timeout out of range - must be between 1 and %llu seconds", MAX_HEARTBEAT_TIMEOUT_SECONDS));
		}
		string session_id = GenerateSessionId();
		auto auth_result = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?, ?)", GetSettingString(db, "quack_authentication_function")),
		    Value(session_id), Value(connection_request_message.AuthString()), Value(Token()));

		if (auth_result.IsNull() ||
		    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
			return make_uniq<ErrorResponse>("Authentication failed");
		}
		string client_id_hash;
		if (!connection_request_message.ClientId().empty()) {
			client_id_hash = ComputeClientHash(server_hmac_key, connection_request_message.ClientId());
		}
		return make_uniq<ConnectionResponseMessage>(
		    CreateNewConnection(session_id, client_id_hash, heartbeat_timeout_seconds), heartbeat_timeout_seconds);
	}
	case MessageType::DISCONNECT_MESSAGE: {
		auto &connection = *connection_p;
		if (!DisconnectConnection(connection.session_id)) {
			return make_uniq<ErrorResponse>("Connection does not exist / already disconnected");
		}
		return make_uniq<SuccessResponse>();
	}
	case MessageType::PREPARE_REQUEST: {
		auto &prepare_request_message = received_message.Cast<PrepareRequestMessage>();
		auto &connection = *connection_p;

		// TODO do not do this if there is no fun set
		auto auth_result = EvaluateAuthQuery(
		    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
		    Value(prepare_request_message.ConnectionId()), Value(prepare_request_message.Query()));
		if (auth_result.IsNull() ||
		    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
			return make_uniq<ErrorResponse>("Authorization failed");
		}
		auto effective_sql = (auth_result.type().id() == LogicalTypeId::VARCHAR) ? auth_result.GetValue<string>()
		                                                                         : prepare_request_message.Query();

		// Stop the previous producer first. This joins its thread, so it frees the statement lock.
		AbortStatement(connection, "superseded by a new query");
		{
			std::unique_lock<std::mutex> lock(connection.lock);
			if (prepare_request_message.QueryUUID() != hugeint_t {0, 0}) {
				// a new client query displaces the retained one, even if caching is now off
				connection.ClearResultCache();
			}
			connection.sql_query = prepare_request_message.Query();
			connection.query_state = QuackQueryState::ACTIVE;
			connection.query_started_at = Timestamp::GetCurrentTimestamp();
			connection.query_uuid = prepare_request_message.QueryUUID();
		}

		auto stream = make_shared_ptr<QuackResultStream>();
		// One buffer exists for each connection, so a fixed size does not suit every server. Core
		// also sizes an optional buffer at a quarter of the memory limit. 0 still means no limit.
		auto producer_bytes =
		    QuackGetUBigintSetting(db, "quack_fetch_producer_buffer_bytes", QUACK_FETCH_PRODUCER_BUFFER_BYTES_DEFAULT);
		auto memory_cap = BufferManager::GetBufferManager(db).GetOperatorMemoryLimit() / 4;
		if (producer_bytes > 0 && memory_cap > 0) {
			producer_bytes = MinValue<idx_t>(producer_bytes, memory_cap);
		}
		stream->buffer.SetCapacity(producer_bytes);
		// Internal catalog traffic carries uuid 0. It must never displace the client's retained result.
		bool client_query = prepare_request_message.QueryUUID() != hugeint_t {0, 0};
		bool retain_result = client_query && ServerCachingEnabled(db);
		if (retain_result) {
			// hold every served payload, instead of releasing it once the client acks
			stream->RetainAll();
		}
		DetachedResultStream superseded;
		{
			// Install under one lock. A concurrent PREPARE can install a producer after the abort
			// above, and an assignment over a joinable std::thread would stop the process.
			lock_guard<mutex> guard(connection.statement.lock);
			superseded.stream = std::move(connection.statement.stream);
			superseded.thread = std::move(connection.statement.thread);
			connection.statement.stream = stream;
			connection.statement.uuid = prepare_request_message.QueryUUID();
			connection.statement.abort_error = ErrorData();
			// A statement that reads a client stream cannot bind its result before the client sends.
			// The scan finds the stream here and raises it, so PREPARE stops waiting.
			if (auto session_state = QuackSessionState::Get(*connection.duckdb_connection->context)) {
				session_state->SetStatement(stream);
			}
			connection.statement.thread = std::thread(DriveQuery, std::ref(connection), stream, effective_sql);
		}
		AbortDetachedStatement(connection, std::move(superseded), "superseded by a new query");

		if (!stream->WaitBound()) {
			// planning failed before a collector existed
			auto error = stream->buffer.GetError();
			AbortStatement(connection, "query failed");
			std::unique_lock<std::mutex> lock(connection.lock);
			connection.sql_query = "";
			connection.query_state = QuackQueryState::CANCELLED;
			return make_uniq<ErrorResponse>(error);
		}

		// Put the leading batches in the PREPARE response, so a small result needs one round trip
		// only. The FETCH indices then move down by the count this drain consumed.
		// A statement that waits for client data must answer before it has a row. The client can then
		// start to send. It asks for that with inline_rows = 0.
		idx_t max_inline_rows =
		    prepare_request_message.InlineRows().IsValid()
		        ? prepare_request_message.InlineRows().GetIndex()
		        : QuackGetUBigintSetting(db, "quack_prepare_inline_rows", QUACK_PREPARE_INLINE_ROWS_DEFAULT);
		vector<unique_ptr<DataChunkWrapper>> results;
		idx_t inline_rows = 0;
		idx_t consumed = 0;
		while (inline_rows < max_inline_rows) {
			QuackFetchPayload entry;
			auto status = stream->buffer.TryPopClaimed(consumed + 1, entry);
			if (status == QuackClaimPopStatus::BATCH) {
				consumed++;
				inline_rows += entry.rows;
				// Batches are stored as header-less chunk blobs. Only this drain decodes them.
				MemoryStream payload_stream(entry.payload->GetData(), entry.payload_size);
				payload_stream.SetPosition(QUACK_PAYLOAD_HEADER_BYTES);
				BinaryDeserializer payload_deserializer(payload_stream);
				for (auto &chunk : DecodeQuackChunkBlob(payload_deserializer, entry.chunk_count)) {
					results.push_back(make_uniq<DataChunkWrapper>(*chunk));
				}
				if (retain_result) {
					// These rows leave in the PREPARE response, so the header for a cache replay is written here.
					FetchResponseMessage header_message;
					header_message.SetChunkCount(entry.chunk_count);
					header_message.SetBatchIndex(consumed);
					auto body_start = QuackPrependHeader(*entry.payload, header_message);
					stream->Retain(consumed, std::move(entry.payload), body_start, entry.rows);
				}
				continue;
			}
			if (status == QuackClaimPopStatus::FINISHED) {
				break;
			}
			if (status == QuackClaimPopStatus::ERRORED) {
				auto error = stream->buffer.GetError();
				AbortStatement(connection, "query failed");
				std::unique_lock<std::mutex> lock(connection.lock);
				connection.sql_query = "";
				connection.query_state = QuackQueryState::CANCELLED;
				return make_uniq<ErrorResponse>(error);
			}
			stream->buffer.WaitForBatch(consumed + 1);
		}
		stream->prepare_batches = consumed;
		if (retain_result) {
			// Created for every cached client query, also one with no rows. An empty result then
			// reports 0, and not "nothing cached".
			{
				std::unique_lock<std::mutex> lock(connection.lock);
				connection.result_cache =
				    make_uniq<QuackResultCache>(prepare_request_message.Query(), prepare_request_message.QueryUUID(),
				                                stream, connection.live_caches);
				RegisterCacheForExpiry(connection);
			}
			SyncResultCache(connection, *stream, db);
		}
		// Report more unless the drain saw a CLEAN end. A wrong true costs one more FETCH. A wrong
		// false on an errored buffer would report a truncated result as complete.
		auto needs_more_fetch = !stream->buffer.Exhausted() || stream->buffer.HasError();
		if (!needs_more_fetch) {
			connection.query_state = QuackQueryState::FINISHED;
		}
		return make_uniq<PrepareResponseMessage>(stream->types, stream->names, std::move(results), needs_more_fetch,
		                                         prepare_request_message.QueryUUID());
	}

	case MessageType::FETCH_REQUEST: {
		auto &fetch_request_message = received_message.Cast<FetchRequestMessage>();
		auto &connection = *connection_p;

		// FETCH touches only the buffer and the served map, so it needs neither lock on the way in.
		shared_ptr<QuackResultStream> stream;
		hugeint_t stream_uuid;
		ErrorData abort_error;
		{
			lock_guard<mutex> guard(connection.statement.lock);
			stream = connection.statement.stream;
			stream_uuid = connection.statement.uuid;
			abort_error = connection.statement.abort_error;
		}
		if (!stream && stream_uuid == fetch_request_message.uuid && abort_error.HasError()) {
			return make_uniq<ErrorResponse>(abort_error);
		}
		if (!stream || stream_uuid != fetch_request_message.uuid) {
			return make_uniq<ErrorResponse>("Result has been closed");
		}
		if (fetch_request_message.batch_index == 0) {
			return make_uniq<ErrorResponse>("FETCH_REQUEST is missing its batch index");
		}

		// The request names its batch, so add back the batches PREPARE consumed inline. Served
		// payloads stay until the client acks them, so a transport retry gets the SAME batch.
		auto dense_index = fetch_request_message.batch_index + stream->prepare_batches;
		auto dense_ack = fetch_request_message.ack_index + stream->prepare_batches;
		{
			lock_guard<mutex> guard(stream->serve_lock);
			// The client has everything up to the ack and cannot ask for it again. A result cache
			// keeps the payloads for longer.
			if (!stream->retain_all) {
				auto last = stream->served.upper_bound(dense_ack);
				for (auto it = stream->served.begin(); it != last; ++it) {
					stream->retained_rows -= it->second.rows;
				}
				stream->served.erase(stream->served.begin(), last);
			}
		}

		while (true) {
			QuackClaimPopStatus status = QuackClaimPopStatus::EMPTY;
			QuackFetchPayload entry;
			shared_ptr<MemoryStream> served_batch;
			idx_t served_start = 0;
			{
				// One critical section for the check, the pop and the retain. A concurrent retry of the
				// same index cannot reach FINISHED while the first serve runs.
				lock_guard<mutex> guard(stream->serve_lock);
				auto retained = stream->served.find(dense_index);
				if (retained != stream->served.end()) {
					// the header with the client-visible index is already written
					served_batch = retained->second.payload;
					served_start = retained->second.body_start;
				} else {
					status = stream->buffer.TryPopClaimed(dense_index, entry);
					if (status == QuackClaimPopStatus::BATCH) {
						// write the header with the client-visible index, then keep the payload for a retry
						FetchResponseMessage header_message;
						header_message.SetChunkCount(entry.chunk_count);
						header_message.SetBatchIndex(fetch_request_message.batch_index);
						served_start = QuackPrependHeader(*entry.payload, header_message);
						shared_ptr<MemoryStream> payload(std::move(entry.payload));
						stream->served.emplace(dense_index,
						                       QuackResultStream::RetainedPayload {payload, served_start, entry.rows});
						stream->retained_rows += entry.rows;
						served_batch = std::move(payload);
					}
				}
			}
			if (served_batch) {
				// outside serve_lock, because the connection's state lock guards the cache
				SyncResultCache(connection, *stream, db);
				return make_uniq<QuackRawPayloadResponse>(MessageType::FETCH_RESPONSE, std::move(served_batch),
				                                          served_start);
			}
			if (status == QuackClaimPopStatus::ERRORED) {
				return make_uniq<ErrorResponse>(stream->buffer.GetError());
			}
			if (status == QuackClaimPopStatus::FINISHED) {
				// The stream ended below this index. The terminal response carries the total, so the
				// client can check that it got every batch.
				if (connection.query_state == QuackQueryState::ACTIVE) {
					connection.query_state = QuackQueryState::FINISHED;
				}
				auto response = make_uniq<FetchResponseMessage>();
				if (stream->announced_total.IsValid()) {
					response->SetTotalBatches(stream->announced_total.GetIndex() - stream->prepare_batches);
				}
				return std::move(response);
			}
			stream->buffer.WaitForBatch(dense_index);
		}
	}

	case MessageType::SEND_DATA_REQUEST: {
		auto &send_data_message = received_message.Cast<SendDataRequestMessage>();
		auto &connection = *connection_p;

		// The streams live on this connection's session, so a batch is authorized by its connection id.
		auto session_state = QuackSessionState::Get(*connection.duckdb_connection->context);
		auto stream = session_state ? session_state->Streams().Find(send_data_message.StreamId()) : nullptr;
		if (!stream) {
			return make_uniq<ErrorResponse>("No active data stream '%s'", send_data_message.StreamId());
		}
		{
			// the statement can fail before its scan sees the stream error
			lock_guard<mutex> guard(connection.statement.lock);
			if (connection.statement.stream && connection.statement.stream->buffer.HasError()) {
				return make_uniq<ErrorResponse>(connection.statement.stream->buffer.GetError());
			}
		}

		auto &incoming_chunks = send_data_message.Chunks();
		if (!incoming_chunks.empty()) {
			if (!send_data_message.BatchIndex().IsValid()) {
				return make_uniq<ErrorResponse>("send_data_request is missing its batch index");
			}
			// The claim buffer drops a duplicate index, so a retry of the same batch is safe.
			stream->buffer.PushBatch(send_data_message.BatchIndex().GetIndex(), std::move(incoming_chunks));
		}

		if (!send_data_message.TotalBatches().IsValid()) {
			if (stream->buffer.HasError()) {
				return make_uniq<ErrorResponse>(stream->buffer.GetError());
			}
			return make_uniq<SendDataResponseMessage>(); // accept_budget unset = unbounded (future flow control)
		}

		// The terminal message. Closing against a count makes a short stream fail loudly; a repeat
		// closes against the same count and gets the same answer. The statement's result, and any
		// later failure, come back through FETCH.
		stream->buffer.Finish(send_data_message.TotalBatches());
		if (stream->buffer.HasError()) {
			return make_uniq<ErrorResponse>(stream->buffer.GetError());
		}
		return make_uniq<SendDataResponseMessage>();
	}
	case MessageType::CANCEL_REQUEST: {
		auto &cancel_request_message = received_message.Cast<CancelRequestMessage>();
		auto &connection = *connection_p;
		// {0,0} is a wildcard — cancel whatever query is running on this connection
		bool is_wildcard = cancel_request_message.query_uuid == hugeint_t {0, 0};
		if (!is_wildcard && connection.query_uuid != cancel_request_message.query_uuid) {
			return make_uniq<ErrorResponse>("Attempted to cancel a different query with id '%s' instead of '%s'",
			                                cancel_request_message.query_uuid, connection.query_uuid);
		}
		// Interrupt() cannot wake a producer parked on the buffer's capacity. The abort's SetError
		// releases it. A client finds a cancel by the "Interrupt" text.
		AbortStatement(connection, "Interrupted: query was cancelled");
		connection.duckdb_connection->Interrupt();
		// The interrupt stops the statement that runs now. Take the state lock, so the change of
		// state cannot race that statement's handler.
		std::unique_lock<std::mutex> lock(connection.lock);
		connection.query_state = QuackQueryState::CANCELLED;
		connection.ClearResultCache();
		return make_uniq<SuccessResponse>();
	}
	case MessageType::ACKNOWLEDGEMENT: {
		auto &acknowledgement_message = received_message.Cast<AcknowledgementMessage>();
		auto &connection = *connection_p;
		std::unique_lock<std::mutex> lock(connection.lock);
		auto &cache = connection.result_cache;
		if (!cache) {
			return make_uniq<SuccessResponse>(); // nothing retained, acknowledging is a no-op
		}
		if (cache->query_uuid != acknowledgement_message.QueryUUID()) {
			return make_uniq<ErrorResponse>("Attempted to acknowledge a different query with id '%s' instead of '%s'",
			                                acknowledgement_message.QueryUUID(), cache->query_uuid);
		}
		// The client confirmed it received the full result, the replay cache has served its purpose
		if (cache->stream) {
			cache->stream->DropRetention();
		}
		connection.ClearResultCache();
		return make_uniq<SuccessResponse>();
	}
	case MessageType::HEARTBEAT_REQUEST: {
		return make_uniq<SuccessResponse>();
	}
	default: {
		return make_uniq<ErrorResponse>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(received_message.Type())));
	}
	}
}
} // namespace duckdb
