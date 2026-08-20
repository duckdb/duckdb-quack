#include "duckdb/common/encryption_state.hpp"
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
#include "quack_result_cache.hpp"
#include "quack_storage.hpp"
#include "quack_data_stream.hpp"
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

//! Finish + join + deregister a detached insert stream; returns any INSERT error. Call WITHOUT the lock.
ErrorData DetachedInsertStream::FinishAndJoin() {
	ErrorData error;
	if (!stream) {
		return error;
	}
	stream->Finish();
	if (thread.joinable()) {
		thread.join();
	}
	if (stream->HasError()) {
		error = stream->GetError();
	}
	QuackStreamRegistry::Get().Erase(id);
	return error;
}

//! Roll the INSERT back + join + deregister a detached insert stream. Call WITHOUT the lock.
void DetachedInsertStream::AbortAndJoin(const string &reason) {
	if (!stream) {
		return;
	}
	stream->SetError(ErrorData(ExceptionType::INVALID_INPUT, reason));
	if (thread.joinable()) {
		thread.join();
	}
	QuackStreamRegistry::Get().Erase(id);
}

DetachedInsertStream QuackInsertState::Detach() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	DetachedInsertStream detached;
	detached.stream = std::move(stream);
	detached.thread = std::move(thread);
	detached.id = std::move(stream_id);
	stream.reset();
	stream_id.clear();
	return detached;
}

DetachedInsertStream QuackInsertState::DetachIfUnrelated(const string &msg_stream_id) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	DetachedInsertStream detached;
	if (!stream || stream_id == msg_stream_id) {
		return detached; // nothing active, or this message continues the active stream
	}
	detached.stream = std::move(stream);
	detached.thread = std::move(thread);
	detached.id = std::move(stream_id);
	stream.reset();
	stream_id.clear();
	return detached;
}

ErrorData QuackInsertState::Finalize(optional_idx watermark) {
	auto detached = Detach();
	if (watermark.IsValid() && detached.stream) {
		detached.stream->SetWatermarkAndDrain(watermark);
	}
	return detached.FinishAndJoin();
}

shared_ptr<QuackDataStream> QuackInsertState::StreamForDeadRangeOrBuffer(const string &sid, idx_t lo, idx_t hi) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (stream && stream_id == sid) {
		return stream;
	}
	// Marker arrived before its stream existed (reordering): buffer it for when the stream is created.
	if (pending_marker_stream_id != sid) {
		pending_marker_stream_id = sid;
		pending_dead_ranges.clear();
	}
	pending_dead_ranges.emplace_back(lo, hi);
	return nullptr;
}

//! A fetch stream and its producer thread, taken off the connection so an abort can run unlocked.
struct DetachedFetchStream {
	shared_ptr<QuackFetchStream> stream;
	std::thread thread;
};

//! Error the buffer, so consumers and parked producers wake. Interrupt the query if it still runs,
//! then join the producer. Call this WITHOUT the connection lock.
static void AbortDetachedFetch(QuackConnection &connection, DetachedFetchStream detached, const string &reason) {
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

//! Stop and join the connection's fetch collector. Call this WITHOUT the connection lock.
static void AbortFetchStream(QuackConnection &connection, const string &reason) {
	DetachedFetchStream detached;
	{
		lock_guard<mutex> guard(connection.fetch.lock);
		detached.stream = std::move(connection.fetch.stream);
		detached.thread = std::move(connection.fetch.thread);
		if (detached.stream) {
			// the abort reason must not hide a real failure
			connection.fetch.abort_error = detached.stream->buffer.HasError()
			                                   ? detached.stream->buffer.GetError()
			                                   : ErrorData(ExceptionType::INTERRUPT, reason);
		}
	}
	AbortDetachedFetch(connection, std::move(detached), reason);
}

//! Runs the client's query with the fetch collector installed, and holds the connection lock for
//! the full duration. The query fills the stream's claim buffer in parallel.
static void RunFetchQuery(QuackConnection &connection, shared_ptr<QuackFetchStream> stream, string sql) {
	try {
		unique_lock<mutex> guard(connection.lock);
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
		} else if (!stream->Bound()) {
			// No collector claimed the stream, because no statement returned a result. Send the last
			// statement's Success/Count result, as the protocol always has. No columns is an error.
			if (result->names.empty()) {
				stream->buffer.SetError(ErrorData(ExceptionType::INVALID_INPUT, "Query did not return any columns"));
				stream->buffer.Finish();
				return;
			}
			stream->SignalBound(result->types, result->names);
			unique_ptr<FetchResponsePayloadWriter> writer;
			idx_t rows = 0;
			while (auto chunk = result->Fetch()) {
				if (chunk->size() == 0) {
					continue;
				}
				if (!writer) {
					writer = make_uniq<FetchResponsePayloadWriter>(context, 0);
				}
				writer->AppendChunk(*chunk);
				rows += chunk->size();
			}
			if (writer) {
				auto sealed = writer->Seal();
				QuackFetchPayload entry;
				entry.payload = std::move(sealed.payload);
				entry.payload_size = sealed.payload_size;
				entry.index_offset = sealed.index_offset;
				entry.rows = rows;
				QuackBatchIndexField::Patch(entry.payload->GetData(), entry.payload_size, entry.index_offset, 1);
				auto bytes = entry.payload_size;
				stream->buffer.PushBatch(1, std::move(entry), bytes);
				stream->announced_total = 1;
			} else {
				stream->announced_total = 0;
			}
		}
	} catch (std::exception &ex) {
		stream->buffer.SetError(ErrorData(ex));
	}
	// Close against the announced total, so a short stream errors instead of truncating.
	stream->buffer.Finish(stream->announced_total);
}

QuackConnection::~QuackConnection() {
	// Abort and join the INSERT and the fetch collector before the members go away.
	insert.Detach().AbortAndJoin("connection closed during insert");
	AbortFetchStream(*this, "connection closed during fetch");
}

//! Background thread: runs the INSERT that drains `stream` via scan_data_from_quack_client, holding
//! the connection lock for the statement's duration (one transactional statement -> atomic).
static void RunInsertStatement(QuackConnection &connection, shared_ptr<QuackDataStream> stream, string stream_id,
                               string schema_name, string table_name) {
	try {
		unique_lock<mutex> lock(connection.lock);
		auto sql = StringUtil::Format("INSERT INTO %s.%s SELECT * FROM scan_data_from_quack_client(%s)",
		                              SQLIdentifier(schema_name), SQLIdentifier(table_name), SQLString(stream_id));
		auto result = connection.duckdb_connection->Query(sql);
		if (result->HasError()) {
			stream->SetError(result->GetErrorObject());
		}
	} catch (std::exception &ex) {
		stream->SetError(ErrorData(ex));
	}
	// Make sure the consumer side is released even on an unexpected early return.
	stream->Finish();
}

//! Stream id a SEND_DATA/FINALIZE belongs to, or "" for any other message.
static string StreamIdForMessage(QuackMessage &msg) {
	if (msg.Type() == MessageType::SEND_DATA_REQUEST) {
		auto &m = msg.Cast<SendDataRequestMessage>();
		return QuackStreamRegistry::MakeId(m.ConnectionId(), m.QueryUUID());
	}
	if (msg.Type() == MessageType::FINALIZE) {
		auto &m = msg.Cast<FinalizeMessage>();
		return QuackStreamRegistry::MakeId(m.ConnectionId(), m.QueryUUID());
	}
	return string();
}

void QuackServer::ValidateToken(const string &token) {
	if (token.size() < 4) {
		throw InvalidInputException("Quack server token must be at least 4 characters long");
	}
}

QuackServer::QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p)
    : db_ptr(context_p.db), uri(uri_p), token(token_p) {
	ValidateToken(token);
	server_hmac_key = GenerateRandomToken(*context_p.db);
}

QuackServer::~QuackServer() {
}

void QuackServer::CleanupExpiredConnection(QuackConnection &connection) {
	if (connection.duckdb_connection) {
		connection.duckdb_connection->Interrupt();
	}
	connection.insert.Detach().AbortAndJoin("connection heartbeat lease expired");
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
	vector<QuackConnectionSnapshot> result;
	std::lock_guard<std::mutex> lock(active_connections_mutex);
	for (auto &[id, conn] : active_connections) {
		QuackConnectionSnapshot snapshot;
		snapshot.session_id = conn->session_id;
		snapshot.client_id_hash = conn->client_id_hash;
		snapshot.sql_query = conn->sql_query;
		snapshot.query_state = conn->query_state;
		snapshot.query_started_at = conn->query_started_at;
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
	for (auto &entry : candidates) {
		auto connection = GetConnection(entry.session_id);
		if (!connection) {
			// disconnected, the cache died with the connection and the slot dies here
			continue;
		}
		std::unique_lock<std::mutex> lock(connection->lock, std::try_to_lock);
		if (!lock.owns_lock()) {
			// busy serving a request right now, by definition not idle; retry on the next sweep
			requeue.push_back(std::move(entry));
			continue;
		}
		auto expired = ExpireCacheIfStale(*connection, now, ttl_micros);
		if (expired) {
			doomed.push_back(std::move(expired));
		}
		if (connection->result_cache) {
			// served (or replaced) since the snapshot was queued, keep the slot under the fresh timestamp
			entry.served_at = connection->result_cache->last_served_at;
			requeue.push_back(std::move(entry));
		} else {
			connection->cache_in_expiry_queue = false;
		}
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
	new_connection->duckdb_connection->context->config.enable_progress_bar = false;
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

static constexpr idx_t kTokenBytes = 16; // 128 bits

static string HexEncode(const data_t *bytes, idx_t n) {
	string result(n * 2, '\0');
	for (idx_t i = 0; i < n; i++) {
		result[2 * i] = Blob::HEX_TABLE[bytes[i] >> 4];
		result[2 * i + 1] = Blob::HEX_TABLE[bytes[i] & 0x0F];
	}
	return result;
}

// Derive a stable, per-client reconnect identifier as HMAC-SHA256(server_hmac_key, client_id)
static string ComputeClientHash(const string &server_hmac_key, const string &client_id) {
	unsigned char digest[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES];
	duckdb_mbedtls::MbedTlsWrapper::Hmac256(server_hmac_key.data(), server_hmac_key.size(), client_id.data(),
	                                        client_id.size(), reinterpret_cast<char *>(digest));
	return HexEncode(digest, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
}

string QuackServer::GenerateRandomToken(DatabaseInstance &db) {
	auto encryption_util = db.GetEncryptionUtil(false);
	auto metadata =
	    make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, kTokenBytes, EncryptionTypes::EncryptionVersion::NONE);
	auto rng = encryption_util->CreateEncryptionState(std::move(metadata));

	data_t bytes[kTokenBytes];
	rng->GenerateRandomData(bytes, kTokenBytes);
	return HexEncode(bytes, kTokenBytes);
}

string QuackServer::GenerateSessionId() {
	data_t bytes[kTokenBytes];
	{
		std::lock_guard<std::mutex> lock(session_id_rng_mutex);
		if (!session_id_rng) {
			auto db = db_ptr.lock();
			if (!db) {
				throw InternalException("Database was closed");
			}
			auto encryption_util = db->GetEncryptionUtil(false);
			auto metadata = make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, kTokenBytes,
			                                                   EncryptionTypes::EncryptionVersion::NONE);
			session_id_rng = encryption_util->CreateEncryptionState(std::move(metadata));
		}
		// Generate under the mutex: the RNG state is shared across concurrent CONNECTION_REQUESTs.
		session_id_rng->GenerateRandomData(bytes, kTokenBytes);
	}
	return HexEncode(bytes, kTokenBytes);
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
	case MessageType::FINALIZE:
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

	// now deserialize the actual message
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
	if (connection_p && received_message.Type() != MessageType::HEARTBEAT_REQUEST) {
		// A message unrelated to the active insert stream means it was abandoned (client source failed, no
		// FINALIZE) — abort it so it rolls back and releases the connection lock.
		connection_p->insert.DetachIfUnrelated(StreamIdForMessage(received_message))
		    .AbortAndJoin("insert stream abandoned");
	}
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

		// Stop the previous producer first. This joins its thread, so it frees the connection lock.
		AbortFetchStream(connection, "superseded by a new query");
		{
			std::unique_lock<std::mutex> lock(connection.lock);
			connection.sql_query = prepare_request_message.Query();
			connection.query_state = QuackQueryState::ACTIVE;
			connection.query_started_at = Timestamp::GetCurrentTimestamp();
			connection.query_uuid = prepare_request_message.QueryUUID();
		}

		auto stream = make_shared_ptr<QuackFetchStream>();
		stream->buffer.SetCapacity(
		    QuackGetUBigintSetting(db, "quack_fetch_producer_buffer_bytes", QUACK_FETCH_PRODUCER_BUFFER_BYTES_DEFAULT));
		DetachedFetchStream superseded;
		{
			// Install under one lock. A concurrent PREPARE can install a producer after the abort
			// above, and an assignment over a joinable std::thread would stop the process.
			lock_guard<mutex> guard(connection.fetch.lock);
			superseded.stream = std::move(connection.fetch.stream);
			superseded.thread = std::move(connection.fetch.thread);
			connection.fetch.stream = stream;
			connection.fetch.uuid = prepare_request_message.QueryUUID();
			connection.fetch.abort_error = ErrorData();
			connection.fetch.thread = std::thread(RunFetchQuery, std::ref(connection), stream, effective_sql);
		}
		AbortDetachedFetch(connection, std::move(superseded), "superseded by a new query");

		if (!stream->WaitBound()) {
			// planning failed before a collector existed
			auto error = stream->buffer.GetError();
			AbortFetchStream(connection, "query failed");
			std::unique_lock<std::mutex> lock(connection.lock);
			connection.sql_query = "";
			connection.query_state = QuackQueryState::CANCELLED;
			return make_uniq<ErrorResponse>(error);
		}

		// Put the leading batches in the PREPARE response, so a small result needs one round trip
		// only. The FETCH indices then move down by the count this drain consumed.
		idx_t max_inline_rows =
		    QuackGetUBigintSetting(db, "quack_prepare_inline_rows", QUACK_PREPARE_INLINE_ROWS_DEFAULT);
		vector<unique_ptr<DataChunkWrapper>> results;
		idx_t inline_rows = 0;
		idx_t consumed = 0;
		while (inline_rows < max_inline_rows) {
			QuackFetchPayload entry;
			auto status = stream->buffer.TryPopClaimed(consumed + 1, entry);
			if (status == QuackClaimPopStatus::BATCH) {
				consumed++;
				inline_rows += entry.rows;
				// Batches are stored serialized. This drain is the only place that decodes them.
				MemoryStream payload_stream(entry.payload->GetData(), entry.payload_size);
				auto inline_message = QuackMessage::FromMemoryStream(payload_stream);
				for (auto &wrapper : inline_message->Cast<FetchResponseMessage>().MutableResults()) {
					results.push_back(std::move(wrapper));
				}
				continue;
			}
			if (status == QuackClaimPopStatus::FINISHED) {
				break;
			}
			if (status == QuackClaimPopStatus::ERRORED) {
				auto error = stream->buffer.GetError();
				AbortFetchStream(connection, "query failed");
				std::unique_lock<std::mutex> lock(connection.lock);
				connection.sql_query = "";
				connection.query_state = QuackQueryState::CANCELLED;
				return make_uniq<ErrorResponse>(error);
			}
			stream->buffer.WaitForBatch(consumed + 1);
		}
		stream->prepare_batches = consumed;
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

		// No connection.lock here: the producer thread holds it, and FETCH touches only the buffer.
		shared_ptr<QuackFetchStream> stream;
		hugeint_t stream_uuid;
		ErrorData abort_error;
		{
			lock_guard<mutex> guard(connection.fetch.lock);
			stream = connection.fetch.stream;
			stream_uuid = connection.fetch.uuid;
			abort_error = connection.fetch.abort_error;
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
			// the client has everything up to the ack, and cannot ask for it again
			stream->served.erase(stream->served.begin(), stream->served.upper_bound(dense_ack));
		}

		while (true) {
			QuackClaimPopStatus status;
			QuackFetchPayload entry;
			{
				// One critical section for the check, the pop and the retain. A concurrent retry of the
				// same index cannot reach FINISHED while the first serve runs.
				lock_guard<mutex> guard(stream->serve_lock);
				auto retained = stream->served.find(dense_index);
				if (retained != stream->served.end()) {
					// already patched with the client-visible index
					return make_uniq<QuackRawPayloadResponse>(MessageType::FETCH_RESPONSE, retained->second);
				}
				status = stream->buffer.TryPopClaimed(dense_index, entry);
				if (status == QuackClaimPopStatus::BATCH) {
					// stamp the client-visible index, then keep the payload for a retry
					QuackBatchIndexField::Patch(entry.payload->GetData(), entry.payload_size, entry.index_offset,
					                            fetch_request_message.batch_index);
					shared_ptr<MemoryStream> payload(std::move(entry.payload));
					stream->served.emplace(dense_index, payload);
					return make_uniq<QuackRawPayloadResponse>(MessageType::FETCH_RESPONSE, std::move(payload));
				}
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

		// we never execute this query, but throw it at the authorization function so it can check if this user gets to
		// insert into this table
		auto dummy_insert_query =
		    StringUtil::Format("INSERT INTO %s.%s VALUES (NULL)", SQLIdentifier(send_data_message.SchemaName()),
		                       SQLIdentifier(send_data_message.TableName()));

		// TODO do not do this if there is no fun set
		{
			auto auth_result = EvaluateAuthQuery(
			    db, StringUtil::Format("SELECT %s(?, ?)", GetSettingString(db, "quack_authorization_function")),
			    Value(send_data_message.ConnectionId()), Value(dummy_insert_query));
			if (auth_result.IsNull() ||
			    (auth_result.type().id() == LogicalTypeId::BOOLEAN && !auth_result.GetValue<bool>())) {
				return make_uniq<ErrorResponse>("Authorization failed");
			}
		}

		// Lazily create the stream + background INSERT on the first message (stream is keyed by query_uuid).
		auto stream_id = QuackStreamRegistry::MakeId(send_data_message.ConnectionId(), send_data_message.QueryUUID());
		bool ordered = send_data_message.BatchIndex().IsValid();
		auto &incoming_chunks = send_data_message.Chunks();

		// Dead-range marker: no chunks, tells the server batches [lo, hi) are dead so the cursor can skip them.
		if (send_data_message.IsDeadRange()) {
			auto lo = send_data_message.BatchIndex().GetIndex();
			auto hi = send_data_message.DeadRangeEnd().GetIndex();
			auto dead_stream = connection.insert.StreamForDeadRangeOrBuffer(stream_id, lo, hi);
			if (dead_stream) {
				dead_stream->PushDeadRange(lo, hi);
			}
			return make_uniq<SendDataResponseMessage>();
		}

		shared_ptr<QuackDataStream> stream;
		vector<std::pair<idx_t, idx_t>> buffered_dead_ranges;
		{
			annotated_lock_guard<annotated_mutex> guard(connection.insert.lock);
			if (connection.insert.stream) {
				stream = connection.insert.stream;
			} else {
				if (incoming_chunks.empty()) {
					// Zero-chunk first message — the client shouldn't produce this (FlushBuffer skips empty
					// unstarted batches), but guard against it gracefully.
					return make_uniq<SendDataResponseMessage>();
				}
				auto types = incoming_chunks[0]->Chunk().GetTypes();
				stream = QuackStreamRegistry::Get().Create(stream_id, types, ordered);
				connection.insert.stream = stream;
				connection.insert.stream_id = stream_id;
				connection.insert.thread = std::thread(RunInsertStatement, std::ref(connection), stream, stream_id,
				                                       send_data_message.SchemaName(), send_data_message.TableName());
				// Apply any dead-range markers that arrived before the stream existed.
				if (connection.insert.pending_marker_stream_id == stream_id) {
					buffered_dead_ranges = std::move(connection.insert.pending_dead_ranges);
					connection.insert.pending_dead_ranges.clear();
					connection.insert.pending_marker_stream_id.clear();
				}
			}
		}

		for (auto &r : buffered_dead_ranges) {
			stream->PushDeadRange(r.first, r.second);
		}

		// Reference the chunks from the message. DataChunkWrapper.Deserialize uses DataChunk::Reference()
		// internally, so the underlying VectorBuffers are ref-counted and outlive the message.
		vector<unique_ptr<DataChunk>> owned_chunks;
		owned_chunks.reserve(incoming_chunks.size());
		for (auto &wrapper : incoming_chunks) {
			auto owned = make_uniq<DataChunk>();
			owned->InitializeEmpty(wrapper->Chunk().GetTypes());
			owned->Reference(wrapper->Chunk());
			owned_chunks.push_back(std::move(owned));
		}

		if (ordered) {
			stream->PushOrdered(std::move(owned_chunks), send_data_message.BatchIndex().GetIndex(),
			                    send_data_message.SequenceIndex(), send_data_message.IsLastInBatch(),
			                    send_data_message.BatchWatermark());
		} else {
			stream->PushUnordered(std::move(owned_chunks));
		}

		if (stream->HasError()) {
			auto error = connection.insert.Finalize();
			return make_uniq<ErrorResponse>(error);
		}
		return make_uniq<SendDataResponseMessage>(); // accept_budget unset = unbounded (future flow control)
	}
	case MessageType::FINALIZE: {
		auto &finalize_message = received_message.Cast<FinalizeMessage>();
		auto &connection = *connection_p;
		auto stream_id = QuackStreamRegistry::MakeId(finalize_message.ConnectionId(), finalize_message.QueryUUID());
		{
			annotated_lock_guard<annotated_mutex> guard(connection.insert.lock);
			if (connection.insert.stream_id != stream_id) {
				return make_uniq<SuccessResponse>(); // no matching stream (e.g. zero-chunk insert)
			}
		}
		auto error = connection.insert.Finalize(finalize_message.MinBatchWatermark());
		if (error.HasError()) {
			return make_uniq<ErrorResponse>(error);
		}
		return make_uniq<SuccessResponse>();
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
		AbortFetchStream(connection, "Interrupted: query was cancelled");
		connection.duckdb_connection->Interrupt();
		// The interrupt aborts any statement currently running under the session lock; take the
		// lock so the state change cannot race that statement's handler.
		std::unique_lock<std::mutex> lock(connection.lock);
		connection.query_state = QuackQueryState::CANCELLED;
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
