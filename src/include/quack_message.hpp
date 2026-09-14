#pragma once

#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {

class BinaryDeserializer;
class ClientContext;

//! Quack wire-protocol version. Client and server agree on it during the connection handshake.
static constexpr idx_t QUACK_VERSION = 3;

//! Upper bound both peers enforce on the heartbeat lease timeout
static constexpr idx_t MAX_HEARTBEAT_TIMEOUT_SECONDS = static_cast<idx_t>(NumericLimits<int64_t>::Maximum() / 1000);

enum class MessageType : uint8_t {
	INVALID = 0,
	CONNECTION_REQUEST = 1,
	CONNECTION_RESPONSE = 2,
	PREPARE_REQUEST = 3,
	PREPARE_RESPONSE = 4,
	FETCH_REQUEST = 7,
	FETCH_RESPONSE = 8,
	SEND_DATA_REQUEST = 9,
	SUCCESS_RESPONSE = 10,
	DISCONNECT_MESSAGE = 11,
	CANCEL_REQUEST = 12,
	SEND_DATA_RESPONSE = 14,
	ACKNOWLEDGEMENT = 15,
	HEARTBEAT_REQUEST = 16,
	ERROR_RESPONSE = 100
};

template <>
const char *EnumUtil::ToChars<MessageType>(MessageType value);
template <>
MessageType EnumUtil::FromString<MessageType>(const char *value);

// workaround for wrong serialization functions signature on DataChunk :/
// TODO: remove in 2.0
class DataChunkWrapper {
public:
	explicit DataChunkWrapper(DataChunk &chunk_p) {
		chunk.InitializeEmpty(chunk_p.GetTypes());
		chunk.Reference(chunk_p);
	}
	DataChunk &Chunk() {
		return chunk;
	}
	void Serialize(Serializer &serializer) const;
	static unique_ptr<DataChunkWrapper> Deserialize(Deserializer &deserializer);

private:
	DataChunk chunk;
};

string MessageTypeToString(MessageType type);

struct MessageHeader {
	MessageHeader(MessageType type_p, string connection_id_p)
	    : type(type_p), connection_id(std::move(connection_id_p)) {
	}

	MessageType type = MessageType::INVALID;
	string connection_id;
	optional_idx client_query_id;

	void Serialize(Serializer &serializer) const;
	static MessageHeader Deserialize(Deserializer &deserializer);
};

class QuackMessage {
public:
	virtual void ToMemoryStream(MemoryStream &write_stream) const;
	static unique_ptr<QuackMessage> FromMemoryStream(MemoryStream &read_stream);

	//! A serialized payload instead of a body. The HTTP layer sends these bytes as they are. It is
	//! null for a normal message.
	virtual optional_ptr<MemoryStream> RawPayload() const {
		return nullptr;
	}

	//! Where the raw payload's wire body starts. The bytes before it are unused header space.
	virtual idx_t RawPayloadStart() const {
		return 0;
	}

	template <class TARGET>
	TARGET &Cast() {
		// a raw-payload carrier has the payload's type tag, but not its layout
		if (header.type != TARGET::TYPE || RawPayload()) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (header.type != TARGET::TYPE || RawPayload()) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual void Serialize(Serializer &serializer) const = 0;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer, MessageType message_type);
	static MessageHeader DeserializeHeader(BinaryDeserializer &deserializer);
	static unique_ptr<QuackMessage> DeserializeMessage(BinaryDeserializer &deserializer, MessageHeader header);

	//! Reads the raw blob that follows this message on the wire. A no-op for a message with none.
	virtual void DecodeBlob(BinaryDeserializer &) {
	}

	const MessageType &Type() const {
		return header.type;
	}

	optional_idx ClientQueryId() const {
		return header.client_query_id;
	}

	void SetClientQueryId(optional_idx query_id) {
		header.client_query_id = query_id;
	}

	virtual ~QuackMessage() {
	}

	const string &ConnectionId() const {
		return header.connection_id;
	}

	//! SQL text to record in request logs; empty for message types that carry none.
	virtual const string &LoggableQuery() const {
		static const string empty;
		return empty;
	}

	void SetHeader(MessageHeader header_p) {
		header = std::move(header_p);
	}

protected:
	explicit QuackMessage(MessageType type);
	explicit QuackMessage(MessageType type, string connection_id_p);

private:
	MessageHeader header;
};

class PrepareRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_REQUEST;

	PrepareRequestMessage(string connection_id_p, string sql_query_p, hugeint_t query_uuid_p)
	    : QuackMessage(TYPE, std::move(connection_id_p)), sql_query(std::move(sql_query_p)), query_uuid(query_uuid_p) {
	}

	//! Rows the response may carry. Unset uses the server setting. Zero makes PREPARE answer before
	//! the query gives a row, which a statement that waits for client data needs.
	void SetInlineRows(optional_idx inline_rows_p) {
		inline_rows = inline_rows_p;
	}
	optional_idx InlineRows() const {
		return inline_rows;
	}

public:
	const string &Query() const {
		return sql_query;
	}
	const string &LoggableQuery() const override {
		return sql_query;
	}
	hugeint_t QueryUUID() const {
		return query_uuid;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<PrepareRequestMessage> Deserialize(Deserializer &deserializer);

protected:
	PrepareRequestMessage() : QuackMessage(TYPE) {
	}

private:
	string sql_query;
	hugeint_t query_uuid;
	optional_idx inline_rows;
};

class PrepareResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::PREPARE_RESPONSE;

	PrepareResponseMessage(const vector<LogicalType> &types_p, const vector<string> &names_p,
	                       vector<unique_ptr<DataChunkWrapper>> results_p, bool needs_more_fetch_p,
	                       hugeint_t query_uuid)
	    : QuackMessage(TYPE), result_types(types_p), result_names(names_p), results(std::move(results_p)),
	      needs_more_fetch(needs_more_fetch_p), query_uuid(query_uuid) {
	}

public:
	const vector<LogicalType> &Types() const {
		return result_types;
	}

	const vector<string> &Names() const {
		return result_names;
	}

	vector<unique_ptr<DataChunkWrapper>> &MutableResults() {
		return results;
	}

	bool NeedsMoreFetch() const {
		return needs_more_fetch;
	}
	hugeint_t QueryUUID() const {
		return query_uuid;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<PrepareResponseMessage> Deserialize(Deserializer &deserializer);

protected:
	PrepareResponseMessage() : QuackMessage(TYPE) {
	}

private:
	vector<LogicalType> result_types;
	vector<string> result_names;
	vector<unique_ptr<DataChunkWrapper>> results;
	bool needs_more_fetch = false;
	hugeint_t query_uuid;
};

// TODO this is where auth goes
class ConnectionRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_REQUEST;

	explicit ConnectionRequestMessage(const string &auth_string_p, string client_id_p,
	                                  idx_t heartbeat_timeout_seconds_p);

public:
	const string &AuthString() const {
		return auth_string;
	}
	const string &ClientId() const {
		return client_id;
	}
	const string &ClientVersion() const {
		return client_duckdb_version;
	}
	const string &ClientPlatform() const {
		return client_platform;
	}
	const idx_t MinimumSupportedQuackVersion() const {
		return min_supported_quack_version;
	}
	const idx_t MaximumSupportedQuackVersion() const {
		return max_supported_quack_version;
	}
	idx_t HeartbeatTimeoutSeconds() const {
		return heartbeat_timeout_seconds;
	}
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ConnectionRequestMessage> Deserialize(Deserializer &deserializer);

protected:
	ConnectionRequestMessage() : QuackMessage(TYPE) {
	}

private:
	string auth_string;
	string client_id;
	string client_duckdb_version;
	string client_platform;
	idx_t min_supported_quack_version;
	idx_t max_supported_quack_version;
	idx_t heartbeat_timeout_seconds;
};

class ConnectionResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_RESPONSE;

	explicit ConnectionResponseMessage(string connection_id_p, idx_t heartbeat_timeout_seconds_p);

protected:
	ConnectionResponseMessage() : QuackMessage(TYPE) {
	}

public:
	const string &ServerVersion() const {
		return server_duckdb_version;
	}
	const string &ServerPlatform() const {
		return server_platform;
	}
	idx_t QuackVersion() const {
		return quack_version;
	}
	idx_t HeartbeatTimeoutSeconds() const {
		return heartbeat_timeout_seconds;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ConnectionResponseMessage> Deserialize(Deserializer &deserializer);

private:
	string server_duckdb_version;
	string server_platform;
	idx_t quack_version;
	idx_t heartbeat_timeout_seconds;
};

class FetchRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_REQUEST;

	FetchRequestMessage(string connection_id_p, hugeint_t uuid, idx_t batch_index, idx_t ack_index)
	    : QuackMessage(TYPE, std::move(connection_id_p)), uuid(uuid), batch_index(batch_index), ack_index(ack_index) {
	}

protected:
	FetchRequestMessage() : QuackMessage(TYPE) {
	}

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<FetchRequestMessage> Deserialize(Deserializer &deserializer);

	hugeint_t uuid;
	//! The dense batch index this request claims, from 1. Every request names its batch, so a
	//! transport retry asks for the SAME batch.
	idx_t batch_index = 0;
	//! All of 1..ack_index arrived. The server can drop the batches it kept below this index.
	idx_t ack_index = 0;
};

// One dense batch of a result, server to client. The chunks travel as a raw blob after the message.
class FetchResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_RESPONSE;

	FetchResponseMessage() : QuackMessage(TYPE) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<FetchResponseMessage> Deserialize(Deserializer &deserializer);

	void DecodeBlob(BinaryDeserializer &deserializer) override;

	vector<unique_ptr<DataChunk>> &MutableResults() {
		return results;
	}

	//! Set by the payload-writer path only. The generated codec never carries the chunks.
	void SetChunkCount(idx_t chunk_count_p) {
		chunk_count = chunk_count_p;
	}
	void SetBatchIndex(optional_idx batch_index_p) {
		batch_index = batch_index_p;
	}
	optional_idx BatchIndex() const {
		return batch_index;
	}

	//! Set on the terminal response only.
	void SetTotalBatches(optional_idx total_batches_p) {
		total_batches = total_batches_p;
	}
	optional_idx TotalBatches() const {
		return total_batches;
	}

private:
	//! The decoded chunks. They travel as the blob, not as a field of this message.
	vector<unique_ptr<DataChunk>> results;
	//! How many chunks the blob holds. Zero on the terminal response.
	idx_t chunk_count = 0;
	optional_idx batch_index;
	optional_idx total_batches;
};

// One dense batch of data for a client stream that scan_data_from_quack_client drains. Batches
// arrive in any order, and the server puts them back in order by index. The client ends the stream
// with a chunk-less message that carries total_batches, so a short stream fails. The statement's
// result (its row count, or RETURNING rows) is fetched like any other result.
class SendDataRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::SEND_DATA_REQUEST;

	SendDataRequestMessage(string connection_id_p, string stream_id_p)
	    : QuackMessage(TYPE, std::move(connection_id_p)), stream_id(std::move(stream_id_p)) {
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<SendDataRequestMessage> Deserialize(Deserializer &deserializer);

	void DecodeBlob(BinaryDeserializer &deserializer) override;

	//! Set by the payload-writer path only. The generated codec never carries the chunks.
	void SetChunkCount(idx_t chunk_count_p) {
		chunk_count = chunk_count_p;
	}
	void SetBatchIndex(optional_idx batch_index_p) {
		batch_index = batch_index_p;
	}
	//! The batch count of the whole stream. Only the terminal message sets it.
	void SetTotalBatches(optional_idx total_batches_p) {
		total_batches = total_batches_p;
	}

	vector<unique_ptr<DataChunk>> &Chunks() {
		return chunks;
	}
	const string &StreamId() const {
		return stream_id;
	}
	optional_idx BatchIndex() const {
		return batch_index;
	}
	optional_idx TotalBatches() const {
		return total_batches;
	}

protected:
	SendDataRequestMessage() : QuackMessage(TYPE) {
	}

private:
	string stream_id;
	//! The decoded chunks. They travel as the blob, not as a field of this message.
	vector<unique_ptr<DataChunk>> chunks;
	//! How many chunks the blob holds. Zero on the terminal message.
	idx_t chunk_count = 0;
	//! Set on the terminal message only. The server closes the stream against it.
	optional_idx total_batches;
	//! The dense batch index (1,2,3,...). Unset on the terminal message.
	optional_idx batch_index;
};

//! Free space at the top of a chunk payload. The header is written into it at emit time.
static constexpr idx_t QUACK_PAYLOAD_HEADER_BYTES = 256;

//! Serializes chunks into one buffer, and keeps free space at the top for the emit-time header.
//! All header fields are known at emit, so nothing in the payload is ever patched.
class QuackChunkPayloadWriter {
public:
	//! capacity_hint is the previous payload's size. It is reserved, so a steady stream never grows.
	explicit QuackChunkPayloadWriter(idx_t capacity_hint);
	~QuackChunkPayloadWriter();

	//! The caller can reuse the chunk after this call.
	void AppendChunk(DataChunk &chunk);
	//! After Seal: the final payload size.
	idx_t SizeBytes() const;
	idx_t AllocatedBytes() const;

	struct SealedPayload {
		unique_ptr<MemoryStream> payload;
		//! The end of the blob. The blob starts at QUACK_PAYLOAD_HEADER_BYTES.
		idx_t payload_size = 0;
		idx_t chunk_count = 0;
	};
	SealedPayload Seal();

private:
	unique_ptr<MemoryStream> stream;
	unique_ptr<BinarySerializer> serializer;
	idx_t chunk_count = 0;
	idx_t sealed_size = 0;
};

//! Writes the header back-aligned into the free space, so it ends where the blob starts. Returns
//! where the wire body starts. (A fixed-width header would also align the blob, for zero-copy.)
idx_t QuackPrependHeader(MemoryStream &payload, const QuackMessage &header_message);

//! Reads chunk_count self-describing chunks: the mirror of QuackChunkPayloadWriter::AppendChunk.
vector<unique_ptr<DataChunk>> DecodeQuackChunkBlob(BinaryDeserializer &deserializer, idx_t chunk_count);

//! Carries a serialized payload. The HTTP layer sends the bytes from RawPayloadStart() as they are.
//! Never Cast<> this to the payload's message type: only the header type tag is the same.
class QuackRawPayloadResponse : public QuackMessage {
public:
	//! A shared_ptr, because a served payload stays on the stream for a safe re-serve.
	QuackRawPayloadResponse(MessageType type, shared_ptr<MemoryStream> payload_p, idx_t body_start_p)
	    : QuackMessage(type), payload(std::move(payload_p)), body_start(body_start_p) {
	}

	void Serialize(Serializer &serializer) const override {
		throw InternalException("QuackRawPayloadResponse must not be serialized; send RawPayload() directly");
	}

	optional_ptr<MemoryStream> RawPayload() const override {
		return payload.get();
	}

	idx_t RawPayloadStart() const override {
		return body_start;
	}

private:
	shared_ptr<MemoryStream> payload;
	//! Where the wire body starts. The bytes before it are unused header space.
	idx_t body_start;
};

// Success reply to a SendDataRequestMessage. `accept_budget` is a placeholder for a future flow-control
// hint (invalid means unbounded); the client currently ignores it.
class SendDataResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::SEND_DATA_RESPONSE;

	explicit SendDataResponseMessage(optional_idx accept_budget_p = optional_idx())
	    : QuackMessage(TYPE), accept_budget(accept_budget_p) {
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<SendDataResponseMessage> Deserialize(Deserializer &deserializer);

	optional_idx AcceptBudget() const {
		return accept_budget;
	}

private:
	optional_idx accept_budget;
};

class DisconnectMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::DISCONNECT_MESSAGE;

	explicit DisconnectMessage(string connection_id_p) : QuackMessage(TYPE, std::move(connection_id_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<DisconnectMessage> Deserialize(Deserializer &deserializer);

protected:
	DisconnectMessage() : QuackMessage(TYPE) {
	}
};

//! Renews a logical connection lease.
class HeartbeatRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::HEARTBEAT_REQUEST;

	explicit HeartbeatRequestMessage(string connection_id_p) : QuackMessage(TYPE, std::move(connection_id_p)) {
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<HeartbeatRequestMessage> Deserialize(Deserializer &deserializer);

protected:
	HeartbeatRequestMessage() : QuackMessage(TYPE) {
	}
};

class SuccessResponse : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::SUCCESS_RESPONSE;

	explicit SuccessResponse() : QuackMessage(TYPE) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<SuccessResponse> Deserialize(Deserializer &deserializer);
};

class AcknowledgementMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::ACKNOWLEDGEMENT;

	explicit AcknowledgementMessage(string connection_id_p, hugeint_t query_uuid_p)
	    : QuackMessage(TYPE, std::move(connection_id_p)), query_uuid(query_uuid_p) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AcknowledgementMessage> Deserialize(Deserializer &deserializer);

	hugeint_t QueryUUID() const {
		return query_uuid;
	}

protected:
	AcknowledgementMessage() : QuackMessage(TYPE) {
	}

private:
	//! Acknowledged query. {0,0} is the deserialization default. Caches need nonzero uuids, so it never matches one.
	hugeint_t query_uuid {0, 0};
};

//! Keys quack adds to an error's extra info. They live in the ErrorData, not in ErrorResponse, so that they
//! survive relaying: a quack server can be the client of another one, and there the error is caught and
//! re-raised through DuckDB code that has never heard of quack. Extra info makes that hop, message fields do
//! not.
namespace QuackErrorInfo {
//! The type the SERVER raised, when it was too severe to rethrow as-is (see ErrorResponse::FromWire)
constexpr const char *ORIGINAL_EXCEPTION_TYPE = "original_exception_type";
} // namespace QuackErrorInfo

//! An error raised while handling a request. Type and extra info travel with the message, so the client
//! rethrows the exception the server raised (a Catalog Error stays a Catalog Error).
class ErrorResponse : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::ERROR_RESPONSE;
	explicit ErrorResponse(ErrorData error_p) : QuackMessage(TYPE), error(std::move(error_p)) {
	}
	explicit ErrorResponse(const string &error_p) : QuackMessage(TYPE), error(ExceptionType::INVALID_INPUT, error_p) {
	}
	template <typename... ARGS>
	explicit ErrorResponse(const string &msg, ARGS &&...params)
	    : ErrorResponse(Exception::ConstructMessage(msg, std::forward<ARGS>(params)...)) {
	}
	const ErrorData &Error() const {
		return error;
	}
	const string &ErrorMessage() const {
		return error.Message();
	}
	//! The exception type in its wire form (e.g. "Catalog"), empty if this response holds no error
	string ExceptionTypeName() const;
	//! The extra info minus the entries that only mean something in the process that raised the error
	unordered_map<string, string> TransferableExtraInfo() const;

	//! Whether the SENDER's database is now invalidated, i.e. the receiver should stop using this connection.
	//! A bare flag rather than the sender's identity: a server behind a proxy does not know the address it is
	//! reachable at, while the receiver always knows which server it dialed.
	bool MustInvalidate() const {
		return must_invalidate;
	}
	void SetMustInvalidate(bool must_invalidate_p) {
		must_invalidate = must_invalidate_p;
	}
	//! Rebuild an error response from the wire fields - the inverse of Serialize
	static unique_ptr<ErrorResponse> FromWire(const string &message, const string &exception_type,
	                                          const unordered_map<string, string> &extra_info, bool must_invalidate);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ErrorResponse> Deserialize(Deserializer &deserializer);

protected:
	ErrorResponse() : QuackMessage(TYPE) {
	}

private:
	ErrorData error;
	//! Set by the server when its OWN database died with this error - see MustInvalidate()
	bool must_invalidate = false;
};

class CancelRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CANCEL_REQUEST;

	explicit CancelRequestMessage(string connection_id_p, hugeint_t query_uuid_p)
	    : QuackMessage(TYPE, std::move(connection_id_p)), query_uuid(query_uuid_p) {
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<CancelRequestMessage> Deserialize(Deserializer &deserializer);

	hugeint_t query_uuid;

protected:
	CancelRequestMessage() : QuackMessage(TYPE) {
	}
};

} // namespace duckdb
