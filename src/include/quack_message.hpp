#pragma once

#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {

//! Quack wire-protocol version. Client and server agree on it during the connection handshake.
static constexpr idx_t QUACK_VERSION = 2;

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
	FINALIZE = 13,
	SEND_DATA_RESPONSE = 14,
	ACKNOWLEDGEMENT = 15,
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
	void ToMemoryStream(MemoryStream &write_stream) const;
	static unique_ptr<QuackMessage> FromMemoryStream(MemoryStream &read_stream);

	template <class TARGET>
	TARGET &Cast() {
		if (header.type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (header.type != TARGET::TYPE) {
			throw InternalException("Failed to cast message to type - message type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual void Serialize(Serializer &serializer) const = 0;
	static unique_ptr<QuackMessage> Deserialize(Deserializer &deserializer, MessageType message_type);
	static MessageHeader DeserializeHeader(BinaryDeserializer &deserializer);
	static unique_ptr<QuackMessage> DeserializeMessage(BinaryDeserializer &deserializer, MessageHeader header);

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

	explicit ConnectionRequestMessage(const string &auth_string_p, string client_id_p = {});

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
};

class ConnectionResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::CONNECTION_RESPONSE;

	explicit ConnectionResponseMessage(string connection_id_p);

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

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ConnectionResponseMessage> Deserialize(Deserializer &deserializer);

private:
	string server_duckdb_version;
	string server_platform;
	idx_t quack_version;
};

class FetchRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_REQUEST;

	explicit FetchRequestMessage(string connection_id_p, hugeint_t uuid)
	    : QuackMessage(TYPE, std::move(connection_id_p)), uuid(uuid) {
	}

protected:
	FetchRequestMessage() : QuackMessage(TYPE) {
	}

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<FetchRequestMessage> Deserialize(Deserializer &deserializer);

	hugeint_t uuid;
};

class FetchResponseMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FETCH_RESPONSE;

	FetchResponseMessage() : QuackMessage(TYPE) {};
	explicit FetchResponseMessage(vector<unique_ptr<DataChunkWrapper>> results_p)
	    : QuackMessage(TYPE), results(std::move(results_p)) {};
	FetchResponseMessage(vector<unique_ptr<DataChunkWrapper>> results_p, optional_idx batch_index_p)
	    : QuackMessage(TYPE), results(std::move(results_p)), batch_index(batch_index_p) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<FetchResponseMessage> Deserialize(Deserializer &deserializer);

	vector<unique_ptr<DataChunkWrapper>> &MutableResults() {
		return results;
	}

	optional_idx BatchIndex() const {
		return batch_index;
	}

private:
	vector<unique_ptr<DataChunkWrapper>> results;
	optional_idx batch_index;
};

// Streams one or more DataChunks of insert data to the server, keyed by (connection_id, query_uuid).
// batch_index + sequence_index + is_last_in_batch are set for ordered inserts; invalid batch_index
// means unordered (arrive and insert in any order). Answered by SendDataResponseMessage or ErrorResponse.
class SendDataRequestMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::SEND_DATA_REQUEST;

	explicit SendDataRequestMessage(string connection_id_p, string schema_name_p, string table_name_p,
	                                vector<unique_ptr<DataChunkWrapper>> chunks_p, hugeint_t query_uuid_p)
	    : QuackMessage(TYPE, std::move(connection_id_p)), schema_name(std::move(schema_name_p)),
	      table_name(std::move(table_name_p)), chunks(std::move(chunks_p)), query_uuid(query_uuid_p) {
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<SendDataRequestMessage> Deserialize(Deserializer &deserializer);

	void SetOrdering(optional_idx batch_index_p, idx_t sequence_index_p, bool is_last_in_batch_p) {
		batch_index = batch_index_p;
		sequence_index = sequence_index_p;
		is_last_in_batch = is_last_in_batch_p;
	}

	void SetBatchWatermark(optional_idx watermark) {
		batch_watermark = watermark;
	}

	//! Turn this into a dead-range marker: batches [batch_index, dead_range_end) produced no rows and will
	//! never arrive. Carries no chunks; batch_index is the (low) range start so it stays near the cursor.
	void SetDeadRange(idx_t dead_start, idx_t dead_end) {
		batch_index = optional_idx(dead_start);
		dead_range_end = optional_idx(dead_end);
	}
	bool IsDeadRange() const {
		return dead_range_end.IsValid();
	}
	optional_idx DeadRangeEnd() const {
		return dead_range_end;
	}

	vector<unique_ptr<DataChunkWrapper>> &Chunks() {
		return chunks;
	}
	const string &SchemaName() const {
		return schema_name;
	}
	const string &TableName() const {
		return table_name;
	}
	hugeint_t QueryUUID() const {
		return query_uuid;
	}
	optional_idx BatchIndex() const {
		return batch_index;
	}
	idx_t SequenceIndex() const {
		return sequence_index;
	}
	bool IsLastInBatch() const {
		return is_last_in_batch;
	}
	optional_idx BatchWatermark() const {
		return batch_watermark;
	}

protected:
	SendDataRequestMessage() : QuackMessage(TYPE) {
	}

private:
	string schema_name;
	string table_name;
	vector<unique_ptr<DataChunkWrapper>> chunks;
	hugeint_t query_uuid;
	optional_idx batch_index;
	idx_t sequence_index = 0;
	bool is_last_in_batch = false;
	//! Minimum batch index that will ever appear in this stream; piggybacked on every ordered message so
	//! the server can initialise its delivery cursor and start draining as soon as batches are complete.
	optional_idx batch_watermark;
	//! Set only on dead-range markers: batches [batch_index, dead_range_end) are dead (a filtered/pruned
	//! gap the sink never crossed). Lets the server skip the gap instead of stalling. Invalid on data messages.
	optional_idx dead_range_end;
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

// End-of-stream marker for a client->server stream (connection_id, query_uuid): server drains and
// replies Success/Error. Used by SEND_DATA inserts today; reusable for future streams (e.g. reads).
class FinalizeMessage : public QuackMessage {
public:
	static constexpr MessageType TYPE = MessageType::FINALIZE;

	explicit FinalizeMessage(string connection_id_p, hugeint_t query_uuid_p)
	    : QuackMessage(TYPE, std::move(connection_id_p)), query_uuid(query_uuid_p) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<FinalizeMessage> Deserialize(Deserializer &deserializer);

	void SetMinBatchWatermark(optional_idx watermark) {
		min_batch_watermark = watermark;
	}
	hugeint_t QueryUUID() const {
		return query_uuid;
	}
	optional_idx MinBatchWatermark() const {
		return min_batch_watermark;
	}

protected:
	FinalizeMessage() : QuackMessage(TYPE) {
	}

private:
	hugeint_t query_uuid;
	//! Minimum batch index in the stream; set when ordered mode was used. Server initialises its delivery
	//! cursor from this value after all data has been received.
	optional_idx min_batch_watermark;
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

	explicit AcknowledgementMessage(string connection_id_p) : QuackMessage(TYPE, std::move(connection_id_p)) {};

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AcknowledgementMessage> Deserialize(Deserializer &deserializer);

protected:
	AcknowledgementMessage() : QuackMessage(TYPE) {
	}
};

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

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ErrorResponse> Deserialize(Deserializer &deserializer);

protected:
	ErrorResponse() : QuackMessage(TYPE) {
	}

private:
	ErrorData error;
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
