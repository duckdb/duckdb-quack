#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/string_util.hpp"

#include "quack_message.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

QuackMessage::QuackMessage(MessageType type) : header(type, string()) {
}
QuackMessage::QuackMessage(MessageType type, string connection_id_p) : header(type, std::move(connection_id_p)) {
}

string MessageTypeToString(MessageType type) {
	return EnumUtil::ToString(type);
}

template <>
MessageType EnumUtil::FromString<MessageType>(const char *value) {
	if (StringUtil::Equals(value, "INVALID")) {
		return MessageType::INVALID;
	}
	if (StringUtil::Equals(value, "CONNECTION_REQUEST")) {
		return MessageType::CONNECTION_REQUEST;
	}
	if (StringUtil::Equals(value, "CONNECTION_RESPONSE")) {
		return MessageType::CONNECTION_RESPONSE;
	}
	if (StringUtil::Equals(value, "PREPARE_REQUEST")) {
		return MessageType::PREPARE_REQUEST;
	}
	if (StringUtil::Equals(value, "PREPARE_RESPONSE")) {
		return MessageType::PREPARE_RESPONSE;
	}
	if (StringUtil::Equals(value, "FETCH_REQUEST")) {
		return MessageType::FETCH_REQUEST;
	}
	if (StringUtil::Equals(value, "FETCH_RESPONSE")) {
		return MessageType::FETCH_RESPONSE;
	}
	if (StringUtil::Equals(value, "SEND_DATA_REQUEST")) {
		return MessageType::SEND_DATA_REQUEST;
	}
	if (StringUtil::Equals(value, "SEND_DATA_RESPONSE")) {
		return MessageType::SEND_DATA_RESPONSE;
	}
	if (StringUtil::Equals(value, "SUCCESS_RESPONSE")) {
		return MessageType::SUCCESS_RESPONSE;
	}
	if (StringUtil::Equals(value, "DISCONNECT_MESSAGE")) {
		return MessageType::DISCONNECT_MESSAGE;
	}
	if (StringUtil::Equals(value, "CANCEL_REQUEST")) {
		return MessageType::CANCEL_REQUEST;
	}
	if (StringUtil::Equals(value, "ACKNOWLEDGEMENT")) {
		return MessageType::ACKNOWLEDGEMENT;
	}
	if (StringUtil::Equals(value, "HEARTBEAT_REQUEST")) {
		return MessageType::HEARTBEAT_REQUEST;
	}
	if (StringUtil::Equals(value, "ERROR_RESPONSE")) {
		return MessageType::ERROR_RESPONSE;
	}

	throw NotImplementedException(StringUtil::Format("Enum value of type MessageType: '%s' not implemented", value));
}

template <>
const char *EnumUtil::ToChars<MessageType>(MessageType value) {
	switch (value) {
	case MessageType::CONNECTION_REQUEST:
		return "CONNECTION_REQUEST";
	case MessageType::CONNECTION_RESPONSE:
		return "CONNECTION_RESPONSE";
	case MessageType::PREPARE_REQUEST:
		return "PREPARE_REQUEST";
	case MessageType::PREPARE_RESPONSE:
		return "PREPARE_RESPONSE";
	case MessageType::FETCH_REQUEST:
		return "FETCH_REQUEST";
	case MessageType::FETCH_RESPONSE:
		return "FETCH_RESPONSE";
	case MessageType::SEND_DATA_REQUEST:
		return "SEND_DATA_REQUEST";
	case MessageType::SEND_DATA_RESPONSE:
		return "SEND_DATA_RESPONSE";
	case MessageType::SUCCESS_RESPONSE:
		return "SUCCESS_RESPONSE";
	case MessageType::DISCONNECT_MESSAGE:
		return "DISCONNECT_MESSAGE";
	case MessageType::CANCEL_REQUEST:
		return "CANCEL_REQUEST";
	case MessageType::ACKNOWLEDGEMENT:
		return "ACKNOWLEDGEMENT";
	case MessageType::HEARTBEAT_REQUEST:
		return "HEARTBEAT_REQUEST";
	case MessageType::ERROR_RESPONSE:
		return "ERROR_RESPONSE";

	default:
		throw NotImplementedException(
		    StringUtil::Format("Enum value of type MessageType: '%d' not implemented", value));
	}
}

//! One place for the wire format options, so the messages and the chunk blobs cannot diverge.
static SerializationOptions QuackWireSerializationOptions() {
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::FromIndex(StorageVersion::V2_0_0);
	return options;
}

void QuackMessage::ToMemoryStream(MemoryStream &write_stream) const {
	write_stream.Rewind();
	BinarySerializer serializer(write_stream, QuackWireSerializationOptions());

	// write the header
	serializer.Begin();
	header.Serialize(serializer);
	serializer.End();
	// write the body
	serializer.Begin();
	Serialize(serializer);
	serializer.End();
}

unique_ptr<QuackMessage> QuackMessage::Deserialize(Deserializer &deserializer, MessageType message_type) {
	switch (message_type) {
	case MessageType::CONNECTION_REQUEST:
		return ConnectionRequestMessage::Deserialize(deserializer);
	case MessageType::CONNECTION_RESPONSE:
		return ConnectionResponseMessage::Deserialize(deserializer);
	case MessageType::PREPARE_REQUEST:
		return PrepareRequestMessage::Deserialize(deserializer);
	case MessageType::PREPARE_RESPONSE:
		return PrepareResponseMessage::Deserialize(deserializer);
	case MessageType::FETCH_REQUEST:
		return FetchRequestMessage::Deserialize(deserializer);
	case MessageType::FETCH_RESPONSE:
		return FetchResponseMessage::Deserialize(deserializer);
	case MessageType::SEND_DATA_REQUEST:
		return SendDataRequestMessage::Deserialize(deserializer);
	case MessageType::SEND_DATA_RESPONSE:
		return SendDataResponseMessage::Deserialize(deserializer);
	case MessageType::SUCCESS_RESPONSE:
		return SuccessResponse::Deserialize(deserializer);
	case MessageType::DISCONNECT_MESSAGE:
		return DisconnectMessage::Deserialize(deserializer);
	case MessageType::CANCEL_REQUEST:
		return CancelRequestMessage::Deserialize(deserializer);
	case MessageType::ACKNOWLEDGEMENT:
		return AcknowledgementMessage::Deserialize(deserializer);
	case MessageType::HEARTBEAT_REQUEST:
		return HeartbeatRequestMessage::Deserialize(deserializer);
	case MessageType::ERROR_RESPONSE:
		return ErrorResponse::Deserialize(deserializer);
	default:
		throw InternalException("Unsupported type for message deserialization");
	}
}

MessageHeader QuackMessage::DeserializeHeader(BinaryDeserializer &deserializer) {
	deserializer.Begin();
	auto header = MessageHeader::Deserialize(deserializer);
	deserializer.End();
	return header;
}

unique_ptr<QuackMessage> QuackMessage::DeserializeMessage(BinaryDeserializer &deserializer, MessageHeader header) {
	// read the body
	deserializer.Begin();
	auto result = Deserialize(deserializer, header.type);
	result->SetHeader(std::move(header));
	deserializer.End();
	// a chunk-carrying message continues with its raw blob, from right after the message
	result->DecodeBlob(deserializer);
	return result;
}

//! Extra info that must not cross the wire, because it describes the process that raised the error:
//! - exception_type / exception_message are reserved by ExceptionToJSONMap (it asserts on them), and a peer
//!   sending them would overwrite the type and message we decoded
//! - position is an offset into the SERVER's SQL - the client would point a caret into its own statement
//! - a stack trace holds frames of the server process, and would hand out the layout of the server binary to
//!   anyone who can trigger an error
static bool IsTransferableErrorInfo(const string &key) {
	static const char *const NON_TRANSFERABLE[] = {"exception_type", "exception_message", "position", "stack_trace",
	                                               "stack_trace_pointers"};
	for (auto &non_transferable : NON_TRANSFERABLE) {
		if (key == non_transferable) {
			return false;
		}
	}
	return true;
}

static unordered_map<string, string> TransferableErrorInfo(const unordered_map<string, string> &extra_info) {
	unordered_map<string, string> result;
	for (auto &entry : extra_info) {
		if (IsTransferableErrorInfo(entry.first)) {
			result.insert(entry);
		}
	}
	return result;
}

//! DuckDB invalidates the whole DatabaseInstance when it sees one of these (client_context.cpp). That verdict
//! is about the SERVER, so rethrowing one verbatim would let a broken server poison THIS database.
static bool InvalidatesDatabase(ExceptionType type) {
	return type == ExceptionType::INTERNAL || Exception::InvalidatesDatabase(type);
}

string ErrorResponse::ExceptionTypeName() const {
	if (!error.HasError()) {
		return string();
	}
	return Exception::ExceptionTypeToString(error.Type());
}

unordered_map<string, string> ErrorResponse::TransferableExtraInfo() const {
	return TransferableErrorInfo(error.ExtraInfo());
}

unique_ptr<ErrorResponse> ErrorResponse::FromWire(const string &message, const string &exception_type,
                                                  const unordered_map<string, string> &extra_info,
                                                  bool must_invalidate) {
	auto type = Exception::StringToExceptionType(exception_type);
	if (type == ExceptionType::INVALID) {
		// the peer sent no type, or one this DuckDB does not know (it may run a different version)
		type = ExceptionType::INVALID_INPUT;
	}
	// the peer controls this map, so filter it here too and not just on the way out
	auto received_info = TransferableErrorInfo(extra_info);

	auto raw_message = message;
	if (InvalidatesDatabase(type)) {
		// downgrade to a type that does not invalidate OUR database, keeping the original one in the extra info
		received_info[QuackErrorInfo::ORIGINAL_EXCEPTION_TYPE] = Exception::ExceptionTypeToString(type);
		raw_message =
		    StringUtil::Format("%s Error on the Quack server: %s", Exception::ExceptionTypeToString(type), raw_message);
		type = ExceptionType::INVALID_INPUT;
	}

	auto result = unique_ptr<ErrorResponse>(new ErrorResponse());
	result->must_invalidate = must_invalidate;
	if (received_info.empty()) {
		result->error = ErrorData(type, raw_message);
	} else {
		// ErrorData has no setter for its extra info: the JSON constructor is the only way to restore it
		result->error = ErrorData(StringUtil::ExceptionToJSONMap(type, raw_message, received_info));
	}
	return result;
}

ConnectionRequestMessage::ConnectionRequestMessage(const string &auth_string_p, string client_id_p,
                                                   idx_t heartbeat_timeout_seconds_p)
    : QuackMessage(TYPE), auth_string(auth_string_p), client_id(std::move(client_id_p)),
      client_duckdb_version(DuckDB::LibraryVersion()), client_platform(DuckDB::Platform()),
      min_supported_quack_version(QUACK_VERSION), max_supported_quack_version(QUACK_VERSION),
      heartbeat_timeout_seconds(heartbeat_timeout_seconds_p) {
}

ConnectionResponseMessage::ConnectionResponseMessage(string connection_id_p, idx_t heartbeat_timeout_seconds_p)
    : QuackMessage(TYPE, std::move(connection_id_p)), server_duckdb_version(DuckDB::LibraryVersion()),
      server_platform(DuckDB::Platform()), quack_version(QUACK_VERSION),
      heartbeat_timeout_seconds(heartbeat_timeout_seconds_p) {
}

unique_ptr<QuackMessage> QuackMessage::FromMemoryStream(MemoryStream &read_stream) {
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);

	// read the header
	auto header = DeserializeHeader(deserializer);
	// read the message
	return DeserializeMessage(deserializer, std::move(header));
}

//===--------------------------------------------------------------------===//
// QuackChunkPayloadWriter
//===--------------------------------------------------------------------===//
QuackChunkPayloadWriter::QuackChunkPayloadWriter(idx_t capacity_hint) {
	auto capacity = NextPowerOfTwo(MaxValue<idx_t>(capacity_hint, 65536));
	stream = make_uniq<MemoryStream>(Allocator::DefaultAllocator(), capacity);
	// The blob starts after the header space. The header itself is written at emit time.
	stream->SetPosition(QUACK_PAYLOAD_HEADER_BYTES);
	serializer = make_uniq<BinarySerializer>(*stream, QuackWireSerializationOptions());
}

QuackChunkPayloadWriter::~QuackChunkPayloadWriter() {
}

void QuackChunkPayloadWriter::AppendChunk(DataChunk &chunk) {
	D_ASSERT(chunk.size() > 0);
	serializer->Begin();
	chunk.Serialize(*serializer);
	serializer->End();
	chunk_count++;
}

idx_t QuackChunkPayloadWriter::SizeBytes() const {
	// The reserved header space is not payload, so it does not count toward the cut measure.
	return (stream ? stream->GetPosition() : sealed_size) - QUACK_PAYLOAD_HEADER_BYTES;
}

idx_t QuackChunkPayloadWriter::AllocatedBytes() const {
	return stream ? stream->GetCapacity() : sealed_size;
}

QuackChunkPayloadWriter::SealedPayload QuackChunkPayloadWriter::Seal() {
	D_ASSERT(stream && chunk_count > 0);
	SealedPayload result;
	result.payload_size = stream->GetPosition();
	result.chunk_count = chunk_count;
	sealed_size = result.payload_size;
	result.payload = std::move(stream);
	return result;
}

//===--------------------------------------------------------------------===//
// Chunk blob helpers
//===--------------------------------------------------------------------===//
idx_t QuackPrependHeader(MemoryStream &payload, const QuackMessage &header_message) {
	// Only a buffer that QuackChunkPayloadWriter made has the header space.
	D_ASSERT(payload.GetPosition() >= QUACK_PAYLOAD_HEADER_BYTES);
	data_t scratch[QUACK_PAYLOAD_HEADER_BYTES];
	// The non-owning scratch cannot grow: an oversized header throws instead of corrupting the blob.
	MemoryStream scratch_stream(scratch, QUACK_PAYLOAD_HEADER_BYTES);
	header_message.ToMemoryStream(scratch_stream);
	auto header_size = scratch_stream.GetPosition();
	auto body_start = QUACK_PAYLOAD_HEADER_BYTES - header_size;
	memcpy(payload.GetData() + body_start, scratch, header_size);
	return body_start;
}

vector<unique_ptr<DataChunk>> DecodeQuackChunkBlob(BinaryDeserializer &deserializer, idx_t chunk_count) {
	vector<unique_ptr<DataChunk>> chunks;
	// The count comes from the wire, so cap the reservation. A false count fails on the read.
	chunks.reserve(MinValue<idx_t>(chunk_count, 1024));
	for (idx_t i = 0; i < chunk_count; i++) {
		auto chunk = make_uniq<DataChunk>();
		deserializer.Begin();
		chunk->Deserialize(deserializer);
		deserializer.End();
		chunks.push_back(std::move(chunk));
	}
	return chunks;
}

void SendDataRequestMessage::DecodeBlob(BinaryDeserializer &deserializer) {
	chunks = DecodeQuackChunkBlob(deserializer, chunk_count);
}

void FetchResponseMessage::DecodeBlob(BinaryDeserializer &deserializer) {
	results = DecodeQuackChunkBlob(deserializer, chunk_count);
}

void DataChunkWrapper::Serialize(Serializer &serializer) const {
	serializer.WriteObject(300, "chunk", [&](Serializer &inner) { chunk.Serialize(inner); });
}

unique_ptr<DataChunkWrapper> DataChunkWrapper::Deserialize(Deserializer &deserializer) {
	DataChunk chunk;
	deserializer.ReadObject(300, "chunk", [&](Deserializer &inner) { chunk.Deserialize(inner); });
	return make_uniq<DataChunkWrapper>(chunk);
}
} // namespace duckdb
