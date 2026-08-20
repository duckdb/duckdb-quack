#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "quack_message.hpp"

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
	if (StringUtil::Equals(value, "FINALIZE")) {
		return MessageType::FINALIZE;
	}
	if (StringUtil::Equals(value, "ACKNOWLEDGEMENT")) {
		return MessageType::ACKNOWLEDGEMENT;
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
	case MessageType::FINALIZE:
		return "FINALIZE";
	case MessageType::ACKNOWLEDGEMENT:
		return "ACKNOWLEDGEMENT";
	case MessageType::ERROR_RESPONSE:
		return "ERROR_RESPONSE";

	default:
		throw NotImplementedException(
		    StringUtil::Format("Enum value of type MessageType: '%d' not implemented", value));
	}
}

void QuackMessage::ToMemoryStream(MemoryStream &write_stream) const {
	write_stream.Rewind();
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::FromIndex(StorageVersion::V2_0_0);

	BinarySerializer serializer(write_stream, options);

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
	case MessageType::FINALIZE:
		return FinalizeMessage::Deserialize(deserializer);
	case MessageType::ACKNOWLEDGEMENT:
		return AcknowledgementMessage::Deserialize(deserializer);
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
	return result;
}

ConnectionRequestMessage::ConnectionRequestMessage(const string &auth_string_p, string client_id_p)
    : QuackMessage(TYPE), auth_string(auth_string_p), client_id(std::move(client_id_p)),
      client_duckdb_version(DuckDB::LibraryVersion()), client_platform(DuckDB::Platform()),
      min_supported_quack_version(QUACK_VERSION), max_supported_quack_version(QUACK_VERSION) {
}

ConnectionResponseMessage::ConnectionResponseMessage(string connection_id_p)
    : QuackMessage(TYPE, std::move(connection_id_p)), server_duckdb_version(DuckDB::LibraryVersion()),
      server_platform(DuckDB::Platform()), quack_version(QUACK_VERSION) {
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
//! Opens the serializer hooks, so one message can be written step by step. The binary format is
//! forward-only, so this works only if the field order is exactly the same.
class QuackChunkPayloadWriter::PayloadSerializer : public BinarySerializer {
public:
	PayloadSerializer(WriteStream &stream, SerializationOptions options)
	    : BinarySerializer(stream, std::move(options)) {
	}

	void BeginProperty(field_id_t field_id, const char *tag) {
		OnPropertyBegin(field_id, tag);
	}

	//! The same bytes as a chunk-list element from WriteValue(const DataChunkWrapper *).
	void WriteChunkElement(DataChunk &chunk) {
		OnNullableBegin(true);
		OnObjectBegin();
		WriteObject(300, "chunk", [&](Serializer &object) { chunk.Serialize(object); });
		OnObjectEnd();
		OnNullableEnd();
	}
};

//! A LEB128 padded with zeros to a fixed width. It decodes to the same value as the short form, and
//! a patch can write the count into it later.
static void EncodePaddedChunkCount(uint64_t value, data_ptr_t out) {
	D_ASSERT(value < (1ULL << (7 * QuackChunkPayloadWriter::CHUNK_COUNT_VARINT_WIDTH)));
	for (idx_t i = 0; i < QuackChunkPayloadWriter::CHUNK_COUNT_VARINT_WIDTH; i++) {
		auto group = static_cast<data_t>((value >> (7 * i)) & 0x7F);
		out[i] = i + 1 < QuackChunkPayloadWriter::CHUNK_COUNT_VARINT_WIDTH ? (group | 0x80) : group;
	}
}

QuackChunkPayloadWriter::QuackChunkPayloadWriter(ClientContext &context, idx_t capacity_hint) {
	auto capacity = NextPowerOfTwo(MaxValue<idx_t>(capacity_hint, 65536));
	stream = make_uniq<MemoryStream>(Allocator::DefaultAllocator(), capacity);
	SerializationOptions options;
	options.storage_compatibility = StorageCompatibility::FromIndex(StorageVersion::V2_0_0);
	serializer = make_uniq<PayloadSerializer>(*stream, std::move(options));
}

QuackChunkPayloadWriter::~QuackChunkPayloadWriter() {
}

void QuackChunkPayloadWriter::OpenMessage(const MessageHeader &header) {
	serializer->Begin();
	header.Serialize(*serializer);
	serializer->End();
	serializer->Begin(); // Seal closes this
}

Serializer &QuackChunkPayloadWriter::Body() {
	return *serializer;
}

void QuackChunkPayloadWriter::BeginChunkList(uint16_t field_id, const char *tag) {
	serializer->BeginProperty(field_id, tag);
	chunk_count_offset = stream->GetPosition();
	data_t padded[CHUNK_COUNT_VARINT_WIDTH];
	EncodePaddedChunkCount(0, padded);
	stream->WriteData(padded, CHUNK_COUNT_VARINT_WIDTH);
}

void QuackChunkPayloadWriter::WriteBatchIndexField(uint16_t field_id, const char *tag) {
	// The same bytes as WritePropertyWithDefault<string>(field_id, tag, <8 bytes>): the field header,
	// the length 8 as a one-byte varint, then the payload. Record where the payload starts.
	serializer->BeginProperty(field_id, tag);
	data_t length_byte = 8;
	stream->WriteData(&length_byte, 1);
	index_offset = stream->GetPosition();
	data_t placeholder[8];
	QuackBatchIndexField::Encode(QuackBatchIndexField::PLACEHOLDER, placeholder);
	stream->WriteData(placeholder, 8);
}

void QuackChunkPayloadWriter::AppendChunk(DataChunk &chunk) {
	D_ASSERT(chunk.size() > 0);
	serializer->WriteChunkElement(chunk);
	chunk_count++;
}

idx_t QuackChunkPayloadWriter::SizeBytes() const {
	return stream ? stream->GetPosition() : sealed_size;
}

idx_t QuackChunkPayloadWriter::AllocatedBytes() const {
	return stream ? stream->GetCapacity() : sealed_size;
}

QuackChunkPayloadWriter::SealedPayload QuackChunkPayloadWriter::Seal() {
	D_ASSERT(stream && chunk_count > 0);
	data_t padded[CHUNK_COUNT_VARINT_WIDTH];
	EncodePaddedChunkCount(chunk_count, padded);
	memcpy(stream->GetData() + chunk_count_offset, padded, CHUNK_COUNT_VARINT_WIDTH);

	WriteTail();
	serializer->End();

	SealedPayload result;
	result.payload_size = stream->GetPosition();
	result.index_offset = index_offset;
	sealed_size = result.payload_size;
	result.payload = std::move(stream);
#ifdef DEBUG
	// The generated codec must decode and re-encode these bytes without a change. The chunk count is
	// the one exception: this writer pads it and the generated encoder does not, so compare its value.
	{
		MemoryStream copy(result.payload_size == 0 ? 1 : NextPowerOfTwo(result.payload_size));
		copy.WriteData(result.payload->GetData(), result.payload_size);
		QuackBatchIndexField::Patch(copy.GetData(), result.payload_size, result.index_offset, 1);
		auto message = QuackMessage::FromMemoryStream(copy);
		if (message->Type() != WrittenType()) {
			throw InternalException("Payload writer round-trip: message type mismatch");
		}
		MemoryStream reserialized;
		message->ToMemoryStream(reserialized);

		idx_t minimal_width = 1;
		for (auto value = chunk_count >> 7; value > 0; value >>= 7) {
			minimal_width++;
		}
		auto tail_offset = chunk_count_offset + CHUNK_COUNT_VARINT_WIDTH;
		uint64_t reser_count = 0;
		for (idx_t i = 0; i < minimal_width; i++) {
			reser_count |= static_cast<uint64_t>(reserialized.GetData()[chunk_count_offset + i] & 0x7F) << (7 * i);
		}
		bool matches = reserialized.GetPosition() == result.payload_size - CHUNK_COUNT_VARINT_WIDTH + minimal_width &&
		               memcmp(reserialized.GetData(), copy.GetData(), chunk_count_offset) == 0 &&
		               reser_count == chunk_count &&
		               memcmp(reserialized.GetData() + chunk_count_offset + minimal_width, copy.GetData() + tail_offset,
		                      result.payload_size - tail_offset) == 0;
		if (!matches) {
			throw InternalException("Payload writer round-trip: bytes differ from generated serialization");
		}
	}
#endif
	return result;
}

//===--------------------------------------------------------------------===//
// FetchResponsePayloadWriter
//===--------------------------------------------------------------------===//
FetchResponsePayloadWriter::FetchResponsePayloadWriter(ClientContext &context, idx_t capacity_hint)
    : QuackChunkPayloadWriter(context, capacity_hint) {
	// A response has an empty connection id, as ToMemoryStream writes it.
	OpenMessage(MessageHeader(MessageType::FETCH_RESPONSE, string()));
	BeginChunkList(1, "results");
}

void FetchResponsePayloadWriter::WriteTail() {
	// Only the terminal response sets total_batches, and that response is not serialized here.
	WriteBatchIndexField(3, "batch_index_fixed");
}

// batch_index is a fixed-width last field, so a patch can write it into an encoded payload. The
// generator hooks in quack_message.json call these two methods.
string FetchResponseMessage::EncodeBatchIndexFixed() const {
	string bytes(8, '\0');
	QuackBatchIndexField::Encode(batch_index.IsValid() ? batch_index.GetIndex() : QuackBatchIndexField::INVALID,
	                             reinterpret_cast<data_ptr_t>(&bytes[0]));
	return bytes;
}

void FetchResponseMessage::ApplyBatchIndexFixed(const string &bytes) {
	if (bytes.size() != 8) {
		throw IOException("FetchResponseMessage: malformed batch index field");
	}
	auto value = QuackBatchIndexField::Decode(reinterpret_cast<const_data_ptr_t>(bytes.data()));
	if (value == QuackBatchIndexField::PLACEHOLDER) {
		throw IOException("FetchResponseMessage: batch arrived unstamped");
	}
	if (value != QuackBatchIndexField::INVALID) {
		batch_index = optional_idx(value);
	}
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
