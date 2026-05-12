#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "quack_message.hpp"

#include "quack_server.hpp"
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
	if (StringUtil::Equals(value, "APPEND_REQUEST")) {
		return MessageType::APPEND_REQUEST;
	}
	if (StringUtil::Equals(value, "SUCCESS_RESPONSE")) {
		return MessageType::SUCCESS_RESPONSE;
	}
	if (StringUtil::Equals(value, "DISCONNECT_MESSAGE")) {
		return MessageType::DISCONNECT_MESSAGE;
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
	case MessageType::APPEND_REQUEST:
		return "APPEND_REQUEST";
	case MessageType::SUCCESS_RESPONSE:
		return "SUCCESS_RESPONSE";
	case MessageType::DISCONNECT_MESSAGE:
		return "DISCONNECT_MESSAGE";
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
	options.serialization_compatibility = SerializationCompatibility::FromIndex(7);
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
	case MessageType::APPEND_REQUEST:
		return AppendRequestMessage::Deserialize(deserializer);
	case MessageType::SUCCESS_RESPONSE:
		return SuccessResponse::Deserialize(deserializer);
	case MessageType::DISCONNECT_MESSAGE:
		return DisconnectMessage::Deserialize(deserializer);
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

ConnectionRequestMessage::ConnectionRequestMessage(const string &auth_string_p)
    : QuackMessage(TYPE), auth_string(auth_string_p), client_duckdb_version(DuckDB::LibraryVersion()),
      client_platform(DuckDB::Platform()), min_supported_quack_version(QuackServer::QUACK_VERSION),
      max_supported_quack_version(QuackServer::QUACK_VERSION) {
}

ConnectionResponseMessage::ConnectionResponseMessage(string connection_id_p)
    : QuackMessage(TYPE, std::move(connection_id_p)), server_duckdb_version(DuckDB::LibraryVersion()),
      server_platform(DuckDB::Platform()), quack_version(QuackServer::QUACK_VERSION) {
}

unique_ptr<QuackMessage> QuackMessage::FromMemoryStream(MemoryStream &read_stream) {
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);

	// read the header
	auto header = DeserializeHeader(deserializer);
	// read the message
	return DeserializeMessage(deserializer, std::move(header));
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
