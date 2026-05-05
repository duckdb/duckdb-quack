#include "message.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

using namespace duckdb;

string duckdb::MessageTypeToString(MessageType type) {
	switch (type) {
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
	case MessageType::CATALOG_REQUEST:
		return "CATALOG_REQUEST";
	case MessageType::CATALOG_RESPONSE:
		return "CATALOG_RESPONSE";
	case MessageType::APPEND_REQUEST:
		return "APPEND_REQUEST";
	case MessageType::APPEND_RESPONSE:
		return "APPEND_RESPONSE";
	case MessageType::BATCH_REQUEST:
		return "BATCH_REQUEST";
	case MessageType::BATCH_RESPONSE:
		return "BATCH_RESPONSE";
	case MessageType::ERROR:
		return "ERROR";
	case MessageType::INVALID:
		break;
	}
	return "INVALID";
}

void ProtocolMessage::ToMemoryStream(MemoryStream &write_stream) const {
	write_stream.Rewind();
	SerializationOptions options;
	options.serialization_compatibility = SerializationCompatibility::FromIndex(10);
	BinarySerializer serializer(write_stream, options);

	serializer.Begin();
	Serialize(serializer);
	serializer.End();
}

unique_ptr<ProtocolMessage> ProtocolMessage::FromMemoryStream(MemoryStream &read_stream) {
	read_stream.Rewind();
	BinaryDeserializer deserializer(read_stream);
	return Deserialize(deserializer);
}

void ProtocolMessage::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<uint8_t>(10, "message_type", static_cast<uint8_t>(message_type));
}

unique_ptr<ProtocolMessage> ProtocolMessage::Deserialize(Deserializer &deserializer) {
	auto message_type = static_cast<MessageType>(deserializer.ReadProperty<uint8_t>(10, "message_type"));
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
	case MessageType::CATALOG_REQUEST:
		return CatalogRequestMessage::Deserialize(deserializer);
	case MessageType::CATALOG_RESPONSE:
		return CatalogResponseMessage::Deserialize(deserializer);
	case MessageType::APPEND_REQUEST:
		return AppendRequestMessage::Deserialize(deserializer);
	case MessageType::APPEND_RESPONSE:
		return AppendResponseMessage::Deserialize(deserializer);
	case MessageType::BATCH_REQUEST:
		return BatchRequestMessage::Deserialize(deserializer);
	case MessageType::BATCH_RESPONSE:
		return BatchResponseMessage::Deserialize(deserializer);
	case MessageType::ERROR:
		return ErrorMessage::Deserialize(deserializer);
	default:
		throw SerializationException("Unsupported type for deserialization of Message!");
	}
}

void ConnectionRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(80, "auth_string", auth_string);
}

unique_ptr<ProtocolMessage> ConnectionRequestMessage::Deserialize(Deserializer &deserializer) {
	auto auth_string = deserializer.ReadProperty<string>(80, "auth_string");
	return make_uniq<ConnectionRequestMessage>(auth_string);
}

void ConnectionResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(98, "connection_id", connection_id);
}

unique_ptr<ProtocolMessage> ConnectionResponseMessage::Deserialize(Deserializer &deserializer) {
	auto connection_id = deserializer.ReadProperty<string>(98, "connection_id");
	return make_uniq<ConnectionResponseMessage>(connection_id);
}

void PrepareRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(98, "connection_id", connection_id);
	serializer.WriteProperty<optional_idx>(99, "client_query_id", client_query_id);
	serializer.WriteProperty<string>(200, "sql_query", sql_query);
	serializer.WriteProperty<bool>(201, "immediately_execute", immediately_execute);
}

unique_ptr<ProtocolMessage> PrepareRequestMessage::Deserialize(Deserializer &deserializer) {
	auto connection_id = deserializer.ReadProperty<string>(98, "connection_id");
	auto cqid = deserializer.ReadProperty<optional_idx>(99, "client_query_id");
	auto sql_query = deserializer.ReadProperty<string>(200, "sql_query");
	auto immediately_execute = deserializer.ReadProperty<bool>(201, "immediately_execute");
	auto msg = make_uniq<PrepareRequestMessage>(connection_id, sql_query, immediately_execute);
	msg->SetClientQueryId(cqid);
	return msg;
}

void PrepareResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<vector<LogicalType>>(210, "result_types", result_types);
	serializer.WriteProperty<vector<string>>(211, "result_names", result_names);
	serializer.WriteProperty<bool>(215, "needs_more_fetch", needs_more_fetch);

	serializer.WriteList(216, "results", results.size(), [&](Serializer::List &list, idx_t i) {
		list.WriteObject([&](Serializer &inner) { results[i]->Serialize(inner); });
	});
}

unique_ptr<ProtocolMessage> PrepareResponseMessage::Deserialize(Deserializer &deserializer) {
	auto result_types = deserializer.ReadProperty<vector<LogicalType>>(210, "result_types");
	auto result_names = deserializer.ReadProperty<vector<string>>(211, "result_names");
	auto needs_more_fetch = deserializer.ReadProperty<bool>(215, "needs_more_fetch");
	unique_ptr<ColumnDataCollection> results_p;

	vector<unique_ptr<DataChunk>> results;
	deserializer.ReadList(216, "results", [&](Deserializer::List &list, idx_t i) {
		auto chunk = make_uniq<DataChunk>();
		list.ReadObject([&](Deserializer &inner) { chunk->Deserialize(inner); });
		results.push_back(std::move(chunk));
	});
	return make_uniq<PrepareResponseMessage>(std::move(result_types), std::move(result_names), std::move(results),
	                                         needs_more_fetch);
}

void FetchRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(98, "connection_id", connection_id);
	serializer.WriteProperty<optional_idx>(99, "client_query_id", client_query_id);
}

unique_ptr<ProtocolMessage> FetchRequestMessage::Deserialize(Deserializer &deserializer) {
	auto connection_id = deserializer.ReadProperty<string>(98, "connection_id");
	auto cqid = deserializer.ReadProperty<optional_idx>(99, "client_query_id");

	auto msg = make_uniq<FetchRequestMessage>(connection_id);
	msg->SetClientQueryId(cqid);
	return msg;
}

void FetchResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<optional_idx>(253, "batch_index", batch_index);
	serializer.WriteList(254, "results", results.size(), [&](Serializer::List &list, idx_t i) {
		list.WriteObject([&](Serializer &inner) { results[i]->Serialize(inner); });
	});
}

unique_ptr<ProtocolMessage> FetchResponseMessage::Deserialize(Deserializer &deserializer) {
	auto batch_index = deserializer.ReadProperty<optional_idx>(253, "batch_index");
	vector<unique_ptr<DataChunk>> results;

	deserializer.ReadList(254, "results", [&](Deserializer::List &list, idx_t i) {
		auto chunk = make_uniq<DataChunk>();
		list.ReadObject([&](Deserializer &inner) { chunk->Deserialize(inner); });
		results.push_back(std::move(chunk));
	});

	return make_uniq<FetchResponseMessage>(std::move(results), batch_index);
}

void ErrorMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(260, "error_message", error_message);
}

unique_ptr<ProtocolMessage> ErrorMessage::Deserialize(Deserializer &deserializer) {
	auto error_message = deserializer.ReadProperty<string>(260, "error_message");
	return make_uniq<ErrorMessage>(error_message);
}

void CatalogRequestMessage::Serialize(Serializer &serializer) const {
	D_ASSERT(parse_info);
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(98, "connection_id", connection_id);
	serializer.WriteProperty<optional_idx>(99, "client_query_id", client_query_id);
	// FIXME this is only required because serialization of parse info is borked
	serializer.WriteProperty<ParseInfoType>(97, "info_type", parse_info->info_type);
	parse_info->Serialize(serializer);
}

unique_ptr<ProtocolMessage> CatalogRequestMessage::Deserialize(Deserializer &deserializer) {
	auto connection_id = deserializer.ReadProperty<string>(98, "connection_id");
	auto cqid = deserializer.ReadProperty<optional_idx>(99, "client_query_id");
	auto info_type = deserializer.ReadProperty<ParseInfoType>(97, "info_type");
	unique_ptr<ParseInfo> parse_info;
	switch (info_type) {
	case ParseInfoType::CREATE_INFO:
		parse_info = CreateInfo::Deserialize(deserializer);
		break;
	default:
		parse_info = ParseInfo::Deserialize(deserializer);
		break;
	}
	auto msg = make_uniq<CatalogRequestMessage>(connection_id, std::move(parse_info));
	msg->SetClientQueryId(cqid);
	return msg;
}

void CatalogResponseMessage::Serialize(Serializer &serializer) const {
	D_ASSERT(parse_info);
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<ParseInfoType>(97, "info_type", parse_info->info_type);
	parse_info->Serialize(serializer);
}

// TODO this is duplicated
unique_ptr<ProtocolMessage> CatalogResponseMessage::Deserialize(Deserializer &deserializer) {
	auto info_type = deserializer.ReadProperty<ParseInfoType>(97, "info_type");
	unique_ptr<ParseInfo> parse_info;
	switch (info_type) {
	case ParseInfoType::CREATE_INFO:
		parse_info = CreateInfo::Deserialize(deserializer);
		break;
	default:
		parse_info = ParseInfo::Deserialize(deserializer);
		break;
	}
	return make_uniq<CatalogResponseMessage>(std::move(parse_info));
}

void AppendRequestMessage::Serialize(Serializer &serializer) const {
	D_ASSERT(append_chunk);
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<string>(98, "connection_id", connection_id);
	serializer.WriteProperty<optional_idx>(99, "client_query_id", client_query_id);
	serializer.WriteProperty<string>(270, "schema_name", schema_name);
	serializer.WriteProperty<string>(271, "table_name", table_name);

	serializer.WriteObject(272, "append_chunk",
	                       [&](Serializer &inner_serializer) { append_chunk->Serialize(inner_serializer); });
}

unique_ptr<ProtocolMessage> AppendRequestMessage::Deserialize(Deserializer &deserializer) {
	auto connection_id_p = deserializer.ReadProperty<string>(98, "connection_id");
	auto cqid = deserializer.ReadProperty<optional_idx>(99, "client_query_id");
	auto schema_name_p = deserializer.ReadProperty<string>(270, "schema_name");
	auto table_name_p = deserializer.ReadProperty<string>(271, "table_name");

	auto append_chunk_p = make_uniq<DataChunk>();
	deserializer.ReadObject(272, "append_chunk",
	                        [&](Deserializer &inner_deserializer) { append_chunk_p->Deserialize(inner_deserializer); });

	auto msg = make_uniq<AppendRequestMessage>(connection_id_p, schema_name_p, table_name_p, std::move(append_chunk_p));
	msg->SetClientQueryId(cqid);
	return msg;
}

void AppendResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
}

unique_ptr<ProtocolMessage> AppendResponseMessage::Deserialize(Deserializer &deserializer) {
	return make_uniq<AppendResponseMessage>();
}

void BatchRequestMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteProperty<bool>(50, "chain_connection_id", chain_connection_id);
	serializer.WriteList(51, "messages", messages.size(), [&](Serializer::List &list, idx_t i) {
		list.WriteObject([&](Serializer &inner) { messages[i]->Serialize(inner); });
	});
}

unique_ptr<ProtocolMessage> BatchRequestMessage::Deserialize(Deserializer &deserializer) {
	auto chain = deserializer.ReadProperty<bool>(50, "chain_connection_id");
	vector<unique_ptr<ProtocolMessage>> messages;
	deserializer.ReadList(51, "messages", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &inner) { messages.push_back(ProtocolMessage::Deserialize(inner)); });
	});
	return make_uniq<BatchRequestMessage>(std::move(messages), chain);
}

void BatchResponseMessage::Serialize(Serializer &serializer) const {
	ProtocolMessage::Serialize(serializer);
	serializer.WriteList(50, "responses", responses.size(), [&](Serializer::List &list, idx_t i) {
		list.WriteObject([&](Serializer &inner) { responses[i]->Serialize(inner); });
	});
}

unique_ptr<ProtocolMessage> BatchResponseMessage::Deserialize(Deserializer &deserializer) {
	vector<unique_ptr<ProtocolMessage>> responses;
	deserializer.ReadList(50, "responses", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &inner) { responses.push_back(ProtocolMessage::Deserialize(inner)); });
	});
	return make_uniq<BatchResponseMessage>(std::move(responses));
}
