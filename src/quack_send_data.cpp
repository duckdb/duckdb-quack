#include "quack_send_data.hpp"

#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/serializer/async_task_queue.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/main/client_context.hpp"

#include "quack_client.hpp"
#include "quack_log.hpp"
#include "quack_message.hpp"
#include "storage/quack_catalog.hpp"
#include "storage/quack_table.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Async send task
//===--------------------------------------------------------------------===//
// Does the SEND_DATA POST on an async-pool thread. The producing thread serialized the payload
// already, so this task only sends the bytes and reads the answer.
class QuackSendDataTask : public AsyncTask {
public:
	QuackSendDataTask(unique_ptr<QuackClientWrapper> client_wrapper_p, unique_ptr<MemoryStream> payload_p,
	                  idx_t payload_size_p, string connection_id_p, optional_idx client_query_id_p,
	                  shared_ptr<Logger> logger_p, idx_t debug_delay_ms_p)
	    : client_wrapper(std::move(client_wrapper_p)), payload(std::move(payload_p)), payload_size(payload_size_p),
	      connection_id(std::move(connection_id_p)), client_query_id(client_query_id_p), logger(std::move(logger_p)),
	      debug_delay_ms(debug_delay_ms_p) {
	}

	void Execute() override {
		if (debug_delay_ms > 0) {
			// DEBUG SETTING: make the send order random, to stress out-of-order arrival on the server
			RandomEngine random;
			ThreadUtil::SleepMs(random.NextRandomInteger(0, NumericCast<uint32_t>(debug_delay_ms)));
		}
		auto &client = client_wrapper->GetClient();
		auto start_time = QuackNowMillis();
		// context=nullptr: this runs off the execution thread, so it must not touch ClientContext.
		// The payload names its batch index, so an HTTP retry sends the SAME batch again and the
		// server's claim buffer drops the duplicate.
		auto response_body = client.PostRaw(nullptr, payload->GetData(), payload_size);
		auto duration_ms = QuackNowMillis() - start_time;

		auto response = QuackClient::DecodeResponse(response_body);

		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		// Log through the context logger taken on the producer thread, so an async SEND_DATA entry
		// carries the query's connection, transaction and query id. The pool thread has no context.
		client.LogRequest(Logger::Get(logger), MessageType::SEND_DATA_REQUEST, connection_id, client_query_id, string(),
		                  duration_ms, response->Type(), error);

		if (response->Type() == MessageType::ERROR_RESPONSE) {
			response->Cast<ErrorResponse>().Error().Throw();
		}
		if (response->Type() != SendDataResponseMessage::TYPE) {
			throw IOException("Expected send_data_response, got %s instead", MessageTypeToString(response->Type()));
		}
	}

private:
	unique_ptr<QuackClientWrapper> client_wrapper;
	unique_ptr<MemoryStream> payload;
	idx_t payload_size;
	string connection_id;
	optional_idx client_query_id;
	shared_ptr<Logger> logger;
	idx_t debug_delay_ms;
};

//===--------------------------------------------------------------------===//
// Fragment builder
//===--------------------------------------------------------------------===//
//! A sealed SEND_DATA payload that has no dense index yet.
struct QuackPreparedSendData : public QuackPreparedBatch {
	unique_ptr<MemoryStream> payload;
	idx_t payload_size = 0;
	//! Where the fixed-width batch-index field starts: the patch target.
	idx_t index_offset = 0;
	//! Taken at encode time, for the async request log. The POST runs with no ClientContext.
	string connection_id;
	optional_idx client_query_id;
};

//! The accumulation buffer IS the wire message, so a cut costs no second serialization and no
//! columnar copy. The stager merges small chunks, to keep the per-chunk framing off the wire.
class QuackSendDataFragmentBuilder : public QuackFragmentBuilder {
public:
	QuackSendDataFragmentBuilder(ClientContext &context, QuackTableCatalogEntry &table, hugeint_t query_uuid,
	                             bool ordered, idx_t size_hint) {
		auto &quack_catalog = table.catalog.Cast<QuackCatalog>();
		connection_id = quack_catalog.GetConnectionId();
		writer = make_uniq<SendDataPayloadWriter>(context, connection_id, table.schema.name.GetIdentifierName(),
		                                          table.name.GetIdentifierName(), query_uuid, ordered, size_hint);
	}

	void Append(ClientContext &context, DataChunk &chunk) override {
		stager.Append(chunk, [&](DataChunk &full) { writer->AppendChunk(full); });
	}

	idx_t SizeBytes() const override {
		return writer->SizeBytes();
	}

	idx_t AllocatedBytes() const override {
		return writer->AllocatedBytes();
	}

	unique_ptr<QuackPreparedBatch> Seal(ClientContext &context) override {
		stager.Flush([&](DataChunk &full) { writer->AppendChunk(full); });
		auto sealed = writer->Seal();
		auto prepared = make_uniq<QuackPreparedSendData>();
		prepared->payload = std::move(sealed.payload);
		prepared->payload_size = sealed.payload_size;
		prepared->index_offset = sealed.index_offset;
		prepared->connection_id = connection_id;
		prepared->client_query_id = writer->ClientQueryId();
		return std::move(prepared);
	}

private:
	unique_ptr<SendDataPayloadWriter> writer;
	QuackChunkStager stager;
	string connection_id;
};

//===--------------------------------------------------------------------===//
// Send-data emitter
//===--------------------------------------------------------------------===//
// Sends each dense batch as one SEND_DATA_REQUEST. The payload was serialized as the chunks
// arrived, with a placeholder index, so a release costs an 8-byte patch. Finish drains the queue,
// then FINALIZE carries the batch count, and the server proves the stream is complete.
class QuackSendDataEmitter : public QuackBatchEmitter {
public:
	QuackSendDataEmitter(ClientContext &context, QuackTableCatalogEntry &table_p, hugeint_t query_uuid_p,
	                     bool ordered_p, idx_t debug_delay_ms_p, bool debug_duplicate_sends_p)
	    : table(table_p), query_uuid(query_uuid_p), ordered(ordered_p), debug_delay_ms(debug_delay_ms_p),
	      debug_duplicate_sends(debug_duplicate_sends_p) {
		queue = make_uniq<ManagedAsyncTaskQueue>(context);
	}

	~QuackSendDataEmitter() override {
		// Finish never runs on an error path. Drain quietly, so a POST cannot outlive the operator.
		if (queue) {
			try {
				queue->Close();
			} catch (...) { // NOLINT: a destructor must not throw
			}
		}
	}

	unique_ptr<QuackFragmentBuilder> OpenFragment(ClientContext &context, idx_t size_hint) override {
		return make_uniq<QuackSendDataFragmentBuilder>(context, table, query_uuid, ordered, size_hint);
	}

	//! Always true: the async queue never refuses work. It holds the producer thread instead, in
	//! ApplyBackpressure. The fetch side parks the task on buffer capacity; see the PR notes.
	bool TryEmitPrepared(ClientContext &context, idx_t dense_index, unique_ptr<QuackPreparedBatch> &batch,
	                     optional_ptr<const InterruptState> interrupt) override {
		auto prepared = unique_ptr_cast<QuackPreparedBatch, QuackPreparedSendData>(std::move(batch));
		QuackBatchIndexField::Patch(prepared->payload->GetData(), prepared->payload_size, prepared->index_offset,
		                            dense_index);

		auto &quack_catalog = table.catalog.Cast<QuackCatalog>();
		auto payload_size = prepared->payload_size;

		// DEBUG SETTING: send the same stamped batch a second time, to imitate a transport retry.
		// The receiver keys on the dense index, so the duplicate must change nothing.
		unique_ptr<MemoryStream> duplicate;
		if (debug_duplicate_sends) {
			duplicate = make_uniq<MemoryStream>(Allocator::DefaultAllocator(), NextPowerOfTwo(payload_size));
			duplicate->WriteData(prepared->payload->GetData(), payload_size);
		}

		auto connection_id = prepared->connection_id;
		auto client_query_id = prepared->client_query_id;
		auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
		queue->Register(make_uniq<QuackSendDataTask>(std::move(client_wrapper), std::move(prepared->payload),
		                                             payload_size, connection_id, client_query_id, context.logger,
		                                             debug_delay_ms),
		                payload_size);
		if (duplicate) {
			auto retry_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
			queue->Register(make_uniq<QuackSendDataTask>(std::move(retry_wrapper), std::move(duplicate), payload_size,
			                                             std::move(connection_id), client_query_id, context.logger,
			                                             debug_delay_ms),
			                payload_size);
		}
		queue->ApplyBackpressure();
		return true;
	}

	void Finish(ClientContext &context, idx_t total_batches) override {
		// Drain every send before FINALIZE. A push lands on the server before its answer, so the
		// server has all the batches before the count arrives.
		queue->Close();
		queue.reset();

		auto &quack_catalog = table.catalog.Cast<QuackCatalog>();
		auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
		auto &client = client_wrapper->GetClient();
		auto finalize_msg = make_uniq<FinalizeMessage>(quack_catalog.GetConnectionId(), query_uuid);
		finalize_msg->SetTotalBatches(total_batches);
		client.Request<SuccessResponse>(context, std::move(finalize_msg));
	}

private:
	QuackTableCatalogEntry &table;
	hugeint_t query_uuid;
	bool ordered;
	idx_t debug_delay_ms;
	bool debug_duplicate_sends;
	//! The shared upload queue: regular threads register payloads, async-pool threads POST them.
	unique_ptr<ManagedAsyncTaskQueue> queue;
};

unique_ptr<QuackBatchEmitter> MakeQuackSendDataEmitter(ClientContext &context, QuackTableCatalogEntry &table,
                                                       hugeint_t query_uuid, bool ordered, idx_t debug_delay_ms,
                                                       bool debug_duplicate_sends) {
	return make_uniq<QuackSendDataEmitter>(context, table, query_uuid, ordered, debug_delay_ms, debug_duplicate_sends);
}

} // namespace duckdb
