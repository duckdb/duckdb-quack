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
// Sends one serialized payload on an async-pool thread, and reads the answer.
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
			// DEBUG SETTING: make the send order random.
			RandomEngine random;
			ThreadUtil::SleepMs(random.NextRandomInteger(0, NumericCast<uint32_t>(debug_delay_ms)));
		}
		auto &client = client_wrapper->GetClient();
		auto start_time = QuackNowMillis();
		// context=nullptr: this thread must not touch ClientContext.
		// The payload holds its batch index, so a retry sends the same batch again.
		auto response_body = client.PostRaw(nullptr, payload->GetData(), payload_size);
		auto duration_ms = QuackNowMillis() - start_time;

		auto response = QuackClient::DecodeResponse(response_body);

		string error;
		if (response->Type() == MessageType::ERROR_RESPONSE) {
			error = response->Cast<ErrorResponse>().ErrorMessage();
		}
		// The pool thread has no context, so log through the producer's logger.
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
	//! Where the batch-index field starts. The stamp writes here.
	idx_t index_offset = 0;
	//! Taken at encode time. The POST has no ClientContext.
	string connection_id;
	optional_idx client_query_id;
};

//! The buffer is the wire message, so a cut needs no second serialization.
class QuackSendDataFragmentBuilder : public QuackFragmentBuilder {
public:
	QuackSendDataFragmentBuilder(ClientContext &context, QuackTableCatalogEntry &table, const string &stream_id,
	                             idx_t size_hint) {
		auto &quack_catalog = table.catalog.Cast<QuackCatalog>();
		connection_id = quack_catalog.GetConnectionId();
		writer = make_uniq<SendDataPayloadWriter>(context, connection_id, stream_id, size_hint);
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
// Sends one dense batch as one SEND_DATA_REQUEST. The payload is serialized already, so a release
// costs an 8-byte patch. Finish drains the sends, then a chunk-less terminal SEND_DATA carries the
// batch count.
class QuackSendDataEmitter : public QuackBatchEmitter {
public:
	QuackSendDataEmitter(ClientContext &context, QuackTableCatalogEntry &table_p, string stream_id_p,
	                     hugeint_t query_uuid_p, idx_t debug_delay_ms_p, bool debug_duplicate_sends_p,
	                     idx_t debug_drop_batch_p)
	    : context(context.shared_from_this()), table(table_p), stream_id(std::move(stream_id_p)),
	      query_uuid(query_uuid_p), debug_delay_ms(debug_delay_ms_p), debug_duplicate_sends(debug_duplicate_sends_p),
	      debug_drop_batch(debug_drop_batch_p) {
		queue = make_uniq<ManagedAsyncTaskQueue>(context);
	}

	~QuackSendDataEmitter() override {
		// Finish does not run on an error path. Drain, then cancel the server statement, which
		// otherwise waits for a batch until the next statement.
		if (!queue) {
			return;
		}
		try {
			queue->Close();
		} catch (...) { // NOLINT: a destructor must not throw
		}
		try {
			if (auto live = context.lock()) {
				auto &quack_catalog = table.catalog.Cast<QuackCatalog>();
				auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(*live);
				auto cancel = make_uniq<CancelRequestMessage>(quack_catalog.GetConnectionId(), query_uuid);
				client_wrapper->GetClient().Request<SuccessResponse>(*live, std::move(cancel));
			}
		} catch (...) { // NOLINT: best effort
		}
	}

	unique_ptr<QuackFragmentBuilder> OpenFragment(ClientContext &context, idx_t size_hint) override {
		return make_uniq<QuackSendDataFragmentBuilder>(context, table, stream_id, size_hint);
	}

	//! Always true: the queue does not refuse work. ApplyBackpressure holds the producer thread.
	bool TryEmitPrepared(ClientContext &context, idx_t dense_index, unique_ptr<QuackPreparedBatch> &batch,
	                     optional_ptr<const InterruptState> interrupt) override {
		auto prepared = unique_ptr_cast<QuackPreparedBatch, QuackPreparedSendData>(std::move(batch));
		QuackBatchIndexField::Patch(prepared->payload->GetData(), prepared->payload_size, prepared->index_offset,
		                            dense_index);

		// DEBUG SETTING: lose one batch on purpose. The terminal message announces the full count, so
		// the server must fail the statement. It must not insert only what arrived.
		if (debug_drop_batch == dense_index) {
			return true;
		}

		auto &quack_catalog = table.catalog.Cast<QuackCatalog>();
		auto payload_size = prepared->payload_size;

		// DEBUG SETTING: send the same batch again, to imitate a retry. The receiver must drop it.
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

	optional_idx Finish(ClientContext &context, idx_t total_batches) override {
		// Drain every send first. The buffer checks the count when the terminal message arrives.
		queue->Close();
		queue.reset();

		auto &quack_catalog = table.catalog.Cast<QuackCatalog>();
		auto client_wrapper = quack_catalog.GetClientConnection()->GetClient(context);
		auto &client = client_wrapper->GetClient();

		// The terminal message is an ordinary SEND_DATA with no chunks. It closes the stream against the
		// batch count, so a lost batch fails the statement. The server answers only when the statement
		// has ended, so the same message reports a failure and the rows the statement changed.
		auto terminal = make_uniq<SendDataRequestMessage>(quack_catalog.GetConnectionId(), stream_id,
		                                                  vector<unique_ptr<DataChunkWrapper>>());
		terminal->SetTotalBatches(total_batches);
		auto response = client.Request<SendDataResponseMessage>(context, std::move(terminal));
		return response->RowCount();
	}

private:
	weak_ptr<ClientContext> context;
	QuackTableCatalogEntry &table;
	string stream_id;
	hugeint_t query_uuid;
	idx_t debug_delay_ms;
	bool debug_duplicate_sends;
	idx_t debug_drop_batch;
	//! Regular threads register payloads. Async-pool threads POST them.
	unique_ptr<ManagedAsyncTaskQueue> queue;
};

unique_ptr<QuackBatchEmitter> MakeQuackSendDataEmitter(ClientContext &context, QuackTableCatalogEntry &table,
                                                       string stream_id, hugeint_t query_uuid, idx_t debug_delay_ms,
                                                       bool debug_duplicate_sends, idx_t debug_drop_batch) {
	return make_uniq<QuackSendDataEmitter>(context, table, std::move(stream_id), query_uuid, debug_delay_ms,
	                                       debug_duplicate_sends, debug_drop_batch);
}

} // namespace duckdb
