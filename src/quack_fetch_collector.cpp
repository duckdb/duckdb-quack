#include "quack_fetch_collector.hpp"

#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include "quack_message.hpp"
#include "quack_rebalancer_sink.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Fetch-buffer emitter
//===--------------------------------------------------------------------===//
//! A fragment serialized into a FETCH_RESPONSE payload, awaiting its dense index.
struct QuackPreparedFetchBatch : public QuackPreparedBatch {
	QuackFetchPayload entry;
};

//! Streams one fragment straight into a FETCH_RESPONSE payload — the accumulation buffer IS the wire
//! response, so the FETCH handler replies with patched bytes instead of serializing on the reply thread.
class QuackFetchFragmentBuilder : public QuackFragmentBuilder {
public:
	QuackFetchFragmentBuilder(ClientContext &context, idx_t size_hint)
	    : writer(make_uniq<FetchResponsePayloadWriter>(context, size_hint)) {
	}

	void Append(ClientContext &context, DataChunk &chunk) override {
		rows += chunk.size();
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
		auto prepared = make_uniq<QuackPreparedFetchBatch>();
		prepared->entry.payload = std::move(sealed.payload);
		prepared->entry.payload_size = sealed.payload_size;
		prepared->entry.index_offset = sealed.index_offset;
		prepared->entry.rows = rows;
		return std::move(prepared);
	}

private:
	unique_ptr<FetchResponsePayloadWriter> writer;
	QuackChunkStager stager;
	idx_t rows = 0;
};

// Stamps each dense batch's index into its sealed payload and publishes it into the stream's claim
// buffer. A full buffer parks the producing task instead of the thread — backpressure into the executor.
class QuackFetchBufferEmitter : public QuackBatchEmitter {
public:
	QuackFetchBufferEmitter(shared_ptr<QuackFetchStream> stream_p, idx_t debug_delay_ms_p)
	    : stream(std::move(stream_p)), debug_delay_ms(debug_delay_ms_p) {
	}

	unique_ptr<QuackFragmentBuilder> OpenFragment(ClientContext &context, idx_t size_hint) override {
		return make_uniq<QuackFetchFragmentBuilder>(context, size_hint);
	}

	//! NO_CAPACITY leaves the batch with the caller for a retry (the patch below is idempotent — a
	//! retry re-stamps the same index); DROPPED/PUSHED both consume it.
	bool TryEmitPrepared(ClientContext &context, idx_t dense_index, unique_ptr<QuackPreparedBatch> &batch,
	                     optional_ptr<const InterruptState> interrupt) override {
		if (debug_delay_ms > 0) {
			// DEBUG SETTING: randomize publish order to stress head-of-stream admission
			RandomEngine random;
			ThreadUtil::SleepMs(random.NextRandomInteger(0, NumericCast<uint32_t>(debug_delay_ms)));
		}
		auto &entry = static_cast<QuackPreparedFetchBatch &>(*batch).entry;
		QuackBatchIndexField::Patch(entry.payload->GetData(), entry.payload_size, entry.index_offset, dense_index);
		auto bytes = entry.payload_size;
		if (stream->buffer.TryPushBatch(dense_index, entry, bytes, interrupt) == QuackPushStatus::NO_CAPACITY) {
			return false;
		}
		batch.reset();
		return true;
	}

	void Finish(ClientContext &context, idx_t total_batches) override {
		// Deliberately NOT finishing the buffer: the claimed statement can sit mid-way through a
		// multi-statement query, and the client must not see the stream end while later statements run.
		// RunFetchQuery closes the stream once the WHOLE query returned, validated against this total.
		stream->announced_total = total_batches;
	}

private:
	shared_ptr<QuackFetchStream> stream;
	idx_t debug_delay_ms;
};

//===--------------------------------------------------------------------===//
// Fetch collector operator
//===--------------------------------------------------------------------===//
// Result collector producing the client-facing dense batch stream via the shared rebalancer sink;
// the statement's own result is empty — the data's real exit is the stream's claim buffer.
class QuackFetchCollector : public PhysicalResultCollector {
public:
	QuackFetchCollector(PhysicalPlan &physical_plan, PreparedStatementData &data, shared_ptr<QuackFetchStream> stream_p,
	                    AppendOrderMode order_mode_p)
	    : PhysicalResultCollector(physical_plan, data), stream(std::move(stream_p)), order_mode(order_mode_p) {
	}

	shared_ptr<QuackFetchStream> stream;
	AppendOrderMode order_mode;

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		auto debug_delay_ms = QuackGetUBigintSetting(context, "quack_debug_emit_delay_ms", 0);
		auto emitter = make_uniq<QuackFetchBufferEmitter>(stream, debug_delay_ms);
		auto state = MakeQuackRebalancerGlobalState(context, types, order_mode, std::move(emitter));
		state->client_context = context.shared_from_this();
		return std::move(state);
	}

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override {
		return make_uniq<QuackRebalancerLocalState>();
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		return QuackRebalancerSink(context, chunk, input);
	}

	SinkNextBatchType NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const override {
		return QuackRebalancerNextBatch(context, input);
	}

	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override {
		return QuackRebalancerCombine(context, input);
	}

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		return QuackRebalancerFinalize(pipeline, event, context, input.global_state.Cast<QuackRebalancerGlobalState>());
	}

	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) const override {
		// the data left through the stream's claim buffer; the statement's own result is empty
		auto &gstate = state.Cast<QuackRebalancerGlobalState>();
		auto context = gstate.client_context.lock();
		if (!context) {
			throw InternalException("client context expired in QuackFetchCollector::GetResult");
		}
		auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
		return make_uniq<MaterializedQueryResult>(statement_type, properties, IdentifiersToStrings(names),
		                                          std::move(collection), context->GetClientProperties());
	}

	bool ParallelSink() const override {
		return order_mode != AppendOrderMode::SERIAL_ORDERED;
	}

	//! Request executor batch indices only for PARALLEL_ORDERED, so the executor's assertion never fires
	//! for sources that don't supply a batch index.
	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return order_mode == AppendOrderMode::PARALLEL_ORDERED ? OperatorPartitionInfo(/*batch_index=*/true)
		                                                       : OperatorPartitionInfo();
	}

	string GetName() const override {
		return "QUACK_FETCH_COLLECTOR";
	}
};

unique_ptr<PhysicalOperator> MakeQuackFetchCollector(ClientContext &context, PreparedStatementData &data,
                                                     shared_ptr<QuackFetchStream> stream) {
	// The stream carries the FIRST result-returning statement of the (possibly multi-statement) query,
	// matching how core picks the head of a result chain; everything else keeps the default collector.
	if (data.properties.return_type != StatementReturnType::QUERY_RESULT || stream->Bound()) {
		return PhysicalResultCollector::GetResultCollector(context, data);
	}
	auto &physical_plan = *data.physical_plan;
	auto &root = physical_plan.Root();

	AppendOrderMode order_mode;
	if (!PhysicalPlanGenerator::PreserveInsertionOrder(context, root)) {
		order_mode = AppendOrderMode::UNORDERED;
	} else if (PhysicalPlanGenerator::UseBatchIndex(context, root)) {
		order_mode = AppendOrderMode::PARALLEL_ORDERED;
	} else {
		order_mode = AppendOrderMode::SERIAL_ORDERED;
	}

	vector<string> result_names;
	for (auto &name : data.names) {
		result_names.push_back(name.GetIdentifierName());
	}
	stream->SignalBound(data.types, std::move(result_names));
	return make_uniq<QuackFetchCollector>(physical_plan, data, std::move(stream), order_mode);
}

} // namespace duckdb
