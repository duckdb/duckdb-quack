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
//! A fragment serialized into a FETCH_RESPONSE payload. It has no dense index yet.
struct QuackPreparedFetchBatch : public QuackPreparedBatch {
	QuackFetchPayload entry;
};

//! The accumulation buffer IS the wire response, so the FETCH handler patches 8 bytes and replies.
//! It serializes nothing on the reply thread.
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

// Stamps the dense index into the sealed payload, then publishes it. A full buffer parks the
// producing task, not the thread.
class QuackFetchBufferEmitter : public QuackBatchEmitter {
public:
	QuackFetchBufferEmitter(shared_ptr<QuackFetchStream> stream_p, idx_t debug_delay_ms_p)
	    : stream(std::move(stream_p)), debug_delay_ms(debug_delay_ms_p) {
	}

	unique_ptr<QuackFragmentBuilder> OpenFragment(ClientContext &context, idx_t size_hint) override {
		return make_uniq<QuackFetchFragmentBuilder>(context, size_hint);
	}

	//! NO_CAPACITY leaves the batch with the caller. A retry stamps the same index again, which is
	//! safe. DROPPED and PUSHED both consume the batch.
	bool TryEmitPrepared(ClientContext &context, idx_t dense_index, unique_ptr<QuackPreparedBatch> &batch,
	                     optional_ptr<const InterruptState> interrupt) override {
		if (debug_delay_ms > 0) {
			// DEBUG SETTING: make the publish order random, to stress head-of-stream admission
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
		// Do NOT finish the buffer here. The claimed statement can sit in the middle of a
		// multi-statement query, and the client must not see the stream end while more statements run.
		// RunFetchQuery closes the stream, and it checks the count against this total.
		stream->announced_total = total_batches;
	}

private:
	shared_ptr<QuackFetchStream> stream;
	idx_t debug_delay_ms;
};

//===--------------------------------------------------------------------===//
// Fetch collector operator
//===--------------------------------------------------------------------===//
// The data leaves through the stream's claim buffer, so the statement's own result stays empty.
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
		// the data left through the claim buffer
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

	//! Ask for batch indices only in PARALLEL_ORDERED mode. A source that supplies none would then
	//! fail an assertion in the executor.
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
	// The stream carries the FIRST statement that returns a result, as core does for a result chain.
	// Every other statement keeps the default collector.
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
