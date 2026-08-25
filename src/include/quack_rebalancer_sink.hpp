//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_rebalancer_sink.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

#include "quack_rebalancer_core.hpp"

namespace duckdb {

class DatabaseInstance;

//! The defaults for the tuning settings. quack_extension.cpp and every read site use these.
static constexpr idx_t QUACK_TARGET_BATCH_BYTES_DEFAULT = 32ULL * 1024ULL * 1024ULL;
static constexpr idx_t QUACK_REBALANCE_BUFFER_BYTES_DEFAULT = 0;
static constexpr idx_t QUACK_FETCH_PRODUCER_BUFFER_BYTES_DEFAULT = 256ULL * 1024ULL * 1024ULL;
static constexpr idx_t QUACK_PREPARE_INLINE_ROWS_DEFAULT = 24576;

//! How a rebalancing sink keeps the source order in the dense batch stream.
enum class AppendOrderMode : uint8_t {
	UNORDERED,        //! preserve_insertion_order is off: arrival order is the order.
	PARALLEL_ORDERED, //! a parallel source: sparse executor batch indices become dense once they
	                  //! settle below the watermark.
	SERIAL_ORDERED    //! a single-threaded sink: every fragment is settled on arrival.
};

//! Sink state shared by the operators that use a QuackRebalancerCore: the client INSERT sink and
//! the server fetch collector. Both route Sink/NextBatch/Combine/Finalize through the functions below.
class QuackRebalancerGlobalState : public GlobalSinkState {
public:
	QuackRebalancerGlobalState(AppendOrderMode order_mode_p, idx_t target_bytes_p, idx_t minimum_memory_per_thread_p)
	    : order_mode(order_mode_p), target_bytes(target_bytes_p),
	      minimum_memory_per_thread(minimum_memory_per_thread_p), row_count(0) {
	}

	idx_t MaxThreads(idx_t source_max_threads) override;

	AppendOrderMode order_mode;
	idx_t target_bytes;
	//! One fragment is one wire message, so the cut size IS the message size.
	idx_t FragmentGrain() const {
		return target_bytes;
	}
	idx_t minimum_memory_per_thread;
	atomic<idx_t> row_count;
	unique_ptr<QuackRebalancerCore> core;
	//! Weak, to prevent a reference cycle. Set only if GetResult needs the client context.
	weak_ptr<ClientContext> client_context;
};

class QuackRebalancerLocalState : public LocalSinkState {
public:
	unique_ptr<QuackFragmentBuilder> builder;
	//! The size of the last sealed fragment: a reservation hint for the next OpenFragment.
	idx_t size_hint = 0;
	//! PARALLEL_ORDERED: the memory the global manager counts for the current builder.
	idx_t local_memory_usage = 0;
	//! The builder's bytes, and the last chunk's part of them. The cut check reads both.
	idx_t fragment_bytes = 0;
	idx_t last_chunk_bytes = 0;
	idx_t local_count = 0;
	//! PARALLEL_ORDERED: the executor batch in the builder. Invalid before the first Sink/NextBatch.
	optional_idx batch_index;
	//! PARALLEL_ORDERED: over budget, so help to emit queued batches instead of sinking.
	bool processing_tasks = false;
	//! SERIAL_ORDERED / UNORDERED: a stamped batch parked on delivery capacity. The next Sink call,
	//! or Combine, retries the same index and the same payload before it does anything else.
	unique_ptr<QuackEmitTask> pending_emit;
};

//! Build the global state and the core. Reads quack_target_batch_bytes and
//! quack_rebalance_buffer_bytes, applies core's batch-operator memory heuristic, wires the emitter.
unique_ptr<QuackRebalancerGlobalState> MakeQuackRebalancerGlobalState(ClientContext &context,
                                                                      const vector<LogicalType> &types,
                                                                      AppendOrderMode order_mode,
                                                                      unique_ptr<QuackBatchEmitter> emitter);

SinkResultType QuackRebalancerSink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input);
SinkNextBatchType QuackRebalancerNextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input);
SinkCombineResultType QuackRebalancerCombine(ExecutionContext &context, OperatorSinkCombineInput &input);
SinkFinalizeType QuackRebalancerFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         QuackRebalancerGlobalState &gstate);

idx_t QuackGetUBigintSetting(ClientContext &context, const char *name, idx_t default_value);
idx_t QuackGetUBigintSetting(DatabaseInstance &db, const char *name, idx_t default_value);

//! 128 random bits, hex encoded. Server tokens, session ids and client stream ids all use it, so an
//! id cannot be guessed from another one.
string QuackRandomToken(DatabaseInstance &db);

} // namespace duckdb
