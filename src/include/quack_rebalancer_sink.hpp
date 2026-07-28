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

//! Single source of truth for the tuning-setting defaults, shared by the registrations in
//! quack_extension.cpp and every read-site fallback.
static constexpr idx_t QUACK_TARGET_BATCH_BYTES_DEFAULT = 32ULL * 1024ULL * 1024ULL;
static constexpr idx_t QUACK_REBALANCE_BUFFER_BYTES_DEFAULT = 0;
static constexpr idx_t QUACK_FETCH_PRODUCER_BUFFER_BYTES_DEFAULT = 256ULL * 1024ULL * 1024ULL;
static constexpr idx_t QUACK_PREPARE_INLINE_ROWS_DEFAULT = 24576;

//! How a rebalancing sink preserves source order in the dense batch stream it produces.
enum class AppendOrderMode : uint8_t {
	UNORDERED,        //! preserve_insertion_order=false → arrival order is the order.
	PARALLEL_ORDERED, //! parallel thread executors (table/parquet scans); sparse executor batch indices
	                  //! are stamped densely once settled below the watermark.
	SERIAL_ORDERED    //! single-threaded sink (e.g. range()); everything is trivially settled.
};

//! Shared sink-side state for operators built around a QuackRebalancerCore (client INSERT sink,
//! server fetch collector); they route Sink/NextBatch/Combine/Finalize through the functions below.
class QuackRebalancerGlobalState : public GlobalSinkState {
public:
	QuackRebalancerGlobalState(AppendOrderMode order_mode_p, idx_t target_bytes_p, idx_t minimum_memory_per_thread_p)
	    : order_mode(order_mode_p), target_bytes(target_bytes_p),
	      minimum_memory_per_thread(minimum_memory_per_thread_p), row_count(0) {
	}

	idx_t MaxThreads(idx_t source_max_threads) override;

	AppendOrderMode order_mode;
	idx_t target_bytes;
	//! One fragment = one wire message: the cut size IS the message size.
	idx_t FragmentGrain() const {
		return target_bytes;
	}
	idx_t minimum_memory_per_thread;
	atomic<idx_t> row_count;
	unique_ptr<QuackRebalancerCore> core;
	//! Weak to avoid a cyclical reference; set by operators whose GetResult needs the client context.
	weak_ptr<ClientContext> client_context;
};

class QuackRebalancerLocalState : public LocalSinkState {
public:
	//! The fragment this thread is currently building (opened lazily on the first chunk).
	unique_ptr<QuackFragmentBuilder> builder;
	//! Size of the last sealed fragment — pre-reservation hint for the next OpenFragment.
	idx_t size_hint = 0;
	//! Memory accounted to the global manager for the current builder (PARALLEL_ORDERED only).
	idx_t local_memory_usage = 0;
	//! Buffered bytes in the current builder and the last chunk's contribution (pre-append cut check).
	idx_t fragment_bytes = 0;
	idx_t last_chunk_bytes = 0;
	idx_t local_count = 0;
	//! PARALLEL_ORDERED: executor batch currently being buffered (invalid before first Sink/NextBatch).
	optional_idx batch_index;
	//! PARALLEL_ORDERED: over budget — help emit queued batches instead of sinking.
	bool processing_tasks = false;
};

//! Build the shared global state + core for a rebalancing sink: reads quack_target_batch_bytes /
//! quack_rebalance_buffer_bytes, applies the core batch-operator memory heuristic, wires the emitter.
unique_ptr<QuackRebalancerGlobalState> MakeQuackRebalancerGlobalState(ClientContext &context,
                                                                      const vector<LogicalType> &types,
                                                                      AppendOrderMode order_mode,
                                                                      unique_ptr<QuackBatchEmitter> emitter);

SinkResultType QuackRebalancerSink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input);
SinkNextBatchType QuackRebalancerNextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input);
SinkCombineResultType QuackRebalancerCombine(ExecutionContext &context, OperatorSinkCombineInput &input);
SinkFinalizeType QuackRebalancerFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         QuackRebalancerGlobalState &gstate);

//! Read an unsigned setting with a fallback default.
idx_t QuackGetUBigintSetting(ClientContext &context, const char *name, idx_t default_value);
idx_t QuackGetUBigintSetting(DatabaseInstance &db, const char *name, idx_t default_value);

} // namespace duckdb
