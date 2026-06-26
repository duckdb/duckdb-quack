//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

//! How an order-preserving INSERT obtains the source-order stamp it puts on each APPEND.
enum class AppendOrderMode : uint8_t {
	UNORDERED,        //! preserve_insertion_order=false → fast path, no stamp (server applies on arrival).
	PARALLEL_ORDERED, //! parallel thread executors (table/parquet scans); finished batches re-mapped to a dense
	                  //! sequence via the executor's batch index, gated by the min-batch watermark (mirror of
	                  //! core's PhysicalBatchCopyToFile).
	SERIAL_ORDERED    //! single-threaded sink (e.g. range()); the lone producer mints the dense sequence directly.
};

class QuackInsert : public PhysicalOperator {
public:
	//! INSERT INTO
	QuackInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table);
	//! CREATE TABLE AS
	QuackInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
	            unique_ptr<BoundCreateTableInfo> info);

	//! The table to insert into
	optional_ptr<TableCatalogEntry> table;
	//! Table schema, in case of CREATE TABLE AS
	optional_ptr<SchemaCatalogEntry> schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;

	//! How this INSERT stamps its appends for order preservation (set at plan time). PARALLEL_ORDERED and
	//! SERIAL_ORDERED both stamp + let the server reorder (uploads stay async); they differ only in where the
	//! dense stamp comes from.
	AppendOrderMode order_mode = AppendOrderMode::UNORDERED;

protected:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	SinkNextBatchType NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	bool IsSink() const override {
		return true;
	}

	//! Each sink thread buffers and ships its own batches, mirroring how the scan parallelizes FETCH. The
	//! SERIAL_ORDERED mode runs single-threaded so the client-minted sequence equals source order.
	bool ParallelSink() const override {
		return order_mode != AppendOrderMode::SERIAL_ORDERED;
	}

	//! Request a source-order batch index only when we actually consume the executor's (PARALLEL_ORDERED mode),
	//! so the executor's batch-index assertion never fires for sources that don't supply one.
	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return order_mode == AppendOrderMode::PARALLEL_ORDERED ? OperatorPartitionInfo(/*batch_index=*/true)
		                                                       : OperatorPartitionInfo();
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
