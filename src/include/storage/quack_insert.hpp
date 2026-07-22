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

//! How an INSERT preserves source order when uploading rows to the server.
enum class AppendOrderMode : uint8_t {
	UNORDERED,        //! preserve_insertion_order=false → fast path, no stamp.
	PARALLEL_ORDERED, //! parallel thread executors (table/parquet scans); each thread stamps chunks with
	                  //! (executor batch_index, sequence_index, is_last_in_batch); server reorders.
	SERIAL_ORDERED    //! single-threaded sink (e.g. range()); the lone producer mints a new batch per flush.
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

	//! Ordering strategy, set at plan time.
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

	bool ParallelSink() const override {
		return order_mode != AppendOrderMode::SERIAL_ORDERED;
	}

	//! Request executor batch indices only for PARALLEL_ORDERED, so the executor's assertion never fires for
	//! sources that don't supply a batch index.
	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return order_mode == AppendOrderMode::PARALLEL_ORDERED ? OperatorPartitionInfo(/*batch_index=*/true)
		                                                       : OperatorPartitionInfo();
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
