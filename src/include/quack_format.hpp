//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/result_format.hpp"
#include "duckdb/main/result_unit.hpp"

#include "quack_fetch_collector.hpp"

namespace duckdb {

class ClientContext;

//! One sealed FETCH_RESPONSE payload, as the engine's result buffer holds it.
class QuackUnit : public ResultUnit {
public:
	QuackUnit(QuackFetchPayload entry, idx_t rows, idx_t bytes);

public:
	unique_ptr<ResultUnit> Copy() const override;

public:
	QuackFetchPayload entry;
};

//! Serializes chunks into wire payloads on the worker threads. A payload is cut once its serialized
//! size reaches target_bytes, or at a batch boundary and at a producer's end of input.
class QuackFormat : public ResultFormat {
public:
	using Unit = QuackUnit;
	using GlobalState = ResultFormatGlobalState;
	static constexpr const char *NAME = "quack";

public:
	QuackFormat(idx_t target_bytes, idx_t debug_delay_ms);

public:
	//! Reads quack_target_batch_bytes and quack_debug_emit_delay_ms on the client thread.
	static shared_ptr<QuackFormat> FromSettings(ClientContext &context);

public:
	const char *Name() const override;
	unique_ptr<ResultFormatGlobalState> InitGlobal(const vector<LogicalType> &types, const vector<Identifier> &names,
	                                               const ClientProperties &properties,
	                                               ResultOrdering ordering) override;
	unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) override;
	void Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate, DataChunk &chunk) override;
	unique_ptr<ResultUnit> Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate,
	                              bool flush_partial) override;

private:
	idx_t target_bytes;
	//! DEBUG (quack_debug_emit_delay_ms): max random delay before a payload is handed over.
	idx_t debug_delay_ms;
};

} // namespace duckdb
