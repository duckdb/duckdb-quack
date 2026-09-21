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
#include "duckdb/main/retained_result_collection.hpp"

#include "quack_fetch_collector.hpp"

namespace duckdb {

class ClientContext;

//! One sealed FETCH_RESPONSE payload, as the engine's result buffer holds it.
class QuackUnit : public ResultUnit {
public:
	QuackUnit(unique_ptr<QuackFetchPayload> entry, idx_t rows, idx_t bytes);

public:
	unique_ptr<QuackFetchPayload> entry;
};

//! Serializes chunks into wire payloads on the worker threads. A payload is cut once its serialized
//! size reaches target_bytes, or at a batch boundary and at a producer's end of input.
class QuackFormat : public ResultFormatBase<QuackFormat> {
public:
	using T = QuackFetchPayload;
	using GlobalState = ResultFormatGlobalState;
	static constexpr const char *NAME = "quack";

public:
	QuackFormat(idx_t target_bytes, idx_t debug_delay_ms);

public:
	//! Reads quack_target_batch_bytes and quack_debug_emit_delay_ms on the client thread.
	static shared_ptr<QuackFormat> FromSettings(ClientContext &context);

public:
	const char *Name() const override;
	unique_ptr<ResultFormatGlobalState> InitGlobal(const ResultFormatContext &context) override;
	unique_ptr<ResultFormatLocalState> InitLocal(ResultFormatGlobalState &gstate) override;
	void AppendToUnit(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate, DataChunk &chunk) override;
	bool IsUnitFinished(ResultFormatLocalState &lstate) override;
	unique_ptr<ResultUnit> FinishUnit(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate) override;

public:
	static unique_ptr<QuackFetchPayload> UnpackUnit(unique_ptr<ResultUnit> unit);

private:
	idx_t target_bytes;
	//! DEBUG (quack_debug_emit_delay_ms): max random delay before a payload is handed over.
	idx_t debug_delay_ms;
};

} // namespace duckdb
