#include "quack_format.hpp"

#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/main/client_context.hpp"

#include "quack_message.hpp"
#include "quack_rebalancer_core.hpp"
#include "quack_rebalancer_sink.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// QuackUnit
//===--------------------------------------------------------------------===//
QuackUnit::QuackUnit(QuackFetchPayload entry_p, idx_t rows, idx_t bytes)
    : ResultUnit(rows, bytes), entry(std::move(entry_p)) {
}

unique_ptr<ResultUnit> QuackUnit::Copy() const {
	QuackFetchPayload copy;
	copy.payload = make_uniq<MemoryStream>(Allocator::DefaultAllocator(), entry.payload->GetCapacity());
	memcpy(copy.payload->GetData(), entry.payload->GetData(), entry.payload_size);
	copy.payload->SetPosition(entry.payload_size);
	copy.payload_size = entry.payload_size;
	copy.chunk_count = entry.chunk_count;
	copy.rows = entry.rows;
	return make_uniq<QuackUnit>(std::move(copy), row_count, byte_size);
}

//===--------------------------------------------------------------------===//
// QuackFormat
//===--------------------------------------------------------------------===//
namespace {

class QuackFormatLocalState : public ResultFormatLocalState {
public:
	//! The payload under construction. Created at the first row that goes into it.
	unique_ptr<QuackChunkPayloadWriter> writer;
	QuackChunkStager stager;
	idx_t rows = 0;
	//! The size of the last sealed payload: a reservation hint for the next writer.
	idx_t size_hint = 0;
};

} // namespace

QuackFormat::QuackFormat(idx_t target_bytes_p, idx_t debug_delay_ms_p)
    : target_bytes(MaxValue<idx_t>(target_bytes_p, 1)), debug_delay_ms(debug_delay_ms_p) {
}

shared_ptr<QuackFormat> QuackFormat::FromSettings(ClientContext &context) {
	auto target_bytes = QuackGetUBigintSetting(context, "quack_target_batch_bytes", QUACK_TARGET_BATCH_BYTES_DEFAULT);
	auto debug_delay_ms = QuackGetUBigintSetting(context, "quack_debug_emit_delay_ms", 0);
	return make_shared_ptr<QuackFormat>(target_bytes, debug_delay_ms);
}

const char *QuackFormat::Name() const {
	return NAME;
}

unique_ptr<ResultFormatGlobalState> QuackFormat::InitGlobal(const vector<LogicalType> &types,
                                                            const vector<Identifier> &names,
                                                            const ClientProperties &properties,
                                                            ResultOrdering ordering) {
	return make_uniq<ResultFormatGlobalState>();
}

unique_ptr<ResultFormatLocalState> QuackFormat::InitLocal(ResultFormatGlobalState &gstate) {
	return make_uniq<QuackFormatLocalState>();
}

void QuackFormat::Append(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	auto &lstate = lstate_p.Cast<QuackFormatLocalState>();
	if (!lstate.writer) {
		lstate.writer = make_uniq<QuackChunkPayloadWriter>(lstate.size_hint);
	}
	lstate.rows += chunk.size();
	lstate.stager.Append(chunk, [&](DataChunk &full) { lstate.writer->AppendChunk(full); });
}

unique_ptr<ResultUnit> QuackFormat::Finish(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p,
                                           bool flush_partial) {
	auto &lstate = lstate_p.Cast<QuackFormatLocalState>();
	if (!lstate.writer) {
		return nullptr;
	}
	if (!flush_partial && lstate.writer->SizeBytes() < target_bytes) {
		return nullptr;
	}
	lstate.stager.Flush([&](DataChunk &full) { lstate.writer->AppendChunk(full); });
	auto sealed = lstate.writer->Seal();
	lstate.size_hint = sealed.payload_size;
	lstate.writer.reset();

	QuackFetchPayload entry;
	entry.payload = std::move(sealed.payload);
	entry.payload_size = sealed.payload_size;
	entry.chunk_count = sealed.chunk_count;
	entry.rows = lstate.rows;
	lstate.rows = 0;
	auto rows = entry.rows;
	auto bytes = entry.payload_size;

	if (debug_delay_ms > 0) {
		// DEBUG SETTING: stagger the hand-over of finished payloads across the producers
		RandomEngine random;
		ThreadUtil::SleepMs(random.NextRandomInteger(0, NumericCast<uint32_t>(debug_delay_ms)));
	}
	return make_uniq<QuackUnit>(std::move(entry), rows, bytes);
}

} // namespace duckdb
