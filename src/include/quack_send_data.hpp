//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_send_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "quack_rebalancer_core.hpp"

namespace duckdb {

class QuackTableCatalogEntry;

//! The client INSERT emitter. A fragment accumulates directly as a serialized SEND_DATA payload, so
//! a cut costs no second serialization. TryEmitPrepared stamps the dense index into the sealed
//! payload and gives it to the async pool as one POST. Finish drains the sends, then it closes the
//! stream with FINALIZE(total_batches), so the server can prove the stream arrived complete.
//! It is the counterpart of the server fetch collector (quack_fetch_collector.hpp): the same
//! machinery, the opposite direction on the wire.
unique_ptr<QuackBatchEmitter> MakeQuackSendDataEmitter(ClientContext &context, QuackTableCatalogEntry &table,
                                                       hugeint_t query_uuid, bool ordered, idx_t debug_delay_ms,
                                                       bool debug_duplicate_sends);

} // namespace duckdb
