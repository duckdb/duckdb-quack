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

//! The client INSERT emitter. It serializes a fragment directly into a SEND_DATA payload, stamps
//! the dense index at release, and POSTs it. It is the mirror of the server fetch collector.
unique_ptr<QuackBatchEmitter> MakeQuackSendDataEmitter(ClientContext &context, QuackTableCatalogEntry &table,
                                                       string stream_id, hugeint_t query_uuid, idx_t debug_delay_ms,
                                                       bool debug_duplicate_sends, idx_t debug_drop_batch);

} // namespace duckdb
