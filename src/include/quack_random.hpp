//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_random.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class DatabaseInstance;

//! Bytes in a token. It is the width of a server token and a session id.
static constexpr idx_t QUACK_TOKEN_BYTES = 16; // 128 bits

//! Lower-case hex of `n` bytes.
string QuackHexEncode(const_data_ptr_t bytes, idx_t n);

//! QUACK_TOKEN_BYTES random bytes, hex encoded. Server tokens and session ids use it, so one gives
//! no help to guess the other.
string QuackRandomToken(DatabaseInstance &db);

} // namespace duckdb
