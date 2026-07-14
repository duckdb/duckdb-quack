#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

enum class QuackCodec : uint8_t { NONE = 0, ZSTD = 2 };

//! Sender-side settings; receivers auto-detect from the frame header.
struct QuackCompressionConfig {
	QuackCodec codec = QuackCodec::NONE;
	int64_t zstd_level = 3;
	idx_t min_size = 4096;

	static QuackCompressionConfig FromContext(ClientContext &context);
	//! Parse 'none', 'zstd' or 'zstd-<1..22>'; throws on anything else.
	void ParseSpec(const string &spec);
};

//! Frame for compressed HTTP bodies: magic (2) + codec (1) + uncompressed size (8) + codec stream.
//! Raw bodies always have 0x00 as their second byte (BinarySerializer field id), so no collision.
struct QuackCompression {
	static constexpr uint8_t MAGIC0 = 0xDB;
	static constexpr uint8_t MAGIC1 = 0xC2;
	static constexpr idx_t FRAME_HEADER_SIZE = 11;
	static constexpr idx_t MAX_DECOMPRESSED_SIZE = idx_t(1) << 32;

	//! Returns false (out untouched) when codec is NONE, size < min_size, or compression doesn't shrink.
	static bool Compress(const QuackCompressionConfig &config, const_data_ptr_t data, idx_t size, MemoryStream &out);
	static bool IsCompressedFrame(const_data_ptr_t data, idx_t size);
	//! Throws IOException on unknown codec, corrupt frame or cap violation.
	static AllocatedData Decompress(const_data_ptr_t data, idx_t size, idx_t &uncompressed_size);
};

} // namespace duckdb
