#include "quack_compression.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

// duckdb's vendored zstd, already compiled into core
#include "zstd.h"

namespace duckdb {

void QuackCompressionConfig::ParseSpec(const string &spec) {
	auto lower = StringUtil::Lower(spec);
	if (lower == "none") {
		codec = QuackCodec::NONE;
		return;
	}
	if (lower == "zstd") {
		codec = QuackCodec::ZSTD;
		zstd_level = 3;
		return;
	}
	if (StringUtil::StartsWith(lower, "zstd-")) {
		int64_t level;
		if (TryCast::Operation(string_t(lower.substr(5)), level) && level >= 1 && level <= 22) {
			codec = QuackCodec::ZSTD;
			zstd_level = level;
			return;
		}
	}
	throw InvalidInputException(
	    "Unknown quack_compression '%s', expected 'none', 'zstd' or 'zstd-<level>' with level between 1 and 22", spec);
}

template <class OP>
static QuackCompressionConfig ReadConfig(OP &&try_get_setting) {
	QuackCompressionConfig result;
	Value val;
	if (try_get_setting("quack_compression", val) && !val.IsNull()) {
		result.ParseSpec(StringValue::Get(val));
	}
	if (try_get_setting("quack_compression_min_size", val) && !val.IsNull()) {
		result.min_size = val.GetValue<idx_t>();
	}
	return result;
}

QuackCompressionConfig QuackCompressionConfig::FromContext(ClientContext &context) {
	return ReadConfig([&](const string &name, Value &val) { return context.TryGetCurrentSetting(name, val); });
}

bool QuackCompression::Compress(const QuackCompressionConfig &config, const_data_ptr_t data, idx_t size,
                                MemoryStream &out) {
	if (config.codec != QuackCodec::ZSTD || size < config.min_size || size == 0) {
		return false;
	}

	idx_t bound = duckdb_zstd::ZSTD_compressBound(size);
	auto scratch = Allocator::DefaultAllocator().Allocate(bound);
	auto ret = duckdb_zstd::ZSTD_compress(scratch.get(), bound, data, size, NumericCast<int>(config.zstd_level));
	if (duckdb_zstd::ZSTD_isError(ret)) {
		throw IOException("quack compression failed: %s", duckdb_zstd::ZSTD_getErrorName(ret));
	}
	idx_t compressed_size = ret;

	if (compressed_size + FRAME_HEADER_SIZE >= size) {
		// not worth it, send raw
		return false;
	}

	uint8_t frame_header[FRAME_HEADER_SIZE];
	frame_header[0] = MAGIC0;
	frame_header[1] = MAGIC1;
	frame_header[2] = static_cast<uint8_t>(config.codec);
	uint64_t uncompressed_size = size;
	memcpy(frame_header + 3, &uncompressed_size, sizeof(uint64_t));
	out.WriteData(frame_header, FRAME_HEADER_SIZE);
	out.WriteData(scratch.get(), compressed_size);
	return true;
}

bool QuackCompression::IsCompressedFrame(const_data_ptr_t data, idx_t size) {
	return size > FRAME_HEADER_SIZE && data[0] == MAGIC0 && data[1] == MAGIC1;
}

AllocatedData QuackCompression::Decompress(const_data_ptr_t data, idx_t size, idx_t &uncompressed_size) {
	D_ASSERT(IsCompressedFrame(data, size));
	auto codec = static_cast<QuackCodec>(data[2]);
	if (codec != QuackCodec::ZSTD) {
		throw IOException("quack decompression failed: unknown codec %d", int(data[2]));
	}
	uint64_t declared_size;
	memcpy(&declared_size, data + 3, sizeof(uint64_t));
	if (declared_size == 0 || declared_size > MAX_DECOMPRESSED_SIZE) {
		throw IOException("quack decompression failed: declared size %llu out of bounds", declared_size);
	}
	auto body = data + FRAME_HEADER_SIZE;
	auto body_size = size - FRAME_HEADER_SIZE;

	auto result = Allocator::DefaultAllocator().Allocate(declared_size);
	auto content_size = duckdb_zstd::ZSTD_getFrameContentSize(body, body_size);
	if (content_size != declared_size) {
		throw IOException("quack decompression failed: frame content size does not match declared size");
	}
	auto ret = duckdb_zstd::ZSTD_decompress(result.get(), declared_size, body, body_size);
	if (duckdb_zstd::ZSTD_isError(ret)) {
		throw IOException("quack decompression failed: %s", duckdb_zstd::ZSTD_getErrorName(ret));
	}
	if (ret != declared_size) {
		throw IOException("quack decompression failed: decompressed size does not match declared size");
	}
	uncompressed_size = declared_size;
	return result;
}

} // namespace duckdb
