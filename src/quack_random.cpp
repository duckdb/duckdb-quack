#include "quack_random.hpp"

#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

string QuackHexEncode(const_data_ptr_t bytes, idx_t n) {
	string result(n * 2, '\0');
	for (idx_t i = 0; i < n; i++) {
		result[2 * i] = Blob::HEX_TABLE[bytes[i] >> 4];
		result[2 * i + 1] = Blob::HEX_TABLE[bytes[i] & 0x0F];
	}
	return result;
}

string QuackRandomToken(DatabaseInstance &db) {
	auto encryption_util = db.GetEncryptionUtil(false);
	auto metadata = make_uniq<EncryptionStateMetadata>(EncryptionTypes::GCM, QUACK_TOKEN_BYTES,
	                                                   EncryptionTypes::EncryptionVersion::NONE);
	auto rng = encryption_util->CreateEncryptionState(std::move(metadata));

	data_t bytes[QUACK_TOKEN_BYTES];
	rng->GenerateRandomData(bytes, QUACK_TOKEN_BYTES);
	return QuackHexEncode(bytes, QUACK_TOKEN_BYTES);
}

} // namespace duckdb
