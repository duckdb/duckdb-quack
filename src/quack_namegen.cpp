#include "quack_namegen.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <cstdio>
#include <random>

namespace duckdb {

namespace {

constexpr const char *ADJECTIVES[] = {
    "brave",   "bold",   "swift",  "sleek",  "gentle", "jolly",  "merry",  "plucky",
    "scrappy", "hardy",  "sturdy", "witty",  "clever", "quick",  "sharp",  "eager",
    "glossy",  "downy",  "dapper", "jaunty", "cheery", "mellow", "snug",   "crisp",
    "golden",  "silver", "copper", "sunny",  "sleepy", "dreamy", "zesty",  "bright"};

constexpr const char *DUCKS[] = {
    "mallard",   "pintail",  "wigeon",   "teal",      "shoveler",  "gadwall",
    "canvasback","scaup",    "goldeneye","bufflehead","merganser", "eider",
    "mandarin",  "muscovy",  "pochard",  "smew",      "garganey",  "ringneck",
    "redhead",   "harlequin","shelduck", "whistler",  "steamer",   "torrent",
    "drake",     "rouen",    "cayuga",   "aylesbury", "nivis",     "eatoni",
    "andium",    "variegata"};

constexpr size_t N_ADJ = sizeof(ADJECTIVES) / sizeof(ADJECTIVES[0]);
constexpr size_t N_DUCK = sizeof(DUCKS) / sizeof(DUCKS[0]);

} // namespace

string GenerateWhoamiName() {
	std::random_device rd;
	const char *adj = ADJECTIVES[rd() % N_ADJ];
	const char *duck = DUCKS[rd() % N_DUCK];
	char suffix[3];
	snprintf(suffix, sizeof(suffix), "%02x", static_cast<unsigned>(rd() & 0xFFu));
	return string(adj) + "-" + duck + "-" + suffix;
}

static void WhoamiRandomNameFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto out = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		out[i] = StringVector::AddString(result, GenerateWhoamiName());
	}
}

ScalarFunction GetWhoamiRandomNameFunction() {
	ScalarFunction fn("whoami_random_name", {}, LogicalType::VARCHAR, WhoamiRandomNameFunction);
	fn.SetVolatile();
	return fn;
}

} // namespace duckdb
