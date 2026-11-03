//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_affinity.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct HTTPHeaders;

//! One stored cookie; expires is timestamp_t::infinity() for a session cookie
struct QuackCookie {
	string name;
	string value;
	timestamp_t expires = timestamp_t::infinity();
};

//! Manages connection affinity, including cookies and connection ID
class QuackConnectionAffinity {
public:
	//! headers for the next request: Cookie (merged with a Cookie from EXTRA_HTTP_HEADERS, if any) and
	//! X-Quack-Connection-Id once the id is known
	void AddRequestHeaders(HTTPHeaders &headers, timestamp_t now);
	//! every Set-Cookie value of a response that arrived - also a non-200 one
	void Absorb(const vector<string> &set_cookie_values, timestamp_t now);
	//! Called once, right after CONNECTION_RESPONSE
	void SetConnectionId(const string &connection_id_p);

private:
	mutex lock;
	//! In first-stored order; name is unique
	vector<QuackCookie> cookies;
	string connection_id;
};

} // namespace duckdb
