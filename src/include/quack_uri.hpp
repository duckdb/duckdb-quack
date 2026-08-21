#pragma once
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

class QuackUri {
public:
	QuackUri() : QuackUri("quack:localhost") {
	} // orrr

	explicit QuackUri(string uri_p, bool ssl_p = true);
	QuackUri(const QuackUri &uri, uint16_t new_port);

	string Http() const;
	string Uri() const {
		return uri;
	}
	//! Fully-qualified canonical form, always `quack:<host>:<port>`
	string CanonicalUri() const {
		return "quack:" + host + ":" + std::to_string(port);
	}
	string Host() const {
		return host;
	}
	uint16_t Port() const {
		return port;
	}
	bool Ssl() const {
		return ssl;
	}
	bool IPv6() const {
		return ipv6;
	}
	bool IsLocal() const {
		return StringUtil::Lower(host) == "localhost" || host == "127.0.0.1" || host == "::1";
	}
	bool operator==(const QuackUri &other) const {
		return other.ssl == ssl && other.ipv6 == ipv6 && other.host == host && other.port == port && other.uri == uri;
	}
	bool operator!=(const QuackUri &other) const {
		return !(*this == other);
	}

private:
	bool ssl;
	bool ipv6;
	string host;
	uint16_t port; // default port!
	string uri;
};

class QuackParseUriFunction {
public:
	static ScalarFunction GetFunction();
};

} // namespace duckdb
