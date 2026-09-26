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
	//! Fully-qualified canonical form, always `quack:<host>:<port>`. An IPv6 host is
	//! bracketed (`quack:[::1]:9494`), as it is in `Http()` — without the brackets the
	//! form is ambiguous and does not parse back into a QuackUri.
	string CanonicalUri() const {
		return "quack:" + (ipv6 ? "[" + host + "]" : host) + ":" + std::to_string(port);
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
	void SetSsl(bool ssl_p) {
		ssl = ssl_p;
	}
	//! Normalized SHA-256 fingerprint of the one server certificate the client trusts (see
	//! NormalizeFingerprint); empty means regular certificate verification.
	const string &SslFingerprint() const {
		return ssl_fingerprint;
	}
	void SetSslFingerprint(const string &fingerprint) {
		ssl_fingerprint = NormalizeFingerprint(fingerprint);
	}
	//! Canonical form of a SHA-256 certificate fingerprint: 64 uppercase hex digits, no separators. Accepts
	//! the colon-separated form `openssl x509 -fingerprint -sha256` prints, with or without a `sha256:` prefix.
	static string NormalizeFingerprint(const string &fingerprint);
	bool IPv6() const {
		return ipv6;
	}
	bool IsLocal() const {
		return StringUtil::Lower(host) == "localhost" || host == "127.0.0.1" || host == "::1";
	}
	bool operator==(const QuackUri &other) const {
		return other.ssl == ssl && other.ipv6 == ipv6 && other.host == host && other.port == port && other.uri == uri &&
		       other.ssl_fingerprint == ssl_fingerprint;
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
	string ssl_fingerprint;
};

class QuackParseUriFunction {
public:
	static ScalarFunction GetFunction();
};

} // namespace duckdb
