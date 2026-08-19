#include "quack_uri.hpp"

namespace duckdb {

QuackUri::QuackUri(const QuackUri &input_p, uint16_t new_port)
    : ssl(input_p.Ssl()), ipv6(input_p.IPv6()), host(input_p.Host()), port(new_port) {
	uri = CanonicalUri();
}

QuackUri::QuackUri(string uri_p, bool ssl_p) : ssl(ssl_p), uri(std::move(uri_p)) {
	// we should really instantiate a parser here instead, but alas
	// whitespace be gone
	ipv6 = false;
	port = 9494;
	StringUtil::Trim(uri);
	// strip the scheme as a prefix only — must not use StringUtil::Replace here,
	// which replaces every occurrence and would mangle hostnames containing "quack:"
	// (e.g. quack://ilum-quack:9494)
	string remainder;
	if (StringUtil::StartsWith(uri, "quack://")) {
		remainder = uri.substr(strlen("quack://"));
	} else if (StringUtil::StartsWith(uri, "quack:")) {
		remainder = uri.substr(strlen("quack:"));
	} else {
		throw InvalidInputException("Invalid DuckDB Quack RPC URI, needs to start with 'quack:'");
	}
	if (remainder.empty()) {
		throw InvalidInputException("Missing hostname");
	}
	// we have an ipv6 URL
	if (StringUtil::StartsWith(remainder, "[")) {
		if (!StringUtil::Contains(remainder, ']')) {
			throw InvalidInputException("Invalid IPv6 URL, missing ']'");
		}
		ipv6 = true;
		auto pos = remainder.find(']');
		host = remainder.substr(1, pos - 1);
		if (host.empty()) {
			throw InvalidInputException("Missing IPv6 Address");
		}
		remainder = remainder.substr(pos + 1);
	}

	// a port was specified
	if (StringUtil::Contains(remainder, ':')) {
		auto pos = remainder.find(':');
		auto port_str = remainder.substr(pos + 1);
		if (port_str.empty()) {
			throw InvalidInputException("Invalid Port \"\"");
		}
		int raw_port;
		try {
			raw_port = stoi(port_str);
			if (raw_port < 0 || raw_port > 65535) {
				throw InvalidInputException("Invalid Port");
			}
		} catch (std::exception &) {
			throw InvalidInputException("Invalid Port \"%s\" - must be between 0 and 65535", port_str);
		}
		port = raw_port;
		remainder = remainder.substr(0, pos);
	}
	// this should be it
	if (!ipv6) {
		host = remainder;
	}
}

string QuackUri::Http() const {
	return StringUtil::Format("http%s://%s:%d", ssl ? "s" : "", ipv6 ? "[" + host + "]" : host, port);
}

static void QuackUriParser(const DataChunk &args, ExpressionState &, Vector &result) {
	if (!args.AllConstant()) {
		throw InvalidInputException("quack_uri_parser expects all arguments to be constant");
	}
	QuackUri parsed(args.GetValue(0, 0).GetValue<string>(), args.GetValue(1, 0).GetValue<bool>());

	result.SetValue(0, Value::STRUCT({{"host", Value(parsed.Host())},
	                                  {"port", Value::USMALLINT(parsed.Port())},
	                                  {"ipv6", Value::BOOLEAN(parsed.IPv6())},
	                                  {"ssl", Value::BOOLEAN(parsed.Ssl())},
	                                  {"url", Value(parsed.Http())}}));
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

// just for testing
ScalarFunction QuackParseUriFunction::GetFunction() {
	ScalarFunction function("quack_uri_parser", {/* uri */ LogicalType::VARCHAR, /* ssl */ LogicalType::BOOLEAN},
	                        LogicalType::STRUCT({{"host", LogicalType::VARCHAR},
	                                             {"port", LogicalType::USMALLINT},
	                                             {"ipv6", LogicalType::BOOLEAN},
	                                             {"ssl", LogicalType::BOOLEAN},
	                                             {"url", LogicalType::VARCHAR}}),
	                        QuackUriParser);
	// parsing rejects malformed URIs at runtime, so constant folding must not treat a throw as internal
	function.SetFallible();
	return function;
}

} // namespace duckdb
