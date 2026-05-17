#include "quack_uri.hpp"

namespace duckdb {

QuackUri::QuackUri(string uri_p, bool ssl_p) : ssl(ssl_p), uri(uri_p) {
	// we should really instantiate a parser here instead, but alas
	// whitespace be gone
	ipv6 = false;
	port = 9494;
	path = "/quack";
	StringUtil::Trim(uri);
	// first off, lets be tolerant and accept this variant, too
	if (StringUtil::StartsWith(uri, "quack://")) {
		uri = StringUtil::Replace(uri, "quack://", "quack:");
	}
	if (!StringUtil::StartsWith(uri, "quack:")) {
		throw InvalidInputException("Invalid DuckDB Quack RPC URI, needs to start with 'quack:'");
	}

	auto remainder = StringUtil::Replace(uri, "quack:", "");
	if (remainder.empty()) {
		throw InvalidInputException("Missing hostname");
	}

	auto path_pos = remainder.find('/');
	if (path_pos != string::npos) {
		path = remainder.substr(path_pos);
		remainder = remainder.substr(0, path_pos);
		if (path.empty()) {
			throw InvalidInputException("Invalid Path");
		}
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
		if (!remainder.empty() && !StringUtil::StartsWith(remainder, ":")) {
			throw InvalidInputException("Invalid IPv6 URL");
		}
	}

	// a port was specified
	if (StringUtil::Contains(remainder, ':')) {
		auto pos = remainder.find(':');
		auto port_str = remainder.substr(pos + 1);
		if (port_str.empty()) {
			throw InvalidInputException("Invalid Port");
		}
		int raw_port;
		try {
			raw_port = stoi(port_str);
		} catch (std::exception &) {
			throw InvalidInputException("Invalid Port");
		}
		if (raw_port < 1 || raw_port > 65535) {
			throw InvalidInputException("Invalid Port");
		}
		port = raw_port;
		remainder = remainder.substr(0, pos);
	}
	// this should be it
	if (!ipv6) {
		host = remainder;
	}
	if (host.empty()) {
		throw InvalidInputException("Missing hostname");
	}
	http = StringUtil::Format("http%s://%s:%d", ssl ? "s" : "", ipv6 ? "[" + host + "]" : host, port);
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
	                                  {"url", Value(parsed.Http())},
	                                  {"path", Value(parsed.Path())},
	                                  {"endpoint", Value(parsed.HttpEndpoint())}}));
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

// just for testing
ScalarFunction QuackParseUriFunction::GetFunction() {
	return ScalarFunction("quack_uri_parser", {/* uri */ LogicalType::VARCHAR, /* ssl */ LogicalType::BOOLEAN},
	                      LogicalType::STRUCT({{"host", LogicalType::VARCHAR},
	                                           {"port", LogicalType::USMALLINT},
	                                           {"ipv6", LogicalType::BOOLEAN},
	                                           {"ssl", LogicalType::BOOLEAN},
	                                           {"url", LogicalType::VARCHAR},
	                                           {"path", LogicalType::VARCHAR},
	                                           {"endpoint", LogicalType::VARCHAR}}),
	                      QuackUriParser);
}

} // namespace duckdb
