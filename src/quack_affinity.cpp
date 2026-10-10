#include "quack_affinity.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"

#include <ctime>
#include <iomanip>
#include <locale>
#include <sstream>

namespace duckdb {

namespace {

//! Name of the header that carries the connection ID.
constexpr const char *CONNECTION_ID_HEADER = "X-Quack-Connection-Id";

//! Maximum size of a cookie in bytes, including name and value.
constexpr idx_t MAX_COOKIE_BYTES = 4096;
//! Cap the Max-Age to this many seconds (roughly 300 years) to avoid overflow.
constexpr int64_t MAX_AGE_CAP_SECONDS = Interval::DAYS_PER_YEAR * Interval::SECS_PER_DAY * 300;
//! Cookies kept per connection; a new name beyond it is skipped (never evict: the affinity cookie came first)
constexpr idx_t MAX_COOKIES = 20;

//! A character that must never get into a request header: < 0x20 except tab, and DEL
bool IsControl(char c) {
	auto byte = static_cast<unsigned char>(c);
	return (byte < 0x20 && c != '\t') || byte == 0x7F;
}

//! Strips spaces and tabs only - StringUtil::Trim would also eat \r and \n
string TrimSpaces(const string &text) {
	idx_t start = 0;
	idx_t end = text.size();

	while (start < end && (text[start] == ' ' || text[start] == '\t')) {
		start++;
	}
	while (end > start && (text[end - 1] == ' ' || text[end - 1] == '\t')) {
		end--;
	}
	return text.substr(start, end - start);
}

//! RFC 6265 5.2.2: an optional '-' followed by digits. False for anything else (the attribute is ignored).
bool ParseMaxAge(const string &value, int64_t &max_age) {
	idx_t pos = 0;
	bool negative = false;
	if (pos < value.size() && value[pos] == '-') {
		negative = true;
		pos++;
	}
	if (pos == value.size()) {
		return false;
	}
	int64_t result = 0;
	for (; pos < value.size(); pos++) {
		auto c = value[pos];
		if (c < '0' || c > '9') {
			return false;
		}
		if (result < MAX_AGE_CAP_SECONDS) {
			result = result * 10 + (c - '0');
		}
	}
	max_age = negative ? -result : MinValue(result, MAX_AGE_CAP_SECONDS);
	return true;
}

//! RFC 1123 HTTP-date ("Sun, 06 Nov 1994 08:49:37 GMT") - the form every current ingress sends.
//! False for anything else: the attribute is ignored and the cookie is a session cookie.
bool ParseHttpDate(const string &text, timestamp_t &result) {
	std::tm parts {};
	std::istringstream stream(text);
	stream.imbue(std::locale::classic());
	stream >> std::get_time(&parts, "%a, %d %b %Y %H:%M:%S");
	if (stream.fail()) {
		return false;
	}
	date_t date;
	if (!Date::TryFromDate(parts.tm_year + 1900, parts.tm_mon + 1, parts.tm_mday, date)) {
		return false;
	}
	return Timestamp::TryFromDatetime(date, Time::FromTime(parts.tm_hour, parts.tm_min, parts.tm_sec), result);
}

//! Parses one Set-Cookie value. False: skip it as a whole (garbage, control characters, too long).
//! An expires <= now means the cookie is deleted.
bool ParseSetCookie(const string &set_cookie, timestamp_t now, QuackCookie &parsed) {
	for (auto c : set_cookie) {
		if (IsControl(c)) {
			return false;
		}
	}
	auto pair_end = set_cookie.find(';');
	auto cookie_pair = set_cookie.substr(0, pair_end);
	auto equals = cookie_pair.find('=');
	if (equals == string::npos) {
		return false;
	}

	parsed.name = TrimSpaces(cookie_pair.substr(0, equals));
	parsed.value = TrimSpaces(cookie_pair.substr(equals + 1));
	if (parsed.name.empty() || parsed.name.size() + parsed.value.size() > MAX_COOKIE_BYTES) {
		return false;
	}
	parsed.expires = timestamp_t::infinity();
	if (pair_end == string::npos) {
		return true;
	}

	bool has_max_age = false;
	for (auto &attribute : StringUtil::Split(set_cookie.substr(pair_end + 1), ';')) {
		auto attribute_equals = attribute.find('=');
		auto attribute_name = TrimSpaces(attribute.substr(0, attribute_equals));
		auto attribute_value =
		    attribute_equals == string::npos ? string() : TrimSpaces(attribute.substr(attribute_equals + 1));
		if (StringUtil::CIEquals(attribute_name, "max-age")) {
			int64_t max_age = 0;
			if (!ParseMaxAge(attribute_value, max_age)) {
				continue;
			}
			parsed.expires = max_age <= 0 ? now : timestamp_t(now.value + max_age * Interval::MICROS_PER_SEC);
			has_max_age = true;
		} else if (StringUtil::CIEquals(attribute_name, "expires") && !has_max_age) {
			timestamp_t expires;
			if (ParseHttpDate(attribute_value, expires)) {
				parsed.expires = expires;
			}
		}
		// Domain, Path, Secure, HttpOnly, SameSite, unknown: ignored - one origin, one path, never persisted.
	}
	return true;
}

//! Cookie header value: the static pairs whose name is not stored, then every stored cookie.
//! A stored value wins over a static one of the same name: the ingress's value is fresh.
string MergeCookieHeader(const string &static_cookie, const vector<QuackCookie> &cookies) {
	string result;
	auto add_pair = [&](const string &pair) {
		if (!result.empty()) {
			result += "; ";
		}
		result += pair;
	};
	for (auto &static_pair : StringUtil::Split(static_cookie, ';')) {
		auto pair = TrimSpaces(static_pair);
		if (pair.empty()) {
			continue;
		}
		auto name = TrimSpaces(pair.substr(0, pair.find('=')));
		auto stored =
		    std::any_of(cookies.begin(), cookies.end(), [&](const QuackCookie &cookie) { return cookie.name == name; });
		if (!stored) {
			add_pair(pair);
		}
	}
	for (auto &cookie : cookies) {
		add_pair(cookie.name + "=" + cookie.value);
	}
	return result;
}

} // namespace

void QuackConnectionAffinity::AddRequestHeaders(HTTPHeaders &headers, timestamp_t now) {
	lock_guard<mutex> guard(lock);

	cookies.erase(std::remove_if(cookies.begin(), cookies.end(),
	                             [&](const QuackCookie &cookie) { return cookie.expires <= now; }),
	              cookies.end());
	if (!cookies.empty()) {
		auto static_cookie = headers.HasHeader("Cookie") ? headers.GetHeaderValue("Cookie") : string();
		headers["Cookie"] = MergeCookieHeader(static_cookie, cookies);
	}
	if (!connection_id.empty()) {
		headers[CONNECTION_ID_HEADER] = connection_id;
	}
}

void QuackConnectionAffinity::Absorb(const vector<string> &set_cookie_values, timestamp_t now) {
	lock_guard<mutex> guard(lock);
	for (auto &set_cookie : set_cookie_values) {
		QuackCookie parsed;
		if (!ParseSetCookie(set_cookie, now, parsed)) {
			continue;
		}
		auto it =
		    std::find_if(cookies.begin(), cookies.end(), [&](const QuackCookie &c) { return c.name == parsed.name; });
		if (parsed.expires <= now) {
			// Max-Age <= 0 or a past Expires: delete, and don't store this one.
			if (it != cookies.end()) {
				cookies.erase(it);
			}
		} else if (it != cookies.end()) {
			it->value = std::move(parsed.value);
			it->expires = parsed.expires;
		} else if (cookies.size() < MAX_COOKIES) {
			cookies.push_back(std::move(parsed));
		}
	}
}

void QuackConnectionAffinity::SetConnectionId(const string &connection_id_p) {
	lock_guard<mutex> guard(lock);
	connection_id = connection_id_p;
}

} // namespace duckdb
