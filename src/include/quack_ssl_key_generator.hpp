#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class FileSystem;
class FileOpener;

class SslKeyGenerator {
public:
	static void GenerateSslKeys(const std::string &cert_filename, const std::string &private_key_filename,
	                            const std::string &dh_filename, size_t cert_days_valid);

	//! `<home>/.duckdb/extension_data/quack`, created if missing. With an opener, the home_directory
	//! setting is honored.
	static constexpr size_t DEFAULT_DAYS_VALID = 3650;

	static std::string GetDefaultCertificateDirectory(FileSystem &fs, optional_ptr<FileOpener> opener = nullptr);
	static std::string GetDefaultCertificateFile(FileSystem &fs, const std::string &certificate_directory);
	static std::string GetDefaultPrivateKeyFile(FileSystem &fs, const std::string &certificate_directory);
	//! Restrict a freshly written key file to its owner where the platform supports it
	static void RestrictPermissions(const std::string &filename);

	//! SHA-256 fingerprint of a PEM certificate, colon-separated uppercase hex like `openssl x509 -fingerprint`
	static std::string CertificateFingerprint(const std::string &cert_filename);
	//! Same, of an in-memory certificate (`X509 *`, kept opaque here so the header needs no OpenSSL)
	static std::string CertificateFingerprint(void *x509);
};
} // namespace duckdb
