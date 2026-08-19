# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(quack
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# Any extra extensions that should be built
duckdb_extension_load(json)
duckdb_extension_load(autocomplete)

duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    # httpfs main including the large 2026-07-30 directory restructure/reimplementation and the curl
    # per-read timeout fix (duckdb-httpfs#336: stalled transfers abort via CURLOPT_LOW_SPEED_* instead of
    # hanging). No APPLY_PATCHES: the carried patches were absorbed upstream. Builds against the bundled
    # duckdb (.github/duckdb-version), which is current main and has SecretPersistType::TRANSACTION.
    GIT_TAG fafb14f2c899ddfd1998f8adf2e07fbbfd28b3fd
)
