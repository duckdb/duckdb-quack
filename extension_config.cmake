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
    # Kept in sync with the httpfs pin in duckdb/.github/config/extensions/httpfs.cmake, so httpfs builds
    # against the bundled duckdb (.github/duckdb-version). Includes the 2026-07-30 directory
    # restructure/reimplementation, the curl per-read timeout fix (duckdb-httpfs#336: stalled transfers abort
    # via CURLOPT_LOW_SPEED_* instead of hanging), and the fixes for duckdb main making configs/secrets take
    # Identifier instead of string. No APPLY_PATCHES: duckdb's only patch for this pin rewrites its own
    # autoloading .test expectations, which this repo does not run.
    GIT_TAG 5dfa24ce370dda2ebb7f24ab80d4237093512260
)
