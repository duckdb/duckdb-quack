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
    # httpfs main as of 2026-07-31, including the large 2026-07-30 restructure/reimplementation
    # and the curl per-read timeout fix (duckdb-httpfs#336).
    GIT_TAG fafb14f2c899ddfd1998f8adf2e07fbbfd28b3fd
)
