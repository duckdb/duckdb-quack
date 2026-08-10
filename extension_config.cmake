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
    # Includes the curl per-read timeout fix (duckdb-httpfs#336): stalled transfers abort via
    # CURLOPT_LOW_SPEED_* instead of hanging forever / requiring a huge total-transfer timeout.
    # Newest commit before the 2026-07-30 directory restructure. No APPLY_PATCHES: the carried
    # patches (prefetch network cost model etc.) were absorbed upstream on 2026-07-03.
    GIT_TAG a6352ca0a4bec678008133cd41edb13e6465fa88
)
