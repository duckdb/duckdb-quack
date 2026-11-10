# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(quack
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# Any extra extensions that should be built
duckdb_extension_load(json)
duckdb_extension_load(autocomplete)

if("httpfs" IN_LIST CORE_EXTENSIONS)
    duckdb_extension_load(httpfs
        GIT_URL https://github.com/duckdb/duckdb-httpfs
        GIT_TAG 7b186077bb232b05549be494b1583c5b0b1ad4ec
    )
endif()
