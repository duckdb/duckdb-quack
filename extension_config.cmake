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
        GIT_TAG a6af106cb55825b01db84e1469143842507e7a60
    )
endif()
