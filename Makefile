PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=rpc
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
EXT_FLAGS=-DCMAKE_CXX_STANDARD=17

# Core extensions that we need for crucial testing
DEFAULT_TEST_EXTENSION_DEPS=httpfs;

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
include extension-ci-tools/makefiles/vcpkg.Makefile
