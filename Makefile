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

.PHONY: test_quack_release test_quack_debug test_quack_reldebug
# Artifact-only runs contain the SQL test runner, not extension C++ test binaries.
ifeq ($(CI_EXTENSION_ARTIFACT_DIR),)
test_release: test_quack_release
test_debug: test_quack_debug
test_reldebug: test_quack_reldebug
endif

test_quack_release test_quack_debug test_quack_reldebug:
	@if [ "$(SKIP_TESTS)" != "1" ]; then \
		./build/$(patsubst test_quack_%,%,$@)/test/unittest_quack; \
	fi
