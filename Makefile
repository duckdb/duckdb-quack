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

# Distribution CI uses these same test targets. Build and run the native
# tests first, so either a build failure or a test failure stops the suite.
ifneq ($(SKIP_TESTS),1)
test_release: test_quack_release
test_debug: test_quack_debug
test_reldebug: test_quack_reldebug

.PHONY: test_quack_release test_quack_debug test_quack_reldebug
test_quack_release test_quack_debug test_quack_reldebug: test_quack_%:
	cmake --build build/$* --config $(if $(filter debug,$*),Debug,$(if $(filter reldebug,$*),RelWithDebInfo,Release)) --target quack_cpp_check
endif
