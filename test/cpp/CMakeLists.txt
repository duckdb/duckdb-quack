# Register test sources here. Catch discovers their TEST_CASEs at startup.
set(QUACK_CPP_TEST_SOURCES early_cancel.cpp)

# Build in the normal compiler environment, including Windows CI's vcvars shell.
add_executable(quack_cpp_tests unittest.cpp ${QUACK_CPP_TEST_SOURCES})
target_include_directories(
  quack_cpp_tests PRIVATE "${DUCKDB_MODULE_BASE_DIR}/third_party/catch")
# httpfs may be configured after quack; name the test dependencies explicitly.
target_link_libraries(quack_cpp_tests core_functions_extension httpfs_extension
                      quack_extension duckdb_static)
duckdb_write_static_extension_loader(
  "${CMAKE_CURRENT_BINARY_DIR}/quack_test_loader.c" core_functions httpfs quack)
target_sources(
  quack_cpp_tests
  PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/quack_test_loader.c"
    "${DUCKDB_MODULE_BASE_DIR}/extension/loader/static_extension_autoregister.cpp"
)

# TARGET_FILE handles executable suffixes and multi-configuration generators.
# Catch returns nonzero on failure, so the SQL suite cannot mask a failed test.
add_custom_target(
  quack_cpp_check
  COMMAND $<TARGET_FILE:quack_cpp_tests>
  DEPENDS quack_cpp_tests
  VERBATIM)
