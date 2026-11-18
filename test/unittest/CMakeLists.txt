include_directories(${CMAKE_SOURCE_DIR}/third_party/catch)

add_executable(unittest_quack test_client_connection.cpp)
set_target_properties(unittest_quack PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                                ${CMAKE_BINARY_DIR}/test)
target_link_libraries(unittest_quack duckdb_static quack_extension)
link_extension_libraries(unittest_quack "")
link_threads(unittest_quack "")
