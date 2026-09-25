cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME quack)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# set(CMAKE_CXX_STANDARD 17)

# OpenSSL: the server can listen over HTTPS (httplib's SSLServer) and generates
# its own self-signed keys. Comes from vcpkg in CI (see vcpkg.json), like it
# does for httpfs.
if(MINGW)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
endif()
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
# Every TU must agree on this: it selects the duckdb_httplib_openssl namespace
# that quack_server.hpp uses
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)

include_directories(
  src/include duckdb/third_party/httplib duckdb/extension/autocomplete/include
  duckdb/third_party/mbedtls/include)

add_subdirectory(src)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_mbedtls ${OPENSSL_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_mbedtls
                      ${OPENSSL_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
