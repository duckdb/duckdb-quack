cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME quack)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

#set(CMAKE_CXX_STANDARD 17)

# The server serves TLS through httplib's SSLServer, which OpenSSL gates behind this define. The define
# also switches httplib into its duckdb_httplib_openssl namespace, keeping these translation units from
# clashing with the plain duckdb_httplib that core compiles into libduckdb. Emscripten has neither
# OpenSSL nor a listening socket - quack_serve already refuses to run there.
if(NOT EMSCRIPTEN)
  find_package(OpenSSL REQUIRED)
  include_directories(${OPENSSL_INCLUDE_DIR})
  add_definitions(-DCPPHTTPLIB_OPENSSL_SUPPORT)
endif()

include_directories(src/include duckdb/third_party/httplib duckdb/extension/autocomplete/include
                    duckdb/third_party/mbedtls/include)
add_subdirectory(src)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_mbedtls ${OPENSSL_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_mbedtls ${OPENSSL_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
