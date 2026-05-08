add_subdirectory(storage)

add_library(
  quack_library OBJECT
  quack_activity.cpp
  quack_client.cpp
  quack_extension.cpp
  quack_http_server.cpp
  quack_log.cpp
  quack_message.cpp
  quack_scan.cpp
  quack_server.cpp
  quack_start_stop.cpp
  quack_storage.cpp
  quack_uri.cpp
  serialize_quack_message.cpp)
set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES} $<TARGET_OBJECTS:quack_library>
    PARENT_SCOPE)
