add_library(
  quack_storage OBJECT
  quack_catalog.cpp
  quack_catalog_set.cpp
  quack_insert.cpp
  quack_optimizer.cpp
  quack_table.cpp
  quack_schema.cpp
  quack_transaction.cpp
  quack_transaction.cpp
  quack_transaction_manager.cpp
  quack_view.cpp)
set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES} $<TARGET_OBJECTS:quack_storage>
    PARENT_SCOPE)
