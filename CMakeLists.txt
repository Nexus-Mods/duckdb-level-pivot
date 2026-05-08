cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME level_pivot)

find_package(leveldb CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include_directories(src/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/yyjson/include)

set(EXTENSION_SOURCES
    src/level_pivot_extension.cpp
    src/core/key_pattern.cpp
    src/core/key_parser.cpp
    src/storage/level_pivot_storage.cpp
    src/storage/level_pivot_transaction.cpp
    src/catalog/level_pivot_catalog.cpp
    src/catalog/level_pivot_schema.cpp
    src/catalog/level_pivot_table_entry.cpp
    src/functions/level_pivot_scan.cpp
    src/functions/level_pivot_insert.cpp
    src/functions/level_pivot_delete.cpp
    src/functions/level_pivot_update.cpp
    src/functions/level_pivot_create_table.cpp
    src/functions/level_pivot_dirty_tables.cpp
    src/functions/level_pivot_stats.cpp
    src/storage/level_pivot_overlay.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(leveldb_FOUND)
    target_link_libraries(${EXTENSION_NAME} leveldb::leveldb)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} leveldb::leveldb)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
