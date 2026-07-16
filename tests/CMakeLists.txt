# Placeholder test target. TODO: adopt GoogleTest (FetchContent) and split into
# per-module unit tests.
add_executable(tidepool_smoke_test smoke_test.cpp)
target_link_libraries(tidepool_smoke_test
  PRIVATE
    tidepool_store
    tidepool_hashring
    tidepool_coordinator)
# Let the smoke test reach the ARC policy's private impl header so it can assert
# on the internal T1/T2/B1/B2 lists and the adaptive parameter p.
target_include_directories(tidepool_smoke_test
  PRIVATE ${PROJECT_SOURCE_DIR}/src/store/eviction)

# When the SSD tier is built against LevelDB, enable its conditional smoke test.
# Dependency resolution is centralized at the project root, where an explicitly
# requested but unavailable LevelDB is a configuration error.
if(TIDEPOOL_WITH_LEVELDB)
  target_include_directories(tidepool_smoke_test PRIVATE ${PROJECT_SOURCE_DIR}/src/store/tier/ssd)
  target_compile_definitions(tidepool_smoke_test PRIVATE TIDEPOOL_WITH_LEVELDB)
endif()

add_test(NAME tidepool_smoke_test COMMAND tidepool_smoke_test)

add_executable(tidepool_lifecycle_test lifecycle_test.cpp)
target_link_libraries(tidepool_lifecycle_test PRIVATE tidepool_store)
add_test(NAME tidepool_lifecycle_test COMMAND tidepool_lifecycle_test)

if(TIDEPOOL_WITH_LEVELDB)
  add_executable(tidepool_ssd_lifecycle_test ssd_lifecycle_test.cpp)
  target_include_directories(tidepool_ssd_lifecycle_test PRIVATE ${PROJECT_SOURCE_DIR}/src/store/tier/ssd)
  target_compile_definitions(tidepool_ssd_lifecycle_test PRIVATE TIDEPOOL_WITH_LEVELDB)
  target_link_libraries(tidepool_ssd_lifecycle_test PRIVATE tidepool_store Threads::Threads)
  add_test(NAME tidepool_ssd_lifecycle_test COMMAND tidepool_ssd_lifecycle_test)
endif()
