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

# When (and only when) the SSD tier is actually built against LevelDB, enable
# the SsdTier round-trip test: reach the private ssd_tier.h header (for Open())
# and define the same toggle the tier library uses so the #ifdef block compiles.
# Mirror the tier's find_package guard so ON-but-not-found still builds/tests.
if(TIDEPOOL_WITH_LEVELDB)
  find_package(leveldb QUIET)
  if(leveldb_FOUND)
    target_include_directories(tidepool_smoke_test PRIVATE ${PROJECT_SOURCE_DIR}/src/store/tier/ssd)
    target_compile_definitions(tidepool_smoke_test PRIVATE TIDEPOOL_WITH_LEVELDB)
  endif()
endif()

add_test(NAME tidepool_smoke_test COMMAND tidepool_smoke_test)
