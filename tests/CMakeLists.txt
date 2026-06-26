# Placeholder test target. TODO: adopt GoogleTest (FetchContent) and split into
# per-module unit tests.
add_executable(tidepool_smoke_test smoke_test.cpp)
target_link_libraries(tidepool_smoke_test
  PRIVATE
    tidepool_store
    tidepool_hashring
    tidepool_coordinator)
add_test(NAME tidepool_smoke_test COMMAND tidepool_smoke_test)
