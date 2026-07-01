# tidepool_store_eviction — replacement policies (LRU baseline, ARC adaptive,
# cost-aware stub).
add_library(tidepool_store_eviction
  lru_eviction.cpp
  arc_eviction.cpp
  cost_aware_eviction.cpp)
target_link_libraries(tidepool_store_eviction PUBLIC tidepool_api)
target_include_directories(tidepool_store_eviction PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
add_library(tidepool::store_eviction ALIAS tidepool_store_eviction)
