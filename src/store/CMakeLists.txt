# Storage-node stack: tiers + index + eviction, aggregated by tidepool_store
# (which adds the StorageNode orchestration on top).
add_subdirectory(tier/dram)
add_subdirectory(tier/ssd)
add_subdirectory(index)
add_subdirectory(eviction)

add_library(tidepool_store storage_node.cpp)
target_link_libraries(tidepool_store
  PUBLIC
    tidepool_api
    tidepool_tier_dram
    tidepool_tier_ssd
    tidepool_store_index
    tidepool_store_eviction)
add_library(tidepool::store ALIAS tidepool_store)
