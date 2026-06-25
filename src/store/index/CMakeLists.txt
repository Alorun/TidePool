# tidepool_store_index — node-local BlockKey -> Location directory (no consensus).
add_library(tidepool_store_index local_index.cpp)
target_link_libraries(tidepool_store_index PUBLIC tidepool_api)
add_library(tidepool::store_index ALIAS tidepool_store_index)
