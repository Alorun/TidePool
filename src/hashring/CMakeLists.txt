# tidepool_hashring — consistent-hash ring (data-plane addressing, derived from
# the control-plane ShardMap).
add_library(tidepool_hashring hash_ring.cpp)
target_link_libraries(tidepool_hashring PUBLIC tidepool_api)
add_library(tidepool::hashring ALIAS tidepool_hashring)
