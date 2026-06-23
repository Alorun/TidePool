# tidepool_coordinator — control plane (MVP single-node Coordinator). Consensus,
# when added (Raft/etcd), lives behind this same target/ABC.
add_library(tidepool_coordinator single_node_coordinator.cpp)
target_link_libraries(tidepool_coordinator PUBLIC tidepool_api tidepool_hashring)
target_include_directories(tidepool_coordinator PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
add_library(tidepool::coordinator ALIAS tidepool_coordinator)
