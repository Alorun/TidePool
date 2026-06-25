# tidepool_client — public Connector API. Composes the hash ring (addressing),
# a Transport (data movement), a Coordinator handle (shard-map refresh), and an
# optional co-located StorageNode (local-hit fast path) — hence the store dep.
add_library(tidepool_client connector.cpp)
target_link_libraries(tidepool_client
  PUBLIC
    tidepool_api
    tidepool_hashring
    tidepool_coordinator
    tidepool_transport_tcp
    tidepool_store)
add_library(tidepool::client ALIAS tidepool_client)
