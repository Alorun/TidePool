# tidepool_rpc_client — one-endpoint Get/Put/Ping RPC client.
#
# This target intentionally does not depend on the legacy client facade,
# routing, or storage implementation.
find_package(Threads REQUIRED)

add_library(tidepool_rpc_client tcp_connector.cpp)
target_link_libraries(tidepool_rpc_client
  PUBLIC
    tidepool_api
    tidepool_protocol
    tidepool_wire_framing
  PRIVATE
    Threads::Threads)
target_compile_features(tidepool_rpc_client PUBLIC cxx_std_17)

add_library(tidepool::rpc_client ALIAS tidepool_rpc_client)
