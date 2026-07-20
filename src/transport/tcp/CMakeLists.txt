# tidepool_transport_tcp — low-level TCP socket ownership and exact I/O, plus
# the preserved first-stage Transport stub.
add_library(tidepool_transport_tcp
  tcp_connection.cpp
  tcp_transport.cpp)
target_link_libraries(tidepool_transport_tcp PUBLIC tidepool_api)
# Allow `#include "tcp_transport.h"` from the .cpp (impl header kept private).
target_include_directories(tidepool_transport_tcp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
add_library(tidepool::transport_tcp ALIAS tidepool_transport_tcp)

# Framing depends on both the low-level connection and the socket-free protocol
# codec. Keeping it separate prevents TcpConnection from learning wire/RPC
# semantics.
add_library(tidepool_wire_framing wire_framing.cpp)
target_link_libraries(tidepool_wire_framing
  PUBLIC
    tidepool_protocol
    tidepool_transport_tcp)
add_library(tidepool::wire_framing ALIAS tidepool_wire_framing)
