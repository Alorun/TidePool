# tidepool_transport_tcp — MVP TCP-socket Transfer Engine.
add_library(tidepool_transport_tcp tcp_transport.cpp)
target_link_libraries(tidepool_transport_tcp PUBLIC tidepool_api)
# Allow `#include "tcp_transport.h"` from the .cpp (impl header kept private).
target_include_directories(tidepool_transport_tcp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
add_library(tidepool::transport_tcp ALIAS tidepool_transport_tcp)
