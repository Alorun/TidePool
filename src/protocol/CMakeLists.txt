# tidepool_protocol — socket-free Wire Protocol v1 value and payload codec.
add_library(tidepool_protocol wire_codec.cpp)
target_link_libraries(tidepool_protocol PUBLIC tidepool_api)
target_compile_features(tidepool_protocol PUBLIC cxx_std_17)

add_library(tidepool::protocol ALIAS tidepool_protocol)
