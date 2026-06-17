# tidepool_api — shared vocabulary types (Status/Result, BlockKey, Block,
# Location, ShardMap). Every other target links this and inherits the public
# include directory.
add_library(tidepool_api
  status.cpp
  block_key.cpp
  location.cpp)

target_include_directories(tidepool_api PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_compile_features(tidepool_api PUBLIC cxx_std_17)

add_library(tidepool::api ALIAS tidepool_api)
