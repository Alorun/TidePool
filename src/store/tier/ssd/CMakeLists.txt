# tidepool_tier_ssd — SSD tier backed by LevelDB (cold level).
# Builds as a NotImplemented stub by default; pass -DTIDEPOOL_WITH_LEVELDB=ON to
# compile the real backend (requires LevelDB available to find_package).
add_library(tidepool_tier_ssd ssd_tier.cpp)
target_link_libraries(tidepool_tier_ssd PUBLIC tidepool_api)
target_include_directories(tidepool_tier_ssd PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(TIDEPOOL_WITH_LEVELDB)
  target_link_libraries(tidepool_tier_ssd PUBLIC leveldb::leveldb)
  target_compile_definitions(tidepool_tier_ssd PRIVATE TIDEPOOL_WITH_LEVELDB)
  message(STATUS "tidepool: SSD tier using LevelDB")
endif()

add_library(tidepool::tier_ssd ALIAS tidepool_tier_ssd)
