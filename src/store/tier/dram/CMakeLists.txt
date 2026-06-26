# tidepool_tier_dram — in-process DRAM tier (hot level).
add_library(tidepool_tier_dram dram_tier.cpp)
target_link_libraries(tidepool_tier_dram PUBLIC tidepool_api)
target_include_directories(tidepool_tier_dram PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
add_library(tidepool::tier_dram ALIAS tidepool_tier_dram)
