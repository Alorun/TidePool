# The repository uses small dependency-free test executables so the default
# build needs no test framework download.
add_executable(tidepool_protocol_codec_test protocol_codec_test.cpp)
target_link_libraries(tidepool_protocol_codec_test PRIVATE tidepool_protocol)
add_test(NAME tidepool_protocol_codec_test COMMAND tidepool_protocol_codec_test)

add_executable(tidepool_smoke_test smoke_test.cpp)
target_link_libraries(tidepool_smoke_test
  PRIVATE
    tidepool_store
    tidepool_hashring
    tidepool_coordinator)
# Let the smoke test reach the ARC policy's private impl header so it can assert
# on the internal T1/T2/B1/B2 lists and the adaptive parameter p.
target_include_directories(tidepool_smoke_test
  PRIVATE ${PROJECT_SOURCE_DIR}/src/store/eviction)

# When the SSD tier is built against LevelDB, enable its conditional smoke test.
# Dependency resolution is centralized at the project root, where an explicitly
# requested but unavailable LevelDB is a configuration error.
if(TIDEPOOL_WITH_LEVELDB)
  target_include_directories(tidepool_smoke_test PRIVATE ${PROJECT_SOURCE_DIR}/src/store/tier/ssd)
  target_compile_definitions(tidepool_smoke_test PRIVATE TIDEPOOL_WITH_LEVELDB)
endif()

add_test(NAME tidepool_smoke_test COMMAND tidepool_smoke_test)

add_executable(tidepool_lifecycle_test lifecycle_test.cpp)
target_link_libraries(tidepool_lifecycle_test PRIVATE tidepool_store)
add_test(NAME tidepool_lifecycle_test COMMAND tidepool_lifecycle_test)

find_package(Threads REQUIRED)

add_executable(tidepool_tcp_connection_test tcp_connection_test.cpp)
target_link_libraries(tidepool_tcp_connection_test
  PRIVATE tidepool_transport_tcp Threads::Threads)
add_test(NAME tidepool_tcp_connection_test COMMAND tidepool_tcp_connection_test)

add_executable(tidepool_wire_framing_test wire_framing_test.cpp)
target_link_libraries(tidepool_wire_framing_test
  PRIVATE tidepool_wire_framing Threads::Threads)
add_test(NAME tidepool_wire_framing_test COMMAND tidepool_wire_framing_test)

add_executable(tidepool_dram_tier_test dram_tier_test.cpp)
target_include_directories(tidepool_dram_tier_test PRIVATE ${PROJECT_SOURCE_DIR}/src/store/tier/dram)
target_link_libraries(tidepool_dram_tier_test PRIVATE tidepool_store Threads::Threads)
add_test(NAME tidepool_dram_tier_test COMMAND tidepool_dram_tier_test)

add_executable(tidepool_eviction_test eviction_test.cpp)
target_include_directories(tidepool_eviction_test PRIVATE ${PROJECT_SOURCE_DIR}/src/store/eviction)
target_link_libraries(tidepool_eviction_test PRIVATE tidepool_store)
add_test(NAME tidepool_eviction_test COMMAND tidepool_eviction_test)

add_executable(tidepool_commit_invariant_test commit_invariant_test.cpp)
target_include_directories(tidepool_commit_invariant_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src/store/eviction
    ${PROJECT_SOURCE_DIR}/src/store/tier/dram)
target_link_libraries(tidepool_commit_invariant_test PRIVATE tidepool_store)
add_test(NAME tidepool_commit_invariant_test COMMAND tidepool_commit_invariant_test)

add_executable(tidepool_storage_migration_test storage_migration_test.cpp)
target_include_directories(tidepool_storage_migration_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src/store/eviction
    ${PROJECT_SOURCE_DIR}/src/store/tier/dram)
target_link_libraries(tidepool_storage_migration_test PRIVATE tidepool_store Threads::Threads)
add_test(NAME tidepool_storage_migration_test COMMAND tidepool_storage_migration_test)

add_executable(tidepool_storage_promotion_test storage_promotion_test.cpp)
target_include_directories(tidepool_storage_promotion_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src/store/eviction
    ${PROJECT_SOURCE_DIR}/src/store/tier/dram)
target_link_libraries(tidepool_storage_promotion_test PRIVATE tidepool_store Threads::Threads)
add_test(NAME tidepool_storage_promotion_test COMMAND tidepool_storage_promotion_test)

add_executable(tidepool_tier_contract_test tier_contract_test.cpp)
target_include_directories(tidepool_tier_contract_test
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src/store/tier/dram)
target_link_libraries(tidepool_tier_contract_test PRIVATE tidepool_store)
add_test(NAME tidepool_tier_contract_test COMMAND tidepool_tier_contract_test)

if(TIDEPOOL_WITH_LEVELDB)
  target_include_directories(tidepool_tier_contract_test
    PRIVATE ${PROJECT_SOURCE_DIR}/src/store/tier/ssd)
  target_compile_definitions(tidepool_tier_contract_test PRIVATE TIDEPOOL_WITH_LEVELDB)

  add_executable(tidepool_ssd_lifecycle_test ssd_lifecycle_test.cpp)
  target_include_directories(tidepool_ssd_lifecycle_test PRIVATE ${PROJECT_SOURCE_DIR}/src/store/tier/ssd)
  target_compile_definitions(tidepool_ssd_lifecycle_test PRIVATE TIDEPOOL_WITH_LEVELDB)
  target_link_libraries(tidepool_ssd_lifecycle_test PRIVATE tidepool_store Threads::Threads)
  add_test(NAME tidepool_ssd_lifecycle_test COMMAND tidepool_ssd_lifecycle_test)

  add_executable(tidepool_ssd_recovery_test ssd_recovery_test.cpp)
  target_include_directories(tidepool_ssd_recovery_test PRIVATE ${PROJECT_SOURCE_DIR}/src/store/tier/ssd)
  target_compile_definitions(tidepool_ssd_recovery_test PRIVATE TIDEPOOL_WITH_LEVELDB)
  target_link_libraries(tidepool_ssd_recovery_test PRIVATE tidepool_store)
  add_test(NAME tidepool_ssd_recovery_test COMMAND tidepool_ssd_recovery_test)
endif()
