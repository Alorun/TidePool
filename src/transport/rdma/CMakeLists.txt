# tidepool_transport_rdma — RDMA (libibverbs) Transfer Engine, stubbed.
# Builds with no system dependency by default; pass -DTIDEPOOL_WITH_RDMA=ON to
# link libibverbs (the verbs calls themselves are still TODO).
add_library(tidepool_transport_rdma rdma_transport.cpp)
target_link_libraries(tidepool_transport_rdma PUBLIC tidepool_api)
target_include_directories(tidepool_transport_rdma PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(TIDEPOOL_WITH_RDMA)
  find_library(IBVERBS_LIB ibverbs)
  if(IBVERBS_LIB)
    target_link_libraries(tidepool_transport_rdma PUBLIC ${IBVERBS_LIB})
    target_compile_definitions(tidepool_transport_rdma PRIVATE TIDEPOOL_WITH_RDMA)
  else()
    message(WARNING "TIDEPOOL_WITH_RDMA=ON but libibverbs not found; "
                    "building NotImplemented stubs only")
  endif()
endif()

add_library(tidepool::transport_rdma ALIAS tidepool_transport_rdma)
