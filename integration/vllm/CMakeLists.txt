# tidepool_vllm_adapter — stub adapter onto a vLLM / LMCache KV backend.
# Builds the integration shim against the public Connector API.
add_library(tidepool_vllm_adapter vllm_adapter.cpp)
target_link_libraries(tidepool_vllm_adapter PUBLIC tidepool_client)
target_include_directories(tidepool_vllm_adapter PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
add_library(tidepool::vllm_adapter ALIAS tidepool_vllm_adapter)
