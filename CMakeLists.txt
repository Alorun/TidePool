# tidepool — distributed KV cache pool for LLM inference.
#
# Top-level build entry point.
#
#   cmake -B build && cmake --build build
#
# Optional features (all default OFF so the scaffold builds out of the box
# without any third-party dependency installed):
#   -DTIDEPOOL_WITH_LEVELDB=ON   link the SSD tier against LevelDB
#   -DTIDEPOOL_WITH_RDMA=ON       link the RDMA transport against libibverbs
cmake_minimum_required(VERSION 3.16)

project(tidepool
  VERSION 0.1.0
  DESCRIPTION "Distributed, cross-instance KV cache pool for LLM inference"
  LANGUAGES CXX)

# ---- Global C++ settings ----------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Emit compile_commands.json so editor tooling (clangd) sees the exact include
# paths/flags. Without it, clangd guesses and reports false errors (e.g. an
# "incomplete type" on a vector iterator in a header it parsed out of context).
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

# Place all binaries / libraries under build/ for convenience.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ---- Feature toggles --------------------------------------------------------
option(TIDEPOOL_WITH_LEVELDB "Build the SSD tier with the LevelDB backend" OFF)
option(TIDEPOOL_WITH_RDMA    "Build the RDMA transport with libibverbs"    OFF)
option(TIDEPOOL_BUILD_TESTS  "Build the (placeholder) test targets"        ON)

message(STATUS "tidepool: TIDEPOOL_WITH_LEVELDB = ${TIDEPOOL_WITH_LEVELDB}")
message(STATUS "tidepool: TIDEPOOL_WITH_RDMA    = ${TIDEPOOL_WITH_RDMA}")

# Enforce a clean build: unused parameters etc. are real warnings, not silenced
# with -w. Stubs name-ignore params (`/*name*/`) so they stay warning-free.
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra)
endif()

# ---- Subdirectories ---------------------------------------------------------
add_subdirectory(src)
add_subdirectory(apps)
add_subdirectory(integration/vllm)

if(TIDEPOOL_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
