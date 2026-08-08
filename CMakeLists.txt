cmake_minimum_required(VERSION 3.24)

project(compute_fabric
  VERSION 1.0.0
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(COMPUTE_FABRIC_BUILD_TESTS     "Build the native test suite"            ON)
option(COMPUTE_FABRIC_BUILD_BENCHMARKS "Build the benchmark suite"             ON)
option(COMPUTE_FABRIC_BUILD_EXAMPLES   "Build the examples"                    ON)
option(COMPUTE_FABRIC_BUILD_CLI        "Build the compute-fabric CLI"          ON)
option(COMPUTE_FABRIC_ENABLE_CUDA      "Enable the CUDA executor backend"      ON)
option(COMPUTE_FABRIC_WARNINGS_AS_ERRORS "Treat compiler warnings as errors"   ON)

set(COMPUTE_FABRIC_CMAKE_ARCH "x64" CACHE STRING "Target architecture (x64)")

if(NOT (CMAKE_SYSTEM_NAME STREQUAL "Windows" OR CMAKE_SYSTEM_NAME STREQUAL "Linux"))
  message(FATAL_ERROR "Compute Fabric currently supports Windows x64 and Linux x64 only.")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
  if(NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(FATAL_ERROR "Compute Fabric requires a 64-bit build on Windows.")
  endif()
endif()

# ---------------------------------------------------------------------------
# Compiler configuration
# ---------------------------------------------------------------------------
set(CF_WARN_FLAGS "")
if(MSVC)
  add_compile_options(
    "$<$<COMPILE_LANGUAGE:CXX>:/W4>"
    "$<$<COMPILE_LANGUAGE:CXX>:/EHsc>"
    "$<$<COMPILE_LANGUAGE:CXX>:/permissive->"
    "$<$<COMPILE_LANGUAGE:CXX>:/utf-8>"
    "$<$<COMPILE_LANGUAGE:CXX>:/FS>"
  )
  add_definitions(-DWIN32_LEAN_AND_MEAN -DNOMINMAX)
  add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:/D_CRT_SECURE_NO_WARNINGS>")
else()
  add_compile_options(
    "$<$<COMPILE_LANGUAGE:CXX>:-Wall>"
    "$<$<COMPILE_LANGUAGE:CXX>:-Wextra>"
  )
endif()

# Warnings-as-errors is applied to host code targets (not CUDA translation
# units, where nvcc-accepted header diagnostics would defeat validation).
function(cf_enable_warnings target)
    if(COMPUTE_FABRIC_WARNINGS_AS_ERRORS)
      if(MSVC)
        target_compile_options(${target} PRIVATE /WX)
      else()
        target_compile_options(${target} PRIVATE -Werror)
      endif()
    endif()
    if(MSVC)
      target_compile_options(${target} PRIVATE /W4)
    else()
      target_compile_options(${target} PRIVATE -Wall -Wextra)
    endif()
  endfunction()

# ---------------------------------------------------------------------------
# CUDA
# ---------------------------------------------------------------------------
if(COMPUTE_FABRIC_ENABLE_CUDA)
  find_package(CUDAToolkit QUIET)
  if(CUDAToolkit_FOUND)
    message(STATUS "CUDA toolkit found: ${CUDAToolkit_VERSION}")
  else()
    message(FATAL_ERROR
      "COMPUTE_FABRIC_ENABLE_CUDA=ON but the CUDA toolkit was not found. "
      "Configure explicitly with -DCOMPUTE_FABRIC_ENABLE_CUDA=OFF for a CPU-only build.")
  endif()
endif()

if(COMPUTE_FABRIC_ENABLE_CUDA)
  enable_language(CUDA)
  set(CMAKE_CUDA_STANDARD 20)
  set(CMAKE_CUDA_STANDARD_REQUIRED ON)
  if(CMAKE_CUDA_COMPILER_VERSION VERSION_LESS "12.8")
    message(FATAL_ERROR "Compute Fabric requires CUDA >= 12.8 for Blackwell (sm_120) support.")
  endif()
  if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
    set(CMAKE_CUDA_ARCHITECTURES "120")
  endif()
  message(STATUS "CUDA architectures: ${CMAKE_CUDA_ARCHITECTURES}")
endif()

# ---------------------------------------------------------------------------
# Core static library (host code)
# ---------------------------------------------------------------------------
set(CF_CORE_SOURCES
  src/core/status.cpp
  src/core/identifiers.cpp
  src/core/digest.cpp
  src/core/random.cpp
  src/core/time_util.cpp
  src/core/json.cpp
  src/core/logging.cpp
  src/core/version.cpp
  src/protocol/frame.cpp
  src/protocol/wire.cpp
  src/protocol/serialize.cpp
  src/protocol/messages.cpp
  src/net/socket.cpp
  src/net/channel.cpp
  src/net/client.cpp
  src/task/task.cpp
  src/task/lifecycle.cpp
  src/task/dag.cpp
  src/task/attempt.cpp
  src/resource/capacity.cpp
  src/resource/reservation.cpp
  src/state/state_locality.cpp
  src/state/context_adapter.cpp
  src/scheduler/cost_model.cpp
  src/scheduler/placements.cpp
  src/scheduler/scheduler.cpp
  src/executor/executor.cpp
  src/executor/cpu_executor.cpp
  src/executor/workloads.cpp
  src/persist/store.cpp
  src/telemetry/telemetry.cpp
  src/coordinator/coordinator.cpp
  src/node/node_runtime.cpp
  src/launcher/launcher.cpp
  src/cli/cli.cpp
  src/cli/benchmark_suite.cpp
)

add_library(compute_fabric STATIC ${CF_CORE_SOURCES})
if(NOT COMPUTE_FABRIC_ENABLE_CUDA)
  target_sources(compute_fabric PRIVATE src/executor/cuda_stubs.cpp)
endif()
target_include_directories(compute_fabric PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(compute_fabric PUBLIC cxx_std_20)

if(WIN32)
  target_link_libraries(compute_fabric PUBLIC ws2_32 bcrypt)
endif()
if(UNIX)
  target_link_libraries(compute_fabric PUBLIC pthread)
endif()

cf_enable_warnings(compute_fabric)

# ---------------------------------------------------------------------------
# CUDA executor library
# ---------------------------------------------------------------------------
if(COMPUTE_FABRIC_ENABLE_CUDA)
  add_library(compute_fabric_cuda STATIC
    src/executor/cuda_executor.cu
  )
  target_include_directories(compute_fabric_cuda PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_compile_features(compute_fabric_cuda PUBLIC cxx_std_20)
  target_link_libraries(compute_fabric_cuda PUBLIC compute_fabric)
  target_link_libraries(compute_fabric_cuda PUBLIC CUDA::cudart)
endif()

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if(COMPUTE_FABRIC_BUILD_CLI)
  add_executable(compute-fabric src/main.cpp)
  target_link_libraries(compute-fabric PRIVATE compute_fabric)
  if(COMPUTE_FABRIC_ENABLE_CUDA)
    target_link_libraries(compute-fabric PRIVATE compute_fabric_cuda)
  endif()
  cf_enable_warnings(compute-fabric)
endif()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(COMPUTE_FABRIC_BUILD_TESTS)
  enable_testing()
  set(CF_TEST_SOURCES
    tests/framework.cpp
    tests/test_main.cpp
    tests/test_identifiers.cpp
    tests/test_lifecycle.cpp
    tests/test_dag.cpp
    tests/test_protocol.cpp
    tests/test_reservation.cpp
    tests/test_placement.cpp
    tests/test_store.cpp
    tests/test_cpu_executor.cpp
    tests/test_cuda_executor.cpp
    tests/test_network.cpp
    tests/test_coordinator.cpp
    tests/test_multi_process.cpp
    tests/test_stress.cpp
  )
  add_executable(compute_fabric_tests ${CF_TEST_SOURCES})
  target_include_directories(compute_fabric_tests PRIVATE tests)
  target_link_libraries(compute_fabric_tests PRIVATE compute_fabric)
  if(COMPUTE_FABRIC_ENABLE_CUDA)
    target_link_libraries(compute_fabric_tests PRIVATE compute_fabric_cuda)
  endif()
  if(MSVC)
    target_link_options(compute_fabric_tests PRIVATE /MAP:$<TARGET_FILE:compute_fabric_tests>.map)
  endif()
  if(TARGET compute-fabric)
    target_compile_definitions(compute_fabric_tests PRIVATE
      CF_CLI_PATH="$<TARGET_FILE:compute-fabric>")
  endif()
  add_test(NAME compute_fabric_tests COMMAND compute_fabric_tests)
  cf_enable_warnings(compute_fabric_tests)
endif()

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------
if(COMPUTE_FABRIC_BUILD_BENCHMARKS)
add_executable(compute_fabric_benchmarks benchmarks/bench_main.cpp)
target_link_libraries(compute_fabric_benchmarks PRIVATE compute_fabric)
if(COMPUTE_FABRIC_ENABLE_CUDA)
  target_link_libraries(compute_fabric_benchmarks PRIVATE compute_fabric_cuda)
endif()
if(TARGET compute-fabric)
  target_compile_definitions(compute_fabric_benchmarks PRIVATE
    CF_CLI_PATH="$<TARGET_FILE:compute-fabric>")
endif()
cf_enable_warnings(compute_fabric_benchmarks)
endif()

# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------
if(COMPUTE_FABRIC_BUILD_EXAMPLES)
  set(CF_EXAMPLES
    example_minimal_task
    example_multi_node_placement
    example_state_locality
    example_retry_on_node_loss
    example_priority
    example_persistence_recovery
    example_cuda_task
    example_cpu_vs_cuda
    example_transfer_vs_recompute_execution
  )
  foreach(ex IN LISTS CF_EXAMPLES)
    add_executable(${ex} examples/${ex}/${ex}.cpp)
    target_link_libraries(${ex} PRIVATE compute_fabric)
    if(COMPUTE_FABRIC_ENABLE_CUDA)
      target_link_libraries(${ex} PRIVATE compute_fabric_cuda)
    endif()
    if(TARGET compute-fabric)
      target_compile_definitions(${ex} PRIVATE
        CF_CLI_PATH="$<TARGET_FILE:compute-fabric>")
    endif()
    cf_enable_warnings(${ex})
  endforeach()
endif()
