cmake_minimum_required(VERSION 3.20)

project(FlumeHostBypassDataPath VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(FLUME_ENABLE_HCCL "Enable the future HCCL/HCOMM-backed transport." OFF)
option(FLUME_BUILD_TESTS "Build tests." ON)
option(FLUME_BUILD_HCOMM_CUSTOM_OP "Build experimental HCOMM custom-op/AICPU payload scheduler." OFF)

find_package(Threads REQUIRED)

add_library(flume STATIC
  src/agent/storage_agent.cc
  src/core/client.cc
  src/hcomm_payload/payload_backend.cc
  src/protocol/framing.cc
  src/storage/posix_file.cc
)

target_include_directories(flume
  PUBLIC
    ${PROJECT_SOURCE_DIR}/include
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(flume PUBLIC Threads::Threads)
target_compile_definitions(flume PUBLIC
  FLUME_BUILD_HCOMM_CUSTOM_OP=$<BOOL:${FLUME_BUILD_HCOMM_CUSTOM_OP}>
)

if(FLUME_ENABLE_HCCL)
  include(CheckCXXSourceCompiles)

  find_path(HCCL_INCLUDE_DIR
    NAMES hccl/hccl.h hccl.h hccl/hccl_types.h
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES hccl/include include include/external
  )
  find_library(HCCL_LIBRARY
    NAMES hccl
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES hccl/lib64 lib64 lib
  )
  find_path(HCOMM_INCLUDE_DIR
    NAMES hccl/hccl_comm.h
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES hcomm/include hccl/include include include/external
  )
  find_library(HCOMM_LIBRARY
    NAMES hcomm
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES hcomm/lib64 hccl/lib64 lib64 lib
  )
  find_path(ACL_INCLUDE_DIR
    NAMES acl/acl.h
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES include include/external runtime/include/external
  )
  find_path(SECUREC_INCLUDE_DIR
    NAMES securec.h
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES include include/external runtime/include/external
  )
  find_library(SECUREC_LIBRARY
    NAMES c_sec libc_sec securec
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES lib64 lib
  )
  find_library(ASCENDCL_LIBRARY
    NAMES ascendcl
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES lib64 lib
  )

  if(NOT HCCL_INCLUDE_DIR OR NOT HCCL_LIBRARY OR
     NOT HCOMM_INCLUDE_DIR OR NOT HCOMM_LIBRARY OR
     NOT ACL_INCLUDE_DIR OR NOT SECUREC_INCLUDE_DIR OR NOT SECUREC_LIBRARY OR
     NOT ASCENDCL_LIBRARY)
    message(FATAL_ERROR
      "FLUME_ENABLE_HCCL=ON requires a CANN/HCCL/HCOMM installation. "
      "Set ASCEND_HOME_PATH or pass HCCL_INCLUDE_DIR/HCCL_LIBRARY/"
      "HCOMM_INCLUDE_DIR/HCOMM_LIBRARY/ACL_INCLUDE_DIR/"
      "SECUREC_INCLUDE_DIR/SECUREC_LIBRARY/ASCENDCL_LIBRARY.")
  endif()

  set(_FLUME_HCCL_CHECK_INCLUDE "
#include <acl/acl.h>
#if __has_include(<hccl/hccl.h>)
#include <hccl/hccl.h>
#elif __has_include(<hccl.h>)
#include <hccl.h>
#else
#error no hccl header
#endif
")
  set(_FLUME_HCOMM_RES_CHECK_INCLUDE "
${_FLUME_HCCL_CHECK_INCLUDE}
#if __has_include(<hccl/hccl_res.h>)
#include <hccl/hccl_res.h>
#else
#error no hccl_res header
#endif
")
  set(_FLUME_HCOMM_RES_EXPT_CHECK_INCLUDE "
${_FLUME_HCOMM_RES_CHECK_INCLUDE}
#if __has_include(<hccl/hccl_res_expt.h>)
#include <hccl/hccl_res_expt.h>
#else
#error no hccl_res_expt header
#endif
")
  set(_FLUME_HCOMM_PRIMITIVES_CHECK_INCLUDE "
${_FLUME_HCOMM_RES_CHECK_INCLUDE}
#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#elif __has_include(<hcomm_primitives.h>)
#include <hcomm_primitives.h>
#else
#error no hcomm_primitives header
#endif
")
  set(_FLUME_HCOMM_RANK_GRAPH_CHECK_INCLUDE "
${_FLUME_HCOMM_RES_CHECK_INCLUDE}
#if __has_include(<hccl/hccl_rank_graph.h>)
#include <hccl/hccl_rank_graph.h>
#else
#error no hccl_rank_graph header
#endif
")
  set(_FLUME_HCCL_AICPU_KERNEL_LAUNCH_CHECK_INCLUDE "
${_FLUME_HCOMM_RES_CHECK_INCLUDE}
#if __has_include(<hccl/hccl_launch.h>)
#include <hccl/hccl_launch.h>
#else
#error no hccl_launch header
#endif
")
  set(_FLUME_OLD_CMAKE_REQUIRED_INCLUDES "${CMAKE_REQUIRED_INCLUDES}")
  set(_FLUME_OLD_CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES}")
  set(_FLUME_OLD_TRY_COMPILE_TARGET_TYPE "${CMAKE_TRY_COMPILE_TARGET_TYPE}")
  set(CMAKE_REQUIRED_INCLUDES
    ${HCCL_INCLUDE_DIR}
    ${HCOMM_INCLUDE_DIR}
    ${ACL_INCLUDE_DIR}
    ${SECUREC_INCLUDE_DIR}
  )
  set(CMAKE_REQUIRED_LIBRARIES
    ${HCCL_LIBRARY}
    ${HCOMM_LIBRARY}
    ${ASCENDCL_LIBRARY}
    ${SECUREC_LIBRARY}
  )
  set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclComm comm = nullptr;
  HcclRootInfo root_info = {};
  (void)HcclCommInitRootInfo(1, &root_info, 0, &comm);
  return 0;
}
" FLUME_HAVE_HCCL_ROOT_INFO)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclCommConfig config;
  HcclCommConfigInit(&config);
  HcclComm comm = nullptr;
  HcclRootInfo root_info = {};
  (void)HcclCommInitRootInfoConfig(1, &root_info, 0, &config, &comm);
  return 0;
}
" FLUME_HAVE_HCCL_ROOT_INFO_CONFIG)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  int32_t devices[1] = {0};
  HcclComm comms[1] = {nullptr};
  (void)HcclCommInitAll(1, devices, comms);
  return 0;
}
" FLUME_HAVE_HCCL_COMM_INIT_ALL)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclCommConfig config;
  HcclCommConfigInit(&config);
  config.hcclBufferSize = 50;
  return 0;
}
" FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclComm comm = nullptr;
  (void)HcclCommInitClusterInfo(\"rank_table.json\", 0, &comm);
  return 0;
}
" FLUME_HAVE_HCCL_CLUSTER_INFO)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclCommConfig config;
  HcclCommConfigInit(&config);
  HcclComm comm = nullptr;
  (void)HcclCommInitClusterInfoConfig(\"rank_table.json\", 0, &config, &comm);
  return 0;
}
" FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclCommConfig config;
  HcclCommConfigInit(&config);
  config.hcclSymWinMaxMemSizePerRank = 1;
  return 0;
}
" FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclCommSymWindow win = nullptr;
  (void)HcclCommSymWinRegister(nullptr, nullptr, 0, &win, 1);
  (void)HcclCommSymWinDeregister(win);
  return 0;
}
" FLUME_HAVE_HCCL_SYM_WINDOW)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  aclrtDrvMemHandle handle = nullptr;
  (void)HcclCommSetMemoryRange(nullptr, nullptr, 0, 0, 0);
  (void)HcclCommUnsetMemoryRange(nullptr, nullptr);
  (void)HcclCommActivateCommMemory(nullptr, nullptr, 0, 0, handle, 0);
  (void)HcclCommDeactivateCommMemory(nullptr, nullptr);
  return 0;
}
" FLUME_HAVE_HCCL_COMM_MEMORY)

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclComm comm = nullptr;
  aclrtStream stream = nullptr;
  (void)HcclSend(nullptr, 0, HCCL_DATA_TYPE_FP32, 1, comm, stream);
  (void)HcclRecv(nullptr, 0, HCCL_DATA_TYPE_FP32, 0, comm, stream);
  return 0;
}
" FLUME_HAVE_HCCL_P2P)

  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
  check_cxx_source_compiles("
${_FLUME_HCOMM_RES_CHECK_INCLUDE}
int main() {
  static_assert(sizeof(CommEngine) > 0, \"CommEngine missing\");
  static_assert(sizeof(CommProtocol) > 0, \"CommProtocol missing\");
  static_assert(sizeof(HcclChannelDesc) > 0, \"HcclChannelDesc missing\");
  static_assert(sizeof(ThreadHandle) > 0, \"ThreadHandle missing\");
  static_assert(sizeof(ChannelHandle) > 0, \"ChannelHandle missing\");
  CommEngine engine = COMM_ENGINE_CPU_TS;
  CommProtocol protocol = COMM_PROTOCOL_HCCS;
  auto get_buffer = &HcclGetHcclBuffer;
  auto acquire_thread = &HcclThreadAcquire;
  auto acquire_thread_with_stream = &HcclThreadAcquireWithStream;
  auto init_desc = &HcclChannelDescInit;
  auto acquire_channel = &HcclChannelAcquire;
  auto get_channel_buffer = &HcclChannelGetHcclBuffer;
  (void)engine;
  (void)protocol;
  (void)get_buffer;
  (void)acquire_thread;
  (void)acquire_thread_with_stream;
  (void)init_desc;
  (void)acquire_channel;
  (void)get_channel_buffer;
  return 0;
}
" FLUME_HAVE_HCOMM_CHANNEL_RES)

  set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)
  check_cxx_source_compiles("
${_FLUME_HCOMM_RES_EXPT_CHECK_INCLUDE}
int main() {
  HcclComm comm = nullptr;
  ThreadHandle thread = 0;
  ThreadHandle exported_thread = 0;
  (void)HcclThreadExportToCommEngine(
      comm, 1, &thread, COMM_ENGINE_AICPU_TS, &exported_thread);
  return 0;
}
" FLUME_HAVE_HCOMM_THREAD_EXPORT)

  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
  check_cxx_source_compiles("
${_FLUME_HCOMM_PRIMITIVES_CHECK_INCLUDE}
int main() {
  ThreadHandle thread = 0;
  ChannelHandle channel = 0;
  void *ptr = nullptr;
  auto local_copy = &HcommLocalCopyOnThread;
  auto read = &HcommReadOnThread;
  auto notify_record = &HcommChannelNotifyRecordOnThread;
  auto notify_wait = &HcommChannelNotifyWaitOnThread;
  auto drain = &HcommChannelDrainOnThread;
  (void)thread;
  (void)channel;
  (void)ptr;
  (void)local_copy;
  (void)read;
  (void)notify_record;
  (void)notify_wait;
  (void)drain;
  return 0;
}
" FLUME_HAVE_HCOMM_PRIMITIVES)

  check_cxx_source_compiles("
${_FLUME_HCOMM_RANK_GRAPH_CHECK_INCLUDE}
int main() {
  static_assert(sizeof(CommLink) > 0, \"CommLink missing\");
  auto get_layers = &HcclRankGraphGetLayers;
  auto get_links = &HcclRankGraphGetLinks;
  CommLink link;
  CommLinkInit(&link, 1);
  (void)get_layers;
  (void)get_links;
  (void)link;
  return 0;
}
" FLUME_HAVE_HCOMM_RANK_GRAPH)

  set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)
  check_cxx_source_compiles("
${_FLUME_HCCL_AICPU_KERNEL_LAUNCH_CHECK_INCLUDE}
int main() {
  HcclOpDesc op_desc;
  HcclKernelFuncInfo func_info = {};
  HcclKernelLaunchCfg launch_cfg;
  ThreadHandle thread = 0;
  aclrtStream stream = nullptr;
  HcclOpDescInit(&op_desc);
  HcclKernelLaunchCfgInit(&launch_cfg);
  (void)HcclAicpuKernelLaunch(nullptr, &op_desc, &func_info, thread,
                              stream, &launch_cfg);
  return 0;
}
" FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH)

  check_cxx_source_compiles("
#include <acl/acl.h>
int main() {
  aclrtBinHandle bin_handle = nullptr;
  aclrtFuncHandle func_handle = nullptr;
  aclrtArgsHandle args_handle = nullptr;
  aclrtParamHandle param_handle = nullptr;
  aclrtBinaryLoadOption option = {};
  option.type = ACL_RT_BINARY_LOAD_OPT_CPU_KERNEL_MODE;
  option.value.cpuKernelMode = 0;
  aclrtBinaryLoadOptions load_options = {};
  load_options.options = &option;
  load_options.numOpt = 1;
  (void)aclrtBinaryLoadFromFile(\"kernel.json\", &load_options, &bin_handle);
  (void)aclrtBinaryGetFunction(bin_handle, \"FlumeHcommNotifyOnlyAicpuKernel\", &func_handle);
  (void)aclrtKernelArgsInit(func_handle, &args_handle);
  (void)aclrtKernelArgsAppend(args_handle, nullptr, 0, &param_handle);
  (void)aclrtKernelArgsFinalize(args_handle);
  aclrtLaunchKernelAttr attr = {};
  attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
  attr.value.timeout = 1;
  aclrtLaunchKernelCfg cfg = {};
  cfg.attrs = &attr;
  cfg.numAttrs = 1;
  (void)aclrtLaunchKernelWithConfig(func_handle, 1, nullptr, &cfg, args_handle, nullptr);
  return 0;
}
" FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH)

  set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)
  check_cxx_source_compiles("
#include <acl/acl.h>
int main() {
  aclrtStream stream = nullptr;
  (void)aclrtSynchronizeStreamWithTimeout(stream, 0);
  return 0;
}
" FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT)

  check_cxx_source_compiles("
#include <acl/acl.h>
int main() {
  int32_t phy_device_id = 0;
  (void)aclrtGetPhyDevIdByLogicDevId(0, &phy_device_id);
  return 0;
}
" FLUME_HAVE_ACL_PHY_DEVICE_ID)

  check_cxx_source_compiles("
#include <acl/acl.h>
int main() {
  aclrtPhysicalMemProp prop = {};
  prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
  prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
  prop.memAttr = ACL_HBM_MEM_HUGE;
  prop.location.id = 0;
  prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
  size_t granularity = 0;
  aclrtDrvMemHandle handle = nullptr;
  void *base = nullptr;
  (void)aclrtMemGetAllocationGranularity(
      &prop, ACL_RT_MEM_ALLOC_GRANULARITY_RECOMMENDED, &granularity);
  (void)aclrtReserveMemAddress(&base, granularity, 0, nullptr, 1);
  (void)aclrtMallocPhysical(&handle, granularity, &prop, 0);
  (void)aclrtMapMem(base, granularity, 0, handle, 0);
  (void)aclrtUnmapMem(base);
  (void)aclrtFreePhysical(handle);
  (void)aclrtReleaseMemAddress(base);
  return 0;
}
" FLUME_HAVE_ACL_VMM)

  set(CMAKE_REQUIRED_INCLUDES "${_FLUME_OLD_CMAKE_REQUIRED_INCLUDES}")
  set(CMAKE_REQUIRED_LIBRARIES "${_FLUME_OLD_CMAKE_REQUIRED_LIBRARIES}")
  set(CMAKE_TRY_COMPILE_TARGET_TYPE "${_FLUME_OLD_TRY_COMPILE_TARGET_TYPE}")

  foreach(_FLUME_FEATURE
      FLUME_HAVE_HCCL_ROOT_INFO
      FLUME_HAVE_HCCL_ROOT_INFO_CONFIG
      FLUME_HAVE_HCCL_COMM_INIT_ALL
      FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE
      FLUME_HAVE_HCCL_CLUSTER_INFO
      FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG
      FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW
      FLUME_HAVE_HCCL_SYM_WINDOW
      FLUME_HAVE_HCCL_COMM_MEMORY
      FLUME_HAVE_HCCL_P2P
      FLUME_HAVE_HCOMM_CHANNEL_RES
      FLUME_HAVE_HCOMM_THREAD_EXPORT
      FLUME_HAVE_HCOMM_PRIMITIVES
      FLUME_HAVE_HCOMM_RANK_GRAPH
      FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH
      FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH
      FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT
      FLUME_HAVE_ACL_PHY_DEVICE_ID
      FLUME_HAVE_ACL_VMM)
    if(DEFINED ${_FLUME_FEATURE})
      set(_FLUME_FEATURE_RESULT "${${_FLUME_FEATURE}}")
    else()
      set(_FLUME_FEATURE_RESULT "")
    endif()
    if(_FLUME_FEATURE_RESULT)
      set(${_FLUME_FEATURE}_VALUE 1)
    else()
      set(${_FLUME_FEATURE}_VALUE 0)
    endif()
    message(STATUS "${_FLUME_FEATURE}: ${${_FLUME_FEATURE}_VALUE}")
  endforeach()

  target_include_directories(flume PRIVATE
    ${HCCL_INCLUDE_DIR}
    ${HCOMM_INCLUDE_DIR}
    ${ACL_INCLUDE_DIR}
    ${SECUREC_INCLUDE_DIR}
  )
  target_link_libraries(flume PUBLIC
    ${HCCL_LIBRARY}
    ${HCOMM_LIBRARY}
    ${ASCENDCL_LIBRARY}
    ${SECUREC_LIBRARY}
  )
  target_compile_definitions(flume PUBLIC
    FLUME_ENABLE_HCCL=1
    FLUME_HAVE_HCCL_ROOT_INFO=${FLUME_HAVE_HCCL_ROOT_INFO_VALUE}
    FLUME_HAVE_HCCL_ROOT_INFO_CONFIG=${FLUME_HAVE_HCCL_ROOT_INFO_CONFIG_VALUE}
    FLUME_HAVE_HCCL_COMM_INIT_ALL=${FLUME_HAVE_HCCL_COMM_INIT_ALL_VALUE}
    FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE=${FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE_VALUE}
    FLUME_HAVE_HCCL_CLUSTER_INFO=${FLUME_HAVE_HCCL_CLUSTER_INFO_VALUE}
    FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG=${FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG_VALUE}
    FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW=${FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW_VALUE}
    FLUME_HAVE_HCCL_SYM_WINDOW=${FLUME_HAVE_HCCL_SYM_WINDOW_VALUE}
    FLUME_HAVE_HCCL_COMM_MEMORY=${FLUME_HAVE_HCCL_COMM_MEMORY_VALUE}
    FLUME_HAVE_HCCL_P2P=${FLUME_HAVE_HCCL_P2P_VALUE}
    FLUME_HAVE_HCOMM_CHANNEL_RES=${FLUME_HAVE_HCOMM_CHANNEL_RES_VALUE}
    FLUME_HAVE_HCOMM_THREAD_EXPORT=${FLUME_HAVE_HCOMM_THREAD_EXPORT_VALUE}
    FLUME_HAVE_HCOMM_PRIMITIVES=${FLUME_HAVE_HCOMM_PRIMITIVES_VALUE}
    FLUME_HAVE_HCOMM_RANK_GRAPH=${FLUME_HAVE_HCOMM_RANK_GRAPH_VALUE}
    FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH=${FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH_VALUE}
    FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH=${FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH_VALUE}
    FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT=${FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT_VALUE}
    FLUME_HAVE_ACL_PHY_DEVICE_ID=${FLUME_HAVE_ACL_PHY_DEVICE_ID_VALUE}
    FLUME_HAVE_ACL_VMM=${FLUME_HAVE_ACL_VMM_VALUE}
  )
else()
  target_compile_definitions(flume PUBLIC
    FLUME_ENABLE_HCCL=0
    FLUME_HAVE_HCCL_ROOT_INFO=0
    FLUME_HAVE_HCCL_ROOT_INFO_CONFIG=0
    FLUME_HAVE_HCCL_COMM_INIT_ALL=0
    FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE=0
    FLUME_HAVE_HCCL_CLUSTER_INFO=0
    FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG=0
    FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW=0
    FLUME_HAVE_HCCL_SYM_WINDOW=0
    FLUME_HAVE_HCCL_COMM_MEMORY=0
    FLUME_HAVE_HCCL_P2P=0
    FLUME_HAVE_HCOMM_CHANNEL_RES=0
    FLUME_HAVE_HCOMM_THREAD_EXPORT=0
    FLUME_HAVE_HCOMM_PRIMITIVES=0
    FLUME_HAVE_HCOMM_RANK_GRAPH=0
    FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH=0
    FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH=0
    FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT=0
    FLUME_HAVE_ACL_PHY_DEVICE_ID=0
    FLUME_HAVE_ACL_VMM=0
  )
endif()

target_include_directories(flume PRIVATE
  ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/include
)

add_executable(flume-store-agent apps/flume-store-agent.cc)
target_link_libraries(flume-store-agent PRIVATE flume)
target_include_directories(flume-store-agent PRIVATE ${PROJECT_SOURCE_DIR}/src)

add_executable(flume-demo-client apps/flume-demo-client.cc)
target_link_libraries(flume-demo-client PRIVATE flume)

add_executable(flume-sim-demo apps/flume-sim-demo.cc)
target_link_libraries(flume-sim-demo PRIVATE flume)
target_include_directories(flume-sim-demo PRIVATE ${PROJECT_SOURCE_DIR}/src)

add_executable(flume-sim-collective-demo apps/flume-sim-collective-demo.cc)
target_link_libraries(flume-sim-collective-demo PRIVATE flume)
target_include_directories(flume-sim-collective-demo PRIVATE ${PROJECT_SOURCE_DIR}/src)

if(FLUME_ENABLE_HCCL)
  add_executable(flume-hccl-collective-smoke apps/flume-hccl-collective-smoke.cc)
  target_link_libraries(flume-hccl-collective-smoke PRIVATE flume)
  target_include_directories(flume-hccl-collective-smoke
    PRIVATE
      ${PROJECT_SOURCE_DIR}/src
      ${HCCL_INCLUDE_DIR}
      ${HCOMM_INCLUDE_DIR}
      ${ACL_INCLUDE_DIR}
      ${SECUREC_INCLUDE_DIR}
  )
endif()

if(FLUME_BUILD_TESTS)
  enable_testing()

  add_executable(test_protocol tests/test_protocol.cc)
  target_link_libraries(test_protocol PRIVATE flume)
  target_include_directories(test_protocol PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME protocol COMMAND test_protocol)

  add_executable(test_mock_pread tests/test_mock_pread.cc)
  target_link_libraries(test_mock_pread PRIVATE flume)
  target_include_directories(test_mock_pread PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME mock_pread COMMAND test_mock_pread)

  add_executable(test_sim_end_to_end tests/test_sim_end_to_end.cc)
  target_link_libraries(test_sim_end_to_end PRIVATE flume)
  target_include_directories(test_sim_end_to_end PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_end_to_end COMMAND test_sim_end_to_end)

  add_executable(test_sim_collectives tests/test_sim_collectives.cc)
  target_link_libraries(test_sim_collectives PRIVATE flume)
  target_include_directories(test_sim_collectives PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_collectives COMMAND test_sim_collectives)

  add_executable(test_sim_p2p_copy tests/test_sim_p2p_copy.cc)
  target_link_libraries(test_sim_p2p_copy PRIVATE flume)
  target_include_directories(test_sim_p2p_copy PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_p2p_copy COMMAND test_sim_p2p_copy)

  add_executable(test_backend_caps tests/test_backend_caps.cc)
  target_link_libraries(test_backend_caps PRIVATE flume)
  target_include_directories(test_backend_caps PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME backend_caps COMMAND test_backend_caps)

  add_executable(test_sim_hcomm_payload_copy tests/test_sim_hcomm_payload_copy.cc)
  target_link_libraries(test_sim_hcomm_payload_copy PRIVATE flume)
  target_include_directories(test_sim_hcomm_payload_copy PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_hcomm_payload_copy COMMAND test_sim_hcomm_payload_copy)

  add_executable(test_sim_hcomm_payload_failures tests/test_sim_hcomm_payload_failures.cc)
  target_link_libraries(test_sim_hcomm_payload_failures PRIVATE flume)
  target_include_directories(test_sim_hcomm_payload_failures PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_hcomm_payload_failures COMMAND test_sim_hcomm_payload_failures)

  add_executable(test_hcomm_payload_plan tests/test_hcomm_payload_plan.cc)
  target_link_libraries(test_hcomm_payload_plan PRIVATE flume)
  target_include_directories(test_hcomm_payload_plan PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME hcomm_payload_plan COMMAND test_hcomm_payload_plan)

  add_executable(test_sim_collective_failures tests/test_sim_collective_failures.cc)
  target_link_libraries(test_sim_collective_failures PRIVATE flume)
  target_include_directories(test_sim_collective_failures PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_collective_failures COMMAND test_sim_collective_failures)

  add_executable(test_sim_a3_symmetric_memory tests/test_sim_a3_symmetric_memory.cc)
  target_link_libraries(test_sim_a3_symmetric_memory PRIVATE flume)
  target_include_directories(test_sim_a3_symmetric_memory PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_a3_symmetric_memory COMMAND test_sim_a3_symmetric_memory)
endif()
