cmake_minimum_required(VERSION 3.20)

project(FlumeHostBypassDataPath VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(FLUME_ENABLE_HCCL "Enable the future HCCL/HCOMM-backed transport." OFF)
option(FLUME_BUILD_TESTS "Build tests." ON)
option(FLUME_BUILD_HCOMM_CUSTOM_OP "Build experimental HCOMM custom-op/AICPU payload scheduler." OFF)
option(FLUME_ENABLE_ROCE_STORAGE "Enable the experimental NPU-RNIC RoCE storage API surface." OFF)
option(FLUME_ENABLE_NATIVE_NVME_HBM_CANARY
  "Build the isolated Linux NVMe namespace to NPU HBM passthrough canary." OFF)

find_package(Threads REQUIRED)

find_path(IBVERBS_INCLUDE_DIR NAMES infiniband/verbs.h)
find_library(IBVERBS_LIBRARY NAMES ibverbs)
if(IBVERBS_INCLUDE_DIR AND IBVERBS_LIBRARY)
  set(FLUME_HAVE_IBVERBS 1)
else()
  set(FLUME_HAVE_IBVERBS 0)
endif()

add_library(flume STATIC
  src/agent/storage_agent.cc
  src/core/client.cc
  src/hcomm_payload/payload_backend.cc
  src/nvme_storage/nvme_hbm_canary.cc
  src/protocol/framing.cc
  src/roce_storage/control_channel.cc
  src/roce_storage/control_proxy.cc
  src/roce_storage/roce_storage.cc
  src/roce_storage/cann_ra_loader.cc
  src/roce_storage/host_ra_session.cc
  src/roce_storage/npu_ra_push_mover.cc
  src/roce_storage/storage_backend.cc
  src/roce_storage/verbs_backend.cc
  src/storage/posix_file.cc
)

target_include_directories(flume
  PUBLIC
    ${PROJECT_SOURCE_DIR}/include
  PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(flume PUBLIC Threads::Threads)
if(CMAKE_DL_LIBS)
  target_link_libraries(flume PRIVATE ${CMAKE_DL_LIBS})
endif()
target_compile_definitions(flume PUBLIC
  FLUME_BUILD_HCOMM_CUSTOM_OP=$<BOOL:${FLUME_BUILD_HCOMM_CUSTOM_OP}>
  FLUME_ENABLE_ROCE_STORAGE=$<BOOL:${FLUME_ENABLE_ROCE_STORAGE}>
  FLUME_HAVE_IBVERBS=${FLUME_HAVE_IBVERBS}
)

if(FLUME_HAVE_IBVERBS)
  target_include_directories(flume PRIVATE ${IBVERBS_INCLUDE_DIR})
  target_link_libraries(flume PUBLIC ${IBVERBS_LIBRARY})
endif()

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
    PATH_SUFFIXES aarch64-linux/include include include/external
                  runtime/include/external
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
    PATH_SUFFIXES aarch64-linux/lib64 lib64 lib
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
  set(_FLUME_HCOMM_COMM_CHECK_INCLUDE "
${_FLUME_HCCL_CHECK_INCLUDE}
#if __has_include(<hccl/hccl_comm.h>)
#include <hccl/hccl_comm.h>
#else
#error no hccl_comm header
#endif
")
  set(_FLUME_HCOMM_RES_EXPT_CHECK_INCLUDE "
${_FLUME_HCOMM_RES_CHECK_INCLUDE}
#if __has_include(<hccl/hccl_res_expt.h>)
#include <hccl/hccl_res_expt.h>
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
  set(_FLUME_OLD_CMAKE_REQUIRED_LINK_OPTIONS "${CMAKE_REQUIRED_LINK_OPTIONS}")
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
  if(UNIX AND NOT APPLE)
    set(CMAKE_REQUIRED_LINK_OPTIONS
      ${CMAKE_REQUIRED_LINK_OPTIONS}
      -Wl,--allow-shlib-undefined
    )
    target_link_options(flume PUBLIC -Wl,--allow-shlib-undefined)
  endif()
  set(_FLUME_HCCL_REQUIRED_LINK_OPTIONS "${CMAKE_REQUIRED_LINK_OPTIONS}")
  set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)

  check_cxx_source_compiles("
${_FLUME_HCOMM_COMM_CHECK_INCLUDE}
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

  check_cxx_source_compiles("
${_FLUME_HCCL_CHECK_INCLUDE}
int main() {
  HcclComm comm = nullptr;
  char comm_name[128] = {};
  (void)HcclGetCommName(comm, comm_name);
  return 0;
}
" FLUME_HAVE_HCCL_COMM_NAME)

  set(CMAKE_REQUIRED_LINK_OPTIONS)
  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
  if(NOT FLUME_HCOMM_PRIMITIVE_PROBE_REVISION STREQUAL "2")
    unset(FLUME_HAVE_HCOMM_PRIMITIVES CACHE)
    unset(FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD CACHE)
    unset(FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY CACHE)
    set(FLUME_HCOMM_PRIMITIVE_PROBE_REVISION "2" CACHE INTERNAL
      "Flume HCOMM primitive capability probe revision" FORCE)
  endif()
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

  set(CMAKE_REQUIRED_LINK_OPTIONS "${_FLUME_HCCL_REQUIRED_LINK_OPTIONS}")
  set(CMAKE_TRY_COMPILE_TARGET_TYPE EXECUTABLE)
  if(NOT FLUME_HCOMM_THREAD_EXPORT_PROBE_REVISION STREQUAL "2")
    unset(FLUME_HAVE_HCOMM_THREAD_EXPORT CACHE)
    set(FLUME_HCOMM_THREAD_EXPORT_PROBE_REVISION "2" CACHE INTERNAL
      "Flume HCOMM thread-export capability probe revision" FORCE)
  endif()
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

  set(CMAKE_REQUIRED_LINK_OPTIONS)
  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
  check_cxx_source_compiles("
${_FLUME_HCOMM_PRIMITIVES_CHECK_INCLUDE}
#include <cstdint>
int main() {
  ThreadHandle thread = 0;
  ThreadHandle peer_thread = 0;
  ChannelHandle channel = 0;
  void *dst = nullptr;
  const void *src = nullptr;
  const char *tag = \"flume-probe\";
  uint64_t bytes = 64;
  uint32_t notify_idx = 0;
  uint32_t timeout_sec = 1;
  auto acquire_ret = HcommAcquireComm(tag);
  auto local_copy_ret = HcommLocalCopyOnThread(thread, dst, src, bytes);
  auto read_ret = HcommReadOnThread(thread, channel, dst, src, bytes);
  auto write_ret = HcommWriteOnThread(thread, channel, dst, src, bytes);
  auto record_ret =
      HcommChannelNotifyRecordOnThread(thread, channel, notify_idx);
  auto wait_ret =
      HcommChannelNotifyWaitOnThread(thread, channel, notify_idx, timeout_sec);
  auto thread_record_ret =
      HcommThreadNotifyRecordOnThread(thread, peer_thread, notify_idx);
  auto thread_wait_ret =
      HcommThreadNotifyWaitOnThread(thread, notify_idx, timeout_sec);
  auto batch_start_ret = HcommBatchModeStart(tag);
  auto batch_end_ret = HcommBatchModeEnd(tag);
  auto release_ret = HcommReleaseComm(tag);
  (void)acquire_ret;
  (void)local_copy_ret;
  (void)read_ret;
  (void)write_ret;
  (void)record_ret;
  (void)wait_ret;
  (void)thread_record_ret;
  (void)thread_wait_ret;
  (void)batch_start_ret;
  (void)batch_end_ret;
  (void)release_ret;
  return 0;
}
" FLUME_HAVE_HCOMM_PRIMITIVES)

  check_cxx_source_compiles("
${_FLUME_HCOMM_PRIMITIVES_CHECK_INCLUDE}
int main() {
  ThreadHandle thread = 0;
  ChannelHandle channel = 0;
  auto ret = HcommChannelFenceOnThread(thread, channel);
  (void)ret;
  return 0;
}
" FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD)

  check_cxx_source_compiles("
${_FLUME_HCOMM_PRIMITIVES_CHECK_INCLUDE}
int main() {
  ChannelHandle channel = 0;
  auto ret = HcommChannelFence(channel);
  (void)ret;
  return 0;
}
" FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY)

  check_cxx_source_compiles("
${_FLUME_HCOMM_PRIMITIVES_CHECK_INCLUDE}
#include <cstdint>
int main() {
  ThreadHandle thread = 0;
  ChannelHandle channel = 0;
  void *dst = nullptr;
  const void *src = nullptr;
  uint64_t bytes = 64;
  uint32_t notify_idx = 0;
  auto ret =
      HcommWriteWithNotifyOnThread(thread, channel, dst, src, bytes,
                                   notify_idx);
  (void)ret;
  return 0;
}
" FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY)

  set(FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_SYMBOL 0)
  if(FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY)
    set(_FLUME_NM_EXECUTABLE "${CMAKE_NM}")
    if(NOT _FLUME_NM_EXECUTABLE)
      find_program(_FLUME_NM_EXECUTABLE NAMES nm llvm-nm)
    endif()
    if(_FLUME_NM_EXECUTABLE AND HCOMM_LIBRARY)
      execute_process(
        COMMAND ${_FLUME_NM_EXECUTABLE} -D ${HCOMM_LIBRARY}
        OUTPUT_VARIABLE _FLUME_HCOMM_NM_OUTPUT
        ERROR_QUIET
        RESULT_VARIABLE _FLUME_HCOMM_NM_RESULT)
      if(NOT _FLUME_HCOMM_NM_RESULT EQUAL 0)
        execute_process(
          COMMAND ${_FLUME_NM_EXECUTABLE} -g ${HCOMM_LIBRARY}
          OUTPUT_VARIABLE _FLUME_HCOMM_NM_OUTPUT
          ERROR_QUIET
          RESULT_VARIABLE _FLUME_HCOMM_NM_RESULT)
      endif()
      if(_FLUME_HCOMM_NM_RESULT EQUAL 0 AND
         _FLUME_HCOMM_NM_OUTPUT MATCHES
         "(^|[\n\r\t ])HcommWriteWithNotifyOnThread([\n\r\t ]|$)")
        set(FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_SYMBOL 1)
      endif()
    endif()
    if(NOT FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_SYMBOL)
      set(FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY 0)
    endif()
  endif()
  message(STATUS
    "FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_SYMBOL: "
    "${FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_SYMBOL}")

  check_cxx_source_compiles("
${_FLUME_HCOMM_PRIMITIVES_CHECK_INCLUDE}
#include <cstdint>
int main() {
  ThreadHandle thread = 0;
  ChannelHandle channel = 0;
  void *dst = nullptr;
  const void *src = nullptr;
  uint64_t bytes = 64;
  uint32_t notify_idx = 0;
  auto ret =
      HcommWriteWithNotifyNbiOnThread(thread, channel, dst, src, bytes,
                                      notify_idx);
  (void)ret;
  return 0;
}
" FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI)

  set(FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI_SYMBOL 0)
  if(FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI)
    set(_FLUME_NM_EXECUTABLE "${CMAKE_NM}")
    if(NOT _FLUME_NM_EXECUTABLE)
      find_program(_FLUME_NM_EXECUTABLE NAMES nm llvm-nm)
    endif()
    if(_FLUME_NM_EXECUTABLE AND HCOMM_LIBRARY)
      execute_process(
        COMMAND ${_FLUME_NM_EXECUTABLE} -D ${HCOMM_LIBRARY}
        OUTPUT_VARIABLE _FLUME_HCOMM_NBI_NM_OUTPUT
        ERROR_QUIET
        RESULT_VARIABLE _FLUME_HCOMM_NBI_NM_RESULT)
      if(NOT _FLUME_HCOMM_NBI_NM_RESULT EQUAL 0)
        execute_process(
          COMMAND ${_FLUME_NM_EXECUTABLE} -g ${HCOMM_LIBRARY}
          OUTPUT_VARIABLE _FLUME_HCOMM_NBI_NM_OUTPUT
          ERROR_QUIET
          RESULT_VARIABLE _FLUME_HCOMM_NBI_NM_RESULT)
      endif()
      if(_FLUME_HCOMM_NBI_NM_RESULT EQUAL 0 AND
         _FLUME_HCOMM_NBI_NM_OUTPUT MATCHES
         "(^|[\n\r\t ])HcommWriteWithNotifyNbiOnThread([\n\r\t ]|$)")
        set(FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI_SYMBOL 1)
      endif()
    endif()
    if(NOT FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI_SYMBOL)
      set(FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI 0)
    endif()
  endif()
  message(STATUS
    "FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI_SYMBOL: "
    "${FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI_SYMBOL}")

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

  set(CMAKE_REQUIRED_LINK_OPTIONS "${_FLUME_HCCL_REQUIRED_LINK_OPTIONS}")
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
  (void)aclrtBinaryUnLoad(bin_handle);
  return 0;
}
" FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH)

  check_cxx_source_compiles("
#include <acl/acl.h>
int main() {
  aclrtFuncHandle func_handle = nullptr;
  aclrtLaunchKernelAttr attr = {};
  attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
  attr.value.timeout = 1;
  aclrtLaunchKernelCfg cfg = {};
  cfg.attrs = &attr;
  cfg.numAttrs = 1;
  unsigned long long arg = 0;
  (void)aclrtLaunchKernelWithHostArgs(func_handle, 1, nullptr, &cfg, &arg,
                                      sizeof(arg), nullptr, 0);
  return 0;
}
" FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS)

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
  set(CMAKE_REQUIRED_LINK_OPTIONS "${_FLUME_OLD_CMAKE_REQUIRED_LINK_OPTIONS}")
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
      FLUME_HAVE_HCCL_COMM_NAME
      FLUME_HAVE_HCOMM_CHANNEL_RES
      FLUME_HAVE_HCOMM_THREAD_EXPORT
      FLUME_HAVE_HCOMM_PRIMITIVES
      FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD
      FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY
      FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY
      FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI
      FLUME_HAVE_HCOMM_RANK_GRAPH
      FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH
      FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH
      FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS
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
    FLUME_HAVE_HCCL_COMM_NAME=${FLUME_HAVE_HCCL_COMM_NAME_VALUE}
    FLUME_HAVE_HCOMM_CHANNEL_RES=${FLUME_HAVE_HCOMM_CHANNEL_RES_VALUE}
    FLUME_HAVE_HCOMM_THREAD_EXPORT=${FLUME_HAVE_HCOMM_THREAD_EXPORT_VALUE}
    FLUME_HAVE_HCOMM_PRIMITIVES=${FLUME_HAVE_HCOMM_PRIMITIVES_VALUE}
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD=${FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD_VALUE}
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY=${FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY_VALUE}
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY=${FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_VALUE}
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI=${FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI_VALUE}
    FLUME_HAVE_HCOMM_RANK_GRAPH=${FLUME_HAVE_HCOMM_RANK_GRAPH_VALUE}
    FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH=${FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH_VALUE}
    FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH=${FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH_VALUE}
    FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS=${FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS_VALUE}
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
    FLUME_HAVE_HCCL_COMM_NAME=0
    FLUME_HAVE_HCOMM_CHANNEL_RES=0
    FLUME_HAVE_HCOMM_THREAD_EXPORT=0
    FLUME_HAVE_HCOMM_PRIMITIVES=0
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD=0
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY=0
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY=0
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI=0
    FLUME_HAVE_HCOMM_RANK_GRAPH=0
    FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH=0
    FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH=0
    FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS=0
    FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT=0
    FLUME_HAVE_ACL_PHY_DEVICE_ID=0
    FLUME_HAVE_ACL_VMM=0
  )
endif()

target_include_directories(flume PRIVATE
  ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/include
)

# Storage canaries use ACL for HBM allocation but do not require HCCL/HCOMM.
# Discover ACL independently when only a storage path is enabled.
if((FLUME_ENABLE_ROCE_STORAGE OR FLUME_ENABLE_NATIVE_NVME_HBM_CANARY) AND
   (NOT ACL_INCLUDE_DIR OR NOT ASCENDCL_LIBRARY))
  find_path(ACL_INCLUDE_DIR
    NAMES acl/acl.h
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES aarch64-linux/include include include/external
                  runtime/include/external
  )
  find_library(ASCENDCL_LIBRARY
    NAMES ascendcl
    PATHS "$ENV{ASCEND_HOME_PATH}" "$ENV{ASCEND_HOME_PATH}/latest"
    PATH_SUFFIXES aarch64-linux/lib64 lib64 lib
  )
endif()

if(FLUME_ENABLE_NATIVE_NVME_HBM_CANARY)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR
      "FLUME_ENABLE_NATIVE_NVME_HBM_CANARY requires Linux NVMe ioctls")
  endif()
  include(CheckIncludeFileCXX)
  check_include_file_cxx("linux/nvme_ioctl.h" FLUME_HAVE_LINUX_NVME_IOCTL_H)
  if(NOT FLUME_HAVE_LINUX_NVME_IOCTL_H)
    message(FATAL_ERROR
      "FLUME_ENABLE_NATIVE_NVME_HBM_CANARY requires linux/nvme_ioctl.h")
  endif()
  if(NOT ACL_INCLUDE_DIR OR NOT ASCENDCL_LIBRARY)
    message(FATAL_ERROR
      "FLUME_ENABLE_NATIVE_NVME_HBM_CANARY requires AscendCL; set "
      "ASCEND_HOME_PATH or ACL_INCLUDE_DIR/ASCENDCL_LIBRARY")
  endif()
endif()

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

add_executable(flume-storage-direct-sim-smoke apps/flume-storage-direct-sim-smoke.cc)
target_link_libraries(flume-storage-direct-sim-smoke PRIVATE flume)
target_include_directories(flume-storage-direct-sim-smoke PRIVATE ${PROJECT_SOURCE_DIR}/src)

add_executable(flume-storage-read-to-rank-demo apps/flume-storage-read-to-rank-demo.cc)
target_link_libraries(flume-storage-read-to-rank-demo PRIVATE flume)
target_include_directories(flume-storage-read-to-rank-demo PRIVATE ${PROJECT_SOURCE_DIR}/src)

add_executable(flume-roce-storage-server apps/flume-roce-storage-server.cc)
target_link_libraries(flume-roce-storage-server PRIVATE flume)
target_include_directories(flume-roce-storage-server PRIVATE ${PROJECT_SOURCE_DIR}/src)

add_executable(flume-roce-storage-proxy apps/flume-roce-storage-proxy.cc)
target_link_libraries(flume-roce-storage-proxy PRIVATE flume)
target_include_directories(flume-roce-storage-proxy PRIVATE ${PROJECT_SOURCE_DIR}/src)

if(FLUME_ENABLE_NATIVE_NVME_HBM_CANARY)
  add_executable(flume-nvme-hbm-canary apps/flume-nvme-hbm-canary.cc)
  target_link_libraries(flume-nvme-hbm-canary PRIVATE flume ${ASCENDCL_LIBRARY})
  target_include_directories(flume-nvme-hbm-canary PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${ACL_INCLUDE_DIR})
endif()

if(FLUME_ENABLE_HCCL)
  add_executable(flume-acl-runtime-probe apps/flume-acl-runtime-probe.cc)
  target_link_libraries(flume-acl-runtime-probe PRIVATE flume)
  target_include_directories(flume-acl-runtime-probe PRIVATE ${ACL_INCLUDE_DIR})

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

if(FLUME_ENABLE_ROCE_STORAGE AND ACL_INCLUDE_DIR AND ASCENDCL_LIBRARY)
  add_executable(flume-roce-storage-client apps/flume-roce-storage-client.cc)
  target_link_libraries(flume-roce-storage-client PRIVATE flume ${ASCENDCL_LIBRARY})
  target_include_directories(flume-roce-storage-client PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${ACL_INCLUDE_DIR})

  add_executable(flume-roce-hbm-write-smoke apps/flume-roce-storage-client.cc)
  target_link_libraries(flume-roce-hbm-write-smoke PRIVATE flume ${ASCENDCL_LIBRARY})
  target_include_directories(flume-roce-hbm-write-smoke PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${ACL_INCLUDE_DIR})

  add_executable(flume-roce-npu-relay-server
    apps/flume-roce-npu-relay-server.cc)
  target_link_libraries(flume-roce-npu-relay-server PRIVATE
    flume ${ASCENDCL_LIBRARY})
  target_include_directories(flume-roce-npu-relay-server PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${ACL_INCLUDE_DIR})
elseif(FLUME_ENABLE_ROCE_STORAGE)
  message(STATUS
    "ACL was not found; Host-RA protocol/server targets remain available, "
    "but the NPU HBM smoke target is omitted")
endif()

if(FLUME_BUILD_TESTS)
  find_package(Python3 COMPONENTS Interpreter REQUIRED)

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

  add_executable(test_storage_direct_sim tests/test_storage_direct_sim.cc)
  target_link_libraries(test_storage_direct_sim PRIVATE flume)
  target_include_directories(test_storage_direct_sim PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME storage_direct_sim COMMAND test_storage_direct_sim)

  add_executable(test_hcomm_payload_plan tests/test_hcomm_payload_plan.cc)
  target_link_libraries(test_hcomm_payload_plan PRIVATE flume)
  target_include_directories(test_hcomm_payload_plan PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME hcomm_payload_plan COMMAND test_hcomm_payload_plan)

  add_executable(test_hcomm_payload_abi tests/test_hcomm_payload_abi.cc)
  target_link_libraries(test_hcomm_payload_abi PRIVATE flume)
  target_include_directories(test_hcomm_payload_abi PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/include)
  add_test(NAME hcomm_payload_abi COMMAND test_hcomm_payload_abi)

  add_executable(test_hcomm_payload_kernel tests/test_hcomm_payload_kernel.cc)
  target_include_directories(test_hcomm_payload_kernel PRIVATE
    ${PROJECT_SOURCE_DIR}/tests/hcomm_kernel_stubs
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/aicpu
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/include)
  target_compile_definitions(test_hcomm_payload_kernel PRIVATE
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD=1
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY=0
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY=1
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI=0)
  add_test(NAME hcomm_payload_kernel COMMAND test_hcomm_payload_kernel)

  add_executable(test_hcomm_payload_kernel_nbi tests/test_hcomm_payload_kernel.cc)
  target_include_directories(test_hcomm_payload_kernel_nbi PRIVATE
    ${PROJECT_SOURCE_DIR}/tests/hcomm_kernel_stubs
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/aicpu
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/include)
  target_compile_definitions(test_hcomm_payload_kernel_nbi PRIVATE
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD=1
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY=0
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY=0
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI=1)
  add_test(NAME hcomm_payload_kernel_nbi COMMAND test_hcomm_payload_kernel_nbi)

  add_executable(test_hcomm_payload_kernel_legacy_fence
    tests/test_hcomm_payload_kernel.cc)
  target_include_directories(test_hcomm_payload_kernel_legacy_fence PRIVATE
    ${PROJECT_SOURCE_DIR}/tests/hcomm_kernel_stubs
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/aicpu
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/include)
  target_compile_definitions(test_hcomm_payload_kernel_legacy_fence PRIVATE
    FLUME_TEST_DISABLE_HCOMM_CHANNEL_FENCE_ON_THREAD=1
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD=0
    FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY=1
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY=1
    FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI=0)
  add_test(NAME hcomm_payload_kernel_legacy_fence
    COMMAND test_hcomm_payload_kernel_legacy_fence)

  add_executable(test_hcomm_canary_kernel tests/test_hcomm_canary_kernel.cc)
  target_include_directories(test_hcomm_canary_kernel PRIVATE
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/aicpu
    ${PROJECT_SOURCE_DIR}/custom_ops/hcomm_payload_copy/include)
  add_test(NAME hcomm_canary_kernel COMMAND test_hcomm_canary_kernel)

  add_test(NAME hcomm_package_preflight
    COMMAND ${Python3_EXECUTABLE}
      ${PROJECT_SOURCE_DIR}/tests/test_hcomm_package_preflight.py
      ${PROJECT_SOURCE_DIR})

  add_test(NAME hcomm_installer
    COMMAND ${Python3_EXECUTABLE}
      ${PROJECT_SOURCE_DIR}/tests/test_hcomm_installer.py
      ${PROJECT_SOURCE_DIR})

  add_test(NAME hcomm_payload_source_gate
    COMMAND ${Python3_EXECUTABLE}
      ${PROJECT_SOURCE_DIR}/tests/test_hcomm_payload_source_gate.py
      ${PROJECT_SOURCE_DIR})

  add_test(NAME strict_positive_decision_tree
    COMMAND ${Python3_EXECUTABLE}
      ${PROJECT_SOURCE_DIR}/tests/test_strict_positive_decision_tree.py
      ${PROJECT_SOURCE_DIR})

  add_test(NAME hccl_smoke_marker_contract
    COMMAND ${Python3_EXECUTABLE}
      ${PROJECT_SOURCE_DIR}/tests/test_hccl_smoke_marker_contract.py
      ${PROJECT_SOURCE_DIR})

  add_executable(test_sim_collective_failures tests/test_sim_collective_failures.cc)
  target_link_libraries(test_sim_collective_failures PRIVATE flume)
  target_include_directories(test_sim_collective_failures PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_collective_failures COMMAND test_sim_collective_failures)

  add_executable(test_sim_a3_symmetric_memory tests/test_sim_a3_symmetric_memory.cc)
  target_link_libraries(test_sim_a3_symmetric_memory PRIVATE flume)
  target_include_directories(test_sim_a3_symmetric_memory PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME sim_a3_symmetric_memory COMMAND test_sim_a3_symmetric_memory)

  add_executable(test_roce_storage_protocol tests/test_roce_storage_protocol.cc)
  target_link_libraries(test_roce_storage_protocol PRIVATE flume)
  target_include_directories(test_roce_storage_protocol PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME roce_storage_protocol COMMAND test_roce_storage_protocol)

  add_executable(test_nvme_hbm_canary tests/test_nvme_hbm_canary.cc)
  target_link_libraries(test_nvme_hbm_canary PRIVATE flume)
  target_include_directories(test_nvme_hbm_canary PRIVATE
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/tests)
  add_test(NAME nvme_hbm_canary COMMAND test_nvme_hbm_canary)

  add_executable(test_roce_tcp_control_sim tests/test_roce_tcp_control_sim.cc)
  target_link_libraries(test_roce_tcp_control_sim PRIVATE flume)
  target_include_directories(test_roce_tcp_control_sim PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME roce_tcp_control_sim COMMAND test_roce_tcp_control_sim)

  add_executable(test_roce_control_proxy tests/test_roce_control_proxy.cc)
  target_link_libraries(test_roce_control_proxy PRIVATE flume)
  target_include_directories(test_roce_control_proxy PRIVATE
    ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME roce_control_proxy COMMAND test_roce_control_proxy)

  add_executable(test_cann_ra_loader tests/test_cann_ra_loader.cc)
  target_link_libraries(test_cann_ra_loader PRIVATE flume)
  target_include_directories(test_cann_ra_loader PRIVATE ${PROJECT_SOURCE_DIR}/src)
  add_test(NAME cann_ra_loader COMMAND test_cann_ra_loader)

  if(UNIX OR APPLE)
    add_library(test_cann_ra_core_fixture SHARED tests/cann_ra_loader_fixture.cc)
    target_compile_definitions(test_cann_ra_core_fixture PRIVATE FLUME_RA_FIXTURE=1)
    target_include_directories(test_cann_ra_core_fixture PRIVATE ${PROJECT_SOURCE_DIR}/src)
    add_library(test_cann_runtime_core_fixture SHARED tests/cann_ra_loader_fixture.cc)
    target_compile_definitions(test_cann_runtime_core_fixture PRIVATE FLUME_RUNTIME_FIXTURE=1)
    target_include_directories(test_cann_runtime_core_fixture PRIVATE ${PROJECT_SOURCE_DIR}/src)
    add_library(test_cann_acl_core_fixture SHARED tests/cann_ra_loader_fixture.cc)
    target_compile_definitions(test_cann_acl_core_fixture PRIVATE FLUME_ACL_FIXTURE=1)
    target_include_directories(test_cann_acl_core_fixture PRIVATE ${PROJECT_SOURCE_DIR}/src)
    add_test(NAME cann_ra_loader_core_only
      COMMAND test_cann_ra_loader
        $<TARGET_FILE:test_cann_ra_core_fixture>
        $<TARGET_FILE:test_cann_runtime_core_fixture>
        $<TARGET_FILE:test_cann_acl_core_fixture>)

    add_library(test_cann_ra_push_fixture SHARED tests/cann_ra_loader_fixture.cc)
    target_compile_definitions(test_cann_ra_push_fixture PRIVATE
      FLUME_RA_PUSH_FIXTURE=1)
    target_include_directories(test_cann_ra_push_fixture PRIVATE
      ${PROJECT_SOURCE_DIR}/src)
    add_library(test_cann_runtime_push_fixture SHARED tests/cann_ra_loader_fixture.cc)
    target_compile_definitions(test_cann_runtime_push_fixture PRIVATE
      FLUME_RUNTIME_PUSH_FIXTURE=1)
    target_include_directories(test_cann_runtime_push_fixture PRIVATE
      ${PROJECT_SOURCE_DIR}/src)
    add_library(test_cann_acl_push_fixture SHARED tests/cann_ra_loader_fixture.cc)
    target_compile_definitions(test_cann_acl_push_fixture PRIVATE
      FLUME_ACL_PUSH_FIXTURE=1)
    target_include_directories(test_cann_acl_push_fixture PRIVATE
      ${PROJECT_SOURCE_DIR}/src)
    add_executable(test_npu_ra_push_mover tests/test_npu_ra_push_mover.cc)
    target_link_libraries(test_npu_ra_push_mover PRIVATE flume)
    target_include_directories(test_npu_ra_push_mover PRIVATE
      ${PROJECT_SOURCE_DIR}/src)
    add_test(NAME npu_ra_push_mover
      COMMAND test_npu_ra_push_mover
        $<TARGET_FILE:test_cann_ra_push_fixture>
        $<TARGET_FILE:test_cann_runtime_push_fixture>
        $<TARGET_FILE:test_cann_acl_push_fixture>)
  endif()

  set(FLUME_CANN_SOURCE_HCCP_INCLUDE
    ${PROJECT_SOURCE_DIR}/refer/cann-src/hcomm/src/base_comm/resources/hccp/inc)
  set(FLUME_CANN_SOURCE_VERBS_FIXTURE_INCLUDE
    ${PROJECT_SOURCE_DIR}/refer/cann-src/hcomm/test/ut/platform/hccp/rdma_service_normal/include)
  if(EXISTS ${FLUME_CANN_SOURCE_HCCP_INCLUDE}/network/hccp.h AND
     EXISTS ${FLUME_CANN_SOURCE_VERBS_FIXTURE_INCLUDE}/infiniband/verbs.h)
    add_executable(test_cann_ra_source_abi tests/test_cann_ra_source_abi.cc)
    target_include_directories(test_cann_ra_source_abi PRIVATE
      ${PROJECT_SOURCE_DIR}/src
      ${PROJECT_SOURCE_DIR}/tests/cann_ra_fixture
      ${FLUME_CANN_SOURCE_HCCP_INCLUDE}
      ${FLUME_CANN_SOURCE_VERBS_FIXTURE_INCLUDE})
    add_test(NAME cann_ra_source_abi COMMAND test_cann_ra_source_abi)

    add_test(NAME roce_verbs_fixture_syntax
      COMMAND ${CMAKE_CXX_COMPILER} -std=c++17 -fsyntax-only
        -DFLUME_HAVE_IBVERBS=1
        -I${PROJECT_SOURCE_DIR}/src
        -I${PROJECT_SOURCE_DIR}/tests/cann_ra_fixture
        -I${FLUME_CANN_SOURCE_VERBS_FIXTURE_INCLUDE}
        ${PROJECT_SOURCE_DIR}/src/roce_storage/verbs_backend.cc)
  endif()

  set(FLUME_CANN_SOURCE_ACL_INCLUDE
    ${PROJECT_SOURCE_DIR}/refer/cann-src/runtime/include/external)
  if(EXISTS ${FLUME_CANN_SOURCE_ACL_INCLUDE}/acl/acl.h)
    add_test(NAME roce_client_cann_fixture_syntax
      COMMAND ${CMAKE_CXX_COMPILER} -std=c++17 -fsyntax-only
        -I${PROJECT_SOURCE_DIR}/src
        -I${FLUME_CANN_SOURCE_ACL_INCLUDE}
        ${PROJECT_SOURCE_DIR}/apps/flume-roce-storage-client.cc)

    add_test(NAME roce_npu_relay_cann_fixture_syntax
      COMMAND ${CMAKE_CXX_COMPILER} -std=c++17 -fsyntax-only
        -I${PROJECT_SOURCE_DIR}/src
        -I${FLUME_CANN_SOURCE_ACL_INCLUDE}
        ${PROJECT_SOURCE_DIR}/apps/flume-roce-npu-relay-server.cc)
  endif()
endif()
