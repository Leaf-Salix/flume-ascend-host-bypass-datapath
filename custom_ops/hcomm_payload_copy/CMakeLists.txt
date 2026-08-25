cmake_minimum_required(VERSION 3.16.0)

find_package(ASC REQUIRED)
find_package(AICPU REQUIRED)
project(flume_hcomm_payload_copy LANGUAGES CXX AICPU ASC)

include(CheckCXXSourceCompiles)

set(FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD_DEFAULT OFF)
foreach(_flume_payload_env
        FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD
        FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY)
  if(DEFINED ENV{${_flume_payload_env}})
    string(TOUPPER "$ENV{${_flume_payload_env}}"
      FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD_ENV)
    if(FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD_ENV STREQUAL "1" OR
       FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD_ENV STREQUAL "ON" OR
       FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD_ENV STREQUAL "TRUE" OR
       FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD_ENV STREQUAL "YES")
      set(FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD_DEFAULT ON)
    endif()
  endif()
endforeach()
option(FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD
  "Build the HCOMM primitive payload kernels using public hcomm_primitives.h APIs."
  ${FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD_DEFAULT})
set(FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY
  ${FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD})
option(FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH
  "Build the legacy public HCCL launch notify-only kernel that requires hccl_launch.h."
  OFF)

set(FLUME_HCOMM_PAYLOAD_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(FLUME_HCOMM_PAYLOAD_INCLUDE_LIST
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/host
  ${CMAKE_CURRENT_SOURCE_DIR}/aicpu
  ${ASCEND_CANN_PACKAGE_PATH}/include
  ${ASCEND_CANN_PACKAGE_PATH}/include/hccl
  ${ASCEND_CANN_PACKAGE_PATH}/include/hcomm
  ${ASCEND_CANN_PACKAGE_PATH}/asc/include/aicpu_api
)

set(FLUME_HCOMM_PAYLOAD_CHANNEL_FENCE_API "not-required")
if(FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD)
  if(NOT FLUME_HCOMM_PAYLOAD_FENCE_PROBE_REVISION STREQUAL "1")
    unset(FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD CACHE)
    unset(FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY CACHE)
    set(FLUME_HCOMM_PAYLOAD_FENCE_PROBE_REVISION "1" CACHE INTERNAL
      "Flume custom-op HCOMM channel fence probe revision" FORCE)
  endif()
  set(_FLUME_HCOMM_PAYLOAD_OLD_REQUIRED_INCLUDES
    "${CMAKE_REQUIRED_INCLUDES}")
  set(_FLUME_HCOMM_PAYLOAD_OLD_TRY_COMPILE_TARGET_TYPE
    "${CMAKE_TRY_COMPILE_TARGET_TYPE}")
  set(CMAKE_REQUIRED_INCLUDES "${FLUME_HCOMM_PAYLOAD_INCLUDE_LIST}")
  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
  set(_FLUME_HCOMM_PAYLOAD_PRIMITIVES_INCLUDE [[
#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#elif __has_include(<hcomm/hcomm_primitives.h>)
#include <hcomm/hcomm_primitives.h>
#elif __has_include(<hcomm_primitives.h>)
#include <hcomm_primitives.h>
#else
#error no hcomm_primitives header
#endif
]])
  check_cxx_source_compiles("
${_FLUME_HCOMM_PAYLOAD_PRIMITIVES_INCLUDE}
int main() {
  ThreadHandle thread = 0;
  ChannelHandle channel = 0;
  return HcommChannelFenceOnThread(thread, channel);
}
" FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD)
  check_cxx_source_compiles("
${_FLUME_HCOMM_PAYLOAD_PRIMITIVES_INCLUDE}
int main() {
  ChannelHandle channel = 0;
  return HcommChannelFence(channel);
}
" FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY)
  set(CMAKE_REQUIRED_INCLUDES
    "${_FLUME_HCOMM_PAYLOAD_OLD_REQUIRED_INCLUDES}")
  set(CMAKE_TRY_COMPILE_TARGET_TYPE
    "${_FLUME_HCOMM_PAYLOAD_OLD_TRY_COMPILE_TARGET_TYPE}")

  if(FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD)
    set(FLUME_HCOMM_PAYLOAD_CHANNEL_FENCE_API "on-thread")
  elseif(FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY)
    set(FLUME_HCOMM_PAYLOAD_CHANNEL_FENCE_API "legacy")
  else()
    message(FATAL_ERROR
      "HCOMM primitive payload requires HcommChannelFenceOnThread(thread, channel) "
      "or legacy HcommChannelFence(channel), but neither call shape is available")
  endif()
else()
  set(FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD 0)
  set(FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY 0)
endif()
message(STATUS
  "FLUME_HCOMM_PAYLOAD_CHANNEL_FENCE_API: ${FLUME_HCOMM_PAYLOAD_CHANNEL_FENCE_API}")
add_library(flume_hcomm_payload_host SHARED)
set_target_properties(flume_hcomm_payload_host PROPERTIES LINKER_LANGUAGE CXX)
target_sources(flume_hcomm_payload_host PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/host/placeholder.cc
)
target_include_directories(flume_hcomm_payload_host PRIVATE
  ${FLUME_HCOMM_PAYLOAD_INCLUDE_LIST}
)
target_compile_options(flume_hcomm_payload_host PRIVATE
  -U_FORTIFY_SOURCE
  -fno-common
  -fno-strict-aliasing
  -pipe
  -g
  -O0
  -std=c++14
  -fstack-protector-all
)
target_compile_definitions(flume_hcomm_payload_host PRIVATE
  _GLIBCXX_USE_CXX11_ABI=0
  HOST_COMPILE
)
target_link_options(flume_hcomm_payload_host PRIVATE
  -Wl,-z,relro
  -Wl,-z,now
  -Wl,-z,noexecstack
)
target_link_directories(flume_hcomm_payload_host PRIVATE
  ${ASCEND_CANN_PACKAGE_PATH}/lib64
)
target_link_libraries(flume_hcomm_payload_host PRIVATE
  hcomm
  c_sec
  ascendcl
)
install(TARGETS flume_hcomm_payload_host
  LIBRARY DESTINATION ${CUSTOM_OPS_OPP_LIB_PATH}
  COMPONENT hccl
)
install(FILES
  include/flume_hcomm_notify_only_abi.h
  DESTINATION ${CUSTOM_OPS_OPP_INC_PATH}
  COMPONENT hccl
)

if(KERNEL_MODE)
  add_library(flume_hcomm_payload_aicpu_kernel SHARED)
  target_sources(flume_hcomm_payload_aicpu_kernel PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/aicpu/direct_acl_canary_kernel.cc
  )
  if(FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD)
    target_sources(flume_hcomm_payload_aicpu_kernel PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/aicpu/notify_only_direct_acl_kernel.cc
      ${CMAKE_CURRENT_SOURCE_DIR}/aicpu/payload_copy_kernel.cc
    )
    target_include_directories(flume_hcomm_payload_aicpu_kernel PRIVATE
      ${FLUME_HCOMM_PAYLOAD_INCLUDE_LIST}
    )
    target_compile_definitions(flume_hcomm_payload_aicpu_kernel PRIVATE
      FLUME_HCOMM_PAYLOAD_ENABLE_PRIMITIVE_PAYLOAD=1
      FLUME_HCOMM_PAYLOAD_ENABLE_INTERNAL_NOTIFY=1
      FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD=$<BOOL:${FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD}>
      FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY=$<BOOL:${FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY}>
    )
  else()
    target_include_directories(flume_hcomm_payload_aicpu_kernel PRIVATE
      ${FLUME_HCOMM_PAYLOAD_INCLUDE_LIST}
    )
  endif()
  target_compile_definitions(flume_hcomm_payload_aicpu_kernel PRIVATE
    AICPU_COMPILE
  )
  target_compile_options(flume_hcomm_payload_aicpu_kernel PRIVATE
    -g
    -O0
  )
  target_link_directories(flume_hcomm_payload_aicpu_kernel PRIVATE
    ${ASCEND_CANN_PACKAGE_PATH}/lib64/device/lib64
    ${ASCEND_CANN_PACKAGE_PATH}/devlib/device
  )
  if(FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD)
    set(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_MODE "unresolved")
    if(TARGET ccl_kernel)
      set(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_MODE "existing-target")
      set(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_ITEM ccl_kernel)
    else()
      find_library(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LIBRARY
        NAMES ccl_kernel
        PATHS
          ${ASCEND_CANN_PACKAGE_PATH}/lib64/device/lib64
          ${ASCEND_CANN_PACKAGE_PATH}/devlib/device
        NO_DEFAULT_PATH
      )
      if(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LIBRARY)
        set(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_MODE "installed-library")
        set(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_ITEM
          ${FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LIBRARY})
      elseif(COMMAND generate_stub)
        # CANN 9.0 packages may omit the link-time device library. Reuse the
        # official HCCL source build's empty import stub in that layout.
        generate_stub(ccl_kernel)
        set(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_MODE "generated-stub")
        set(FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_ITEM ccl_kernel)
      else()
        message(FATAL_ERROR
          "HCOMM payload requires ccl_kernel, but no target/library exists "
          "and the HCCL generate_stub helper is unavailable")
      endif()
    endif()
    message(STATUS
      "ccl_kernel_link_mode=${FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_MODE}")
    target_link_libraries(flume_hcomm_payload_aicpu_kernel PRIVATE
      ${FLUME_HCOMM_PAYLOAD_CCL_KERNEL_LINK_ITEM}
    )
  endif()
  if(FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH)
    target_sources(flume_hcomm_payload_aicpu_kernel PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/aicpu/notify_only_kernel.cc
    )
    target_compile_definitions(flume_hcomm_payload_aicpu_kernel PRIVATE
      FLUME_HCOMM_PAYLOAD_ENABLE_PUBLIC_HCCL_LAUNCH=1
    )
  endif()

  cann_pack_targets_and_files(
    OUTPUT_TARGET "package_aicpu_flume_hcomm_payload"
    OUTPUT "aicpu_flume_hcomm_payload.tar.gz"
    TARGETS flume_hcomm_payload_aicpu_kernel
    MANIFEST "bin_hash.cfg"
    TAR_ROOT_DIR "aicpu_kernels_device"
  )

  add_cann_sign_file(
    INPUT "aicpu_flume_hcomm_payload.tar.gz"
    CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/scripts/flume_hcomm_payload_check_cfg.xml
    RESULT_VAR "sign_aicpu_flume_hcomm_payload_result"
    DEPENDS package_aicpu_flume_hcomm_payload
  )

  register_custom_aicpu_target(sign_aicpu_flume_hcomm_payload)
else()
  set(FLUME_HCOMM_PAYLOAD_AICPU_JSON
    ${CMAKE_CURRENT_SOURCE_DIR}/aicpu/libflume_hcomm_payload_aicpu_kernel_canary.json)
  if(FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD)
    set(FLUME_HCOMM_PAYLOAD_AICPU_JSON
      ${CMAKE_CURRENT_SOURCE_DIR}/aicpu/libflume_hcomm_payload_aicpu_kernel_payload.json)
  endif()
  file(RELATIVE_PATH FLUME_HCOMM_PAYLOAD_BINARY_RELATIVE_PATH
    "${CMAKE_BINARY_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")
  set(FLUME_HCOMM_PAYLOAD_SIGNED_AICPU_PACKAGE
    "${BUILD_DEVICE_DIR}/${FLUME_HCOMM_PAYLOAD_BINARY_RELATIVE_PATH}/signatures_aicpu_flume_hcomm_payload_tar_gz/aicpu_flume_hcomm_payload.tar.gz")
  message(STATUS
    "signed_aicpu_install_source=${FLUME_HCOMM_PAYLOAD_SIGNED_AICPU_PACKAGE}")
  install(FILES
    ${FLUME_HCOMM_PAYLOAD_SIGNED_AICPU_PACKAGE}
    DESTINATION ${CUSTOM_OPS_OPP_AICPU_KERNEL_PATH}
    COMPONENT hccl
  )
  install(FILES
    ${FLUME_HCOMM_PAYLOAD_AICPU_JSON}
    DESTINATION ${CUSTOM_OPS_OPP_AICPU_CONFIG_PATH}
    RENAME libflume_hcomm_payload_aicpu_kernel.json
    COMPONENT hccl
  )
  message(STATUS
    "flume_custom_installer=parked-todo-v1")
  install(PROGRAMS
    "${CMAKE_CURRENT_SOURCE_DIR}/scripts/flume_install.sh"
    DESTINATION ${CUSTOM_OPS_OPP_SCRIPTS_PATH}
    RENAME install.sh
    COMPONENT hccl
  )
endif()
