cmake_minimum_required(VERSION 3.16.0)

find_package(ASC REQUIRED)
find_package(AICPU REQUIRED)
project(flume_hcomm_payload_copy LANGUAGES CXX AICPU ASC)

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
    target_link_libraries(flume_hcomm_payload_aicpu_kernel PRIVATE
      ccl_kernel
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
  install(FILES
    ${BUILD_DEVICE_DIR}/signatures/aicpu_flume_hcomm_payload.tar.gz
    DESTINATION ${CUSTOM_OPS_OPP_AICPU_KERNEL_PATH}
    COMPONENT hccl
  )
  install(FILES
    ${FLUME_HCOMM_PAYLOAD_AICPU_JSON}
    DESTINATION ${CUSTOM_OPS_OPP_AICPU_CONFIG_PATH}
    RENAME libflume_hcomm_payload_aicpu_kernel.json
    COMPONENT hccl
  )
endif()
