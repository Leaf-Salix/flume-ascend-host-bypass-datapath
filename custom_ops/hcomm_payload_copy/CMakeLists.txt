cmake_minimum_required(VERSION 3.16.0)

find_package(ASC REQUIRED)
find_package(AICPU REQUIRED)
project(flume_hcomm_payload_copy LANGUAGES CXX AICPU ASC)

set(FLUME_HCOMM_PAYLOAD_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(FLUME_HCOMM_PAYLOAD_INCLUDE_LIST
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/host
  ${CMAKE_CURRENT_SOURCE_DIR}/aicpu
  ${ASCEND_CANN_PACKAGE_PATH}/include
  ${ASCEND_CANN_PACKAGE_PATH}/include/hccl
  ${ASCEND_CANN_PACKAGE_PATH}/asc/include/aicpu_api
  ${ASCEND_CANN_PACKAGE_PATH}/pkg_inc
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
    ${CMAKE_CURRENT_SOURCE_DIR}/aicpu/notify_only_kernel.cc
  )
  target_include_directories(flume_hcomm_payload_aicpu_kernel PRIVATE
    ${FLUME_HCOMM_PAYLOAD_INCLUDE_LIST}
  )
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
  target_link_libraries(flume_hcomm_payload_aicpu_kernel PRIVATE
    ccl_kernel
  )

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
  install(FILES
    ${BUILD_DEVICE_DIR}/signatures/aicpu_flume_hcomm_payload.tar.gz
    DESTINATION ${CUSTOM_OPS_OPP_AICPU_KERNEL_PATH}
    COMPONENT hccl
  )
  install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/aicpu/libflume_hcomm_payload_aicpu_kernel.json
    DESTINATION ${CUSTOM_OPS_OPP_AICPU_CONFIG_PATH}
    COMPONENT hccl
  )
endif()
