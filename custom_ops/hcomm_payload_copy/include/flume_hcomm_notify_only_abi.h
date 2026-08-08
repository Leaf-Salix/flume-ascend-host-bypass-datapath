#ifndef FLUME_HCOMM_NOTIFY_ONLY_ABI_H_
#define FLUME_HCOMM_NOTIFY_ONLY_ABI_H_

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FLUME_HCOMM_NOTIFY_ONLY_MAGIC 0x46484e4fU
#define FLUME_HCOMM_NOTIFY_ONLY_VERSION 1U
#define FLUME_HCOMM_NOTIFY_ONLY_KERNEL_SO "libflume_hcomm_payload_aicpu_kernel.so"
#define FLUME_HCOMM_NOTIFY_ONLY_KERNEL_FUNC "FlumeHcommNotifyOnlyAicpuKernel"
#define FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC \
  "FlumeHcommNotifyOnlyDirectAclrtKernel"
#define FLUME_HCOMM_NOTIFY_STATUS_SUCCESS 0U
#define FLUME_HCOMM_NOTIFY_STATUS_INVALID_ARGUMENT 1U
#define FLUME_HCOMM_NOTIFY_STATUS_HCOMM_ERROR 2U
#define FLUME_HCOMM_CANARY_MAGIC 0x4643414eU
#define FLUME_HCOMM_CANARY_VERSION 1U
#define FLUME_HCOMM_CANARY_TOKEN 0x43414e59U
#define FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC \
  "FlumeHcommCanaryDirectAclrtKernel"
#define FLUME_HCOMM_PAYLOAD_COPY_MAGIC 0x46504350U
#define FLUME_HCOMM_PAYLOAD_COPY_VERSION 2U
#define FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC_LEGACY \
  "FlumeHcommPayloadCopyDirectAclrtKernel"
#define FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC \
  "FlumeHcommPayloadCopyDirectAclrtKernelV2"
#define FLUME_HCOMM_PAYLOAD_BUILD_MODE_CANARY_ONLY_FUNC \
  "FlumeHcommPayloadBuildModeCanaryOnly"
#define FLUME_HCOMM_PAYLOAD_BUILD_MODE_INTERNAL_PAYLOAD_FUNC \
  "FlumeHcommPayloadBuildModeInternalPayload"
#define FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_FUNC \
  "FlumeHcommPayloadCopyAbiVersion"
#define FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V2_FUNC \
  "FlumeHcommPayloadCopyAbiVersion2"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION 3U
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_FUNC \
  "FlumeHcommPayloadCopySemanticVersion"
#define FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES 48U
#define FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE 0U
#define FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU 1U
#define FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY 0U
#define FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN 1U
#define FLUME_HCOMM_PAYLOAD_DEFAULT_TIMEOUT_SEC 60U
#define FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS 0U
#define FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT 1U
#define FLUME_HCOMM_PAYLOAD_STATUS_HCOMM_ERROR 2U
#define FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED 3U
#define FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED 4U
#define FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED 5U
#define FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED 6U
#define FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED 7U
#define FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED 8U
#define FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED 9U
#define FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED 10U
#define FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED 11U
#define FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED 12U
#define FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED 13U

typedef enum {
  FLUME_HCOMM_NOTIFY_ROLE_SEND = 0,
  FLUME_HCOMM_NOTIFY_ROLE_RECV = 1
} flume_hcomm_notify_role_t;

typedef struct {
  uint32_t magic;
  uint32_t version;
  uint32_t size;
  uint32_t role;
  uint32_t local_rank;
  uint32_t peer_rank;
  uint32_t rank_size;
  uint32_t ready_notify_idx;
  uint32_t done_notify_idx;
  uint32_t timeout_sec;
  uint32_t reserved0;
  uint64_t aicpu_thread;
  uint64_t channel_handle;
  uint64_t local_hccl_buffer;
  uint64_t remote_hccl_buffer;
  uint64_t local_hccl_buffer_bytes;
  uint64_t remote_hccl_buffer_bytes;
  uint64_t status_word;
  uint64_t reserved1[7];
} flume_hcomm_notify_only_desc_v1;

typedef struct {
  uint32_t magic;
  uint32_t version;
  uint32_t size;
  uint32_t local_rank;
  uint32_t peer_rank;
  uint32_t rank_size;
  uint32_t expected_token;
  uint32_t observed_token;
  uint64_t status_word;
  uint64_t observed_token_word;
  uint64_t reserved[6];
} flume_hcomm_canary_desc_v1;

typedef struct {
  uint32_t magic;
  uint32_t version;
  uint32_t size;
  uint32_t role;
  uint32_t local_rank;
  uint32_t peer_rank;
  uint32_t rank_size;
  uint32_t ready_notify_idx;
  uint32_t done_notify_idx;
  uint32_t timeout_sec;
  uint32_t thread_notify_mode;
  uint32_t completion_mode;
  uint64_t bytes;
  uint64_t aicpu_thread;
  uint64_t channel_handle;
  uint64_t user_buffer;
  uint64_t local_hccl_buffer;
  uint64_t remote_hccl_buffer;
  uint64_t local_hccl_buffer_bytes;
  uint64_t remote_hccl_buffer_bytes;
  uint64_t status_word;
  char batch_tag[FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES];
  uint64_t cpu_thread_on_aicpu;
} flume_hcomm_payload_copy_desc_v1;

static inline void flume_hcomm_notify_only_desc_init(
    flume_hcomm_notify_only_desc_v1* desc) {
  if (desc == 0) {
    return;
  }
  desc->magic = FLUME_HCOMM_NOTIFY_ONLY_MAGIC;
  desc->version = FLUME_HCOMM_NOTIFY_ONLY_VERSION;
  desc->size = (uint32_t)sizeof(flume_hcomm_notify_only_desc_v1);
}

static inline void flume_hcomm_canary_desc_init(
    flume_hcomm_canary_desc_v1* desc) {
  if (desc == 0) {
    return;
  }
  desc->magic = FLUME_HCOMM_CANARY_MAGIC;
  desc->version = FLUME_HCOMM_CANARY_VERSION;
  desc->size = (uint32_t)sizeof(flume_hcomm_canary_desc_v1);
  desc->expected_token = FLUME_HCOMM_CANARY_TOKEN;
}

static inline void flume_hcomm_payload_copy_desc_init(
    flume_hcomm_payload_copy_desc_v1* desc) {
  if (desc == 0) {
    return;
  }
  desc->magic = FLUME_HCOMM_PAYLOAD_COPY_MAGIC;
  desc->version = FLUME_HCOMM_PAYLOAD_COPY_VERSION;
  desc->size = (uint32_t)sizeof(flume_hcomm_payload_copy_desc_v1);
  desc->timeout_sec = FLUME_HCOMM_PAYLOAD_DEFAULT_TIMEOUT_SEC;
  desc->thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE;
  desc->completion_mode = FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY;
}

#ifdef __cplusplus
}
#endif

#endif  // FLUME_HCOMM_NOTIFY_ONLY_ABI_H_
