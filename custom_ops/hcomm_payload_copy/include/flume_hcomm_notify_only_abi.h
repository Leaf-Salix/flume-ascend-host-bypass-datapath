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
#define FLUME_HCOMM_PAYLOAD_COPY_VERSION 4U
#define FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC_LEGACY \
  "FlumeHcommPayloadCopyDirectAclrtKernel"
#define FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC_V2 \
  "FlumeHcommPayloadCopyDirectAclrtKernelV2"
#define FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC_V3 \
  "FlumeHcommPayloadCopyDirectAclrtKernelV3"
#define FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC \
  "FlumeHcommPayloadCopyDirectAclrtKernelV4"
#define FLUME_HCOMM_PAYLOAD_BUILD_MODE_CANARY_ONLY_FUNC \
  "FlumeHcommPayloadBuildModeCanaryOnly"
#define FLUME_HCOMM_PAYLOAD_BUILD_MODE_INTERNAL_PAYLOAD_FUNC \
  "FlumeHcommPayloadBuildModeInternalPayload"
#define FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_FUNC \
  "FlumeHcommPayloadCopyAbiVersion"
#define FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V2_FUNC \
  "FlumeHcommPayloadCopyAbiVersion2"
#define FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V3_FUNC \
  "FlumeHcommPayloadCopyAbiVersion3"
#define FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V4_FUNC \
  "FlumeHcommPayloadCopyAbiVersion4"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION 16U
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_FUNC \
  "FlumeHcommPayloadCopySemanticVersion"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5_FUNC \
  "FlumeHcommPayloadCopySemanticVersion5"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6_FUNC \
  "FlumeHcommPayloadCopySemanticVersion6"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7_FUNC \
  "FlumeHcommPayloadCopySemanticVersion7"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8_FUNC \
  "FlumeHcommPayloadCopySemanticVersion8"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V9_FUNC \
  "FlumeHcommPayloadCopySemanticVersion9"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V10_FUNC \
  "FlumeHcommPayloadCopySemanticVersion10"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V11_FUNC \
  "FlumeHcommPayloadCopySemanticVersion11"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V12_FUNC \
  "FlumeHcommPayloadCopySemanticVersion12"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V13_FUNC \
  "FlumeHcommPayloadCopySemanticVersion13"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V14_FUNC \
  "FlumeHcommPayloadCopySemanticVersion14"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V15_FUNC \
  "FlumeHcommPayloadCopySemanticVersion15"
#define FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V16_FUNC \
  "FlumeHcommPayloadCopySemanticVersion16"
#define FLUME_HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE_FUNC \
  "FlumeHcommPayloadCopyRequiresCommAcquire"
#define FLUME_HCOMM_PAYLOAD_COPY_SUPPORTS_OFFICIAL_P2P_LAYOUT_FUNC \
  "FlumeHcommPayloadCopySupportsOfficialP2pLayout"
#define FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION_FUNC \
  "FlumeHcommPayloadStatusSchemaVersion"
#define FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT_FUNC \
  "FlumeHcommPayloadStatusWordCount"
#define FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION_FUNC \
  "FlumeHcommPayloadTraceSchemaVersion"
#define FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT_FUNC \
  "FlumeHcommPayloadTraceWordCount"
#define FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES 48U
#define FLUME_HCOMM_PAYLOAD_DEFAULT_BATCH_TAG "flume_hcomm_payload"
#define FLUME_HCOMM_PAYLOAD_COMM_NAME_BYTES 128U
#define FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT 17U
#define FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION 7U
#define FLUME_HCOMM_PAYLOAD_TRACE_HEADER_WORD_COUNT 18U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY 32U
#define FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT \
  (FLUME_HCOMM_PAYLOAD_TRACE_HEADER_WORD_COUNT + \
   FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY + \
   FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY)
#define FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION 3U
#define FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE 0U
#define FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU 1U
#define FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY 0U
#define FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN 1U
#define FLUME_HCOMM_PAYLOAD_COMPLETION_MODE_MASK 0x0000FFFFU
#define FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY 0x10000000U
#define FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH 0x20000000U
#define FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING 0x40000000U
#define FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE 0x80000000U
#define FLUME_HCOMM_PAYLOAD_BATCH_MODE_DEFAULT 0U
#define FLUME_HCOMM_PAYLOAD_BATCH_MODE_DISABLED 1U
#define FLUME_HCOMM_PAYLOAD_RECV_PATH_LOCAL_BUFFER 0U
#define FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT 1U
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
#define FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED 14U
#define FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED 15U
#define FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED 16U
#define FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_WRITE_FAILED 17U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_NONE 0U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_ENTER 1U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_ENTER 2U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_DONE 3U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_ENTER 4U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_DONE 5U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_ENTER 6U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_DONE 7U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_ENTER 8U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_DONE 9U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_ENTER 10U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_DONE 11U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_ENTER 12U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_DONE 13U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_ENTER 14U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_DONE 15U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_ENTER 16U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_DONE 17U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_ENTER 18U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_DONE 19U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_ENTER 20U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_DONE 21U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_ENTER 22U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_DONE 23U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_ENTER 24U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_DONE 25U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_ENTER 26U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_DONE 27U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_ENTER 28U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_DONE 29U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_ENTER 30U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_DONE 31U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_ENTER 32U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_DONE 33U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT 34U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_ENTER 35U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_DONE 36U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_ENTER 37U
#define FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_DONE 38U

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
  char comm_name[FLUME_HCOMM_PAYLOAD_COMM_NAME_BYTES];
  uint32_t status_word_count;
  uint32_t status_schema_version;
  uint64_t reserved2[3];
} flume_hcomm_payload_copy_desc_v1;

static inline uint64_t flume_hcomm_payload_hash_u64(uint64_t hash,
                                                    uint64_t value) {
  hash ^= value;
  hash *= UINT64_C(1099511628211);
  return hash;
}

static inline uint64_t flume_hcomm_payload_hash_bytes(
    uint64_t hash, const char* data, uint32_t len) {
  uint32_t i = 0;
  for (; i < len; ++i) {
    hash ^= (uint64_t)(uint8_t)data[i];
    hash *= UINT64_C(1099511628211);
  }
  return hash;
}

static inline uint64_t flume_hcomm_payload_copy_desc_fingerprint(
    const flume_hcomm_payload_copy_desc_v1* desc) {
  if (desc == 0) {
    return 0;
  }
  uint64_t hash = UINT64_C(1469598103934665603);
  hash = flume_hcomm_payload_hash_u64(hash, desc->magic);
  hash = flume_hcomm_payload_hash_u64(hash, desc->version);
  hash = flume_hcomm_payload_hash_u64(hash, desc->size);
  hash = flume_hcomm_payload_hash_u64(hash, desc->role);
  hash = flume_hcomm_payload_hash_u64(hash, desc->local_rank);
  hash = flume_hcomm_payload_hash_u64(hash, desc->peer_rank);
  hash = flume_hcomm_payload_hash_u64(hash, desc->rank_size);
  hash = flume_hcomm_payload_hash_u64(hash, desc->ready_notify_idx);
  hash = flume_hcomm_payload_hash_u64(hash, desc->done_notify_idx);
  hash = flume_hcomm_payload_hash_u64(hash, desc->timeout_sec);
  hash = flume_hcomm_payload_hash_u64(hash, desc->thread_notify_mode);
  hash = flume_hcomm_payload_hash_u64(hash, desc->completion_mode);
  hash = flume_hcomm_payload_hash_u64(hash, desc->bytes);
  hash = flume_hcomm_payload_hash_u64(hash, desc->aicpu_thread);
  hash = flume_hcomm_payload_hash_u64(hash, desc->channel_handle);
  hash = flume_hcomm_payload_hash_u64(hash, desc->user_buffer);
  hash = flume_hcomm_payload_hash_u64(hash, desc->local_hccl_buffer);
  hash = flume_hcomm_payload_hash_u64(hash, desc->remote_hccl_buffer);
  hash = flume_hcomm_payload_hash_u64(hash, desc->local_hccl_buffer_bytes);
  hash = flume_hcomm_payload_hash_u64(hash, desc->remote_hccl_buffer_bytes);
  hash = flume_hcomm_payload_hash_u64(hash, desc->status_word);
  hash = flume_hcomm_payload_hash_bytes(
      hash, desc->batch_tag, FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES);
  hash = flume_hcomm_payload_hash_u64(hash, desc->cpu_thread_on_aicpu);
  hash = flume_hcomm_payload_hash_bytes(
      hash, desc->comm_name, FLUME_HCOMM_PAYLOAD_COMM_NAME_BYTES);
  hash = flume_hcomm_payload_hash_u64(hash, desc->status_word_count);
  hash = flume_hcomm_payload_hash_u64(hash, desc->status_schema_version);
  hash = flume_hcomm_payload_hash_u64(hash, desc->reserved2[0]);
  hash = flume_hcomm_payload_hash_u64(hash, desc->reserved2[1]);
  hash = flume_hcomm_payload_hash_u64(hash, desc->reserved2[2]);
  return hash;
}

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
  desc->status_word_count = FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT;
  desc->status_schema_version = FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION;
}

#ifdef __cplusplus
}
#endif

#endif  // FLUME_HCOMM_NOTIFY_ONLY_ABI_H_
