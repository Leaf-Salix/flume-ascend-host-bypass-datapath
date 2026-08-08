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
#define FLUME_HCOMM_CANARY_MAGIC 0x4643414eU
#define FLUME_HCOMM_CANARY_VERSION 1U
#define FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC \
  "FlumeHcommCanaryDirectAclrtKernel"
#define FLUME_HCOMM_PAYLOAD_COPY_MAGIC 0x46504350U
#define FLUME_HCOMM_PAYLOAD_COPY_VERSION 1U
#define FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC \
  "FlumeHcommPayloadCopyDirectAclrtKernel"

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
  uint64_t reserved1[8];
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
  uint64_t reserved[8];
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
  uint32_t reserved0;
  uint64_t bytes;
  uint64_t aicpu_thread;
  uint64_t channel_handle;
  uint64_t user_buffer;
  uint64_t local_hccl_buffer;
  uint64_t remote_hccl_buffer;
  uint64_t local_hccl_buffer_bytes;
  uint64_t remote_hccl_buffer_bytes;
  uint64_t reserved1[8];
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
  desc->expected_token = 0x43414e59U;
}

static inline void flume_hcomm_payload_copy_desc_init(
    flume_hcomm_payload_copy_desc_v1* desc) {
  if (desc == 0) {
    return;
  }
  desc->magic = FLUME_HCOMM_PAYLOAD_COPY_MAGIC;
  desc->version = FLUME_HCOMM_PAYLOAD_COPY_VERSION;
  desc->size = (uint32_t)sizeof(flume_hcomm_payload_copy_desc_v1);
  desc->timeout_sec = 1800;
}

#ifdef __cplusplus
}
#endif

#endif  // FLUME_HCOMM_NOTIFY_ONLY_ABI_H_
