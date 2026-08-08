#include "flume_hcomm_notify_only_abi.h"

#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#else
#include <hcomm_primitives.h>
#endif

namespace {

constexpr unsigned int kFlumePayloadSuccess =
    FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS;
constexpr unsigned int kFlumePayloadInvalidArgument =
    FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT;
constexpr unsigned int kFlumePayloadHcommError =
    FLUME_HCOMM_PAYLOAD_STATUS_HCOMM_ERROR;

bool HasPayloadDescHeader(const flume_hcomm_payload_copy_desc_v1& desc) {
  return desc.magic == FLUME_HCOMM_PAYLOAD_COPY_MAGIC &&
         desc.version == FLUME_HCOMM_PAYLOAD_COPY_VERSION &&
         desc.size == sizeof(flume_hcomm_payload_copy_desc_v1);
}

bool HasCommName(const flume_hcomm_payload_copy_desc_v1& desc) {
  for (unsigned int i = 0; i < FLUME_HCOMM_PAYLOAD_COMM_NAME_BYTES; ++i) {
    if (desc.comm_name[i] == '\0') {
      return i != 0;
    }
  }
  return false;
}

void StorePayloadStatus(const flume_hcomm_payload_copy_desc_v1& desc,
                        unsigned int status) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0 ||
      desc.status_word_count < 1U) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[0] = status;
}

void StorePayloadPrimitiveRet(const flume_hcomm_payload_copy_desc_v1& desc,
                              int32_t ret) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0 ||
      desc.status_word_count < 2U) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[1] = static_cast<unsigned int>(ret);
}

void BeginPayloadPrimitive(const flume_hcomm_payload_copy_desc_v1& desc,
                           unsigned int pending_status) {
  StorePayloadStatus(desc, pending_status);
  StorePayloadPrimitiveRet(desc, -1);
}

void StorePayloadEcho(const flume_hcomm_payload_copy_desc_v1& desc) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0 ||
      desc.status_word_count < FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[2] = desc.role;
  status_words[3] = desc.peer_rank;
  status_words[4] = static_cast<unsigned int>(desc.bytes & 0xFFFFFFFFU);
  status_words[5] = static_cast<unsigned int>(desc.bytes >> 32U);
  status_words[6] = desc.local_rank;
  status_words[7] = desc.completion_mode;
}

bool CanRecordPayloadCompletionNotify(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  return HasPayloadDescHeader(desc) &&
         desc.thread_notify_mode ==
             FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU &&
         desc.aicpu_thread != 0 && desc.cpu_thread_on_aicpu != 0;
}

void BestEffortPayloadCompletionNotify(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  if (!CanRecordPayloadCompletionNotify(desc)) {
    return;
  }
  (void)HcommThreadNotifyRecordOnThread(
      static_cast<ThreadHandle>(desc.aicpu_thread),
      static_cast<ThreadHandle>(desc.cpu_thread_on_aicpu), 0);
}

bool ValidatePayloadDesc(const flume_hcomm_payload_copy_desc_v1& desc) {
  return HasPayloadDescHeader(desc) && desc.rank_size == 2 &&
         (desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND ||
          desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV) &&
         desc.local_rank < desc.rank_size &&
         desc.peer_rank < desc.rank_size && desc.local_rank != desc.peer_rank &&
         desc.ready_notify_idx != desc.done_notify_idx && desc.bytes != 0 &&
         (desc.thread_notify_mode == FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE ||
          desc.thread_notify_mode ==
              FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU) &&
         (desc.completion_mode ==
              FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY ||
          desc.completion_mode ==
              FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN) &&
         desc.aicpu_thread != 0 && desc.channel_handle != 0 &&
         desc.user_buffer != 0 && desc.local_hccl_buffer != 0 &&
         desc.remote_hccl_buffer != 0 &&
         desc.bytes <= desc.local_hccl_buffer_bytes &&
         desc.bytes <= desc.remote_hccl_buffer_bytes &&
         desc.status_word != 0 &&
         desc.status_word_count >= FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT &&
         desc.status_schema_version ==
             FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION &&
         HasCommName(desc) &&
         (desc.thread_notify_mode !=
              FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU ||
          desc.cpu_thread_on_aicpu != 0);
}

unsigned int RunPayloadCopyBody(const flume_hcomm_payload_copy_desc_v1& desc) {
  ThreadHandle thread = static_cast<ThreadHandle>(desc.aicpu_thread);
  ChannelHandle channel = static_cast<ChannelHandle>(desc.channel_handle);
  void* user_buffer = reinterpret_cast<void*>(desc.user_buffer);
  void* local_hccl_buffer = reinterpret_cast<void*>(desc.local_hccl_buffer);
  void* remote_hccl_buffer = reinterpret_cast<void*>(desc.remote_hccl_buffer);

  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND) {
    BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED);
    int32_t ret =
        HcommLocalCopyOnThread(thread, local_hccl_buffer, user_buffer,
                               desc.bytes);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED;
    }
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED);
    ret = HcommChannelNotifyRecordOnThread(
        thread, channel, desc.ready_notify_idx);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED;
    }
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED);
    ret = HcommChannelNotifyWaitOnThread(
        thread, channel, desc.done_notify_idx, desc.timeout_sec);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED;
    }
    return kFlumePayloadSuccess;
  }

  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED);
    int32_t ret = HcommChannelNotifyWaitOnThread(
        thread, channel, desc.ready_notify_idx, desc.timeout_sec);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED;
    }
    BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED);
    ret = HcommReadOnThread(thread, channel, local_hccl_buffer,
                            remote_hccl_buffer, desc.bytes);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED;
    }
    if (desc.completion_mode ==
        FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN) {
      BeginPayloadPrimitive(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED);
      ret = HcommChannelFenceOnThread(thread, channel);
      if (ret != 0) {
        StorePayloadPrimitiveRet(desc, ret);
        return FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED;
      }
    }
    BeginPayloadPrimitive(desc,
                          FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED);
    ret = HcommLocalCopyOnThread(thread, user_buffer, local_hccl_buffer,
                                 desc.bytes);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED;
    }
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED);
    ret = HcommChannelNotifyRecordOnThread(
        thread, channel, desc.done_notify_idx);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED;
    }
    return kFlumePayloadSuccess;
  }

  return kFlumePayloadInvalidArgument;
}

unsigned int RunPayloadCopy(const flume_hcomm_payload_copy_desc_v1& desc) {
  if (!ValidatePayloadDesc(desc)) {
    StorePayloadStatus(desc, kFlumePayloadInvalidArgument);
    BestEffortPayloadCompletionNotify(desc);
    return kFlumePayloadInvalidArgument;
  }
  StorePayloadStatus(desc, kFlumePayloadHcommError);
  StorePayloadEcho(desc);

  ThreadHandle thread = static_cast<ThreadHandle>(desc.aicpu_thread);
  const bool use_thread_notify =
      desc.thread_notify_mode ==
      FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
  int32_t ret = HcommAcquireComm(desc.comm_name);
  if (ret != 0) {
    StorePayloadPrimitiveRet(desc, ret);
    StorePayloadStatus(desc,
                       FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
    BestEffortPayloadCompletionNotify(desc);
    return FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED;
  }

  unsigned int result = kFlumePayloadSuccess;
  bool batch_started = false;

  if (result == kFlumePayloadSuccess) {
    BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED);
    ret = HcommBatchModeStart(desc.batch_tag);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      result = FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED;
    } else {
      batch_started = true;
    }
  }

  if (result == kFlumePayloadSuccess && use_thread_notify) {
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED);
    ret = HcommThreadNotifyWaitOnThread(thread, 0, desc.timeout_sec);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      result = FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED;
    }
  }

  if (result == kFlumePayloadSuccess) {
    result = RunPayloadCopyBody(desc);
  }
  StorePayloadStatus(desc, result == kFlumePayloadSuccess ?
                               kFlumePayloadSuccess :
                               result);
  if (use_thread_notify) {
    if (result == kFlumePayloadSuccess) {
      BeginPayloadPrimitive(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED);
    }
    ret = HcommThreadNotifyRecordOnThread(
        thread, static_cast<ThreadHandle>(desc.cpu_thread_on_aicpu), 0);
    if (ret != 0 && result == kFlumePayloadSuccess) {
      StorePayloadPrimitiveRet(desc, ret);
      StorePayloadStatus(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED);
      result = FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED;
    }
  }
  if (batch_started) {
    if (result == kFlumePayloadSuccess) {
      BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED);
    }
    ret = HcommBatchModeEnd(desc.batch_tag);
    if (ret != 0 && result == kFlumePayloadSuccess) {
      StorePayloadPrimitiveRet(desc, ret);
      result = FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED;
    }
  }
  if (result == kFlumePayloadSuccess) {
    StorePayloadStatus(desc, kFlumePayloadSuccess);
  } else {
    StorePayloadStatus(desc, result);
  }
  if (result == kFlumePayloadSuccess) {
    StorePayloadPrimitiveRet(desc, 0);
    BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED);
  }
  ret = HcommReleaseComm(desc.comm_name);
  if (ret != 0 && result == kFlumePayloadSuccess) {
    StorePayloadPrimitiveRet(desc, ret);
    StorePayloadStatus(desc,
                       FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED);
    return FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED;
  }
  if (result == kFlumePayloadSuccess) {
    StorePayloadStatus(desc, kFlumePayloadSuccess);
    StorePayloadPrimitiveRet(desc, 0);
  }
  return result;
}

}  // namespace

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV3(void* param) {
  if (param == nullptr) {
    return kFlumePayloadInvalidArgument;
  }
  auto* desc = static_cast<flume_hcomm_payload_copy_desc_v1*>(param);
  return RunPayloadCopy(*desc);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV4(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV3(param);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV2(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV4(param);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernel(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV4(param);
}

extern "C" unsigned int FlumeHcommPayloadBuildModeInternalPayload() {
  return 1U;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion2() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION >= 2U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion3() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION >= 3U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion4() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION == 4U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadCopyRequiresCommAcquire() {
  return 1U;
}

extern "C" unsigned int FlumeHcommPayloadStatusSchemaVersion() {
  return FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadStatusWordCount() {
  return FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT;
}
