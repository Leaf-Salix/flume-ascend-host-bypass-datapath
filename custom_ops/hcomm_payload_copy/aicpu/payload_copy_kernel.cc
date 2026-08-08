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

void StorePayloadStatus(const flume_hcomm_payload_copy_desc_v1& desc,
                        unsigned int status) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[0] = status;
}

void StorePayloadPrimitiveRet(const flume_hcomm_payload_copy_desc_v1& desc,
                              int32_t ret) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[1] = static_cast<unsigned int>(ret);
}

bool ValidatePayloadDesc(const flume_hcomm_payload_copy_desc_v1& desc) {
  return HasPayloadDescHeader(desc) && desc.rank_size == 2 &&
         desc.local_rank < desc.rank_size &&
         desc.peer_rank < desc.rank_size && desc.local_rank != desc.peer_rank &&
         desc.ready_notify_idx != desc.done_notify_idx && desc.bytes != 0 &&
         (desc.thread_notify_mode == FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE ||
          desc.thread_notify_mode ==
              FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU) &&
         desc.aicpu_thread != 0 && desc.channel_handle != 0 &&
         desc.user_buffer != 0 && desc.local_hccl_buffer != 0 &&
         desc.remote_hccl_buffer != 0 &&
         desc.bytes <= desc.local_hccl_buffer_bytes &&
         desc.bytes <= desc.remote_hccl_buffer_bytes &&
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
    int32_t ret =
        HcommLocalCopyOnThread(thread, local_hccl_buffer, user_buffer,
                               desc.bytes);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED;
    }
    ret = HcommChannelNotifyRecordOnThread(
        thread, channel, desc.ready_notify_idx);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED;
    }
    ret = HcommChannelNotifyWaitOnThread(
        thread, channel, desc.done_notify_idx, desc.timeout_sec);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED;
    }
    return kFlumePayloadSuccess;
  }

  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    int32_t ret = HcommChannelNotifyWaitOnThread(
        thread, channel, desc.ready_notify_idx, desc.timeout_sec);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED;
    }
    ret = HcommReadOnThread(thread, channel, user_buffer, remote_hccl_buffer,
                            desc.bytes);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED;
    }
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
    return kFlumePayloadInvalidArgument;
  }
  StorePayloadStatus(desc, kFlumePayloadHcommError);

  ThreadHandle thread = static_cast<ThreadHandle>(desc.aicpu_thread);
  const bool use_thread_notify =
      desc.thread_notify_mode ==
      FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  if (use_thread_notify) {
    int32_t ret =
        HcommThreadNotifyWaitOnThread(thread, 0, desc.timeout_sec);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      StorePayloadStatus(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED);
      return FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED;
    }
  }

  bool batch_started = false;
  const bool use_batch_mode = desc.batch_tag[0] != '\0';
  if (use_batch_mode) {
    int32_t ret = HcommBatchModeStart(desc.batch_tag);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      StorePayloadStatus(desc, FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED);
      return FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED;
    }
    batch_started = true;
  }

  unsigned int result = RunPayloadCopyBody(desc);
  if (batch_started) {
    int32_t ret = HcommBatchModeEnd(desc.batch_tag);
    if (ret != 0 && result == kFlumePayloadSuccess) {
      StorePayloadPrimitiveRet(desc, ret);
      result = FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED;
    }
  }
  if (result == kFlumePayloadSuccess) {
    StorePayloadStatus(desc, kFlumePayloadSuccess);
  } else if (result == kFlumePayloadInvalidArgument) {
    StorePayloadStatus(desc, kFlumePayloadInvalidArgument);
  } else {
    StorePayloadStatus(desc, kFlumePayloadHcommError);
  }
  if (use_thread_notify) {
    int32_t notify_ret = HcommThreadNotifyRecordOnThread(
        thread, static_cast<ThreadHandle>(desc.cpu_thread_on_aicpu), 0);
    if (notify_ret != 0 && result == kFlumePayloadSuccess) {
      StorePayloadPrimitiveRet(desc, notify_ret);
      StorePayloadStatus(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED);
      return FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED;
    }
  }
  return result;
}

}  // namespace

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV2(void* param) {
  if (param == nullptr) {
    return kFlumePayloadInvalidArgument;
  }
  auto* desc = static_cast<flume_hcomm_payload_copy_desc_v1*>(param);
  return RunPayloadCopy(*desc);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernel(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV2(param);
}
