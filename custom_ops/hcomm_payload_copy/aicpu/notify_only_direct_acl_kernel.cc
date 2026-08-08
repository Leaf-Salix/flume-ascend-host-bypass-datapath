#include "flume_hcomm_notify_only_abi.h"

#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#else
#include <hcomm_primitives.h>
#endif

namespace {

constexpr unsigned int kFlumeKernelSuccess = 0;
constexpr unsigned int kFlumeKernelInvalidArgument = 1;
constexpr unsigned int kFlumeKernelHcommError = 2;

bool HasNotifyDescHeader(const flume_hcomm_notify_only_desc_v1& desc) {
  return desc.magic == FLUME_HCOMM_NOTIFY_ONLY_MAGIC &&
         desc.version == FLUME_HCOMM_NOTIFY_ONLY_VERSION &&
         desc.size == sizeof(flume_hcomm_notify_only_desc_v1);
}

void StoreNotifyStatus(const flume_hcomm_notify_only_desc_v1& desc,
                       unsigned int status) {
  if (!HasNotifyDescHeader(desc) || desc.status_word == 0) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[0] = status;
}

void StoreNotifyHcommRet(const flume_hcomm_notify_only_desc_v1& desc,
                         int32_t ret) {
  if (!HasNotifyDescHeader(desc) || desc.status_word == 0) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[1] = static_cast<unsigned int>(ret);
}

unsigned int RunNotifyOnlyDirectAcl(
    const flume_hcomm_notify_only_desc_v1& desc) {
  if (!HasNotifyDescHeader(desc) || desc.rank_size != 2 ||
      desc.local_rank == desc.peer_rank ||
      desc.ready_notify_idx == desc.done_notify_idx ||
      desc.channel_handle == 0 || desc.aicpu_thread == 0) {
    StoreNotifyStatus(desc, FLUME_HCOMM_NOTIFY_STATUS_INVALID_ARGUMENT);
    return kFlumeKernelInvalidArgument;
  }
  StoreNotifyStatus(desc, FLUME_HCOMM_NOTIFY_STATUS_HCOMM_ERROR);

  ThreadHandle thread = static_cast<ThreadHandle>(desc.aicpu_thread);
  ChannelHandle channel = static_cast<ChannelHandle>(desc.channel_handle);
  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND) {
    int32_t ret = HcommChannelNotifyRecordOnThread(
        thread, channel, desc.ready_notify_idx);
    if (ret != 0) {
      StoreNotifyHcommRet(desc, ret);
      return kFlumeKernelHcommError;
    }
    ret = HcommChannelNotifyWaitOnThread(
        thread, channel, desc.done_notify_idx, desc.timeout_sec);
    if (ret != 0) {
      StoreNotifyHcommRet(desc, ret);
      return kFlumeKernelHcommError;
    }
    StoreNotifyStatus(desc, FLUME_HCOMM_NOTIFY_STATUS_SUCCESS);
    return kFlumeKernelSuccess;
  }

  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    int32_t ret = HcommChannelNotifyWaitOnThread(
        thread, channel, desc.ready_notify_idx, desc.timeout_sec);
    if (ret != 0) {
      StoreNotifyHcommRet(desc, ret);
      return kFlumeKernelHcommError;
    }
    ret = HcommChannelNotifyRecordOnThread(
        thread, channel, desc.done_notify_idx);
    if (ret != 0) {
      StoreNotifyHcommRet(desc, ret);
      return kFlumeKernelHcommError;
    }
    StoreNotifyStatus(desc, FLUME_HCOMM_NOTIFY_STATUS_SUCCESS);
    return kFlumeKernelSuccess;
  }

  StoreNotifyStatus(desc, FLUME_HCOMM_NOTIFY_STATUS_INVALID_ARGUMENT);
  return kFlumeKernelInvalidArgument;
}

}  // namespace

extern "C" unsigned int FlumeHcommNotifyOnlyDirectAclrtKernel(void* param) {
  if (param == nullptr) {
    return kFlumeKernelInvalidArgument;
  }
  auto* desc = static_cast<flume_hcomm_notify_only_desc_v1*>(param);
  return RunNotifyOnlyDirectAcl(*desc);
}
