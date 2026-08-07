#include "flume_hcomm_notify_only_abi.h"

#include <hccl/hccl_launch.h>
#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#else
#include <hcomm_primitives.h>
#endif

namespace {

constexpr unsigned int kFlumeKernelSuccess = 0;
constexpr unsigned int kFlumeKernelInvalidArgument = 1;
constexpr unsigned int kFlumeKernelHcommError = 2;

unsigned int RunNotifyOnly(const flume_hcomm_notify_only_desc_v1& desc,
                           ThreadHandle fallback_thread) {
  if (desc.magic != FLUME_HCOMM_NOTIFY_ONLY_MAGIC ||
      desc.version != FLUME_HCOMM_NOTIFY_ONLY_VERSION ||
      desc.size != sizeof(flume_hcomm_notify_only_desc_v1) ||
      desc.rank_size != 2 || desc.local_rank == desc.peer_rank ||
      desc.ready_notify_idx == desc.done_notify_idx ||
      desc.channel_handle == 0) {
    return kFlumeKernelInvalidArgument;
  }

  ThreadHandle thread = desc.aicpu_thread != 0 ?
                            static_cast<ThreadHandle>(desc.aicpu_thread) :
                            fallback_thread;
  ChannelHandle channel = static_cast<ChannelHandle>(desc.channel_handle);
  if (thread == 0) {
    return kFlumeKernelInvalidArgument;
  }

  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND) {
    if (HcommChannelNotifyRecordOnThread(
            thread, channel, desc.ready_notify_idx) != 0) {
      return kFlumeKernelHcommError;
    }
    if (HcommChannelNotifyWaitOnThread(
            thread, channel, desc.done_notify_idx, desc.timeout_sec) != 0) {
      return kFlumeKernelHcommError;
    }
    return kFlumeKernelSuccess;
  }

  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    if (HcommChannelNotifyWaitOnThread(
            thread, channel, desc.ready_notify_idx, desc.timeout_sec) != 0) {
      return kFlumeKernelHcommError;
    }
    if (HcommChannelNotifyRecordOnThread(
            thread, channel, desc.done_notify_idx) != 0) {
      return kFlumeKernelHcommError;
    }
    return kFlumeKernelSuccess;
  }

  return kFlumeKernelInvalidArgument;
}

}  // namespace

extern "C" unsigned int FlumeHcommNotifyOnlyAicpuKernel(void* param) {
  if (param == nullptr) {
    return kFlumeKernelInvalidArgument;
  }
  auto* p2p_param = static_cast<HcclP2pKernelParam*>(param);
  auto* desc = reinterpret_cast<flume_hcomm_notify_only_desc_v1*>(
      p2p_param->opParams);
  return RunNotifyOnly(*desc, p2p_param->sendRecvThread);
}
