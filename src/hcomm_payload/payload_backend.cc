#include "hcomm_payload/payload_backend.h"

#include <algorithm>
#include <sstream>
#include <utility>

#ifndef FLUME_BUILD_HCOMM_CUSTOM_OP
#define FLUME_BUILD_HCOMM_CUSTOM_OP 0
#endif

namespace flume {
namespace hcomm_payload {

SchedulerStatus CurrentSchedulerStatus() {
#if FLUME_BUILD_HCOMM_CUSTOM_OP
  return SchedulerStatus::kCustomOpLaunchMissing;
#else
  return SchedulerStatus::kCustomOpBuildDisabled;
#endif
}

const char* SchedulerStatusMessage(SchedulerStatus status) {
  switch (status) {
    case SchedulerStatus::kCustomOpBuildDisabled:
      return "custom-op/AICPU scheduler build disabled";
    case SchedulerStatus::kCustomOpLaunchMissing:
      return "custom-op/AICPU scheduler launch missing";
    case SchedulerStatus::kReady:
      return "custom-op/AICPU scheduler ready";
    case SchedulerStatus::kUnavailable:
    default:
      return "HCOMM payload scheduler unavailable";
  }
}

const char* PayloadRoleName(PayloadRole role) {
  switch (role) {
    case PayloadRole::kSend:
      return "send";
    case PayloadRole::kRecv:
      return "recv";
  }
  return "unknown";
}

const char* PayloadStepName(PayloadStep step) {
  switch (step) {
    case PayloadStep::kLocalCopyInputToHcclBuffer:
      return "HcommLocalCopyOnThread(input->local_hccl_buffer)";
    case PayloadStep::kChannelNotifyRecordReady:
      return "HcommChannelNotifyRecordOnThread(ready)";
    case PayloadStep::kChannelNotifyWaitReady:
      return "HcommChannelNotifyWaitOnThread(ready)";
    case PayloadStep::kChannelReadRemoteToLocalHcclBuffer:
      return "HcommReadOnThread(remote_hccl_buffer->local_hccl_buffer)";
    case PayloadStep::kChannelNotifyWaitDone:
      return "HcommChannelNotifyWaitOnThread(done)";
    case PayloadStep::kChannelNotifyRecordDone:
      return "HcommChannelNotifyRecordOnThread(done)";
    case PayloadStep::kLocalCopyLocalHcclBufferToOutput:
      return "HcommLocalCopyOnThread(local_hccl_buffer->output)";
  }
  return "unknown";
}

const char* CustomOpLaunchSmokeStepName(CustomOpLaunchSmokeStep step) {
  switch (step) {
    case CustomOpLaunchSmokeStep::kResolveScheduler:
      return "resolve custom-op/AICPU scheduler";
    case CustomOpLaunchSmokeStep::kPackageNoopKernelArgs:
      return "package no-op kernel args";
    case CustomOpLaunchSmokeStep::kSubmitNoopCustomOp:
      return "submit no-op custom-op";
    case CustomOpLaunchSmokeStep::kSyncStream:
      return "sync launch stream";
  }
  return "unknown";
}

const char* NotifyOnlyStepName(NotifyOnlyStep step) {
  switch (step) {
    case NotifyOnlyStep::kConsumeResourceDescriptor:
      return "consume resource descriptor";
    case NotifyOnlyStep::kChannelNotifyRecordReady:
      return "HcommChannelNotifyRecordOnThread(ready)";
    case NotifyOnlyStep::kChannelNotifyWaitReady:
      return "HcommChannelNotifyWaitOnThread(ready)";
    case NotifyOnlyStep::kChannelNotifyRecordDone:
      return "HcommChannelNotifyRecordOnThread(done)";
    case NotifyOnlyStep::kChannelNotifyWaitDone:
      return "HcommChannelNotifyWaitOnThread(done)";
  }
  return "unknown";
}

bool BuildPairCopyPlan(PayloadRole role,
                       uint32_t local_rank,
                       uint32_t peer_rank,
                       uint32_t rank_size,
                       uint64_t bytes,
                       PayloadPlan* out,
                       std::string* error) {
  if (out == nullptr) {
    if (error != nullptr) {
      *error = "payload plan destination is null";
    }
    return false;
  }
  if (rank_size != 2) {
    if (error != nullptr) {
      *error = "HCOMM payload pair-copy plan requires exactly two ranks";
    }
    return false;
  }
  if (local_rank >= rank_size || peer_rank >= rank_size ||
      local_rank == peer_rank) {
    if (error != nullptr) {
      *error = "invalid HCOMM payload peer rank";
    }
    return false;
  }
  if (bytes == 0) {
    if (error != nullptr) {
      *error = "HCOMM payload plan requires non-zero bytes";
    }
    return false;
  }

  PayloadPlan plan;
  plan.role = role;
  plan.local_rank = local_rank;
  plan.peer_rank = peer_rank;
  plan.rank_size = rank_size;
  plan.bytes = bytes;
  if (role == PayloadRole::kSend) {
    plan.steps = {
        PayloadStep::kLocalCopyInputToHcclBuffer,
        PayloadStep::kChannelNotifyRecordReady,
        PayloadStep::kChannelNotifyWaitDone,
    };
  } else {
    plan.steps = {
        PayloadStep::kChannelNotifyWaitReady,
        PayloadStep::kChannelReadRemoteToLocalHcclBuffer,
        PayloadStep::kLocalCopyLocalHcclBufferToOutput,
        PayloadStep::kChannelNotifyRecordDone,
    };
  }
  *out = std::move(plan);
  return true;
}

std::string DescribePlan(const PayloadPlan& plan) {
  std::ostringstream out;
  out << "stage3b_plan=pair-copy"
      << " role=" << PayloadRoleName(plan.role)
      << " local_rank=" << plan.local_rank
      << " peer_rank=" << plan.peer_rank
      << " bytes=" << plan.bytes
      << " ready_notify_idx=" << plan.ready_notify_idx
      << " done_notify_idx=" << plan.done_notify_idx
      << " scheduler=" << SchedulerStatusMessage(CurrentSchedulerStatus())
      << " steps=[";
  for (size_t i = 0; i < plan.steps.size(); ++i) {
    if (i != 0) {
      out << " -> ";
    }
    out << PayloadStepName(plan.steps[i]);
  }
  out << "]";
  return out.str();
}

bool BuildCustomOpLaunchSmokePlan(uint32_t local_rank,
                                  uint32_t peer_rank,
                                  uint32_t rank_size,
                                  CustomOpLaunchSmokePlan* out,
                                  std::string* error) {
  if (out == nullptr) {
    if (error != nullptr) {
      *error = "custom-op launch smoke plan destination is null";
    }
    return false;
  }
  if (rank_size != 2) {
    if (error != nullptr) {
      *error = "custom-op launch smoke requires exactly two ranks";
    }
    return false;
  }
  if (local_rank >= rank_size || peer_rank >= rank_size ||
      local_rank == peer_rank) {
    if (error != nullptr) {
      *error = "invalid custom-op launch smoke peer rank";
    }
    return false;
  }

  CustomOpLaunchSmokePlan plan;
  plan.local_rank = local_rank;
  plan.peer_rank = peer_rank;
  plan.rank_size = rank_size;
  plan.steps = {
      CustomOpLaunchSmokeStep::kResolveScheduler,
      CustomOpLaunchSmokeStep::kPackageNoopKernelArgs,
      CustomOpLaunchSmokeStep::kSubmitNoopCustomOp,
      CustomOpLaunchSmokeStep::kSyncStream,
  };
  *out = std::move(plan);
  return true;
}

std::string DescribeCustomOpLaunchSmokePlan(
    const CustomOpLaunchSmokePlan& plan) {
  std::ostringstream out;
  out << "stage3b1_launch_plan=noop-custom-op"
      << " local_rank=" << plan.local_rank
      << " peer_rank=" << plan.peer_rank
      << " scheduler=" << SchedulerStatusMessage(CurrentSchedulerStatus())
      << " steps=[";
  for (size_t i = 0; i < plan.steps.size(); ++i) {
    if (i != 0) {
      out << " -> ";
    }
    out << CustomOpLaunchSmokeStepName(plan.steps[i]);
  }
  out << "]";
  return out.str();
}

bool BuildResourceDescriptor(uint32_t local_rank,
                             uint32_t peer_rank,
                             uint32_t rank_size,
                             uint32_t channel_count,
                             uint32_t notify_num,
                             uint64_t local_hccl_buffer_bytes,
                             uint64_t remote_hccl_buffer_bytes,
                             bool thread_export_required,
                             std::string resolved_engine,
                             std::string resolved_protocol,
                             std::string channel_desc_source,
                             ResourceDescriptor* out,
                             std::string* error) {
  if (out == nullptr) {
    if (error != nullptr) {
      *error = "resource descriptor destination is null";
    }
    return false;
  }
  if (rank_size != 2) {
    if (error != nullptr) {
      *error = "HCOMM resource descriptor smoke requires exactly two ranks";
    }
    return false;
  }
  if (local_rank >= rank_size || peer_rank >= rank_size ||
      local_rank == peer_rank) {
    if (error != nullptr) {
      *error = "invalid HCOMM resource descriptor peer rank";
    }
    return false;
  }
  if (channel_count == 0) {
    if (error != nullptr) {
      *error = "HCOMM resource descriptor requires at least one channel";
    }
    return false;
  }
  if (notify_num < 2) {
    if (error != nullptr) {
      *error = "HCOMM resource descriptor requires at least two notifies";
    }
    return false;
  }
  if (local_hccl_buffer_bytes == 0 || remote_hccl_buffer_bytes == 0) {
    if (error != nullptr) {
      *error = "HCOMM resource descriptor requires non-empty HCCL buffers";
    }
    return false;
  }
  if (resolved_engine.empty() || resolved_protocol.empty() ||
      channel_desc_source.empty()) {
    if (error != nullptr) {
      *error = "HCOMM resource descriptor metadata is incomplete";
    }
    return false;
  }

  ResourceDescriptor descriptor;
  descriptor.local_rank = local_rank;
  descriptor.peer_rank = peer_rank;
  descriptor.rank_size = rank_size;
  descriptor.channel_count = channel_count;
  descriptor.notify_num = notify_num;
  descriptor.local_hccl_buffer_bytes = local_hccl_buffer_bytes;
  descriptor.remote_hccl_buffer_bytes = remote_hccl_buffer_bytes;
  descriptor.usable_hccl_buffer_bytes =
      std::min(local_hccl_buffer_bytes, remote_hccl_buffer_bytes);
  descriptor.local_hccl_buffer_acquired = true;
  descriptor.remote_hccl_buffer_acquired = true;
  descriptor.thread_export_required = thread_export_required;
  descriptor.resolved_engine = std::move(resolved_engine);
  descriptor.resolved_protocol = std::move(resolved_protocol);
  descriptor.channel_desc_source = std::move(channel_desc_source);
  *out = std::move(descriptor);
  return true;
}

std::string DescribeResourceDescriptor(const ResourceDescriptor& descriptor) {
  std::ostringstream out;
  out << "stage3b2_resource_descriptor=host-packaged"
      << " local_rank=" << descriptor.local_rank
      << " peer_rank=" << descriptor.peer_rank
      << " channel_count=" << descriptor.channel_count
      << " notify_num=" << descriptor.notify_num
      << " ready_notify_idx=" << descriptor.ready_notify_idx
      << " done_notify_idx=" << descriptor.done_notify_idx
      << " local_hccl_buffer=acquired"
      << " remote_hccl_buffer=acquired"
      << " usable_hccl_buffer_bytes="
      << descriptor.usable_hccl_buffer_bytes
      << " resolved_engine=" << descriptor.resolved_engine
      << " resolved_protocol=" << descriptor.resolved_protocol
      << " channel_desc=" << descriptor.channel_desc_source
      << " thread_export="
      << (descriptor.thread_export_required ? "required" : "not-required")
      << " handoff=missing";
  return out.str();
}

bool BuildNotifyOnlyPlan(PayloadRole role,
                         uint32_t local_rank,
                         uint32_t peer_rank,
                         uint32_t rank_size,
                         uint32_t ready_notify_idx,
                         uint32_t done_notify_idx,
                         NotifyOnlyPlan* out,
                         std::string* error) {
  if (out == nullptr) {
    if (error != nullptr) {
      *error = "notify-only plan destination is null";
    }
    return false;
  }
  if (rank_size != 2) {
    if (error != nullptr) {
      *error = "HCOMM notify-only smoke requires exactly two ranks";
    }
    return false;
  }
  if (local_rank >= rank_size || peer_rank >= rank_size ||
      local_rank == peer_rank) {
    if (error != nullptr) {
      *error = "invalid HCOMM notify-only peer rank";
    }
    return false;
  }
  if (ready_notify_idx == done_notify_idx) {
    if (error != nullptr) {
      *error = "HCOMM notify-only plan requires distinct ready/done notify indices";
    }
    return false;
  }

  NotifyOnlyPlan plan;
  plan.role = role;
  plan.local_rank = local_rank;
  plan.peer_rank = peer_rank;
  plan.rank_size = rank_size;
  plan.ready_notify_idx = ready_notify_idx;
  plan.done_notify_idx = done_notify_idx;
  if (role == PayloadRole::kSend) {
    plan.steps = {
        NotifyOnlyStep::kConsumeResourceDescriptor,
        NotifyOnlyStep::kChannelNotifyRecordReady,
        NotifyOnlyStep::kChannelNotifyWaitDone,
    };
  } else {
    plan.steps = {
        NotifyOnlyStep::kConsumeResourceDescriptor,
        NotifyOnlyStep::kChannelNotifyWaitReady,
        NotifyOnlyStep::kChannelNotifyRecordDone,
    };
  }
  *out = std::move(plan);
  return true;
}

std::string DescribeNotifyOnlyPlan(const NotifyOnlyPlan& plan) {
  std::ostringstream out;
  out << "stage3b2_notify_only_plan=channel-notify"
      << " role=" << PayloadRoleName(plan.role)
      << " local_rank=" << plan.local_rank
      << " peer_rank=" << plan.peer_rank
      << " ready_notify_idx=" << plan.ready_notify_idx
      << " done_notify_idx=" << plan.done_notify_idx
      << " timeout_sec=" << plan.timeout_sec
      << " scheduler=" << SchedulerStatusMessage(CurrentSchedulerStatus())
      << " steps=[";
  for (size_t i = 0; i < plan.steps.size(); ++i) {
    if (i != 0) {
      out << " -> ";
    }
    out << NotifyOnlyStepName(plan.steps[i]);
  }
  out << "]";
  return out.str();
}

}  // namespace hcomm_payload
}  // namespace flume
