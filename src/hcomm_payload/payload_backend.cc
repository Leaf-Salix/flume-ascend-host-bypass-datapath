#include "hcomm_payload/payload_backend.h"

#include <sstream>
#include <utility>

#ifndef FLUME_BUILD_HCOMM_CUSTOM_OP
#define FLUME_BUILD_HCOMM_CUSTOM_OP 0
#endif

namespace flume::hcomm_payload {

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
    case PayloadStep::kChannelReadRemoteToOutput:
      return "HcommReadOnThread(remote_hccl_buffer->output)";
    case PayloadStep::kChannelNotifyWaitDone:
      return "HcommChannelNotifyWaitOnThread(done)";
    case PayloadStep::kChannelNotifyRecordDone:
      return "HcommChannelNotifyRecordOnThread(done)";
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
        PayloadStep::kChannelReadRemoteToOutput,
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

}  // namespace flume::hcomm_payload
