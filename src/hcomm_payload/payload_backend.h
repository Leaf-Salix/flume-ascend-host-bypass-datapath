#ifndef FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_
#define FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_

#include <stdint.h>

#include <string>
#include <vector>

namespace flume::hcomm_payload {

enum class SchedulerStatus {
  kUnavailable = 0,
  kCustomOpBuildDisabled = 1,
  kCustomOpLaunchMissing = 2,
  kReady = 3,
};

enum class PayloadRole {
  kSend = 0,
  kRecv = 1,
};

enum class PayloadStep {
  kLocalCopyInputToHcclBuffer = 0,
  kChannelNotifyRecordReady = 1,
  kChannelNotifyWaitReady = 2,
  kChannelReadRemoteToOutput = 3,
  kChannelNotifyWaitDone = 4,
  kChannelNotifyRecordDone = 5,
};

enum class CustomOpLaunchSmokeStep {
  kResolveScheduler = 0,
  kPackageNoopKernelArgs = 1,
  kSubmitNoopCustomOp = 2,
  kSyncStream = 3,
};

struct PayloadPlan {
  PayloadRole role = PayloadRole::kSend;
  uint32_t local_rank = 0;
  uint32_t peer_rank = 0;
  uint32_t rank_size = 0;
  uint64_t bytes = 0;
  uint32_t ready_notify_idx = 0;
  uint32_t done_notify_idx = 1;
  std::vector<PayloadStep> steps;
};

struct CustomOpLaunchSmokePlan {
  uint32_t local_rank = 0;
  uint32_t peer_rank = 0;
  uint32_t rank_size = 0;
  std::vector<CustomOpLaunchSmokeStep> steps;
};

SchedulerStatus CurrentSchedulerStatus();
const char* SchedulerStatusMessage(SchedulerStatus status);
const char* PayloadRoleName(PayloadRole role);
const char* PayloadStepName(PayloadStep step);
const char* CustomOpLaunchSmokeStepName(CustomOpLaunchSmokeStep step);

bool BuildPairCopyPlan(PayloadRole role,
                       uint32_t local_rank,
                       uint32_t peer_rank,
                       uint32_t rank_size,
                       uint64_t bytes,
                       PayloadPlan* out,
                       std::string* error);

std::string DescribePlan(const PayloadPlan& plan);

bool BuildCustomOpLaunchSmokePlan(uint32_t local_rank,
                                  uint32_t peer_rank,
                                  uint32_t rank_size,
                                  CustomOpLaunchSmokePlan* out,
                                  std::string* error);

std::string DescribeCustomOpLaunchSmokePlan(
    const CustomOpLaunchSmokePlan& plan);

}  // namespace flume::hcomm_payload

#endif  // FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_
