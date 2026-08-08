#ifndef FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_
#define FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_

#include <stdint.h>

#include <string>
#include <vector>

namespace flume {
namespace hcomm_payload {

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
  kChannelReadRemoteToLocalHcclBuffer = 3,
  kChannelNotifyWaitDone = 4,
  kChannelNotifyRecordDone = 5,
  kLocalCopyLocalHcclBufferToOutput = 6,
};

enum class CustomOpLaunchSmokeStep {
  kResolveScheduler = 0,
  kPackageNoopKernelArgs = 1,
  kSubmitNoopCustomOp = 2,
  kSyncStream = 3,
};

enum class NotifyOnlyStep {
  kConsumeResourceDescriptor = 0,
  kChannelNotifyRecordReady = 1,
  kChannelNotifyWaitReady = 2,
  kChannelNotifyRecordDone = 3,
  kChannelNotifyWaitDone = 4,
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

struct NotifyOnlyPlan {
  PayloadRole role = PayloadRole::kSend;
  uint32_t local_rank = 0;
  uint32_t peer_rank = 0;
  uint32_t rank_size = 0;
  uint32_t ready_notify_idx = 0;
  uint32_t done_notify_idx = 1;
  uint32_t timeout_sec = 60;
  std::vector<NotifyOnlyStep> steps;
};

struct CustomOpLaunchSmokePlan {
  uint32_t local_rank = 0;
  uint32_t peer_rank = 0;
  uint32_t rank_size = 0;
  std::vector<CustomOpLaunchSmokeStep> steps;
};

struct ResourceDescriptor {
  uint32_t local_rank = 0;
  uint32_t peer_rank = 0;
  uint32_t rank_size = 0;
  uint32_t channel_count = 0;
  uint32_t notify_num = 0;
  uint32_t ready_notify_idx = 0;
  uint32_t done_notify_idx = 1;
  uint64_t local_hccl_buffer_bytes = 0;
  uint64_t remote_hccl_buffer_bytes = 0;
  uint64_t usable_hccl_buffer_bytes = 0;
  bool local_hccl_buffer_acquired = false;
  bool remote_hccl_buffer_acquired = false;
  bool thread_export_required = false;
  std::string resolved_engine;
  std::string resolved_protocol;
  std::string channel_desc_source;
};

SchedulerStatus CurrentSchedulerStatus();
const char* SchedulerStatusMessage(SchedulerStatus status);
const char* PayloadRoleName(PayloadRole role);
const char* PayloadStepName(PayloadStep step);
const char* CustomOpLaunchSmokeStepName(CustomOpLaunchSmokeStep step);
const char* NotifyOnlyStepName(NotifyOnlyStep step);

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
                             std::string* error);

std::string DescribeResourceDescriptor(const ResourceDescriptor& descriptor);

bool BuildNotifyOnlyPlan(PayloadRole role,
                         uint32_t local_rank,
                         uint32_t peer_rank,
                         uint32_t rank_size,
                         uint32_t ready_notify_idx,
                         uint32_t done_notify_idx,
                         NotifyOnlyPlan* out,
                         std::string* error);

std::string DescribeNotifyOnlyPlan(const NotifyOnlyPlan& plan);

}  // namespace hcomm_payload
}  // namespace flume

#endif  // FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_
