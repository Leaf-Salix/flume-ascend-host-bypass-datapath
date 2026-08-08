#include "test_util.h"

#include <string>

#include "hcomm_payload/payload_backend.h"

int main() {
  using flume::hcomm_payload::BuildPairCopyPlan;
  using flume::hcomm_payload::BuildCustomOpLaunchSmokePlan;
  using flume::hcomm_payload::CustomOpLaunchSmokePlan;
  using flume::hcomm_payload::CurrentSchedulerStatus;
  using flume::hcomm_payload::DescribeCustomOpLaunchSmokePlan;
  using flume::hcomm_payload::DescribePlan;
  using flume::hcomm_payload::DescribeResourceDescriptor;
  using flume::hcomm_payload::PayloadPlan;
  using flume::hcomm_payload::PayloadRole;
  using flume::hcomm_payload::PayloadStep;
  using flume::hcomm_payload::BuildResourceDescriptor;
  using flume::hcomm_payload::BuildNotifyOnlyPlan;
  using flume::hcomm_payload::ResourceDescriptor;
  using flume::hcomm_payload::DescribeNotifyOnlyPlan;
  using flume::hcomm_payload::NotifyOnlyPlan;
  using flume::hcomm_payload::NotifyOnlyStep;
  using flume::hcomm_payload::SchedulerStatus;
  using flume::hcomm_payload::SchedulerStatusMessage;

  PayloadPlan send;
  std::string error;
  FLUME_TEST_CHECK(BuildPairCopyPlan(PayloadRole::kSend, 0, 1, 2, 4096,
                                     &send, &error));
  FLUME_TEST_CHECK(send.role == PayloadRole::kSend);
  FLUME_TEST_CHECK(send.local_rank == 0);
  FLUME_TEST_CHECK(send.peer_rank == 1);
  FLUME_TEST_CHECK(send.bytes == 4096);
  FLUME_TEST_CHECK(send.steps.size() == 3);
  FLUME_TEST_CHECK(send.steps[0] ==
                   PayloadStep::kLocalCopyInputToHcclBuffer);
  FLUME_TEST_CHECK(send.steps[1] ==
                   PayloadStep::kChannelNotifyRecordReady);
  FLUME_TEST_CHECK(send.steps[2] ==
                   PayloadStep::kChannelNotifyWaitDone);
  std::string send_desc = DescribePlan(send);
  FLUME_TEST_CHECK(send_desc.find("stage3b_plan=pair-copy") !=
                   std::string::npos);
  FLUME_TEST_CHECK(send_desc.find("role=send") != std::string::npos);
  FLUME_TEST_CHECK(send_desc.find("HcommLocalCopyOnThread") !=
                   std::string::npos);

  PayloadPlan recv;
  FLUME_TEST_CHECK(BuildPairCopyPlan(PayloadRole::kRecv, 1, 0, 2, 4096,
                                     &recv, &error));
  FLUME_TEST_CHECK(recv.steps.size() == 4);
  FLUME_TEST_CHECK(recv.steps[0] ==
                   PayloadStep::kChannelNotifyWaitReady);
  FLUME_TEST_CHECK(recv.steps[1] ==
                   PayloadStep::kChannelReadRemoteToLocalHcclBuffer);
  FLUME_TEST_CHECK(recv.steps[2] ==
                   PayloadStep::kLocalCopyLocalHcclBufferToOutput);
  FLUME_TEST_CHECK(recv.steps[3] ==
                   PayloadStep::kChannelNotifyRecordDone);
  std::string recv_desc = DescribePlan(recv);
  FLUME_TEST_CHECK(recv_desc.find("role=recv") != std::string::npos);
  FLUME_TEST_CHECK(recv_desc.find("HcommReadOnThread") != std::string::npos);
  FLUME_TEST_CHECK(recv_desc.find("local_hccl_buffer->output") !=
                   std::string::npos);

  PayloadPlan invalid;
  FLUME_TEST_CHECK(!BuildPairCopyPlan(PayloadRole::kSend, 0, 1, 3, 4096,
                                      &invalid, &error));
  FLUME_TEST_CHECK(error.find("exactly two ranks") != std::string::npos);
  FLUME_TEST_CHECK(!BuildPairCopyPlan(PayloadRole::kSend, 0, 0, 2, 4096,
                                      &invalid, &error));
  FLUME_TEST_CHECK(error.find("peer rank") != std::string::npos);
  FLUME_TEST_CHECK(!BuildPairCopyPlan(PayloadRole::kSend, 0, 1, 2, 0,
                                      &invalid, &error));
  FLUME_TEST_CHECK(error.find("non-zero bytes") != std::string::npos);

  CustomOpLaunchSmokePlan launch;
  FLUME_TEST_CHECK(BuildCustomOpLaunchSmokePlan(0, 1, 2, &launch, &error));
  FLUME_TEST_CHECK(launch.local_rank == 0);
  FLUME_TEST_CHECK(launch.peer_rank == 1);
  FLUME_TEST_CHECK(launch.steps.size() == 4);
  std::string launch_desc = DescribeCustomOpLaunchSmokePlan(launch);
  FLUME_TEST_CHECK(launch_desc.find("stage3b1_launch_plan=noop-custom-op") !=
                   std::string::npos);
  FLUME_TEST_CHECK(launch_desc.find("submit no-op custom-op") !=
                   std::string::npos);
  FLUME_TEST_CHECK(!BuildCustomOpLaunchSmokePlan(0, 1, 3, &launch, &error));
  FLUME_TEST_CHECK(error.find("exactly two ranks") != std::string::npos);

  ResourceDescriptor descriptor;
  FLUME_TEST_CHECK(BuildResourceDescriptor(
      0, 1, 2, 1, 2, 8192, 4096, false, "cpu-ts", "hccs",
      "rank-graph", &descriptor, &error));
  FLUME_TEST_CHECK(descriptor.usable_hccl_buffer_bytes == 4096);
  std::string descriptor_desc = DescribeResourceDescriptor(descriptor);
  FLUME_TEST_CHECK(descriptor_desc.find(
                       "stage3b2_resource_descriptor=host-packaged") !=
                   std::string::npos);
  FLUME_TEST_CHECK(descriptor_desc.find("local_hccl_buffer=acquired") !=
                   std::string::npos);
  FLUME_TEST_CHECK(descriptor_desc.find("handoff=missing") !=
                   std::string::npos);
  FLUME_TEST_CHECK(!BuildResourceDescriptor(
      0, 1, 2, 1, 1, 8192, 4096, false, "cpu-ts", "hccs", "rank-graph",
      &descriptor, &error));
  FLUME_TEST_CHECK(error.find("two notifies") != std::string::npos);

  NotifyOnlyPlan notify_send;
  FLUME_TEST_CHECK(BuildNotifyOnlyPlan(PayloadRole::kSend, 0, 1, 2, 0, 1,
                                       &notify_send, &error));
  FLUME_TEST_CHECK(notify_send.steps.size() == 3);
  FLUME_TEST_CHECK(notify_send.steps[0] ==
                   NotifyOnlyStep::kConsumeResourceDescriptor);
  FLUME_TEST_CHECK(notify_send.steps[1] ==
                   NotifyOnlyStep::kChannelNotifyRecordReady);
  FLUME_TEST_CHECK(notify_send.steps[2] ==
                   NotifyOnlyStep::kChannelNotifyWaitDone);
  std::string notify_desc = DescribeNotifyOnlyPlan(notify_send);
  FLUME_TEST_CHECK(notify_desc.find(
                       "stage3b2_notify_only_plan=channel-notify") !=
                   std::string::npos);
  FLUME_TEST_CHECK(notify_desc.find("HcommChannelNotifyWaitOnThread(done)") !=
                   std::string::npos);

  NotifyOnlyPlan notify_recv;
  FLUME_TEST_CHECK(BuildNotifyOnlyPlan(PayloadRole::kRecv, 1, 0, 2, 0, 1,
                                       &notify_recv, &error));
  FLUME_TEST_CHECK(notify_recv.steps[1] ==
                   NotifyOnlyStep::kChannelNotifyWaitReady);
  FLUME_TEST_CHECK(notify_recv.steps[2] ==
                   NotifyOnlyStep::kChannelNotifyRecordDone);
  FLUME_TEST_CHECK(!BuildNotifyOnlyPlan(PayloadRole::kSend, 0, 1, 2, 0, 0,
                                        &notify_send, &error));
  FLUME_TEST_CHECK(error.find("distinct") != std::string::npos);

  SchedulerStatus status = CurrentSchedulerStatus();
  FLUME_TEST_CHECK(status == SchedulerStatus::kCustomOpBuildDisabled ||
                   status == SchedulerStatus::kCustomOpLaunchMissing);
  FLUME_TEST_CHECK(std::string(SchedulerStatusMessage(status))
                       .find("custom-op/AICPU") != std::string::npos);
  return 0;
}
