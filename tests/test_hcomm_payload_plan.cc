#include "test_util.h"

#include <string>

#include "hcomm_payload/payload_backend.h"

int main() {
  using flume::hcomm_payload::BuildPairCopyPlan;
  using flume::hcomm_payload::CurrentSchedulerStatus;
  using flume::hcomm_payload::DescribePlan;
  using flume::hcomm_payload::PayloadPlan;
  using flume::hcomm_payload::PayloadRole;
  using flume::hcomm_payload::PayloadStep;
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
  FLUME_TEST_CHECK(recv.steps.size() == 3);
  FLUME_TEST_CHECK(recv.steps[0] ==
                   PayloadStep::kChannelNotifyWaitReady);
  FLUME_TEST_CHECK(recv.steps[1] ==
                   PayloadStep::kChannelReadRemoteToOutput);
  FLUME_TEST_CHECK(recv.steps[2] ==
                   PayloadStep::kChannelNotifyRecordDone);
  std::string recv_desc = DescribePlan(recv);
  FLUME_TEST_CHECK(recv_desc.find("role=recv") != std::string::npos);
  FLUME_TEST_CHECK(recv_desc.find("HcommReadOnThread") != std::string::npos);

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

  SchedulerStatus status = CurrentSchedulerStatus();
  FLUME_TEST_CHECK(status == SchedulerStatus::kCustomOpBuildDisabled ||
                   status == SchedulerStatus::kCustomOpLaunchMissing);
  FLUME_TEST_CHECK(std::string(SchedulerStatusMessage(status))
                       .find("custom-op/AICPU") != std::string::npos);
  return 0;
}
