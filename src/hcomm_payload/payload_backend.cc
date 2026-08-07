#include "hcomm_payload/payload_backend.h"

namespace flume::hcomm_payload {

SchedulerStatus CurrentSchedulerStatus() {
  return SchedulerStatus::kCustomOpMissing;
}

const char* SchedulerStatusMessage(SchedulerStatus status) {
  switch (status) {
    case SchedulerStatus::kCustomOpMissing:
      return "custom-op/AICPU scheduler missing";
    case SchedulerStatus::kUnavailable:
    default:
      return "HCOMM payload scheduler unavailable";
  }
}

}  // namespace flume::hcomm_payload
