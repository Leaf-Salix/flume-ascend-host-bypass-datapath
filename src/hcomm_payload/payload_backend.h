#ifndef FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_
#define FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_

namespace flume::hcomm_payload {

enum class SchedulerStatus {
  kUnavailable = 0,
  kCustomOpMissing = 1,
};

SchedulerStatus CurrentSchedulerStatus();
const char* SchedulerStatusMessage(SchedulerStatus status);

}  // namespace flume::hcomm_payload

#endif  // FLUME_HCOMM_PAYLOAD_PAYLOAD_BACKEND_H_
