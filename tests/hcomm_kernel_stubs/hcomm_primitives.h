#ifndef FLUME_TEST_HCOMM_PRIMITIVES_H_
#define FLUME_TEST_HCOMM_PRIMITIVES_H_

#include <stdint.h>
#include <string.h>

typedef uint64_t ThreadHandle;
typedef uint64_t ChannelHandle;

namespace flume_hcomm_payload_kernel_mock {

enum Call {
  kBatchStart = 1,
  kThreadWait = 2,
  kThreadRecord = 3,
  kLocalCopy = 4,
  kRead = 5,
  kNotifyRecord = 6,
  kNotifyWait = 7,
  kBatchEnd = 8,
  kChannelDrain = 9,
};

extern int32_t batch_start_ret;
extern int32_t batch_end_ret;
extern int32_t thread_wait_ret;
extern int32_t thread_record_ret;
extern int32_t local_copy_ret;
extern int32_t read_ret;
extern int32_t notify_record_ret;
extern int32_t notify_wait_ret;
extern int32_t channel_drain_ret;
extern int calls[32];
extern int call_count;

void RecordCall(int call);

}  // namespace flume_hcomm_payload_kernel_mock

inline int32_t HcommBatchModeStart(const char*) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kBatchStart);
  return batch_start_ret;
}

inline int32_t HcommBatchModeEnd(const char*) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kBatchEnd);
  return batch_end_ret;
}

inline int32_t HcommThreadNotifyWaitOnThread(ThreadHandle, uint32_t,
                                             uint32_t) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kThreadWait);
  return thread_wait_ret;
}

inline int32_t HcommThreadNotifyRecordOnThread(ThreadHandle, ThreadHandle,
                                               uint32_t) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kThreadRecord);
  return thread_record_ret;
}

inline int32_t HcommLocalCopyOnThread(ThreadHandle, void* dst, const void* src,
                                      uint64_t bytes) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kLocalCopy);
  if (local_copy_ret == 0) {
    memcpy(dst, src, static_cast<size_t>(bytes));
  }
  return local_copy_ret;
}

inline int32_t HcommReadOnThread(ThreadHandle, ChannelHandle, void* dst,
                                 const void* src, uint64_t bytes) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kRead);
  if (read_ret == 0) {
    memcpy(dst, src, static_cast<size_t>(bytes));
  }
  return read_ret;
}

inline int32_t HcommChannelNotifyRecordOnThread(ThreadHandle, ChannelHandle,
                                                uint32_t) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kNotifyRecord);
  return notify_record_ret;
}

inline int32_t HcommChannelNotifyWaitOnThread(ThreadHandle, ChannelHandle,
                                              uint32_t, uint32_t) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kNotifyWait);
  return notify_wait_ret;
}

inline int32_t HcommChannelDrainOnThread(ThreadHandle, ChannelHandle) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kChannelDrain);
  return channel_drain_ret;
}

#endif  // FLUME_TEST_HCOMM_PRIMITIVES_H_
