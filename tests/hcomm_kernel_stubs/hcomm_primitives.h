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
  kChannelFence = 9,
  kAcquireComm = 10,
  kReleaseComm = 11,
  kWrite = 12,
};

extern int32_t acquire_comm_ret;
extern int32_t release_comm_ret;
extern int32_t batch_start_ret;
extern int32_t batch_end_ret;
extern int32_t thread_wait_ret;
extern int32_t thread_record_ret;
extern int32_t local_copy_ret;
extern int32_t read_ret;
extern int32_t write_ret;
extern int32_t notify_record_ret;
extern int32_t notify_wait_ret;
extern int32_t channel_drain_ret;
extern int calls[32];
extern int call_count;
extern char batch_start_tag[64];
extern char batch_end_tag[64];
extern void* last_local_copy_dst;
extern const void* last_local_copy_src;
extern void* last_read_dst;
extern const void* last_read_src;
extern void* last_write_dst;
extern const void* last_write_src;
extern uint32_t* status_probe_words;
extern int status_probe_call;
extern uint32_t status_observed_at_probe;

void RecordCall(int call);

}  // namespace flume_hcomm_payload_kernel_mock

inline int32_t HcommAcquireComm(const char*) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kAcquireComm);
  return acquire_comm_ret;
}

inline int32_t HcommReleaseComm(const char*) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kReleaseComm);
  return release_comm_ret;
}

inline int32_t HcommBatchModeStart(const char* tag) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kBatchStart);
  strncpy(batch_start_tag, tag == nullptr ? "<null>" : tag,
          sizeof(batch_start_tag) - 1);
  batch_start_tag[sizeof(batch_start_tag) - 1] = '\0';
  return batch_start_ret;
}

inline int32_t HcommBatchModeEnd(const char* tag) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kBatchEnd);
  strncpy(batch_end_tag, tag == nullptr ? "<null>" : tag,
          sizeof(batch_end_tag) - 1);
  batch_end_tag[sizeof(batch_end_tag) - 1] = '\0';
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
  last_local_copy_dst = dst;
  last_local_copy_src = src;
  if (local_copy_ret == 0) {
    memcpy(dst, src, static_cast<size_t>(bytes));
  }
  return local_copy_ret;
}

inline int32_t HcommReadOnThread(ThreadHandle, ChannelHandle, void* dst,
                                 const void* src, uint64_t bytes) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kRead);
  last_read_dst = dst;
  last_read_src = src;
  if (read_ret == 0) {
    memcpy(dst, src, static_cast<size_t>(bytes));
  }
  return read_ret;
}

inline int32_t HcommWriteOnThread(ThreadHandle, ChannelHandle, void* dst,
                                  const void* src, uint64_t bytes) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kWrite);
  last_write_dst = dst;
  last_write_src = src;
  if (write_ret == 0) {
    memcpy(dst, src, static_cast<size_t>(bytes));
  }
  return write_ret;
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

inline int32_t HcommChannelFenceOnThread(ThreadHandle, ChannelHandle) {
  using namespace flume_hcomm_payload_kernel_mock;
  RecordCall(kChannelFence);
  return channel_drain_ret;
}

#endif  // FLUME_TEST_HCOMM_PRIMITIVES_H_
