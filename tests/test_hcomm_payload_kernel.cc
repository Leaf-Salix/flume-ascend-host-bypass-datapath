#include "test_util.h"

#include <cstdint>
#include <cstring>

#include "flume_hcomm_notify_only_abi.h"

namespace flume_hcomm_payload_kernel_mock {

int32_t batch_start_ret = 0;
int32_t batch_end_ret = 0;
int32_t acquire_comm_ret = 0;
int32_t release_comm_ret = 0;
int32_t thread_wait_ret = 0;
int32_t thread_record_ret = 0;
int32_t local_copy_ret = 0;
int32_t read_ret = 0;
int32_t notify_record_ret = 0;
int32_t notify_wait_ret = 0;
int32_t channel_drain_ret = 0;
int calls[32] = {};
int call_count = 0;
char batch_start_tag[64] = {};
char batch_end_tag[64] = {};
void* last_local_copy_dst = nullptr;
const void* last_local_copy_src = nullptr;
void* last_read_dst = nullptr;
const void* last_read_src = nullptr;
uint32_t* status_probe_words = nullptr;
int status_probe_call = 0;
uint32_t status_observed_at_probe = 0xFFFFFFFFU;

void RecordCall(int call) {
  if (status_probe_words != nullptr && call == status_probe_call) {
    status_observed_at_probe = status_probe_words[0];
  }
  if (call_count < static_cast<int>(sizeof(calls) / sizeof(calls[0]))) {
    calls[call_count++] = call;
  }
}

void Reset() {
  batch_start_ret = 0;
  batch_end_ret = 0;
  acquire_comm_ret = 0;
  release_comm_ret = 0;
  thread_wait_ret = 0;
  thread_record_ret = 0;
  local_copy_ret = 0;
  read_ret = 0;
  notify_record_ret = 0;
  notify_wait_ret = 0;
  channel_drain_ret = 0;
  std::memset(calls, 0, sizeof(calls));
  std::memset(batch_start_tag, 0, sizeof(batch_start_tag));
  std::memset(batch_end_tag, 0, sizeof(batch_end_tag));
  last_local_copy_dst = nullptr;
  last_local_copy_src = nullptr;
  last_read_dst = nullptr;
  last_read_src = nullptr;
  status_probe_words = nullptr;
  status_probe_call = 0;
  status_observed_at_probe = 0xFFFFFFFFU;
  call_count = 0;
}

bool CallsEqual(const int* expected, int count) {
  if (call_count != count) {
    return false;
  }
  for (int i = 0; i < count; ++i) {
    if (calls[i] != expected[i]) {
      return false;
    }
  }
  return true;
}

}  // namespace flume_hcomm_payload_kernel_mock

#include "payload_copy_kernel.cc"

namespace {

flume_hcomm_payload_copy_desc_v1 MakeDesc(
    uint32_t role,
    uint8_t* user_buffer,
    uint8_t* local_hccl_buffer,
    uint8_t* remote_hccl_buffer,
    uint32_t* status_words,
    uint32_t* trace_words = nullptr) {
  flume_hcomm_payload_copy_desc_v1 desc = {};
  flume_hcomm_payload_copy_desc_init(&desc);
  desc.role = role;
  desc.local_rank = role == FLUME_HCOMM_NOTIFY_ROLE_SEND ? 0 : 1;
  desc.peer_rank = role == FLUME_HCOMM_NOTIFY_ROLE_SEND ? 1 : 0;
  desc.rank_size = 2;
  desc.ready_notify_idx = 0;
  desc.done_notify_idx = 1;
  desc.bytes = 16;
  desc.aicpu_thread = 0x100;
  desc.channel_handle = 0x200;
  desc.user_buffer = reinterpret_cast<uint64_t>(user_buffer);
  desc.local_hccl_buffer = reinterpret_cast<uint64_t>(local_hccl_buffer);
  desc.remote_hccl_buffer = reinterpret_cast<uint64_t>(remote_hccl_buffer);
  desc.local_hccl_buffer_bytes = 64;
  desc.remote_hccl_buffer_bytes = 64;
  desc.status_word = reinterpret_cast<uint64_t>(status_words);
  if (trace_words != nullptr) {
    desc.reserved2[2] = reinterpret_cast<uint64_t>(trace_words);
  }
  std::memcpy(desc.batch_tag, FLUME_HCOMM_PAYLOAD_DEFAULT_BATCH_TAG,
              sizeof(FLUME_HCOMM_PAYLOAD_DEFAULT_BATCH_TAG));
  std::memcpy(desc.comm_name, "flume_unit_comm", sizeof("flume_unit_comm"));
  return desc;
}

}  // namespace

int main() {
  using namespace flume_hcomm_payload_kernel_mock;

  FLUME_TEST_CHECK(FlumeHcommPayloadCopyAbiVersion() ==
                   FLUME_HCOMM_PAYLOAD_COPY_VERSION);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyAbiVersion2() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyAbiVersion3() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyAbiVersion4() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion() ==
                   FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion5() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion6() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion7() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion8() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyRequiresCommAcquire() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadStatusSchemaVersion() ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION);
  FLUME_TEST_CHECK(FlumeHcommPayloadStatusWordCount() ==
                   FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT);

  uint8_t user[64] = {};
  uint8_t local[64] = {};
  uint8_t remote[64] = {};
  uint32_t status[FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT];
  uint32_t trace[FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT];
  auto reset_status = [&status]() {
    for (uint32_t& word : status) {
      word = 0xFFFFFFFFU;
    }
  };
  auto reset_trace = [&trace]() {
    for (uint32_t& word : trace) {
      word = 0xFFFFFFFFU;
    }
  };

  for (uint8_t i = 0; i < 16; ++i) {
    user[i] = static_cast<uint8_t>(i + 1);
  }
  Reset();
  reset_status();
  reset_trace();
  auto send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                            status, trace);
  FLUME_TEST_CHECK(std::strcmp(send_desc.batch_tag,
                               FLUME_HCOMM_PAYLOAD_DEFAULT_BATCH_TAG) == 0);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);
  FLUME_TEST_CHECK(status[2] == FLUME_HCOMM_NOTIFY_ROLE_SEND);
  FLUME_TEST_CHECK(status[3] == 1U);
  FLUME_TEST_CHECK(status[4] == 16U);
  FLUME_TEST_CHECK(status[5] == 0U);
  FLUME_TEST_CHECK(status[6] == 0U);
  FLUME_TEST_CHECK(status[7] == FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY);
  FLUME_TEST_CHECK(trace[0] == FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION);
  FLUME_TEST_CHECK(trace[1] == FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT);
  FLUME_TEST_CHECK(trace[2] == FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT);
  FLUME_TEST_CHECK(trace[3] == 0U);
  FLUME_TEST_CHECK(trace[4] > 0U);
  FLUME_TEST_CHECK(trace[5] == FLUME_HCOMM_NOTIFY_ROLE_SEND);
  FLUME_TEST_CHECK(trace[6] == 0U);
  FLUME_TEST_CHECK(trace[7] == 1U);
  FLUME_TEST_CHECK(trace[8] == 16U);
  FLUME_TEST_CHECK(trace[15] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  const uint32_t event_base = FLUME_HCOMM_PAYLOAD_TRACE_HEADER_WORD_COUNT;
  const uint32_t send_events[] = {
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT,
  };
  FLUME_TEST_CHECK(trace[4] == sizeof(send_events) / sizeof(send_events[0]));
  for (uint32_t i = 0; i < trace[4]; ++i) {
    FLUME_TEST_CHECK(trace[event_base + i] == send_events[i]);
  }
  FLUME_TEST_CHECK(std::memcmp(local, user, 16) == 0);
  const int send_calls[] = {kAcquireComm, kBatchStart, kLocalCopy,
                            kNotifyRecord, kNotifyWait, kBatchEnd,
                            kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(send_calls, 7));
  FLUME_TEST_CHECK(std::strcmp(batch_start_tag,
                               FLUME_HCOMM_PAYLOAD_DEFAULT_BATCH_TAG) == 0);
  FLUME_TEST_CHECK(std::strcmp(batch_end_tag,
                               FLUME_HCOMM_PAYLOAD_DEFAULT_BATCH_TAG) == 0);

  Reset();
  reset_status();
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.reserved2[0] = FLUME_HCOMM_PAYLOAD_BATCH_MODE_DISABLED;
  send_desc.batch_tag[0] = '\0';
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  const int send_no_batch_calls[] = {kAcquireComm, kLocalCopy,
                                     kNotifyRecord, kNotifyWait,
                                     kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(send_no_batch_calls, 5));

  Reset();
  reset_status();
  reset_trace();
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status, trace);
  send_desc.completion_mode |=
      FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);
  FLUME_TEST_CHECK(status[7] == send_desc.completion_mode);
  const int send_no_comm_calls[] = {kBatchStart, kLocalCopy, kNotifyRecord,
                                    kNotifyWait, kBatchEnd};
  FLUME_TEST_CHECK(CallsEqual(send_no_comm_calls, 5));
  const uint32_t send_no_comm_events[] = {
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_ENTER,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_DONE,
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT,
  };
  FLUME_TEST_CHECK(trace[4] == sizeof(send_no_comm_events) /
                                   sizeof(send_no_comm_events[0]));
  for (uint32_t i = 0; i < trace[4]; ++i) {
    FLUME_TEST_CHECK(trace[event_base + i] == send_no_comm_events[i]);
  }

  Reset();
  reset_status();
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.comm_name[0] = '\0';
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);

  Reset();
  reset_status();
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.comm_name[0] = '\0';
  send_desc.completion_mode |=
      FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE |
      FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(CallsEqual(send_no_comm_calls, 5));

  Reset();
  reset_status();
  reset_trace();
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status, trace);
  send_desc.completion_mode |=
      FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE |
      FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);
  FLUME_TEST_CHECK(status[7] == send_desc.completion_mode);
  FLUME_TEST_CHECK(trace[12] == send_desc.completion_mode);
  FLUME_TEST_CHECK(CallsEqual(send_no_comm_calls, 5));
  FLUME_TEST_CHECK(trace[4] == sizeof(send_no_comm_events) /
                                   sizeof(send_no_comm_events[0]));
  for (uint32_t i = 0; i < trace[4]; ++i) {
    FLUME_TEST_CHECK(trace[event_base + i] == send_no_comm_events[i]);
  }

  Reset();
  reset_status();
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  status_probe_words = status;
  status_probe_call = kLocalCopy;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status_observed_at_probe ==
                   FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);

  Reset();
  std::memset(user, 0, sizeof(user));
  std::memset(local, 0, sizeof(local));
  for (uint8_t i = 0; i < 16; ++i) {
    remote[i] = static_cast<uint8_t>(0x80U + i);
  }
  reset_status();
  auto recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                            status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);
  FLUME_TEST_CHECK(status[2] == FLUME_HCOMM_NOTIFY_ROLE_RECV);
  FLUME_TEST_CHECK(status[3] == 0U);
  FLUME_TEST_CHECK(status[4] == 16U);
  FLUME_TEST_CHECK(status[5] == 0U);
  FLUME_TEST_CHECK(status[6] == 1U);
  FLUME_TEST_CHECK(status[7] == FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY);
  FLUME_TEST_CHECK(std::memcmp(user, remote, 16) == 0);
  FLUME_TEST_CHECK(std::memcmp(local, remote, 16) == 0);
  FLUME_TEST_CHECK(last_read_dst == local);
  FLUME_TEST_CHECK(last_read_src == remote);
  FLUME_TEST_CHECK(last_local_copy_dst == user);
  FLUME_TEST_CHECK(last_local_copy_src == local);
  const int recv_calls[] = {kAcquireComm, kBatchStart, kNotifyWait,
                            kRead, kLocalCopy, kNotifyRecord, kBatchEnd,
                            kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(recv_calls, 8));

  Reset();
  std::memset(user, 0, sizeof(user));
  std::memset(local, 0, sizeof(local));
  reset_status();
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  recv_desc.reserved2[1] = FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);
  FLUME_TEST_CHECK(std::memcmp(user, remote, 16) == 0);
  FLUME_TEST_CHECK(std::memcmp(local, remote, 16) != 0);
  FLUME_TEST_CHECK(last_read_dst == user);
  FLUME_TEST_CHECK(last_read_src == remote);
  FLUME_TEST_CHECK(last_local_copy_dst == nullptr);
  FLUME_TEST_CHECK(last_local_copy_src == nullptr);
  const int recv_direct_output_calls[] = {
      kAcquireComm, kBatchStart, kNotifyWait, kRead, kNotifyRecord,
      kBatchEnd, kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(recv_direct_output_calls, 7));

  Reset();
  std::memset(user, 0, sizeof(user));
  reset_status();
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  status_probe_words = status;
  status_probe_call = kRead;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status_observed_at_probe ==
                   FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);

  Reset();
  std::memset(user, 0, sizeof(user));
  reset_status();
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  status_probe_words = status;
  status_probe_call = kLocalCopy;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status_observed_at_probe ==
                   FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);

  Reset();
  std::memset(user, 0, sizeof(user));
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  recv_desc.completion_mode = FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0U);
  const int recv_drain_calls[] = {
      kAcquireComm, kBatchStart, kNotifyWait, kRead, kChannelFence,
      kLocalCopy, kNotifyRecord, kBatchEnd, kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(recv_drain_calls, 9));

  Reset();
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  send_desc.cpu_thread_on_aicpu = 0x300;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  const int thread_notify_send_calls[] = {
      kAcquireComm, kBatchStart, kThreadWait, kLocalCopy, kNotifyRecord,
      kNotifyWait, kThreadRecord, kBatchEnd, kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(thread_notify_send_calls, 9));

  Reset();
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(call_count == 0);

  Reset();
  acquire_comm_ret = 99;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
  FLUME_TEST_CHECK(status[1] == 99U);
  const int acquire_fail_calls[] = {kAcquireComm};
  FLUME_TEST_CHECK(CallsEqual(acquire_fail_calls, 1));

  Reset();
  acquire_comm_ret = 100;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  send_desc.cpu_thread_on_aicpu = 0x300;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
  FLUME_TEST_CHECK(status[1] == 100U);
  const int acquire_notify_fail_calls[] = {kAcquireComm, kThreadRecord};
  FLUME_TEST_CHECK(CallsEqual(acquire_notify_fail_calls, 2));

  Reset();
  release_comm_ret = 98;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED);
  FLUME_TEST_CHECK(status[1] == 98U);

  Reset();
  batch_start_ret = 33;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED);
  FLUME_TEST_CHECK(status[1] == 33U);
  const int batch_start_fail_calls[] = {kAcquireComm, kBatchStart,
                                        kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(batch_start_fail_calls, 3));

  Reset();
  batch_end_ret = 34;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED);
  FLUME_TEST_CHECK(status[1] == 34U);
  const int batch_end_fail_calls[] = {
      kAcquireComm, kBatchStart, kLocalCopy, kNotifyRecord, kNotifyWait,
      kBatchEnd, kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(batch_end_fail_calls, 7));

  Reset();
  local_copy_ret = 77;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED);
  FLUME_TEST_CHECK(status[1] == 77U);

  Reset();
  local_copy_ret = 78;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  send_desc.cpu_thread_on_aicpu = 0x300;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED);
  FLUME_TEST_CHECK(status[1] == 78U);
  const int thread_notify_failure_calls[] = {
      kAcquireComm, kBatchStart, kThreadWait, kLocalCopy,
      kThreadRecord, kBatchEnd, kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(thread_notify_failure_calls, 7));

  Reset();
  notify_record_ret = 22;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED);
  FLUME_TEST_CHECK(status[1] == 22U);
  const int ready_record_fail_calls[] = {
      kAcquireComm, kBatchStart, kLocalCopy, kNotifyRecord, kBatchEnd,
      kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(ready_record_fail_calls, 6));

  Reset();
  thread_wait_ret = 79;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  send_desc.cpu_thread_on_aicpu = 0x300;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED);
  FLUME_TEST_CHECK(status[1] == 79U);
  const int thread_wait_failure_calls[] = {
      kAcquireComm, kBatchStart, kThreadWait, kThreadRecord, kBatchEnd,
      kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(thread_wait_failure_calls, 6));

  Reset();
  thread_record_ret = 58;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  send_desc.cpu_thread_on_aicpu = 0x300;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED);
  FLUME_TEST_CHECK(status[1] == 58U);
  const int thread_record_failure_calls[] = {
      kAcquireComm, kBatchStart, kThreadWait, kLocalCopy, kNotifyRecord,
      kNotifyWait, kThreadRecord, kBatchEnd, kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(thread_record_failure_calls, 9));

  Reset();
  notify_wait_ret = 56;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED);
  FLUME_TEST_CHECK(status[1] == 56U);
  const int ready_wait_failure_calls[] = {
      kAcquireComm, kBatchStart, kNotifyWait, kBatchEnd, kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(ready_wait_failure_calls, 5));

  Reset();
  read_ret = 88;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED);
  FLUME_TEST_CHECK(status[1] == 88U);

  Reset();
  local_copy_ret = 91;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED);
  FLUME_TEST_CHECK(status[1] == 91U);
  const int output_copy_failure_calls[] = {
      kAcquireComm, kBatchStart, kNotifyWait, kRead, kLocalCopy, kBatchEnd,
      kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(output_copy_failure_calls, 7));

  Reset();
  channel_drain_ret = 44;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  recv_desc.completion_mode = FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED);
  FLUME_TEST_CHECK(status[1] == 44U);

  Reset();
  notify_record_ret = 57;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED);
  FLUME_TEST_CHECK(status[1] == 57U);
  const int done_record_failure_calls[] = {
      kAcquireComm, kBatchStart, kNotifyWait, kRead, kLocalCopy,
      kNotifyRecord, kBatchEnd, kReleaseComm};
  FLUME_TEST_CHECK(CallsEqual(done_record_failure_calls, 8));

  Reset();
  batch_end_ret = 66;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED);
  FLUME_TEST_CHECK(status[1] == 66U);

  Reset();
  notify_wait_ret = 55;
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED);
  FLUME_TEST_CHECK(status[1] == 55U);

  Reset();
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.bytes = 65;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(call_count == 0);

  Reset();
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(99U, user, local, remote, status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(call_count == 0);

  Reset();
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(99U, user, local, remote, status);
  send_desc.thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  send_desc.cpu_thread_on_aicpu = 0x300;
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  const int invalid_role_notify_calls[] = {kThreadRecord};
  FLUME_TEST_CHECK(CallsEqual(invalid_role_notify_calls, 1));

  Reset();
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.batch_tag[0] = '\0';
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(call_count == 0);

  Reset();
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.comm_name[0] = '\0';
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(call_count == 0);

  Reset();
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                       status);
  send_desc.thread_notify_mode = FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  send_desc.cpu_thread_on_aicpu = 0x300;
  send_desc.comm_name[0] = '\0';
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(status[0] ==
                   FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT);
  const int invalid_notify_calls[] = {kThreadRecord};
  FLUME_TEST_CHECK(CallsEqual(invalid_notify_calls, 1));

  return 0;
}
