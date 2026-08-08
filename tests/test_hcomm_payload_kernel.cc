#include "test_util.h"

#include <cstdint>
#include <cstring>

#include "flume_hcomm_notify_only_abi.h"

namespace flume_hcomm_payload_kernel_mock {

int32_t batch_start_ret = 0;
int32_t batch_end_ret = 0;
int32_t thread_wait_ret = 0;
int32_t thread_record_ret = 0;
int32_t local_copy_ret = 0;
int32_t read_ret = 0;
int32_t notify_record_ret = 0;
int32_t notify_wait_ret = 0;
int calls[32] = {};
int call_count = 0;

void RecordCall(int call) {
  if (call_count < static_cast<int>(sizeof(calls) / sizeof(calls[0]))) {
    calls[call_count++] = call;
  }
}

void Reset() {
  batch_start_ret = 0;
  batch_end_ret = 0;
  thread_wait_ret = 0;
  thread_record_ret = 0;
  local_copy_ret = 0;
  read_ret = 0;
  notify_record_ret = 0;
  notify_wait_ret = 0;
  std::memset(calls, 0, sizeof(calls));
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
    uint32_t* status_words) {
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
  std::memcpy(desc.batch_tag, "unit_payload", sizeof("unit_payload"));
  return desc;
}

}  // namespace

int main() {
  using namespace flume_hcomm_payload_kernel_mock;

  uint8_t user[64] = {};
  uint8_t local[64] = {};
  uint8_t remote[64] = {};
  uint32_t status[2] = {0xFFFFFFFFU, 0xFFFFFFFFU};

  for (uint8_t i = 0; i < 16; ++i) {
    user[i] = static_cast<uint8_t>(i + 1);
  }
  Reset();
  auto send_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_SEND, user, local, remote,
                            status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&send_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[1] == 0xFFFFFFFFU);
  FLUME_TEST_CHECK(std::memcmp(local, user, 16) == 0);
  const int send_calls[] = {
      kBatchStart, kLocalCopy, kNotifyRecord, kNotifyWait, kBatchEnd};
  FLUME_TEST_CHECK(CallsEqual(send_calls, 5));

  Reset();
  std::memset(user, 0, sizeof(user));
  std::memset(local, 0, sizeof(local));
  for (uint8_t i = 0; i < 16; ++i) {
    remote[i] = static_cast<uint8_t>(0x80U + i);
  }
  status[0] = 0xFFFFFFFFU;
  status[1] = 0xFFFFFFFFU;
  auto recv_desc = MakeDesc(FLUME_HCOMM_NOTIFY_ROLE_RECV, user, local, remote,
                            status);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(&recv_desc) ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(status[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS);
  FLUME_TEST_CHECK(std::memcmp(user, remote, 16) == 0);
  const int recv_calls[] = {
      kBatchStart, kNotifyWait, kRead, kNotifyRecord, kBatchEnd};
  FLUME_TEST_CHECK(CallsEqual(recv_calls, 5));

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

  return 0;
}
