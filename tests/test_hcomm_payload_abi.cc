#include "test_util.h"

#include <cstddef>
#include <cstdint>
#include <cstring>

#include "flume_hcomm_notify_only_abi.h"

int main() {
  static_assert(alignof(flume_hcomm_notify_only_desc_v1) == alignof(uint64_t),
                "notify descriptor must stay 64-bit aligned");
  static_assert(alignof(flume_hcomm_payload_copy_desc_v1) == alignof(uint64_t),
                "payload descriptor must stay 64-bit aligned");
  static_assert(sizeof(flume_hcomm_notify_only_desc_v1) == 160,
                "notify descriptor ABI size changed");
  static_assert(sizeof(flume_hcomm_canary_desc_v1) == 96,
                "canary descriptor ABI size changed");
  static_assert(sizeof(flume_hcomm_payload_copy_desc_v1) == 176,
                "payload descriptor ABI size changed");
  static_assert(offsetof(flume_hcomm_payload_copy_desc_v1, bytes) == 48,
                "payload bytes offset changed");
  static_assert(offsetof(flume_hcomm_payload_copy_desc_v1, aicpu_thread) == 56,
                "payload thread offset changed");
  static_assert(offsetof(flume_hcomm_payload_copy_desc_v1, user_buffer) == 72,
                "payload user buffer offset changed");
  static_assert(offsetof(flume_hcomm_payload_copy_desc_v1, status_word) == 112,
                "payload status word offset changed");
  static_assert(offsetof(flume_hcomm_payload_copy_desc_v1, batch_tag) == 120,
                "payload batch tag offset changed");
  static_assert(
      offsetof(flume_hcomm_payload_copy_desc_v1, cpu_thread_on_aicpu) == 168,
      "payload host/AICPU thread notify offset changed");
  static_assert(FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES == 48,
                "payload batch tag capacity changed");
  static_assert(FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE == 0,
                "default payload thread notify mode changed");
  static_assert(FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU == 1,
                "host/AICPU payload thread notify mode changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS == 0,
                "payload success status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT == 1,
                "payload invalid-argument status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_HCOMM_ERROR == 2,
                "payload generic HCOMM status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED == 3,
                "payload thread-notify wait status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED == 4,
                "payload batch-start status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED == 5,
                "payload local-copy status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED == 6,
                "payload ready-notify record status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED == 7,
                "payload done-notify wait status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED == 8,
                "payload ready-notify wait status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED == 9,
                "payload remote-read status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED == 10,
                "payload done-notify record status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED == 11,
                "payload batch-end status changed");
  static_assert(FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED == 12,
                "payload thread-notify record status changed");
  FLUME_TEST_CHECK(std::strcmp(
      FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC,
      "FlumeHcommPayloadCopyDirectAclrtKernelV2") == 0);
  FLUME_TEST_CHECK(std::strcmp(
      FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC_LEGACY,
      "FlumeHcommPayloadCopyDirectAclrtKernel") == 0);

  flume_hcomm_payload_copy_desc_v1 payload = {};
  flume_hcomm_payload_copy_desc_init(&payload);
  FLUME_TEST_CHECK(payload.magic == FLUME_HCOMM_PAYLOAD_COPY_MAGIC);
  FLUME_TEST_CHECK(payload.version == FLUME_HCOMM_PAYLOAD_COPY_VERSION);
  FLUME_TEST_CHECK(payload.size == sizeof(flume_hcomm_payload_copy_desc_v1));
  FLUME_TEST_CHECK(payload.timeout_sec == 1800);
  FLUME_TEST_CHECK(payload.thread_notify_mode ==
                   FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE);
  FLUME_TEST_CHECK(payload.status_word == 0);
  FLUME_TEST_CHECK(sizeof(uint32_t[2]) == 8);
  FLUME_TEST_CHECK(payload.batch_tag[0] == '\0');
  FLUME_TEST_CHECK(payload.cpu_thread_on_aicpu == 0);

  flume_hcomm_canary_desc_v1 canary = {};
  flume_hcomm_canary_desc_init(&canary);
  FLUME_TEST_CHECK(canary.expected_token == 0x43414e59U);
  FLUME_TEST_CHECK(canary.observed_token == 0);

  flume_hcomm_notify_only_desc_v1 notify = {};
  flume_hcomm_notify_only_desc_init(&notify);
  FLUME_TEST_CHECK(notify.magic == FLUME_HCOMM_NOTIFY_ONLY_MAGIC);
  FLUME_TEST_CHECK(notify.size == sizeof(flume_hcomm_notify_only_desc_v1));
  return 0;
}
