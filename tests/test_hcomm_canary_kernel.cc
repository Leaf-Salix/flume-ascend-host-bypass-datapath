#include "test_util.h"

#include <cstdint>

#include "flume_hcomm_notify_only_abi.h"

#include "direct_acl_canary_kernel.cc"

int main() {
  FLUME_TEST_CHECK(FlumeHcommPayloadBuildModeCanaryOnly() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyAbiVersion2() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyAbiVersion3() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyAbiVersion4() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion() ==
                   FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion5() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion6() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion7() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion8() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopySemanticVersion9() == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadStatusSchemaVersion() ==
                   FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION);
  FLUME_TEST_CHECK(FlumeHcommPayloadStatusWordCount() ==
                   FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT);
  FLUME_TEST_CHECK(FlumeHcommPayloadTraceSchemaVersion() ==
                   FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION);
  FLUME_TEST_CHECK(FlumeHcommPayloadTraceWordCount() ==
                   FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV2(nullptr) == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV3(nullptr) == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernelV4(nullptr) == 1U);
  FLUME_TEST_CHECK(FlumeHcommPayloadCopyDirectAclrtKernel(nullptr) == 1U);
  FLUME_TEST_CHECK(FlumeHcommNotifyOnlyDirectAclrtKernel(nullptr) == 1U);
  FLUME_TEST_CHECK(FlumeHcommNotifyOnlyAicpuKernel(nullptr) == 1U);

  uint32_t status = 0xFFFFFFFFU;
  uint32_t observed = 0U;
  flume_hcomm_canary_desc_v1 desc = {};
  flume_hcomm_canary_desc_init(&desc);
  desc.local_rank = 0;
  desc.peer_rank = 1;
  desc.rank_size = 2;
  desc.status_word = reinterpret_cast<uint64_t>(&status);
  desc.observed_token_word = reinterpret_cast<uint64_t>(&observed);

  FLUME_TEST_CHECK(FlumeHcommCanaryDirectAclrtKernel(&desc) == 0U);
  FLUME_TEST_CHECK(status == 0U);
  FLUME_TEST_CHECK(desc.observed_token == FLUME_HCOMM_CANARY_TOKEN);
  FLUME_TEST_CHECK(observed == FLUME_HCOMM_CANARY_TOKEN);

  flume_hcomm_canary_desc_v1 bad = desc;
  bad.rank_size = 1;
  status = 0xFFFFFFFFU;
  FLUME_TEST_CHECK(FlumeHcommCanaryDirectAclrtKernel(&bad) == 1U);
  FLUME_TEST_CHECK(status == 1U);
  return 0;
}
