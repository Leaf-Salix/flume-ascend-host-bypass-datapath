#include "flume_hcomm_notify_only_abi.h"

namespace {

constexpr unsigned int kFlumeCanarySuccess = 0;
constexpr unsigned int kFlumeCanaryInvalidArgument = 1;

bool ValidateCanaryDesc(const flume_hcomm_canary_desc_v1& desc) {
  return desc.magic == FLUME_HCOMM_CANARY_MAGIC &&
         desc.version == FLUME_HCOMM_CANARY_VERSION &&
         desc.size == sizeof(flume_hcomm_canary_desc_v1) &&
         desc.rank_size == 2 && desc.local_rank < desc.rank_size &&
         desc.peer_rank < desc.rank_size && desc.local_rank != desc.peer_rank;
}

void StoreCanaryStatus(const flume_hcomm_canary_desc_v1& desc,
                       unsigned int status) {
  if (desc.magic != FLUME_HCOMM_CANARY_MAGIC || desc.status_word == 0) {
    return;
  }
  auto* status_word = reinterpret_cast<unsigned int*>(desc.status_word);
  *status_word = status;
}

void StoreCanaryToken(const flume_hcomm_canary_desc_v1& desc,
                      unsigned int token) {
  if (desc.magic != FLUME_HCOMM_CANARY_MAGIC ||
      desc.observed_token_word == 0) {
    return;
  }
  auto* token_word = reinterpret_cast<unsigned int*>(
      desc.observed_token_word);
  *token_word = token;
}

}  // namespace

extern "C" unsigned int FlumeHcommCanaryDirectAclrtKernel(void* param) {
  if (param == nullptr) {
    return kFlumeCanaryInvalidArgument;
  }
  auto* desc = static_cast<flume_hcomm_canary_desc_v1*>(param);
  if (!ValidateCanaryDesc(*desc)) {
    StoreCanaryStatus(*desc, kFlumeCanaryInvalidArgument);
    return kFlumeCanaryInvalidArgument;
  }
  desc->observed_token = desc->expected_token;
  StoreCanaryToken(*desc, desc->expected_token);
  StoreCanaryStatus(*desc, kFlumeCanarySuccess);
  return kFlumeCanarySuccess;
}

#if !defined(FLUME_HCOMM_PAYLOAD_ENABLE_PRIMITIVE_PAYLOAD) && \
    !defined(FLUME_HCOMM_PAYLOAD_ENABLE_INTERNAL_NOTIFY)
extern "C" unsigned int FlumeHcommPayloadBuildModeCanaryOnly() {
  return 1U;
}

extern "C" unsigned int FlumeHcommNotifyOnlyDirectAclrtKernel(void* param) {
  (void)param;
  return kFlumeCanaryInvalidArgument;
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV3(void* param) {
  (void)param;
  return kFlumeCanaryInvalidArgument;
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV4(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV3(param);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV2(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV4(param);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernel(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV4(param);
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion2() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION >= 2U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion3() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION >= 3U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion4() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION == 4U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion5() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 5U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion6() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 6U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion7() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 7U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion8() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION == 8U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadStatusSchemaVersion() {
  return FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadStatusWordCount() {
  return FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT;
}
#endif

#ifndef FLUME_HCOMM_PAYLOAD_ENABLE_PUBLIC_HCCL_LAUNCH
extern "C" unsigned int FlumeHcommNotifyOnlyAicpuKernel(void* param) {
  (void)param;
  return kFlumeCanaryInvalidArgument;
}
#endif
