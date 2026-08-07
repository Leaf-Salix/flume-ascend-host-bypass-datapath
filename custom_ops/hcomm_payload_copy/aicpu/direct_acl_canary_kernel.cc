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

}  // namespace

extern "C" unsigned int FlumeHcommCanaryDirectAclrtKernel(void* param) {
  if (param == nullptr) {
    return kFlumeCanaryInvalidArgument;
  }
  auto* desc = static_cast<flume_hcomm_canary_desc_v1*>(param);
  if (!ValidateCanaryDesc(*desc)) {
    return kFlumeCanaryInvalidArgument;
  }
  desc->observed_token = desc->expected_token;
  return kFlumeCanarySuccess;
}
