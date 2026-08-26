#ifndef FLUME_NVME_STORAGE_NVME_HBM_CANARY_H_
#define FLUME_NVME_STORAGE_NVME_HBM_CANARY_H_

#include <cstdint>
#include <string>

namespace flume::nvme {

enum class CanaryDirection {
  kReadToHbm,
  kWriteRoundTrip,
};

struct CanaryConfig {
  std::string namespace_path;
  CanaryDirection direction = CanaryDirection::kReadToHbm;
  uint64_t slba = 0;
  uint64_t bytes = 4096;
  uint64_t lba_bytes = 0;
  uint64_t namespace_bytes = 0;
  uint32_t timeout_ms = 30000;
  bool destructive_write_confirmed = false;
};

struct IoPlan {
  uint8_t opcode = 0;
  uint32_t nsid = 0;
  uint32_t data_len = 0;
  uint32_t cdw10 = 0;
  uint32_t cdw11 = 0;
  uint32_t cdw12 = 0;
  uint32_t timeout_ms = 0;
  uint64_t byte_offset = 0;
  uint64_t block_count = 0;
};

bool BuildCanaryPlan(const CanaryConfig& config, uint32_t nsid,
                     IoPlan* plan, std::string* error);
const char* CanaryDirectionName(CanaryDirection direction);

}  // namespace flume::nvme

#endif  // FLUME_NVME_STORAGE_NVME_HBM_CANARY_H_
