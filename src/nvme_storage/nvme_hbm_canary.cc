#include "nvme_storage/nvme_hbm_canary.h"

#include <limits>

namespace flume::nvme {
namespace {

constexpr uint8_t kNvmeWriteOpcode = 0x01;
constexpr uint8_t kNvmeReadOpcode = 0x02;
constexpr uint64_t kMaxNvmeBlocksPerCommand = 65536;
constexpr uint64_t kProtectedNamespacePrefixBytes = 1024U * 1024U;

bool IsPowerOfTwo(uint64_t value) {
  return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace

bool BuildCanaryPlan(const CanaryConfig& config, uint32_t nsid,
                     IoPlan* plan, std::string* error) {
  if (plan == nullptr || config.namespace_path.empty() || nsid == 0 ||
      config.bytes == 0 || config.bytes > std::numeric_limits<uint32_t>::max() ||
      config.timeout_ms == 0) {
    if (error != nullptr) *error = "namespace, NSID, size, timeout, and output are required";
    return false;
  }
  if (config.lba_bytes < 512 || !IsPowerOfTwo(config.lba_bytes) ||
      config.bytes % config.lba_bytes != 0) {
    if (error != nullptr) *error = "I/O size must be aligned to a power-of-two namespace LBA size";
    return false;
  }
  const uint64_t blocks = config.bytes / config.lba_bytes;
  if (blocks == 0 || blocks > kMaxNvmeBlocksPerCommand ||
      config.slba > std::numeric_limits<uint64_t>::max() - blocks) {
    if (error != nullptr) *error = "NVMe command block count or LBA range is invalid";
    return false;
  }
  if (config.namespace_bytes < config.lba_bytes ||
      config.namespace_bytes % config.lba_bytes != 0 ||
      config.slba >= config.namespace_bytes / config.lba_bytes ||
      blocks > config.namespace_bytes / config.lba_bytes - config.slba) {
    if (error != nullptr) *error = "requested LBA range exceeds the namespace";
    return false;
  }
  if (config.direction == CanaryDirection::kWriteRoundTrip &&
      !config.destructive_write_confirmed) {
    if (error != nullptr) {
      *error = "write-roundtrip requires explicit scratch-namespace confirmation";
    }
    return false;
  }
  if (config.direction == CanaryDirection::kWriteRoundTrip &&
      config.slba * config.lba_bytes < kProtectedNamespacePrefixBytes) {
    if (error != nullptr) {
      *error = "write-roundtrip refuses the first 1 MiB of a namespace";
    }
    return false;
  }

  plan->opcode = config.direction == CanaryDirection::kReadToHbm ?
      kNvmeReadOpcode : kNvmeWriteOpcode;
  plan->nsid = nsid;
  plan->data_len = static_cast<uint32_t>(config.bytes);
  plan->cdw10 = static_cast<uint32_t>(config.slba);
  plan->cdw11 = static_cast<uint32_t>(config.slba >> 32);
  plan->cdw12 = static_cast<uint32_t>(blocks - 1);
  plan->timeout_ms = config.timeout_ms;
  plan->byte_offset = config.slba * config.lba_bytes;
  plan->block_count = blocks;
  return true;
}

const char* CanaryDirectionName(CanaryDirection direction) {
  switch (direction) {
    case CanaryDirection::kReadToHbm:
      return "read-to-hbm";
    case CanaryDirection::kWriteRoundTrip:
      return "write-roundtrip";
  }
  return "unknown";
}

}  // namespace flume::nvme
