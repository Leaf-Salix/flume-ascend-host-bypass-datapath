#ifndef FLUME_ROCE_STORAGE_NPU_RA_PUSH_MOVER_H_
#define FLUME_ROCE_STORAGE_NPU_RA_PUSH_MOVER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "roce_storage/roce_storage.h"

namespace flume::roce {

struct NpuRaPushConfig {
  std::string npu_rnic_ip;
  uint32_t logical_device = 0;
  int32_t physical_device = -1;
  uint32_t gid_index = 0;
  uint32_t timeout_ms = 30000;
  int hdc_type = 18;
};

class NpuRaPushMover {
 public:
  NpuRaPushMover();
  ~NpuRaPushMover();
  NpuRaPushMover(const NpuRaPushMover&) = delete;
  NpuRaPushMover& operator=(const NpuRaPushMover&) = delete;

  bool Open(const NpuRaPushConfig& config, const Endpoint& peer,
            Endpoint* local, std::string* error);
  bool Push(void* source_hbm, size_t length, const MemoryWindow& target,
            void* acl_stream, std::string* error);
  bool Close(std::string* error);
  bool available() const;
  bool capability_available() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_NPU_RA_PUSH_MOVER_H_
