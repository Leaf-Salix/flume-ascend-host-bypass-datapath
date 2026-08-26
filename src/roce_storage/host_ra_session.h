#ifndef FLUME_ROCE_STORAGE_HOST_RA_SESSION_H_
#define FLUME_ROCE_STORAGE_HOST_RA_SESSION_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "roce_storage/roce_storage.h"

namespace flume::roce {

struct HostRaConfig {
  std::string storage_server;
  std::string npu_rnic_ip;
  uint32_t logical_device = 0;
  uint32_t gid_index = 0;
  uint32_t bootstrap_port = 0;
  uint32_t timeout_ms = 30000;
  int hdc_type = 18;
  ControlMode control_mode = ControlMode::kTcp;
  TransferMode transfer_mode = TransferMode::kPush;
};

struct HostRaResult {
  size_t bytes = 0;
  uint32_t checksum = 0;
  std::string marker;
};

class HostRaSession {
 public:
  HostRaSession();
  ~HostRaSession();
  HostRaSession(const HostRaSession&) = delete;
  HostRaSession& operator=(const HostRaSession&) = delete;

  bool Open(const HostRaConfig& config, std::string* error);
  bool SubmitAndWait(Operation operation, uint64_t request_id, uint64_t object_id,
                     uint64_t storage_offset, void* npu_buffer, size_t length,
                     void* acl_stream, HostRaResult* result, std::string* error);
  bool Close(std::string* error);
  bool available() const;
  bool capability_available() const;
  uint64_t namespace_capacity() const;
  uint64_t max_transfer_bytes() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_HOST_RA_SESSION_H_
