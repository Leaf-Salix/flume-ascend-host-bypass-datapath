#ifndef FLUME_ROCE_STORAGE_VERBS_BACKEND_H_
#define FLUME_ROCE_STORAGE_VERBS_BACKEND_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace flume::roce {

struct VerbsEndpoint {
  std::array<uint8_t, 16> gid{};
  uint32_t qpn = 0;
  uint32_t psn = 0;
  uint8_t port = 1;
  uint8_t gid_index = 0;
  uint8_t mtu = 3;
};

struct VerbsMemoryRegion {
  void* address = nullptr;
  size_t length = 0;
  uint32_t lkey = 0;
  uint32_t rkey = 0;
  void* opaque = nullptr;
};

// Standard libibverbs resource owner for the storage-node endpoint. It owns
// only CPU RNIC objects; NPU RA/HCCP resources remain in a separate adapter.
class VerbsBackend {
 public:
  VerbsBackend();
  ~VerbsBackend();
  VerbsBackend(const VerbsBackend&) = delete;
  VerbsBackend& operator=(const VerbsBackend&) = delete;

  bool Open(const std::string& device_name, uint8_t port, uint8_t gid_index,
            uint32_t queue_depth, std::string* error);
  bool Connect(const VerbsEndpoint& peer, std::string* error);
  bool Register(void* address, size_t length, bool remote_read, bool remote_write,
                VerbsMemoryRegion* out, std::string* error);
  bool Deregister(VerbsMemoryRegion* region, std::string* error);
  bool PostReceive(const VerbsMemoryRegion& region, std::string* error);
  bool WaitForReceive(uint32_t timeout_ms, std::string* error);
  bool Read(const VerbsMemoryRegion& local, uint64_t remote_address, uint32_t remote_rkey,
            size_t length, uint32_t timeout_ms, std::string* error);
  bool Write(const VerbsMemoryRegion& local, uint64_t remote_address, uint32_t remote_rkey,
             size_t length, uint32_t timeout_ms, std::string* error);
  bool available() const;
  VerbsEndpoint endpoint() const;
  uint32_t selected_path_mtu_bytes() const;
  void Close();

 private:
  bool Transfer(bool read, const VerbsMemoryRegion& local, uint64_t remote_address,
                uint32_t remote_rkey, size_t length, uint32_t timeout_ms,
                std::string* error);
  void* context_ = nullptr;
  void* pd_ = nullptr;
  void* cq_ = nullptr;
  void* qp_ = nullptr;
  VerbsEndpoint endpoint_;
  uint8_t selected_path_mtu_ = 0;
};

bool VerbsAvailable();
const char* VerbsUnavailableReason();

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_VERBS_BACKEND_H_
