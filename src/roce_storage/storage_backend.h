#ifndef FLUME_ROCE_STORAGE_STORAGE_BACKEND_H_
#define FLUME_ROCE_STORAGE_STORAGE_BACKEND_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace flume::roce {

// Storage-node payload ownership. This is deliberately independent from RoCE:
// the verbs layer only sees a registered byte range supplied by this backend.
class StorageBackend {
 public:
  virtual ~StorageBackend() = default;
  virtual uint64_t size() const = 0;
  virtual bool Read(uint64_t offset, void* dst, size_t len, std::string* error) = 0;
  virtual bool Write(uint64_t offset, const void* src, size_t len, std::string* error) = 0;
};

class MemoryStorageBackend final : public StorageBackend {
 public:
  explicit MemoryStorageBackend(size_t bytes);
  uint64_t size() const override;
  bool Read(uint64_t offset, void* dst, size_t len, std::string* error) override;
  bool Write(uint64_t offset, const void* src, size_t len, std::string* error) override;

 private:
  std::vector<uint8_t> bytes_;
};

class PosixStorageBackend final : public StorageBackend {
 public:
  explicit PosixStorageBackend(std::string path);
  uint64_t size() const override;
  bool Read(uint64_t offset, void* dst, size_t len, std::string* error) override;
  bool Write(uint64_t offset, const void* src, size_t len, std::string* error) override;

 private:
  std::string path_;
  uint64_t size_ = 0;
};

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_STORAGE_BACKEND_H_
