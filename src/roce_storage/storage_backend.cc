#include "roce_storage/storage_backend.h"

#include <cstring>
#include <fstream>
#include <limits>

namespace flume::roce {
namespace {

bool ValidRange(uint64_t size, uint64_t offset, size_t len) {
  return offset <= size && len <= size - offset;
}

void SetError(std::string* error, const char* text) {
  if (error != nullptr) *error = text;
}

}  // namespace

MemoryStorageBackend::MemoryStorageBackend(size_t bytes) : bytes_(bytes) {
  for (size_t index = 0; index < bytes_.size(); ++index) {
    bytes_[index] = static_cast<uint8_t>((index * 29U + 11U) & 0xffU);
  }
}

uint64_t MemoryStorageBackend::size() const { return bytes_.size(); }

bool MemoryStorageBackend::Read(uint64_t offset, void* dst, size_t len, std::string* error) {
  if (dst == nullptr || !ValidRange(size(), offset, len)) {
    SetError(error, "memory storage read range is invalid");
    return false;
  }
  std::memcpy(dst, bytes_.data() + offset, len);
  return true;
}

bool MemoryStorageBackend::Write(uint64_t offset, const void* src, size_t len, std::string* error) {
  if (src == nullptr || !ValidRange(size(), offset, len)) {
    SetError(error, "memory storage write range is invalid");
    return false;
  }
  std::memcpy(bytes_.data() + offset, src, len);
  return true;
}

PosixStorageBackend::PosixStorageBackend(std::string path) : path_(std::move(path)) {
  std::ifstream in(path_, std::ios::binary | std::ios::ate);
  if (in) size_ = static_cast<uint64_t>(in.tellg());
}

uint64_t PosixStorageBackend::size() const { return size_; }

bool PosixStorageBackend::Read(uint64_t offset, void* dst, size_t len, std::string* error) {
  if (dst == nullptr || !ValidRange(size_, offset, len)) {
    SetError(error, "POSIX storage read range is invalid");
    return false;
  }
  std::ifstream in(path_, std::ios::binary);
  if (!in) {
    SetError(error, "failed to open POSIX storage file for read");
    return false;
  }
  in.seekg(static_cast<std::streamoff>(offset));
  in.read(static_cast<char*>(dst), static_cast<std::streamsize>(len));
  if (in.gcount() != static_cast<std::streamsize>(len)) {
    SetError(error, "failed to read requested POSIX storage bytes");
    return false;
  }
  return true;
}

bool PosixStorageBackend::Write(uint64_t offset, const void* src, size_t len, std::string* error) {
  if (src == nullptr || !ValidRange(size_, offset, len)) {
    SetError(error, "POSIX storage write range is invalid");
    return false;
  }
  std::fstream out(path_, std::ios::binary | std::ios::in | std::ios::out);
  if (!out) {
    SetError(error, "failed to open POSIX storage file for write");
    return false;
  }
  out.seekp(static_cast<std::streamoff>(offset));
  out.write(static_cast<const char*>(src), static_cast<std::streamsize>(len));
  if (!out) {
    SetError(error, "failed to write requested POSIX storage bytes");
    return false;
  }
  return true;
}

}  // namespace flume::roce
