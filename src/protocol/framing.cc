#include "protocol/framing.h"

#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <limits>

namespace flume {
namespace protocol {
namespace {

constexpr size_t kHeaderSize = 28;

bool WriteAll(int fd, const uint8_t* data, size_t size, std::string* error) {
  size_t done = 0;
  while (done < size) {
    ssize_t n = send(fd, data + done, size - done, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (error != nullptr) {
        *error = std::string("send failed: ") + strerror(errno);
      }
      return false;
    }
    if (n == 0) {
      if (error != nullptr) {
        *error = "send returned 0";
      }
      return false;
    }
    done += static_cast<size_t>(n);
  }
  return true;
}

bool ReadAll(int fd, uint8_t* data, size_t size, std::string* error) {
  size_t done = 0;
  while (done < size) {
    ssize_t n = recv(fd, data + done, size - done, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (error != nullptr) {
        *error = std::string("recv failed: ") + strerror(errno);
      }
      return false;
    }
    if (n == 0) {
      if (error != nullptr) {
        *error = "peer closed connection";
      }
      return false;
    }
    done += static_cast<size_t>(n);
  }
  return true;
}

uint16_t LoadU16(const uint8_t* data) {
  return static_cast<uint16_t>((static_cast<uint16_t>(data[0]) << 8U) |
                               static_cast<uint16_t>(data[1]));
}

uint32_t LoadU32(const uint8_t* data) {
  return (static_cast<uint32_t>(data[0]) << 24U) |
         (static_cast<uint32_t>(data[1]) << 16U) |
         (static_cast<uint32_t>(data[2]) << 8U) |
         static_cast<uint32_t>(data[3]);
}

uint64_t LoadU64(const uint8_t* data) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8U) | static_cast<uint64_t>(data[i]);
  }
  return value;
}

}  // namespace

bool WriteFrame(int fd, const Frame& frame, std::string* error) {
  if (frame.body.size() > kMaxBodySize) {
    if (error != nullptr) {
      *error = "frame body is too large";
    }
    return false;
  }

  std::vector<uint8_t> header;
  header.reserve(kHeaderSize);
  AppendU32(&header, kMagic);
  AppendU16(&header, kVersion);
  AppendU16(&header, static_cast<uint16_t>(frame.type));
  AppendU16(&header, 0);
  AppendU16(&header, 0);
  AppendU64(&header, frame.request_id);
  AppendU32(&header, static_cast<uint32_t>(frame.body.size()));
  AppendU32(&header, 0);

  return WriteAll(fd, header.data(), header.size(), error) &&
         (frame.body.empty() ||
          WriteAll(fd, frame.body.data(), frame.body.size(), error));
}

bool ReadFrame(int fd, Frame* frame, std::string* error) {
  if (frame == nullptr) {
    if (error != nullptr) {
      *error = "frame pointer is null";
    }
    return false;
  }

  uint8_t header[kHeaderSize] = {};
  if (!ReadAll(fd, header, sizeof(header), error)) {
    return false;
  }

  if (LoadU32(header) != kMagic) {
    if (error != nullptr) {
      *error = "invalid frame magic";
    }
    return false;
  }
  if (LoadU16(header + 4) != kVersion) {
    if (error != nullptr) {
      *error = "unsupported frame version";
    }
    return false;
  }

  uint32_t body_len = LoadU32(header + 20);
  if (body_len > kMaxBodySize) {
    if (error != nullptr) {
      *error = "frame body exceeds maximum size";
    }
    return false;
  }

  frame->type = static_cast<FrameType>(LoadU16(header + 6));
  frame->request_id = LoadU64(header + 12);
  frame->body.assign(body_len, 0);
  if (body_len == 0) {
    return true;
  }
  return ReadAll(fd, frame->body.data(), frame->body.size(), error);
}

void AppendU16(std::vector<uint8_t>* out, uint16_t value) {
  out->push_back(static_cast<uint8_t>((value >> 8U) & 0xffU));
  out->push_back(static_cast<uint8_t>(value & 0xffU));
}

void AppendU32(std::vector<uint8_t>* out, uint32_t value) {
  out->push_back(static_cast<uint8_t>((value >> 24U) & 0xffU));
  out->push_back(static_cast<uint8_t>((value >> 16U) & 0xffU));
  out->push_back(static_cast<uint8_t>((value >> 8U) & 0xffU));
  out->push_back(static_cast<uint8_t>(value & 0xffU));
}

void AppendU64(std::vector<uint8_t>* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<uint8_t>((value >> static_cast<uint32_t>(shift)) & 0xffU));
  }
}

void AppendBytes(std::vector<uint8_t>* out, const void* data, size_t size) {
  if (size == 0) {
    return;
  }
  const auto* bytes = static_cast<const uint8_t*>(data);
  out->insert(out->end(), bytes, bytes + size);
}

void AppendString(std::vector<uint8_t>* out, const std::string& value) {
  AppendU32(out, static_cast<uint32_t>(value.size()));
  AppendBytes(out, value.data(), value.size());
}

Reader::Reader(const std::vector<uint8_t>& data) : data_(data) {}

bool Reader::ReadU16(uint16_t* value) {
  const uint8_t* raw = nullptr;
  if (!ReadRaw(2, &raw)) {
    return false;
  }
  *value = LoadU16(raw);
  return true;
}

bool Reader::ReadU32(uint32_t* value) {
  const uint8_t* raw = nullptr;
  if (!ReadRaw(4, &raw)) {
    return false;
  }
  *value = LoadU32(raw);
  return true;
}

bool Reader::ReadU64(uint64_t* value) {
  const uint8_t* raw = nullptr;
  if (!ReadRaw(8, &raw)) {
    return false;
  }
  *value = LoadU64(raw);
  return true;
}

bool Reader::ReadString(std::string* value) {
  uint32_t size = 0;
  if (!ReadU32(&size)) {
    return false;
  }
  const uint8_t* raw = nullptr;
  if (!ReadRaw(size, &raw)) {
    return false;
  }
  value->assign(reinterpret_cast<const char*>(raw), size);
  return true;
}

bool Reader::ReadBytes(size_t size, std::vector<uint8_t>* value) {
  const uint8_t* raw = nullptr;
  if (!ReadRaw(size, &raw)) {
    return false;
  }
  value->assign(raw, raw + size);
  return true;
}

bool Reader::ReadRaw(size_t size, const uint8_t** data) {
  if (data == nullptr || size > data_.size() - offset_) {
    return false;
  }
  *data = data_.data() + offset_;
  offset_ += size;
  return true;
}

bool Reader::Done() const {
  return offset_ == data_.size();
}

uint32_t Checksum32(const void* data, size_t size) {
  const auto* bytes = static_cast<const uint8_t*>(data);
  uint32_t hash = 2166136261U;
  for (size_t i = 0; i < size; ++i) {
    hash ^= bytes[i];
    hash *= 16777619U;
  }
  return hash;
}

}  // namespace protocol
}  // namespace flume
