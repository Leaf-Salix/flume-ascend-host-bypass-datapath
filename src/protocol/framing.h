#ifndef FLUME_PROTOCOL_FRAMING_H_
#define FLUME_PROTOCOL_FRAMING_H_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <vector>

namespace flume {
namespace protocol {

constexpr uint32_t kMagic = 0x464C554DU;  // FLUM
constexpr uint16_t kVersion = 1;
constexpr uint32_t kMaxBodySize = 128U * 1024U * 1024U;

enum class FrameType : uint16_t {
  kHello = 1,
  kHelloAck = 2,
  kOpenReq = 3,
  kOpenResp = 4,
  kReadReq = 5,
  kReadResp = 6,
  kCloseReq = 7,
  kCloseResp = 8,
  kError = 9,
};

struct Frame {
  FrameType type = FrameType::kError;
  uint64_t request_id = 0;
  std::vector<uint8_t> body;
};

bool WriteFrame(int fd, const Frame& frame, std::string* error);
bool ReadFrame(int fd, Frame* frame, std::string* error);

void AppendU16(std::vector<uint8_t>* out, uint16_t value);
void AppendU32(std::vector<uint8_t>* out, uint32_t value);
void AppendU64(std::vector<uint8_t>* out, uint64_t value);
void AppendBytes(std::vector<uint8_t>* out, const void* data, size_t size);
void AppendString(std::vector<uint8_t>* out, const std::string& value);

class Reader {
 public:
  explicit Reader(const std::vector<uint8_t>& data);

  bool ReadU16(uint16_t* value);
  bool ReadU32(uint32_t* value);
  bool ReadU64(uint64_t* value);
  bool ReadString(std::string* value);
  bool ReadBytes(size_t size, std::vector<uint8_t>* value);
  bool ReadRaw(size_t size, const uint8_t** data);
  bool Done() const;

 private:
  const std::vector<uint8_t>& data_;
  size_t offset_ = 0;
};

uint32_t Checksum32(const void* data, size_t size);

}  // namespace protocol
}  // namespace flume

#endif  // FLUME_PROTOCOL_FRAMING_H_
