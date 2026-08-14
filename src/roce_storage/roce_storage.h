#ifndef FLUME_ROCE_STORAGE_ROCE_STORAGE_H_
#define FLUME_ROCE_STORAGE_ROCE_STORAGE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace flume::roce {

constexpr uint32_t kProtocolMagic = 0x464c5243U;  // "FLRC"
constexpr uint16_t kProtocolVersion = 1;
constexpr size_t kCommandWireBytes = 56;
constexpr size_t kCompletionWireBytes = 24;
constexpr size_t kEndpointWireBytes = 32;

enum class Operation : uint16_t {
  kRead = 1,
  kWrite = 2,
};

struct Command {
  uint64_t request_id = 0;
  Operation operation = Operation::kRead;
  uint64_t object_id = 0;
  uint64_t storage_offset = 0;
  uint64_t length = 0;
  uint64_t npu_address = 0;
  uint32_t npu_rkey = 0;
};

struct Completion {
  uint64_t request_id = 0;
  uint32_t status = 0;
  uint64_t bytes = 0;
  uint32_t checksum = 0;
};

// These are the minimal fields needed to bring an RC QP to RTR/RTS over a
// separate TCP control connection.  They deliberately describe standard
// InfiniBand/RoCE concepts rather than any CANN private handle layout.
struct Endpoint {
  std::array<uint8_t, 16> gid{};
  uint32_t qpn = 0;
  uint32_t psn = 0;
  uint8_t port = 0;
  uint8_t gid_index = 0;
  uint8_t mtu = 0;
};

bool EncodeCommand(const Command& command, std::vector<uint8_t>* wire);
bool DecodeCommand(const uint8_t* wire, size_t len, Command* command);
bool EncodeCompletion(const Completion& completion, std::vector<uint8_t>* wire);
bool DecodeCompletion(const uint8_t* wire, size_t len, Completion* completion);
bool EncodeEndpoint(const Endpoint& endpoint, std::vector<uint8_t>* wire);
bool DecodeEndpoint(const uint8_t* wire, size_t len, Endpoint* endpoint);
uint32_t Checksum(const uint8_t* data, size_t len);

// The ABI and session API compile on every development host. Native RA/HCCP
// and verbs posting is intentionally not enabled until its CANN-target probe
// and kernel packages are wired in a later hardware change.
bool NativeTransportCompiled();
const char* NativeTransportReason();

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_ROCE_STORAGE_H_
