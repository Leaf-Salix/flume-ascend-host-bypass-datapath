#ifndef FLUME_ROCE_STORAGE_ROCE_STORAGE_H_
#define FLUME_ROCE_STORAGE_ROCE_STORAGE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace flume::roce {

constexpr uint32_t kProtocolMagic = 0x464c5243U;  // "FLRC"
constexpr uint16_t kProtocolVersion = 2;
constexpr size_t kSessionRequestWireBytes = 64;
constexpr size_t kSessionResponseWireBytes = 64;
constexpr size_t kCommandWireBytes = 64;
constexpr size_t kCompletionWireBytes = 40;
constexpr size_t kEndpointWireBytes = 32;

constexpr uint32_t kMemoryRemoteWrite = 1U << 0;
constexpr uint32_t kMemoryRemoteRead = 1U << 1;

constexpr uint32_t kServerCapabilityMemoryNamespace = 1U << 0;
constexpr uint32_t kServerCapabilityPosixNamespace = 1U << 1;
constexpr uint16_t kSessionFlagTcpControl = 1U << 0;
constexpr uint16_t kSessionKnownFlags = kSessionFlagTcpControl;

enum class ControlMode : uint16_t {
  kTcp = 0,
  kNpuRa = 1,
};

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
  uint32_t npu_access = 0;
};

struct Completion {
  uint64_t request_id = 0;
  uint32_t status = 0;
  uint64_t bytes = 0;
  uint32_t checksum = 0;
  uint32_t flags = 0;
};

struct MemoryWindow {
  uint64_t address = 0;
  uint64_t length = 0;
  uint32_t rkey = 0;
  uint32_t access = 0;
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

struct SessionRequest {
  Endpoint endpoint;
  MemoryWindow completion;
  uint16_t flags = 0;
};

struct SessionResponse {
  Endpoint endpoint;
  uint64_t namespace_capacity = 0;
  uint64_t max_transfer_bytes = 0;
  uint32_t server_capabilities = 0;
  uint16_t status = 0;
};

enum class SessionState {
  kCreated,
  kLocalResourcesReady,
  kBootstrapped,
  kConnected,
  kRequestInFlight,
  kClosed,
  kFailed,
};

class SessionLifecycle {
 public:
  bool LocalResourcesReady();
  bool Bootstrapped();
  bool Connected();
  bool BeginRequest(uint64_t request_id);
  bool CompleteRequest(uint64_t request_id);
  bool Fail();
  bool Close();

  SessionState state() const { return state_; }
  uint64_t active_request_id() const { return active_request_id_; }

 private:
  SessionState state_ = SessionState::kCreated;
  uint64_t active_request_id_ = 0;
};

bool EncodeCommand(const Command& command, std::vector<uint8_t>* wire);
bool DecodeCommand(const uint8_t* wire, size_t len, Command* command);
bool EncodeCompletion(const Completion& completion, std::vector<uint8_t>* wire);
bool DecodeCompletion(const uint8_t* wire, size_t len, Completion* completion);
bool EncodeEndpoint(const Endpoint& endpoint, std::vector<uint8_t>* wire);
bool DecodeEndpoint(const uint8_t* wire, size_t len, Endpoint* endpoint);
bool EncodeSessionRequest(const SessionRequest& request, std::vector<uint8_t>* wire);
bool DecodeSessionRequest(const uint8_t* wire, size_t len, SessionRequest* request);
bool EncodeSessionResponse(const SessionResponse& response, std::vector<uint8_t>* wire);
bool DecodeSessionResponse(const uint8_t* wire, size_t len, SessionResponse* response);
uint32_t Checksum(const uint8_t* data, size_t len);
const char* ControlModeName(ControlMode mode);
std::string MakeHostRaSuccessMarker(ControlMode mode, Operation operation,
                                    uint32_t server_capabilities);

// The protocol compiles on every development host. Native Host-RA support is
// enabled explicitly and resolves CANN symbols at runtime, so missing toolkits
// remain a diagnosable capability result rather than a link failure.
bool NativeTransportCompiled();
const char* NativeTransportReason();

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_ROCE_STORAGE_H_
