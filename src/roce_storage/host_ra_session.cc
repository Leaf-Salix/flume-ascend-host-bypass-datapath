#include "roce_storage/host_ra_session.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

#include "roce_storage/cann_ra_loader.h"
#include "roce_storage/control_channel.h"

namespace flume::roce {
namespace {

int ConnectTcp(const std::string& host, uint32_t port, uint32_t timeout_ms,
               std::string* error) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* addresses = nullptr;
  const std::string service = std::to_string(port);
  if (getaddrinfo(host.c_str(), service.c_str(), &hints, &addresses) != 0) {
    if (error != nullptr) *error = "failed to resolve storage control endpoint";
    return -1;
  }
  int fd = -1;
  for (addrinfo* address = addresses; address != nullptr; address = address->ai_next) {
    fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (fd < 0) continue;
    timeval timeout{};
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = static_cast<suseconds_t>((timeout_ms % 1000) * 1000);
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    if (connect(fd, address->ai_addr, address->ai_addrlen) == 0) break;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(addresses);
  if (fd < 0 && error != nullptr) *error = "failed to connect storage control endpoint";
  return fd;
}

Endpoint ToEndpoint(const cann::TypicalQp& qp) {
  Endpoint endpoint;
  std::memcpy(endpoint.gid.data(), qp.gid, endpoint.gid.size());
  endpoint.qpn = qp.qpn;
  endpoint.psn = qp.psn;
  endpoint.port = 1;
  endpoint.gid_index = static_cast<uint8_t>(qp.gid_index);
  endpoint.mtu = 5;
  return endpoint;
}

cann::TypicalQp ToTypicalQp(const Endpoint& endpoint) {
  cann::TypicalQp qp{};
  qp.qpn = endpoint.qpn;
  qp.psn = endpoint.psn;
  qp.gid_index = endpoint.gid_index;
  std::memcpy(qp.gid, endpoint.gid.data(), endpoint.gid.size());
  qp.retry_count = 7;
  qp.retry_time = 14;
  return qp;
}

}  // namespace

class HostRaSession::Impl {
 public:
  ~Impl() { Close(nullptr); }

  bool Open(const HostRaConfig& requested, std::string* error) {
    std::lock_guard<std::mutex> lock(mu);
    if (lifecycle.state() != SessionState::kCreated) {
      if (error != nullptr) *error = "Host-RA session was already opened";
      return false;
    }
    config = requested;
    const bool npu_command_posting = config.control_mode == ControlMode::kNpuRa;
    if (config.control_mode != ControlMode::kTcp && !npu_command_posting) {
      if (error != nullptr) *error = "unsupported Host-RA control mode";
      return Fail();
    }
    if (config.transfer_mode != TransferMode::kPush) {
      if (error != nullptr) {
        *error = "pull transfer mode is reserved but not implemented; use push";
      }
      return Fail();
    }
    if (!api.Open(npu_command_posting, error)) return Fail();
    capability_loaded = true;
    if (api.SetDevice(static_cast<int32_t>(config.logical_device)) !=
            cann::kSuccess ||
        !api.ResolvePhysicalDevice(static_cast<int32_t>(config.logical_device),
                                   config.physical_device, &physical_device,
                                   error)) {
      if (error != nullptr && error->empty()) {
        *error = "failed to select NPU device or resolve physical device id";
      }
      return Fail();
    }
    const std::string hdc_arg = "--hdcType=" + std::to_string(config.hdc_type);
    cann::RtProcExtParam parameter{hdc_arg.c_str(), hdc_arg.size()};
    cann::RtNetServiceOpenArgs open_args{&parameter, 1};
    if (api.OpenNetService(&open_args) != cann::kSuccess) {
      if (error != nullptr) *error = "rtOpenNetService failed for Host-RA";
      return Fail();
    }
    net_service_open = true;
    init_config = {physical_device, cann::kNicDeploymentDevice,
                   config.hdc_type, false};
    if (api.Init(&init_config) != cann::kSuccess) {
      if (error != nullptr) *error = "RaInit failed";
      return Fail();
    }
    ra_initialized = true;
    cann::Rdev rdev{};
    rdev.phy_id = physical_device;
    rdev.family = AF_INET;
    if (inet_pton(AF_INET, config.npu_rnic_ip.c_str(), &rdev.local_ip.addr) != 1) {
      if (error != nullptr) *error = "NPU RNIC IP must be an IPv4 address";
      return Fail();
    }
    cann::RdevInitInfo rdev_init{};
    rdev_init.mode = cann::kNetworkOffline;
    rdev_init.notify_type = cann::kNotify;
    if (api.RdevInit(rdev_init, rdev, &rdma_handle) != cann::kSuccess ||
        rdma_handle == nullptr) {
      if (error != nullptr) {
        *error = std::string(api.rdev_init_profile_name()) + " failed";
      }
      return Fail();
    }
    local_qp.gid_index = config.gid_index;
    if (api.TypicalQpCreate(rdma_handle, 0, cann::kOpbaseQpMode, &local_qp,
                            &qp_handle) != cann::kSuccess ||
        qp_handle == nullptr) {
      if (error != nullptr) *error = "RaTypicalQpCreate failed";
      return Fail();
    }
    if (npu_command_posting) {
      if (!AllocateAndRegister(kCommandWireBytes, cann::kRaAccessLocalWrite,
                               &command_device, &command_mr, &command_mr_handle, error) ||
          !AllocateAndRegister(kCompletionWireBytes,
                               cann::kRaAccessLocalWrite | cann::kRaAccessRemoteWrite,
                               &completion_device, &completion_mr, &completion_mr_handle, error)) {
        return Fail();
      }
    }
    if (!lifecycle.LocalResourcesReady()) return Fail();

    control_fd = ConnectTcp(config.storage_server, config.bootstrap_port,
                            config.timeout_ms, error);
    if (control_fd < 0) return Fail();
    SessionRequest request;
    request.endpoint = ToEndpoint(local_qp);
    if (config.control_mode == ControlMode::kTcp) {
      request.flags = kSessionFlagTcpControl;
    } else {
      request.completion = {reinterpret_cast<uint64_t>(completion_device),
                            kCompletionWireBytes, completion_mr.rkey,
                            kMemoryRemoteWrite};
    }
    std::vector<uint8_t> wire;
    if (!EncodeSessionRequest(request, &wire) ||
        !ControlWriteAll(control_fd, wire.data(), wire.size(), error)) {
      if (error != nullptr) *error = "failed to send Host-RA session request";
      return Fail();
    }
    wire.assign(kSessionResponseWireBytes, 0);
    SessionResponse response;
    if (ControlReadAll(control_fd, wire.data(), wire.size(), error) !=
            ControlReadResult::kSuccess ||
        !DecodeSessionResponse(wire.data(), wire.size(), &response) || response.status != 0) {
      if (error != nullptr) *error = "storage server rejected Host-RA session bootstrap";
      return Fail();
    }
    if (!lifecycle.Bootstrapped()) return Fail();
    cann::TypicalQp remote_qp = ToTypicalQp(response.endpoint);
    local_qp.retry_count = 7;
    local_qp.retry_time = 14;
    if (api.TypicalQpModify(qp_handle, &local_qp, &remote_qp) !=
        cann::kSuccess) {
      if (error != nullptr) *error = "RaTypicalQpModify failed";
      return Fail();
    }
    namespace_bytes = response.namespace_capacity;
    max_transfer = response.max_transfer_bytes;
    server_capabilities = response.server_capabilities;
    if (!lifecycle.Connected()) return Fail();
    return true;
  }

  bool SubmitAndWait(Operation operation, uint64_t request_id, uint64_t object_id,
                     uint64_t storage_offset, void* npu_buffer, size_t length,
                     void* acl_stream, HostRaResult* result, std::string* error) {
    std::lock_guard<std::mutex> lock(mu);
    if (result == nullptr || npu_buffer == nullptr || length == 0 ||
        (operation != Operation::kRead && operation != Operation::kWrite) ||
        length > max_transfer || storage_offset > namespace_bytes ||
        length > namespace_bytes - storage_offset ||
        !lifecycle.BeginRequest(request_id)) {
      if (error != nullptr) *error = "invalid or concurrent Host-RA storage request";
      return false;
    }
    if (operation == Operation::kWrite &&
        (server_capabilities & kServerCapabilityStorageWrite) == 0) {
      lifecycle.CompleteRequest(request_id);
      if (error != nullptr) {
        *error = "storage server is read-only; restart it with explicit write permission";
      }
      return false;
    }
    cann::MrInfo payload_mr{};
    payload_mr.address = npu_buffer;
    payload_mr.size = length;
    payload_mr.access = cann::kRaAccessLocalWrite |
        (operation == Operation::kRead ? cann::kRaAccessRemoteWrite : cann::kRaAccessRemoteRead);
    void* payload_mr_handle = nullptr;
    if (api.RegisterMr(rdma_handle, &payload_mr, &payload_mr_handle) !=
            cann::kSuccess ||
        payload_mr_handle == nullptr) {
      if (error != nullptr) *error = "RaRegisterMr failed for NPU HBM payload window";
      lifecycle.Fail();
      return false;
    }
    Command command;
    command.request_id = request_id;
    command.operation = operation;
    command.object_id = object_id;
    command.storage_offset = storage_offset;
    command.length = length;
    command.npu_address = reinterpret_cast<uint64_t>(npu_buffer);
    command.npu_rkey = payload_mr.rkey;
    command.npu_access = operation == Operation::kRead ? kMemoryRemoteWrite : kMemoryRemoteRead;
    Completion completion;
    bool ok = true;
    if (config.control_mode == ControlMode::kTcp) {
      ok = SendCommand(control_fd, command, error) &&
           ReceiveCompletion(control_fd, &completion, error) ==
               ControlReadResult::kSuccess;
    } else {
      std::vector<uint8_t> command_wire;
      std::vector<uint8_t> completion_zero(kCompletionWireBytes, 0);
      ok = EncodeCommand(command, &command_wire) &&
          api.DeviceMemcpy(command_device, kCommandWireBytes,
                           command_wire.data(), command_wire.size(),
                           cann::kAclMemcpyHostToDevice) == cann::kSuccess &&
          api.DeviceMemcpy(completion_device, kCompletionWireBytes,
                           completion_zero.data(), completion_zero.size(),
                           cann::kAclMemcpyHostToDevice) == cann::kSuccess;
      cann::SgList sge{reinterpret_cast<uint64_t>(command_device),
                       static_cast<uint32_t>(kCommandWireBytes), command_mr.lkey};
      cann::SendWr wr{&sge, 1, 0, 0, cann::kRaWrSend, cann::kRaSendSignaled};
      cann::SendWrResponse response{};
      if (ok) {
        ok = api.TypicalSendWr(qp_handle, &wr, &response) == cann::kSuccess;
      }
      if (ok) {
        ok = api.RdmaDbSend(response.db.index, response.db.info, acl_stream) ==
             cann::kSuccess;
      }

      const auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(config.timeout_ms);
      std::vector<uint8_t> completion_wire(kCompletionWireBytes);
      while (ok && std::chrono::steady_clock::now() < deadline) {
        ok = api.DeviceMemcpy(completion_wire.data(), completion_wire.size(),
                              completion_device, kCompletionWireBytes,
                              cann::kAclMemcpyDeviceToHost) == cann::kSuccess;
        if (ok && DecodeCompletion(completion_wire.data(), completion_wire.size(), &completion) &&
            completion.request_id == request_id) {
          break;
        }
        completion = {};
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    if (!ok) {
      if (error != nullptr && error->empty()) {
        *error = config.control_mode == ControlMode::kTcp ?
            "TCP command/completion exchange failed" :
            "CANN RA command submission or HBM completion polling failed";
      }
    } else if (completion.request_id != request_id) {
      ok = false;
      if (error != nullptr) *error = "timed out waiting for RDMA completion in NPU HBM";
    } else if (completion.status != 0) {
      ok = false;
      if (error != nullptr) *error = "storage server reported RDMA request failure";
    } else if (completion.bytes != length) {
      ok = false;
      if (error != nullptr) *error = "storage server returned a short RDMA completion";
    }
    const int deregister_status =
        api.DeregisterMr(rdma_handle, payload_mr_handle);
    if (deregister_status != cann::kSuccess) {
      if (error != nullptr) {
        if (!error->empty()) *error += "; ";
        *error += "RaDeregisterMr failed for NPU HBM payload window";
      }
      ok = false;
    }
    if (!ok) {
      lifecycle.Fail();
      return false;
    }
    lifecycle.CompleteRequest(request_id);
    result->bytes = static_cast<size_t>(completion.bytes);
    result->checksum = completion.checksum;
    result->marker = MakeHostRaSuccessMarker(config.control_mode,
                                             config.transfer_mode, operation,
                                             server_capabilities) +
        " cann_ra_symbol_profile=" + api.symbol_profile_name() +
        " cann_ra_rdev_init=" + api.rdev_init_profile_name();
    return true;
  }

  bool Close(std::string* error) {
    std::lock_guard<std::mutex> lock(mu);
    if (lifecycle.state() == SessionState::kRequestInFlight) {
      if (error != nullptr) *error = "cannot close Host-RA session with an in-flight request";
      return false;
    }
    Cleanup();
    lifecycle.Close();
    return true;
  }

  bool available() const { return lifecycle.state() == SessionState::kConnected; }
  bool capability_available() const { return capability_loaded; }

  bool AllocateAndRegister(size_t bytes, int access, void** address,
                           cann::MrInfo* mr, void** mr_handle, std::string* error) {
    if (api.DeviceMalloc(address, bytes, cann::kAclMallocNormalOnly) !=
        cann::kSuccess) {
      if (error != nullptr) *error = "aclrtMalloc failed for Host-RA control buffer";
      return false;
    }
    mr->address = *address;
    mr->size = bytes;
    mr->access = access;
    if (api.RegisterMr(rdma_handle, mr, mr_handle) != cann::kSuccess ||
        *mr_handle == nullptr) {
      if (error != nullptr) *error = "RaRegisterMr failed for Host-RA control buffer";
      return false;
    }
    return true;
  }

  bool Fail() {
    lifecycle.Fail();
    Cleanup();
    return false;
  }

  void Cleanup() {
    if (control_fd >= 0) { close(control_fd); control_fd = -1; }
    if (completion_mr_handle != nullptr) {
      api.DeregisterMr(rdma_handle, completion_mr_handle);
    }
    if (command_mr_handle != nullptr) {
      api.DeregisterMr(rdma_handle, command_mr_handle);
    }
    completion_mr_handle = nullptr;
    command_mr_handle = nullptr;
    if (completion_device != nullptr) api.DeviceFree(completion_device);
    if (command_device != nullptr) api.DeviceFree(command_device);
    completion_device = nullptr;
    command_device = nullptr;
    if (qp_handle != nullptr) api.QpDestroy(qp_handle);
    qp_handle = nullptr;
    if (rdma_handle != nullptr) api.RdevDeinit(rdma_handle, cann::kNotify);
    rdma_handle = nullptr;
    if (ra_initialized) api.Deinit(&init_config);
    ra_initialized = false;
    if (net_service_open) api.CloseNetService();
    net_service_open = false;
    api.Close();
  }

  mutable std::mutex mu;
  CannRaApi api;
  HostRaConfig config;
  SessionLifecycle lifecycle;
  unsigned physical_device = 0;
  cann::RaInitConfig init_config{};
  cann::TypicalQp local_qp{};
  cann::MrInfo command_mr{};
  cann::MrInfo completion_mr{};
  void* rdma_handle = nullptr;
  void* qp_handle = nullptr;
  void* command_device = nullptr;
  void* completion_device = nullptr;
  void* command_mr_handle = nullptr;
  void* completion_mr_handle = nullptr;
  int control_fd = -1;
  bool net_service_open = false;
  bool ra_initialized = false;
  bool capability_loaded = false;
  uint64_t namespace_bytes = 0;
  uint64_t max_transfer = 0;
  uint32_t server_capabilities = 0;
};

HostRaSession::HostRaSession() : impl_(std::make_unique<Impl>()) {}
HostRaSession::~HostRaSession() = default;
bool HostRaSession::Open(const HostRaConfig& config, std::string* error) { return impl_->Open(config, error); }
bool HostRaSession::SubmitAndWait(Operation operation, uint64_t request_id, uint64_t object_id,
                                  uint64_t storage_offset, void* npu_buffer, size_t length,
                                  void* acl_stream, HostRaResult* result, std::string* error) {
  return impl_->SubmitAndWait(operation, request_id, object_id, storage_offset, npu_buffer,
                              length, acl_stream, result, error);
}
bool HostRaSession::Close(std::string* error) { return impl_->Close(error); }
bool HostRaSession::available() const { return impl_->available(); }
bool HostRaSession::capability_available() const { return impl_->capability_available(); }
uint64_t HostRaSession::namespace_capacity() const { return impl_->namespace_bytes; }
uint64_t HostRaSession::max_transfer_bytes() const { return impl_->max_transfer; }

}  // namespace flume::roce
