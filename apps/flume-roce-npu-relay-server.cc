#include <acl/acl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "roce_storage/control_channel.h"
#include "roce_storage/npu_ra_push_mover.h"
#include "roce_storage/roce_storage.h"
#include "roce_storage/storage_backend.h"

namespace {

constexpr uint64_t kDefaultNamespaceBytes = 64U * 1024U * 1024U;

bool ParseU64(const std::string& text, uint64_t* value) {
  if (value == nullptr || text.empty() || text[0] == '-') return false;
  try {
    size_t parsed = 0;
    const uint64_t result = std::stoull(text, &parsed, 0);
    if (parsed != text.size()) return false;
    *value = result;
    return true;
  } catch (...) {
    return false;
  }
}

int Listen(const std::string& ip, uint16_t port, std::string* error) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    *error = "failed to create NPU relay listener";
    return -1;
  }
  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (inet_pton(AF_INET, ip.c_str(), &address.sin_addr) != 1 ||
      bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(fd, 8) != 0) {
    close(fd);
    *error = "failed to bind/listen on NPU relay control endpoint";
    return -1;
  }
  return fd;
}

}  // namespace

int main(int argc, char** argv) {
  std::string listen_ip = "0.0.0.0";
  std::string storage_file;
  std::string npu_rnic_ip;
  uint64_t parsed_control_port = 0;
  uint64_t parsed_logical_device = 0;
  uint64_t parsed_physical_device = 0;
  uint64_t parsed_gid_index = 0;
  uint64_t parsed_path_mtu = 1024;
  uint64_t parsed_timeout_ms = 30000;
  uint64_t parsed_namespace_bytes = kDefaultNamespaceBytes;
  bool parse_ok = true;
  bool physical_device_set = false;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    auto next_u64 = [&](uint64_t* output) {
      return index + 1 < argc && ParseU64(argv[++index], output);
    };
    if (arg == "--listen" && index + 1 < argc) {
      listen_ip = argv[++index];
    } else if (arg == "--storage-file" && index + 1 < argc) {
      storage_file = argv[++index];
    } else if (arg == "--namespace-bytes") {
      parse_ok = next_u64(&parsed_namespace_bytes) && parse_ok;
    } else if (arg == "--npu-rnic-ip" && index + 1 < argc) {
      npu_rnic_ip = argv[++index];
    } else if (arg == "--device") {
      parse_ok = next_u64(&parsed_logical_device) && parse_ok;
    } else if (arg == "--physical-device") {
      parse_ok = next_u64(&parsed_physical_device) && parse_ok;
      physical_device_set = true;
    } else if (arg == "--gid-index") {
      parse_ok = next_u64(&parsed_gid_index) && parse_ok;
    } else if (arg == "--path-mtu") {
      parse_ok = next_u64(&parsed_path_mtu) && parse_ok;
    } else if (arg == "--control-port") {
      parse_ok = next_u64(&parsed_control_port) && parse_ok;
    } else if (arg == "--timeout-ms") {
      parse_ok = next_u64(&parsed_timeout_ms) && parse_ok;
    } else {
      std::cerr << "usage: flume-roce-npu-relay-server [--listen ip] "
                   "[--storage-file path|--namespace-bytes bytes] "
                   "--npu-rnic-ip ip --device logical-id "
                   "[--physical-device physical-id] "
                   "--gid-index N [--path-mtu 1024|2048|4096] "
                   "--control-port port [--timeout-ms ms]\n";
      return arg == "--help" ? 0 : 2;
    }
  }
  if (!parse_ok || npu_rnic_ip.empty() || parsed_control_port == 0 ||
      parsed_control_port > UINT16_MAX || parsed_timeout_ms == 0 ||
      parsed_timeout_ms > std::numeric_limits<uint32_t>::max() ||
      parsed_logical_device >
          static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ||
      parsed_physical_device >
          static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ||
      parsed_gid_index > UINT8_MAX || parsed_namespace_bytes == 0 ||
      parsed_namespace_bytes > std::numeric_limits<size_t>::max()) {
    std::cerr << "NPU relay requires valid NPU and control endpoints\n";
    return 2;
  }
  const uint32_t control_port = static_cast<uint32_t>(parsed_control_port);
  const uint32_t logical_device =
      static_cast<uint32_t>(parsed_logical_device);
  const int32_t physical_device =
      physical_device_set ? static_cast<int32_t>(parsed_physical_device) : -1;
  const uint32_t gid_index = static_cast<uint32_t>(parsed_gid_index);
  uint8_t path_mtu = 0;
  if (parsed_path_mtu > std::numeric_limits<uint32_t>::max() ||
      !flume::roce::PathMtuFromBytes(static_cast<uint32_t>(parsed_path_mtu),
                                     &path_mtu)) {
    std::cerr << "NPU relay path MTU must be 1024, 2048, or 4096\n";
    return 2;
  }
  const uint32_t timeout_ms = static_cast<uint32_t>(parsed_timeout_ms);
  const size_t namespace_bytes = static_cast<size_t>(parsed_namespace_bytes);

  std::unique_ptr<flume::roce::StorageBackend> storage;
  if (storage_file.empty()) {
    storage = std::make_unique<flume::roce::MemoryStorageBackend>(namespace_bytes);
  } else {
    storage = std::make_unique<flume::roce::PosixStorageBackend>(storage_file);
    if (storage->size() == 0) {
      std::cerr << "failed to open nonempty storage file\n";
      return 1;
    }
  }

  bool acl_initialized = false;
  bool device_set = false;
  aclrtStream stream = nullptr;
  int downstream = -1;
  int listener = -1;
  int exit_code = 1;
  std::string error;
  flume::roce::NpuRaPushMover mover;
  if (aclInit(nullptr) != ACL_SUCCESS) {
    std::cerr << "NPU relay failed: aclInit\n";
    goto cleanup;
  }
  acl_initialized = true;
  if (aclrtSetDevice(static_cast<int32_t>(logical_device)) != ACL_SUCCESS) {
    std::cerr << "NPU relay failed: aclrtSetDevice\n";
    goto cleanup;
  }
  device_set = true;
  if (aclrtCreateStream(&stream) != ACL_SUCCESS) {
    std::cerr << "NPU relay failed: aclrtCreateStream\n";
    goto cleanup;
  }
  listener = Listen(listen_ip, static_cast<uint16_t>(control_port), &error);
  if (listener < 0) {
    std::cerr << error << "\n";
    goto cleanup;
  }
  std::cout << "flume NPU-RA relay waiting: transfer_mode=push "
               "data_mover=npu-ra-relay storage_side_staging=host+hbm\n";
  downstream = accept(listener, nullptr, nullptr);
  close(listener);
  listener = -1;
  if (downstream < 0) {
    std::cerr << "failed to accept NPU relay client\n";
    goto cleanup;
  }

  {
    std::vector<uint8_t> wire(flume::roce::kSessionRequestWireBytes);
    flume::roce::SessionRequest request;
    if (flume::roce::ControlReadAll(downstream, wire.data(), wire.size(),
                                    &error) !=
            flume::roce::ControlReadResult::kSuccess ||
        !flume::roce::DecodeSessionRequest(wire.data(), wire.size(), &request) ||
        (request.flags & flume::roce::kSessionFlagTcpControl) == 0 ||
        flume::roce::SessionTransferMode(request) !=
            flume::roce::TransferMode::kPush) {
      std::cerr << "NPU relay requires a TCP-controlled push session\n";
      goto cleanup;
    }

    flume::roce::NpuRaPushConfig config;
    config.npu_rnic_ip = npu_rnic_ip;
    config.logical_device = logical_device;
    config.physical_device = physical_device;
    config.gid_index = gid_index;
    config.path_mtu = path_mtu;
    config.timeout_ms = timeout_ms;
    flume::roce::Endpoint local_endpoint;
    if (!mover.Open(config, request.endpoint, &local_endpoint, &error)) {
      std::cerr << "NPU relay QP setup failed: " << error << "\n";
      goto cleanup;
    }

    flume::roce::SessionResponse response;
    response.endpoint = local_endpoint;
    response.namespace_capacity = storage->size();
    response.max_transfer_bytes =
        std::min<uint64_t>(storage->size(), kDefaultNamespaceBytes);
    response.server_capabilities =
        (storage_file.empty() ?
             flume::roce::kServerCapabilityMemoryNamespace :
             flume::roce::kServerCapabilityPosixNamespace) |
        flume::roce::kServerCapabilityNpuRaRelay;
    if (!flume::roce::EncodeSessionResponse(response, &wire) ||
        !flume::roce::ControlWriteAll(downstream, wire.data(), wire.size(),
                                      &error)) {
      std::cerr << "failed to send NPU relay session response\n";
      goto cleanup;
    }

    while (true) {
      flume::roce::Command command;
      const auto read = flume::roce::ReceiveCommand(downstream, &command,
                                                     &error);
      if (read == flume::roce::ControlReadResult::kPeerClosed) {
        exit_code = 0;
        break;
      }
      if (read != flume::roce::ControlReadResult::kSuccess) {
        std::cerr << "failed to receive NPU relay command: " << error << "\n";
        break;
      }
      flume::roce::Completion completion;
      completion.request_id = command.request_id;
      if (command.operation != flume::roce::Operation::kRead ||
          command.object_id != 0 || command.length > response.max_transfer_bytes ||
          command.storage_offset > storage->size() ||
          command.length > storage->size() - command.storage_offset) {
        completion.status = command.operation == flume::roce::Operation::kRead ?
            flume::roce::kCompletionStatusInvalidRequest :
            flume::roce::kCompletionStatusUnsupported;
      } else {
        std::vector<uint8_t> staging(static_cast<size_t>(command.length));
        void* relay_hbm = nullptr;
        bool ok = storage->Read(command.storage_offset, staging.data(),
                                staging.size(), &error) &&
            aclrtMalloc(&relay_hbm, staging.size(), ACL_MEM_MALLOC_HUGE_FIRST) ==
                ACL_SUCCESS;
        if (ok) {
          ok = aclrtMemcpy(relay_hbm, staging.size(), staging.data(),
                           staging.size(), ACL_MEMCPY_HOST_TO_DEVICE) ==
                   ACL_SUCCESS;
        }
        flume::roce::MemoryWindow target{command.npu_address, command.length,
                                         command.npu_rkey,
                                         command.npu_access};
        if (ok) {
          ok = mover.Push(relay_hbm, staging.size(), target, stream, &error);
        }
        if (relay_hbm != nullptr) aclrtFree(relay_hbm);
        completion.status = ok ? flume::roce::kCompletionStatusSuccess :
                                 flume::roce::kCompletionStatusBackendError;
        completion.bytes = ok ? command.length : 0;
        completion.checksum = ok ?
            flume::roce::Checksum(staging.data(), staging.size()) : 0;
        if (!ok) std::cerr << "NPU relay request failed: " << error << "\n";
      }
      if (!flume::roce::SendCompletion(downstream, completion, &error)) {
        std::cerr << "failed to send NPU relay completion: " << error << "\n";
        break;
      }
    }
  }

cleanup:
  mover.Close(nullptr);
  if (downstream >= 0) close(downstream);
  if (listener >= 0) close(listener);
  if (stream != nullptr) aclrtDestroyStream(stream);
  if (device_set) aclrtResetDevice(static_cast<int32_t>(logical_device));
  if (acl_initialized) aclFinalize();
  return exit_code;
}
