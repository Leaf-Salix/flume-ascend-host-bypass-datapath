#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "roce_storage/roce_storage.h"
#include "roce_storage/storage_backend.h"
#include "roce_storage/verbs_backend.h"

namespace {

constexpr uint32_t kDefaultQueueDepth = 32;
constexpr uint32_t kDefaultTransferTimeoutMs = 30000;
constexpr uint64_t kDefaultMaxTransferBytes = 64U * 1024U * 1024U;

bool ReadAll(int fd, void* dst, size_t bytes) {
  auto* cursor = static_cast<uint8_t*>(dst);
  while (bytes != 0) {
    const ssize_t n = recv(fd, cursor, bytes, 0);
    if (n <= 0) return false;
    cursor += n;
    bytes -= static_cast<size_t>(n);
  }
  return true;
}

bool WriteAll(int fd, const void* src, size_t bytes) {
  const auto* cursor = static_cast<const uint8_t*>(src);
  while (bytes != 0) {
    const ssize_t n = send(fd, cursor, bytes, 0);
    if (n <= 0) return false;
    cursor += n;
    bytes -= static_cast<size_t>(n);
  }
  return true;
}

bool OpenListener(const std::string& listen, uint16_t port, int* listener, std::string* error) {
  if (listener == nullptr || port == 0) return false;
  *listener = socket(AF_INET, SOCK_STREAM, 0);
  if (*listener < 0) {
    if (error != nullptr) *error = "failed to create TCP control socket";
    return false;
  }
  int reuse = 1;
  setsockopt(*listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (inet_pton(AF_INET, listen.c_str(), &address.sin_addr) != 1 ||
      bind(*listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(*listener, 8) != 0) {
    if (error != nullptr) *error = "failed to bind/listen on TCP control endpoint";
    close(*listener);
    *listener = -1;
    return false;
  }
  return true;
}

flume::roce::Endpoint ToWireEndpoint(const flume::roce::VerbsEndpoint& endpoint) {
  flume::roce::Endpoint wire;
  wire.gid = endpoint.gid;
  wire.qpn = endpoint.qpn;
  wire.psn = endpoint.psn;
  wire.port = endpoint.port;
  wire.gid_index = endpoint.gid_index;
  wire.mtu = endpoint.mtu;
  return wire;
}

flume::roce::VerbsEndpoint ToVerbsEndpoint(const flume::roce::Endpoint& endpoint) {
  flume::roce::VerbsEndpoint verbs;
  verbs.gid = endpoint.gid;
  verbs.qpn = endpoint.qpn;
  verbs.psn = endpoint.psn;
  verbs.port = endpoint.port;
  verbs.gid_index = endpoint.gid_index;
  verbs.mtu = endpoint.mtu;
  return verbs;
}

int ServeOneClient(int fd, flume::roce::StorageBackend* storage, const std::string& verbs_device,
                   uint8_t verbs_port, uint8_t gid_index, uint32_t timeout_ms) {
  std::vector<uint8_t> wire(flume::roce::kSessionRequestWireBytes);
  flume::roce::SessionRequest request;
  if (!ReadAll(fd, wire.data(), wire.size()) ||
      !flume::roce::DecodeSessionRequest(wire.data(), wire.size(), &request)) {
    std::cerr << "invalid RoCE session request received on control connection\n";
    return 1;
  }
  flume::roce::VerbsBackend verbs;
  std::string error;
  if (!verbs.Open(verbs_device, verbs_port, gid_index, kDefaultQueueDepth, &error) ||
      !verbs.Connect(ToVerbsEndpoint(request.endpoint), &error)) {
    std::cerr << "verbs connection setup failed: " << error << "\n";
    return 1;
  }
  std::vector<uint8_t> command_wire(flume::roce::kCommandWireBytes);
  std::vector<uint8_t> completion_wire(flume::roce::kCompletionWireBytes);
  flume::roce::VerbsMemoryRegion command_mr;
  flume::roce::VerbsMemoryRegion completion_mr;
  if (!verbs.Register(command_wire.data(), command_wire.size(), false, false,
                      &command_mr, &error)) {
    std::cerr << "failed to prepare server command/completion buffers: " << error << "\n";
    return 1;
  }
  if (!verbs.Register(completion_wire.data(), completion_wire.size(), false, false,
                      &completion_mr, &error) || !verbs.PostReceive(command_mr, &error)) {
    verbs.Deregister(&completion_mr, nullptr);
    verbs.Deregister(&command_mr, nullptr);
    std::cerr << "failed to prepare server command/completion buffers: " << error << "\n";
    return 1;
  }
  struct RegionCleanup {
    flume::roce::VerbsBackend* verbs;
    flume::roce::VerbsMemoryRegion* command;
    flume::roce::VerbsMemoryRegion* completion;
    ~RegionCleanup() {
      verbs->Deregister(completion, nullptr);
      verbs->Deregister(command, nullptr);
    }
  } region_cleanup{&verbs, &command_mr, &completion_mr};
  flume::roce::SessionResponse response;
  response.endpoint = ToWireEndpoint(verbs.endpoint());
  response.namespace_capacity = storage->size();
  response.max_transfer_bytes = std::min<uint64_t>(storage->size(), kDefaultMaxTransferBytes);
  response.server_capabilities = dynamic_cast<flume::roce::MemoryStorageBackend*>(storage) != nullptr ?
      flume::roce::kServerCapabilityMemoryNamespace :
      flume::roce::kServerCapabilityPosixNamespace;
  if (!flume::roce::EncodeSessionResponse(response, &wire) ||
      !WriteAll(fd, wire.data(), wire.size())) {
    std::cerr << "failed to send storage-node RoCE session response\n";
    return 1;
  }
  while (true) {
    if (!verbs.WaitForReceive(timeout_ms, &error)) {
      std::cerr << "failed waiting for NPU RA command: " << error << "\n";
      return 1;
    }
    flume::roce::Command command;
    if (!flume::roce::DecodeCommand(command_wire.data(), command_wire.size(), &command)) {
      std::cerr << "invalid RoCE storage command\n";
      return 1;
    }
    flume::roce::Completion completion;
    completion.request_id = command.request_id;
    if (command.object_id != 0 || command.length > response.max_transfer_bytes ||
        command.length > static_cast<uint64_t>(SIZE_MAX) ||
        command.storage_offset > storage->size() ||
        command.length > storage->size() - command.storage_offset) {
      completion.status = 1;
    } else {
      std::vector<uint8_t> staging(static_cast<size_t>(command.length));
      flume::roce::VerbsMemoryRegion mr;
      const bool registered = verbs.Register(staging.data(), staging.size(), false, false, &mr, &error);
      bool ok = registered;
      if (ok && command.operation == flume::roce::Operation::kRead) {
        ok = storage->Read(command.storage_offset, staging.data(), staging.size(), &error) &&
             verbs.Write(mr, command.npu_address, command.npu_rkey, staging.size(), timeout_ms, &error);
      } else if (ok && command.operation == flume::roce::Operation::kWrite) {
        ok = verbs.Read(mr, command.npu_address, command.npu_rkey, staging.size(), timeout_ms, &error) &&
             storage->Write(command.storage_offset, staging.data(), staging.size(), &error);
      }
      if (registered) verbs.Deregister(&mr, nullptr);
      completion.status = ok ? 0 : 2;
      completion.bytes = ok ? command.length : 0;
      completion.checksum = ok ? flume::roce::Checksum(staging.data(), staging.size()) : 0;
      if (!ok) std::cerr << "request " << command.request_id << " failed: " << error << "\n";
    }
    if (!flume::roce::EncodeCompletion(completion, &completion_wire) ||
        !verbs.Write(completion_mr, request.completion.address,
                     request.completion.rkey, completion_wire.size(), timeout_ms, &error) ||
        !verbs.PostReceive(command_mr, &error)) {
      std::cerr << "failed to publish RDMA completion or repost command receive: " << error << "\n";
      return 1;
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::string listen = "0.0.0.0";
  std::string storage_file;
  std::string verbs_device;
  size_t namespace_bytes = 64U * 1024U * 1024U;
  uint32_t control_port = 0;
  uint32_t timeout_ms = kDefaultTransferTimeoutMs;
  uint32_t verbs_port = 1;
  uint32_t gid_index = 0;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--listen" && index + 1 < argc) {
      listen = argv[++index];
    } else if (arg == "--storage-file" && index + 1 < argc) {
      storage_file = argv[++index];
    } else if (arg == "--verbs-device" && index + 1 < argc) {
      verbs_device = argv[++index];
    } else if (arg == "--namespace-bytes" && index + 1 < argc) {
      namespace_bytes = static_cast<size_t>(std::strtoull(argv[++index], nullptr, 10));
    } else if (arg == "--control-port" && index + 1 < argc) {
      control_port = static_cast<uint32_t>(std::strtoul(argv[++index], nullptr, 10));
    } else if (arg == "--timeout-ms" && index + 1 < argc) {
      timeout_ms = static_cast<uint32_t>(std::strtoul(argv[++index], nullptr, 10));
    } else if (arg == "--verbs-port" && index + 1 < argc) {
      verbs_port = static_cast<uint32_t>(std::strtoul(argv[++index], nullptr, 10));
    } else if (arg == "--gid-index" && index + 1 < argc) {
      gid_index = static_cast<uint32_t>(std::strtoul(argv[++index], nullptr, 10));
    } else {
      std::cerr << "usage: flume-roce-storage-server [--listen ip] "
                << "[--namespace-bytes bytes] [--storage-file path] "
                << "[--verbs-device name] [--verbs-port N] [--gid-index N] "
                << "[--control-port port] [--timeout-ms ms]\n";
      return 2;
    }
  }
  std::unique_ptr<flume::roce::StorageBackend> storage;
  if (storage_file.empty()) {
    storage = std::make_unique<flume::roce::MemoryStorageBackend>(namespace_bytes);
  } else {
    storage = std::make_unique<flume::roce::PosixStorageBackend>(storage_file);
    if (storage->size() == 0) {
      std::cerr << "failed to open nonempty storage file: " << storage_file << "\n";
      return 1;
    }
  }
  std::cout << "flume RoCE storage server scaffold "
            << "listen=" << listen << " namespace_bytes=" << storage->size() << " "
            << "protocol=flume-roce-v2 control_plane=tcp-bootstrap "
            << "command_plane=npu-ra-send completion_plane=rdma-write-to-npu-hbm "
            << "storage_backend="
            << (storage_file.empty() ? "memory" : "posix") << " "
            << "verbs_backend=" << (flume::roce::VerbsAvailable() ? "available" : "unavailable") << " "
            << "native_transport=" << (flume::roce::NativeTransportCompiled() ? "on" : "off")
            << " detail=\"" << flume::roce::NativeTransportReason() << "\"\n";
  if (control_port != 0 && verbs_device.empty()) {
    std::cerr << "--control-port requires --verbs-device\n";
    return 2;
  }
  if (!verbs_device.empty() && control_port == 0) {
    flume::roce::VerbsBackend verbs;
    std::string error;
    if (verbs_port == 0 || verbs_port > UINT8_MAX || gid_index > UINT8_MAX ||
        !verbs.Open(verbs_device, static_cast<uint8_t>(verbs_port),
                    static_cast<uint8_t>(gid_index), 16, &error)) {
      std::cerr << "verbs endpoint initialization failed: " << error << "\n";
      return 1;
    }
    const auto endpoint = verbs.endpoint();
    std::cout << "verbs endpoint initialized qpn=" << endpoint.qpn
              << " psn=" << endpoint.psn << " port=" << static_cast<int>(endpoint.port) << "\n";
  }
  if (control_port != 0) {
    int listener = -1;
    std::string error;
    if (!OpenListener(listen, static_cast<uint16_t>(control_port), &listener, &error)) {
      std::cerr << error << "\n";
      return 1;
    }
    std::cout << "waiting for RoCE control peer at " << listen << ":" << control_port << "\n";
    const int client = accept(listener, nullptr, nullptr);
    close(listener);
    if (client < 0) {
      std::cerr << "failed to accept TCP control peer\n";
      return 1;
    }
    if (verbs_port == 0 || verbs_port > UINT8_MAX || gid_index > UINT8_MAX) {
      std::cerr << "verbs port and GID index must fit in uint8\n";
      close(client);
      return 2;
    }
    const int result = ServeOneClient(client, storage.get(), verbs_device,
                                      static_cast<uint8_t>(verbs_port),
                                      static_cast<uint8_t>(gid_index), timeout_ms);
    close(client);
    return result;
  }
  return 0;
}
