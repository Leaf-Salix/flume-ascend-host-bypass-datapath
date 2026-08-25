#include "roce_storage/control_channel.h"

#include <cerrno>
#include <cstring>
#include <sys/socket.h>

#include <vector>

namespace flume::roce {
namespace {

void SetSocketError(const char* operation, std::string* error) {
  if (error != nullptr) {
    *error = std::string(operation) + ": " + std::strerror(errno);
  }
}

}  // namespace

bool ControlWriteAll(int fd, const void* src, size_t bytes, std::string* error) {
  if (fd < 0 || (src == nullptr && bytes != 0)) {
    if (error != nullptr) *error = "invalid TCP control write arguments";
    return false;
  }
#ifdef SO_NOSIGPIPE
  int no_sigpipe = 1;
  (void)setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe,
                   sizeof(no_sigpipe));
#endif
  const auto* cursor = static_cast<const uint8_t*>(src);
  while (bytes != 0) {
#ifdef MSG_NOSIGNAL
    const ssize_t count = send(fd, cursor, bytes, MSG_NOSIGNAL);
#else
    const ssize_t count = send(fd, cursor, bytes, 0);
#endif
    if (count < 0 && errno == EINTR) continue;
    if (count <= 0) {
      SetSocketError("TCP control write failed", error);
      return false;
    }
    cursor += count;
    bytes -= static_cast<size_t>(count);
  }
  return true;
}

ControlReadResult ControlReadAll(int fd, void* dst, size_t bytes,
                                 std::string* error) {
  if (fd < 0 || (dst == nullptr && bytes != 0)) {
    if (error != nullptr) *error = "invalid TCP control read arguments";
    return ControlReadResult::kError;
  }
  auto* cursor = static_cast<uint8_t*>(dst);
  size_t received = 0;
  while (received != bytes) {
    const ssize_t count = recv(fd, cursor + received, bytes - received, 0);
    if (count < 0 && errno == EINTR) continue;
    if (count == 0) {
      if (received == 0) return ControlReadResult::kPeerClosed;
      if (error != nullptr) *error = "TCP control peer closed mid-frame";
      return ControlReadResult::kError;
    }
    if (count < 0) {
      SetSocketError("TCP control read failed", error);
      return ControlReadResult::kError;
    }
    received += static_cast<size_t>(count);
  }
  return ControlReadResult::kSuccess;
}

bool SendCommand(int fd, const Command& command, std::string* error) {
  std::vector<uint8_t> wire;
  if (!EncodeCommand(command, &wire)) {
    if (error != nullptr) *error = "invalid TCP control command";
    return false;
  }
  return ControlWriteAll(fd, wire.data(), wire.size(), error);
}

ControlReadResult ReceiveCommand(int fd, Command* command, std::string* error) {
  if (command == nullptr) {
    if (error != nullptr) *error = "TCP control command output is null";
    return ControlReadResult::kError;
  }
  std::vector<uint8_t> wire(kCommandWireBytes);
  const ControlReadResult read = ControlReadAll(fd, wire.data(), wire.size(), error);
  if (read != ControlReadResult::kSuccess) return read;
  if (!DecodeCommand(wire.data(), wire.size(), command)) {
    if (error != nullptr) *error = "invalid TCP control command frame";
    return ControlReadResult::kError;
  }
  return ControlReadResult::kSuccess;
}

bool SendCompletion(int fd, const Completion& completion, std::string* error) {
  std::vector<uint8_t> wire;
  if (!EncodeCompletion(completion, &wire)) {
    if (error != nullptr) *error = "invalid TCP control completion";
    return false;
  }
  return ControlWriteAll(fd, wire.data(), wire.size(), error);
}

ControlReadResult ReceiveCompletion(int fd, Completion* completion,
                                    std::string* error) {
  if (completion == nullptr) {
    if (error != nullptr) *error = "TCP control completion output is null";
    return ControlReadResult::kError;
  }
  std::vector<uint8_t> wire(kCompletionWireBytes);
  const ControlReadResult read = ControlReadAll(fd, wire.data(), wire.size(), error);
  if (read != ControlReadResult::kSuccess) return read;
  if (!DecodeCompletion(wire.data(), wire.size(), completion)) {
    if (error != nullptr) *error = "invalid TCP control completion frame";
    return ControlReadResult::kError;
  }
  return ControlReadResult::kSuccess;
}

}  // namespace flume::roce
