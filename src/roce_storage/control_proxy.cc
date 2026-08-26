#include "roce_storage/control_proxy.h"

#include <vector>

#include "roce_storage/control_channel.h"
#include "roce_storage/roce_storage.h"

namespace flume::roce {

bool ProxyPushControlSession(int downstream_fd, int upstream_fd,
                             ControlProxyStats* stats, std::string* error) {
  if (downstream_fd < 0 || upstream_fd < 0 || stats == nullptr) {
    if (error != nullptr) *error = "control proxy requires two sockets and stats";
    return false;
  }
  *stats = {};

  std::vector<uint8_t> request_wire(kSessionRequestWireBytes);
  SessionRequest request;
  if (ControlReadAll(downstream_fd, request_wire.data(), request_wire.size(),
                     error) != ControlReadResult::kSuccess ||
      !DecodeSessionRequest(request_wire.data(), request_wire.size(), &request)) {
    if (error != nullptr && error->empty()) {
      *error = "invalid downstream session request";
    }
    return false;
  }
  if ((request.flags & kSessionFlagTcpControl) == 0) {
    if (error != nullptr) *error = "remote-RNIC proxy requires TCP control mode";
    return false;
  }
  if (SessionTransferMode(request) != TransferMode::kPush) {
    if (error != nullptr) *error = "pull transfer mode is reserved but not implemented";
    return false;
  }
  if (!ControlWriteAll(upstream_fd, request_wire.data(), request_wire.size(),
                       error)) {
    return false;
  }
  stats->session_bytes += request_wire.size();

  std::vector<uint8_t> response_wire(kSessionResponseWireBytes);
  SessionResponse response;
  if (ControlReadAll(upstream_fd, response_wire.data(), response_wire.size(),
                     error) != ControlReadResult::kSuccess ||
      !DecodeSessionResponse(response_wire.data(), response_wire.size(),
                             &response)) {
    if (error != nullptr && error->empty()) {
      *error = "invalid upstream session response";
    }
    return false;
  }
  response.server_capabilities |= kServerCapabilityControlProxyUsed;
  if (!EncodeSessionResponse(response, &response_wire) ||
      !ControlWriteAll(downstream_fd, response_wire.data(),
                       response_wire.size(), error)) {
    return false;
  }
  stats->session_bytes += response_wire.size();

  while (true) {
    Command command;
    const ControlReadResult read = ReceiveCommand(downstream_fd, &command, error);
    if (read == ControlReadResult::kPeerClosed) return true;
    if (read != ControlReadResult::kSuccess) return false;

    if (command.operation != Operation::kRead) {
      Completion unsupported;
      unsupported.request_id = command.request_id;
      unsupported.status = kCompletionStatusUnsupported;
      if (!SendCompletion(downstream_fd, unsupported, error)) return false;
      stats->completion_bytes += kCompletionWireBytes;
      ++stats->requests;
      continue;
    }
    if (!SendCommand(upstream_fd, command, error)) return false;
    stats->command_bytes += kCommandWireBytes;

    Completion completion;
    if (ReceiveCompletion(upstream_fd, &completion, error) !=
            ControlReadResult::kSuccess) {
      return false;
    }
    if (completion.request_id != command.request_id) {
      if (error != nullptr) *error = "upstream completion request id mismatch";
      return false;
    }
    if (!SendCompletion(downstream_fd, completion, error)) return false;
    stats->completion_bytes += kCompletionWireBytes;
    ++stats->requests;
  }
}

}  // namespace flume::roce
