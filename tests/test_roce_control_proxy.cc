#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "roce_storage/control_channel.h"
#include "roce_storage/control_proxy.h"
#include "roce_storage/roce_storage.h"
#include "test_util.h"

namespace {

flume::roce::Endpoint Endpoint(uint32_t qpn, uint8_t gid_tail) {
  flume::roce::Endpoint endpoint;
  endpoint.gid[15] = gid_tail;
  endpoint.qpn = qpn;
  endpoint.psn = qpn + 10;
  endpoint.port = 1;
  endpoint.gid_index = 0;
  endpoint.mtu = 5;
  return endpoint;
}

void RunUpstream(int fd, std::vector<uint8_t>* target) {
  std::string error;
  std::vector<uint8_t> wire(flume::roce::kSessionRequestWireBytes);
  flume::roce::SessionRequest request;
  FLUME_TEST_CHECK(flume::roce::ControlReadAll(
      fd, wire.data(), wire.size(), &error) ==
      flume::roce::ControlReadResult::kSuccess);
  FLUME_TEST_CHECK(flume::roce::DecodeSessionRequest(
      wire.data(), wire.size(), &request));
  FLUME_TEST_CHECK(request.endpoint.qpn == 101);

  flume::roce::SessionResponse response;
  response.endpoint = Endpoint(202, 2);
  response.namespace_capacity = 4096;
  response.max_transfer_bytes = 4096;
  response.server_capabilities =
      flume::roce::kServerCapabilityPosixNamespace;
  FLUME_TEST_CHECK(flume::roce::EncodeSessionResponse(response, &wire));
  FLUME_TEST_CHECK(flume::roce::ControlWriteAll(
      fd, wire.data(), wire.size(), &error));

  flume::roce::Command command;
  FLUME_TEST_CHECK(flume::roce::ReceiveCommand(fd, &command, &error) ==
                   flume::roce::ControlReadResult::kSuccess);
  FLUME_TEST_CHECK(command.npu_address ==
                   reinterpret_cast<uint64_t>(target->data()));
  FLUME_TEST_CHECK(command.npu_rkey == 0x1234U);
  for (size_t index = 0; index < command.length; ++index) {
    (*target)[index] = static_cast<uint8_t>((index * 13U + 7U) & 0xffU);
  }
  flume::roce::Completion completion;
  completion.request_id = command.request_id;
  completion.bytes = command.length;
  completion.checksum = flume::roce::Checksum(target->data(), command.length);
  FLUME_TEST_CHECK(flume::roce::SendCompletion(fd, completion, &error));
  close(fd);
}

}  // namespace

int main() {
  int downstream[2] = {-1, -1};
  int upstream[2] = {-1, -1};
  FLUME_TEST_CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, downstream) == 0);
  FLUME_TEST_CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, upstream) == 0);
  std::vector<uint8_t> target(1024, 0);
  flume::roce::ControlProxyStats stats;
  bool proxy_ok = false;
  std::string proxy_error;
  std::thread proxy([&] {
    proxy_ok = flume::roce::ProxyPushControlSession(
        downstream[1], upstream[0], &stats, &proxy_error);
    close(downstream[1]);
    close(upstream[0]);
  });
  std::thread data_node(RunUpstream, upstream[1], &target);

  flume::roce::SessionRequest request;
  request.endpoint = Endpoint(101, 1);
  request.flags = flume::roce::kSessionFlagTcpControl;
  std::vector<uint8_t> wire;
  std::string error;
  FLUME_TEST_CHECK(flume::roce::EncodeSessionRequest(request, &wire));
  FLUME_TEST_CHECK(flume::roce::ControlWriteAll(
      downstream[0], wire.data(), wire.size(), &error));
  wire.assign(flume::roce::kSessionResponseWireBytes, 0);
  FLUME_TEST_CHECK(flume::roce::ControlReadAll(
      downstream[0], wire.data(), wire.size(), &error) ==
      flume::roce::ControlReadResult::kSuccess);
  flume::roce::SessionResponse response;
  FLUME_TEST_CHECK(flume::roce::DecodeSessionResponse(
      wire.data(), wire.size(), &response));
  FLUME_TEST_CHECK(response.endpoint.qpn == 202);
  FLUME_TEST_CHECK((response.server_capabilities &
                    flume::roce::kServerCapabilityControlProxyUsed) != 0);

  flume::roce::Command command;
  command.request_id = 1;
  command.operation = flume::roce::Operation::kRead;
  command.length = target.size();
  command.npu_address = reinterpret_cast<uint64_t>(target.data());
  command.npu_rkey = 0x1234U;
  command.npu_access = flume::roce::kMemoryRemoteWrite;
  FLUME_TEST_CHECK(flume::roce::SendCommand(downstream[0], command, &error));
  flume::roce::Completion completion;
  FLUME_TEST_CHECK(flume::roce::ReceiveCompletion(
      downstream[0], &completion, &error) ==
      flume::roce::ControlReadResult::kSuccess);
  FLUME_TEST_CHECK(completion.status == 0);
  FLUME_TEST_CHECK(completion.checksum ==
                   flume::roce::Checksum(target.data(), target.size()));
  close(downstream[0]);
  data_node.join();
  proxy.join();
  FLUME_TEST_CHECK(proxy_ok);
  FLUME_TEST_CHECK(proxy_error.empty());
  FLUME_TEST_CHECK(stats.requests == 1);
  FLUME_TEST_CHECK(stats.payload_bytes == 0);
  FLUME_TEST_CHECK(stats.command_bytes == flume::roce::kCommandWireBytes);

  int pull_downstream[2] = {-1, -1};
  int pull_upstream[2] = {-1, -1};
  FLUME_TEST_CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, pull_downstream) == 0);
  FLUME_TEST_CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, pull_upstream) == 0);
  request.flags = flume::roce::kSessionFlagTcpControl |
                  flume::roce::kSessionFlagPullTransfer;
  FLUME_TEST_CHECK(flume::roce::EncodeSessionRequest(request, &wire));
  FLUME_TEST_CHECK(flume::roce::ControlWriteAll(
      pull_downstream[0], wire.data(), wire.size(), &error));
  flume::roce::ControlProxyStats pull_stats;
  std::string pull_error;
  FLUME_TEST_CHECK(!flume::roce::ProxyPushControlSession(
      pull_downstream[1], pull_upstream[0], &pull_stats, &pull_error));
  FLUME_TEST_CHECK(pull_error.find("pull transfer mode") != std::string::npos);
  close(pull_downstream[0]);
  close(pull_downstream[1]);
  close(pull_upstream[0]);
  close(pull_upstream[1]);
  return 0;
}
