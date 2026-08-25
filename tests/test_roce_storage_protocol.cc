#include <iostream>
#include <vector>

#include "roce_storage/roce_storage.h"
#include "test_util.h"

int main() {
  flume::roce::Command command;
  command.request_id = 7;
  command.operation = flume::roce::Operation::kRead;
  command.object_id = 23;
  command.storage_offset = 4096;
  command.length = 8192;
  command.npu_address = 0x12340000ULL;
  command.npu_rkey = 99;
  command.npu_access = flume::roce::kMemoryRemoteWrite;
  std::vector<uint8_t> wire;
  FLUME_TEST_CHECK(flume::roce::EncodeCommand(command, &wire));
  FLUME_TEST_CHECK(wire.size() == flume::roce::kCommandWireBytes);
  flume::roce::Command decoded;
  FLUME_TEST_CHECK(flume::roce::DecodeCommand(wire.data(), wire.size(), &decoded));
  FLUME_TEST_CHECK(decoded.request_id == command.request_id);
  FLUME_TEST_CHECK(decoded.operation == command.operation);
  FLUME_TEST_CHECK(decoded.object_id == command.object_id);
  FLUME_TEST_CHECK(decoded.storage_offset == command.storage_offset);
  FLUME_TEST_CHECK(decoded.length == command.length);
  FLUME_TEST_CHECK(decoded.npu_address == command.npu_address);
  FLUME_TEST_CHECK(decoded.npu_rkey == command.npu_rkey);
  FLUME_TEST_CHECK(decoded.npu_access == command.npu_access);
  wire[0] = 0;
  FLUME_TEST_CHECK(!flume::roce::DecodeCommand(wire.data(), wire.size(), &decoded));
  command.operation = flume::roce::Operation::kWrite;
  command.npu_access = flume::roce::kMemoryRemoteRead;
  FLUME_TEST_CHECK(flume::roce::EncodeCommand(command, &wire));
  FLUME_TEST_CHECK(flume::roce::DecodeCommand(wire.data(), wire.size(), &decoded));
  FLUME_TEST_CHECK(decoded.operation == flume::roce::Operation::kWrite);
  command.npu_access = flume::roce::kMemoryRemoteWrite;
  FLUME_TEST_CHECK(!flume::roce::EncodeCommand(command, &wire));

  flume::roce::Completion completion;
  completion.request_id = 42;
  completion.bytes = command.length;
  completion.checksum = flume::roce::Checksum(wire.data(), wire.size());
  completion.flags = 7;
  FLUME_TEST_CHECK(flume::roce::EncodeCompletion(completion, &wire));
  FLUME_TEST_CHECK(wire.size() == flume::roce::kCompletionWireBytes);
  flume::roce::Completion decoded_completion;
  FLUME_TEST_CHECK(flume::roce::DecodeCompletion(wire.data(), wire.size(), &decoded_completion));
  FLUME_TEST_CHECK(decoded_completion.request_id == completion.request_id);
  FLUME_TEST_CHECK(decoded_completion.bytes == completion.bytes);
  FLUME_TEST_CHECK(decoded_completion.checksum == completion.checksum);
  FLUME_TEST_CHECK(decoded_completion.flags == completion.flags);

  flume::roce::Endpoint endpoint;
  endpoint.gid[15] = 1;
  endpoint.qpn = 12;
  endpoint.psn = 34;
  endpoint.port = 1;
  endpoint.gid_index = 3;
  endpoint.mtu = 5;
  FLUME_TEST_CHECK(flume::roce::EncodeEndpoint(endpoint, &wire));
  FLUME_TEST_CHECK(wire.size() == flume::roce::kEndpointWireBytes);
  flume::roce::Endpoint decoded_endpoint;
  FLUME_TEST_CHECK(flume::roce::DecodeEndpoint(wire.data(), wire.size(), &decoded_endpoint));
  FLUME_TEST_CHECK(decoded_endpoint.qpn == endpoint.qpn);
  FLUME_TEST_CHECK(decoded_endpoint.psn == endpoint.psn);
  FLUME_TEST_CHECK(decoded_endpoint.gid == endpoint.gid);

  flume::roce::SessionRequest request;
  request.endpoint = endpoint;
  request.completion.address = 0x56780000ULL;
  request.completion.length = flume::roce::kCompletionWireBytes;
  request.completion.rkey = 101;
  request.completion.access = flume::roce::kMemoryRemoteWrite;
  request.flags = 0;
  FLUME_TEST_CHECK(flume::roce::EncodeSessionRequest(request, &wire));
  FLUME_TEST_CHECK(wire.size() == flume::roce::kSessionRequestWireBytes);
  flume::roce::SessionRequest decoded_request;
  FLUME_TEST_CHECK(flume::roce::DecodeSessionRequest(wire.data(), wire.size(),
                                                     &decoded_request));
  FLUME_TEST_CHECK(decoded_request.endpoint.qpn == endpoint.qpn);
  FLUME_TEST_CHECK(decoded_request.completion.address == request.completion.address);
  FLUME_TEST_CHECK(decoded_request.completion.rkey == request.completion.rkey);
  FLUME_TEST_CHECK(decoded_request.flags == request.flags);
  request.completion.access = flume::roce::kMemoryRemoteRead;
  FLUME_TEST_CHECK(!flume::roce::EncodeSessionRequest(request, &wire));
  request = {};
  request.endpoint = endpoint;
  request.flags = flume::roce::kSessionFlagTcpControl;
  FLUME_TEST_CHECK(flume::roce::EncodeSessionRequest(request, &wire));
  FLUME_TEST_CHECK(flume::roce::DecodeSessionRequest(
      wire.data(), wire.size(), &decoded_request));
  FLUME_TEST_CHECK(decoded_request.flags == flume::roce::kSessionFlagTcpControl);
  request.completion.address = 1;
  FLUME_TEST_CHECK(!flume::roce::EncodeSessionRequest(request, &wire));

  flume::roce::SessionResponse response;
  response.endpoint = endpoint;
  response.namespace_capacity = 64U * 1024U * 1024U;
  response.max_transfer_bytes = 16U * 1024U * 1024U;
  response.server_capabilities = flume::roce::kServerCapabilityMemoryNamespace;
  FLUME_TEST_CHECK(flume::roce::EncodeSessionResponse(response, &wire));
  FLUME_TEST_CHECK(wire.size() == flume::roce::kSessionResponseWireBytes);
  flume::roce::SessionResponse decoded_response;
  FLUME_TEST_CHECK(flume::roce::DecodeSessionResponse(wire.data(), wire.size(),
                                                      &decoded_response));
  FLUME_TEST_CHECK(decoded_response.namespace_capacity == response.namespace_capacity);
  FLUME_TEST_CHECK(decoded_response.max_transfer_bytes == response.max_transfer_bytes);
  FLUME_TEST_CHECK(decoded_response.server_capabilities == response.server_capabilities);

  const std::string tcp_marker = flume::roce::MakeHostRaSuccessMarker(
      flume::roce::ControlMode::kTcp, flume::roce::Operation::kRead,
      flume::roce::kServerCapabilityMemoryNamespace);
  FLUME_TEST_CHECK(tcp_marker.find("control_path=tcp") != std::string::npos);
  FLUME_TEST_CHECK(tcp_marker.find(
      "payload_path=server-memory->rnic->npu-hbm") != std::string::npos);
  FLUME_TEST_CHECK(tcp_marker.find("compute_host_payload_bytes=0") !=
                   std::string::npos);
  FLUME_TEST_CHECK(tcp_marker.find("fallback=none") != std::string::npos);
  const std::string npu_ra_marker = flume::roce::MakeHostRaSuccessMarker(
      flume::roce::ControlMode::kNpuRa, flume::roce::Operation::kWrite,
      flume::roce::kServerCapabilityPosixNamespace);
  FLUME_TEST_CHECK(npu_ra_marker.find("control_path=npu-ra") != std::string::npos);
  FLUME_TEST_CHECK(npu_ra_marker.find(
      "payload_path=npu-hbm->rnic->server-posix") != std::string::npos);

  flume::roce::SessionLifecycle lifecycle;
  FLUME_TEST_CHECK(lifecycle.LocalResourcesReady());
  FLUME_TEST_CHECK(lifecycle.Bootstrapped());
  FLUME_TEST_CHECK(lifecycle.Connected());
  FLUME_TEST_CHECK(lifecycle.BeginRequest(42));
  FLUME_TEST_CHECK(!lifecycle.BeginRequest(43));
  FLUME_TEST_CHECK(!lifecycle.Close());
  FLUME_TEST_CHECK(!lifecycle.CompleteRequest(43));
  FLUME_TEST_CHECK(lifecycle.CompleteRequest(42));
  FLUME_TEST_CHECK(lifecycle.Close());
  FLUME_TEST_CHECK(!lifecycle.Close());
  std::cout << "roce storage protocol test passed\n";
  return 0;
}
