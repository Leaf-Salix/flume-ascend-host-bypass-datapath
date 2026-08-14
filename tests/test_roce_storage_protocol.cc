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
  std::vector<uint8_t> wire;
  FLUME_TEST_CHECK(flume::roce::EncodeCommand(command, &wire));
  FLUME_TEST_CHECK(wire.size() == 56);
  flume::roce::Command decoded;
  FLUME_TEST_CHECK(flume::roce::DecodeCommand(wire.data(), wire.size(), &decoded));
  FLUME_TEST_CHECK(decoded.request_id == command.request_id);
  FLUME_TEST_CHECK(decoded.operation == command.operation);
  FLUME_TEST_CHECK(decoded.object_id == command.object_id);
  FLUME_TEST_CHECK(decoded.storage_offset == command.storage_offset);
  FLUME_TEST_CHECK(decoded.length == command.length);
  FLUME_TEST_CHECK(decoded.npu_address == command.npu_address);
  FLUME_TEST_CHECK(decoded.npu_rkey == command.npu_rkey);
  wire[0] = 0;
  FLUME_TEST_CHECK(!flume::roce::DecodeCommand(wire.data(), wire.size(), &decoded));

  flume::roce::Completion completion;
  completion.request_id = 42;
  completion.bytes = command.length;
  completion.checksum = flume::roce::Checksum(wire.data(), wire.size());
  FLUME_TEST_CHECK(flume::roce::EncodeCompletion(completion, &wire));
  FLUME_TEST_CHECK(wire.size() == flume::roce::kCompletionWireBytes);
  flume::roce::Completion decoded_completion;
  FLUME_TEST_CHECK(flume::roce::DecodeCompletion(wire.data(), wire.size(), &decoded_completion));
  FLUME_TEST_CHECK(decoded_completion.request_id == completion.request_id);
  FLUME_TEST_CHECK(decoded_completion.bytes == completion.bytes);
  FLUME_TEST_CHECK(decoded_completion.checksum == completion.checksum);

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
  FLUME_TEST_CHECK(!flume::roce::NativeTransportCompiled());
  std::cout << "roce storage protocol test passed\n";
  return 0;
}
