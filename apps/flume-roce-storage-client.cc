#include <acl/acl.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "flume/flume.h"
#include "roce_storage/roce_storage.h"

namespace {

void Usage(const char* name) {
  std::cerr << "usage: " << name
            << " --agent-endpoint <host:port> --storage-server <host>"
            << " --npu-rnic-ip <hccn-ip> --device <logical-id>"
            << " --control-port <port> [--bytes N] [--offset N]\n";
}

}  // namespace

int main(int argc, char** argv) {
  std::string agent_endpoint;
  std::string storage_server;
  std::string npu_rnic_ip;
  uint32_t device = 0;
  uint32_t control_port = 0;
  size_t bytes = 4U * 1024U * 1024U;
  uint64_t offset = 0;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--agent-endpoint" && index + 1 < argc) agent_endpoint = argv[++index];
    else if (arg == "--storage-server" && index + 1 < argc) storage_server = argv[++index];
    else if (arg == "--npu-rnic-ip" && index + 1 < argc) npu_rnic_ip = argv[++index];
    else if (arg == "--device" && index + 1 < argc) device = std::stoul(argv[++index]);
    else if (arg == "--control-port" && index + 1 < argc) control_port = std::stoul(argv[++index]);
    else if (arg == "--bytes" && index + 1 < argc) bytes = std::stoull(argv[++index]);
    else if (arg == "--offset" && index + 1 < argc) offset = std::stoull(argv[++index]);
    else { Usage(argv[0]); return arg == "--help" ? 0 : 2; }
  }
  if (agent_endpoint.empty() || storage_server.empty() || npu_rnic_ip.empty() ||
      control_port == 0 || bytes == 0) {
    Usage(argv[0]);
    return 2;
  }

  flume_client_t* client = nullptr;
  flume_buffer_t* buffer = nullptr;
  flume_roce_storage_session_t* session = nullptr;
  flume_io_t* io = nullptr;
  void* hbm = nullptr;
  aclrtStream stream = nullptr;
  int exit_code = 1;
  if (aclInit(nullptr) != ACL_SUCCESS || aclrtSetDevice(static_cast<int32_t>(device)) != ACL_SUCCESS ||
      aclrtCreateStream(&stream) != ACL_SUCCESS ||
      aclrtMalloc(&hbm, bytes, ACL_MEM_MALLOC_HUGE_FIRST) != ACL_SUCCESS) {
    std::cerr << "roce_storage_smoke=failed step=acl-setup\n";
    goto cleanup;
  }
  if (flume_client_open(agent_endpoint.c_str(), &client) != FLUME_OK ||
      flume_register_buffer(client, hbm, bytes, FLUME_BUFFER_ASCEND_HBM, &buffer) != FLUME_OK) {
    std::cerr << "roce_storage_smoke=failed step=flume-client-or-buffer\n";
    goto cleanup;
  }
  {
    flume_roce_storage_options_t options{};
    options.size = sizeof(options);
    options.storage_server = storage_server.c_str();
    options.npu_rnic_ip = npu_rnic_ip.c_str();
    options.storage_rnic_ip = storage_server.c_str();
    options.npu_device = device;
    options.bootstrap_port = control_port;
    options.timeout_ms = 30000;
    options.post_mode = FLUME_ROCE_POST_HOST_RA;
    options.storage_backend = FLUME_ROCE_STORAGE_POSIX;
    options.require_compute_host_bypass = 1;
    if (flume_roce_storage_session_open(client, &options, &session) != FLUME_OK ||
        flume_roce_storage_read_async(session, 0, offset, buffer, 0, bytes,
                                      stream, &io) != FLUME_OK) {
      std::cerr << "roce_storage_smoke=failed step=session-or-submit\n";
      goto cleanup;
    }
  }
  {
    const int wait_status = flume_wait(io, 60000);
    if (wait_status != FLUME_OK) {
      std::cerr << "roce_storage_smoke=failed step=wait status="
                << flume_status_string(wait_status) << " detail=\""
                << flume_io_error_message(io) << "\"\n";
      goto cleanup;
    }
    std::vector<uint8_t> host(bytes);
    if (aclrtMemcpy(host.data(), host.size(), hbm, bytes,
                    ACL_MEMCPY_DEVICE_TO_HOST) != ACL_SUCCESS) {
      std::cerr << "roce_storage_smoke=failed step=verification-d2h\n";
      goto cleanup;
    }
    const uint32_t checksum = flume::roce::Checksum(host.data(), host.size());
    if (checksum != flume_io_checksum(io)) {
      std::cerr << "roce_storage_smoke=failed step=checksum expected="
                << flume_io_checksum(io) << " actual=" << checksum << "\n";
      goto cleanup;
    }
    std::cout << "roce_storage_smoke=passed bytes=" << flume_io_bytes(io)
              << " checksum=" << checksum << " " << flume_io_error_message(io) << "\n";
    exit_code = 0;
  }

cleanup:
  if (io != nullptr) flume_io_release(io);
  if (session != nullptr) flume_roce_storage_session_close(session);
  if (buffer != nullptr) flume_buffer_release(buffer);
  if (client != nullptr) flume_client_close(client);
  if (hbm != nullptr) aclrtFree(hbm);
  if (stream != nullptr) aclrtDestroyStream(stream);
  aclrtResetDevice(static_cast<int32_t>(device));
  aclFinalize();
  return exit_code;
}
