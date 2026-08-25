#include <acl/acl.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "roce_storage/host_ra_session.h"
#include "roce_storage/roce_storage.h"

namespace {

void Usage(const char* name) {
  std::cerr << "usage: " << name
            << " --storage-server <host> --npu-rnic-ip <hccn-ip>"
            << " --device <logical-id> --control-port <port>"
            << " [--control-mode tcp|npu-ra] [--bytes N] [--offset N]"
            << " [--gid-index N] [--timeout-ms N]\n";
}

bool ParseControlMode(const std::string& value,
                      flume::roce::ControlMode* mode) {
  if (value == "tcp") {
    *mode = flume::roce::ControlMode::kTcp;
    return true;
  }
  if (value == "npu-ra") {
    *mode = flume::roce::ControlMode::kNpuRa;
    return true;
  }
  return false;
}

}  // namespace

int main(int argc, char** argv) {
  std::string storage_server;
  std::string npu_rnic_ip;
  uint32_t device = 0;
  uint32_t control_port = 0;
  uint32_t gid_index = 0;
  uint32_t timeout_ms = 30000;
  size_t bytes = 4096;
  uint64_t offset = 0;
  flume::roce::ControlMode control_mode = flume::roce::ControlMode::kTcp;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--storage-server" && index + 1 < argc) storage_server = argv[++index];
    else if (arg == "--npu-rnic-ip" && index + 1 < argc) npu_rnic_ip = argv[++index];
    else if (arg == "--device" && index + 1 < argc) device = std::stoul(argv[++index]);
    else if (arg == "--control-port" && index + 1 < argc) control_port = std::stoul(argv[++index]);
    else if (arg == "--gid-index" && index + 1 < argc) gid_index = std::stoul(argv[++index]);
    else if (arg == "--timeout-ms" && index + 1 < argc) timeout_ms = std::stoul(argv[++index]);
    else if (arg == "--bytes" && index + 1 < argc) bytes = std::stoull(argv[++index]);
    else if (arg == "--offset" && index + 1 < argc) offset = std::stoull(argv[++index]);
    else if (arg == "--control-mode" && index + 1 < argc &&
             ParseControlMode(argv[index + 1], &control_mode)) {
      ++index;
    } else {
      Usage(argv[0]);
      return arg == "--help" ? 0 : 2;
    }
  }
  if (storage_server.empty() || npu_rnic_ip.empty() || control_port == 0 ||
      bytes == 0) {
    Usage(argv[0]);
    return 2;
  }

  void* hbm = nullptr;
  aclrtStream stream = nullptr;
  bool acl_initialized = false;
  bool device_set = false;
  int exit_code = 1;
  flume::roce::HostRaSession session;
  std::string error;
  if (aclInit(nullptr) != ACL_SUCCESS) {
    std::cerr << "roce_hbm_write_smoke=failed step=acl-init\n";
    goto cleanup;
  }
  acl_initialized = true;
  if (aclrtSetDevice(static_cast<int32_t>(device)) != ACL_SUCCESS) {
    std::cerr << "roce_hbm_write_smoke=failed step=device-select\n";
    goto cleanup;
  }
  device_set = true;
  if (aclrtCreateStream(&stream) != ACL_SUCCESS ||
      aclrtMalloc(&hbm, bytes, ACL_MEM_MALLOC_HUGE_FIRST) != ACL_SUCCESS) {
    std::cerr << "roce_hbm_write_smoke=failed step=hbm-allocate\n";
    goto cleanup;
  }
  {
    flume::roce::HostRaConfig config;
    config.storage_server = storage_server;
    config.npu_rnic_ip = npu_rnic_ip;
    config.logical_device = device;
    config.gid_index = gid_index;
    config.bootstrap_port = control_port;
    config.timeout_ms = timeout_ms;
    config.control_mode = control_mode;
    if (!session.Open(config, &error)) {
      std::cerr << "roce_hbm_write_smoke="
                << (session.capability_available() ? "failed" : "unsupported")
                << " step="
                << (session.capability_available() ? "host-ra-open" : "capability-load")
                << " fallback=none detail=\"" << error << "\"\n";
      goto cleanup;
    }
  }
  {
    flume::roce::HostRaResult result;
    if (!session.SubmitAndWait(flume::roce::Operation::kRead, 1, 0, offset,
                               hbm, bytes, stream, &result, &error)) {
      std::cerr << "roce_hbm_write_smoke=failed step=rdma-request detail=\""
                << error << "\"\n";
      goto cleanup;
    }
    std::vector<uint8_t> host(bytes);
    if (aclrtMemcpy(host.data(), host.size(), hbm, bytes,
                    ACL_MEMCPY_DEVICE_TO_HOST) != ACL_SUCCESS) {
      std::cerr << "roce_hbm_write_smoke=failed step=verification-d2h\n";
      goto cleanup;
    }
    const uint32_t checksum = flume::roce::Checksum(host.data(), host.size());
    if (checksum != result.checksum) {
      std::cerr << "roce_hbm_write_smoke=failed step=checksum expected="
                << result.checksum << " actual=" << checksum << "\n";
      goto cleanup;
    }
    std::cout << "roce_hbm_write_smoke=passed bytes=" << result.bytes
              << " checksum=matched verification_d2h_bytes=" << bytes << " "
              << result.marker << "\n";
    exit_code = 0;
  }

cleanup:
  session.Close(nullptr);
  if (hbm != nullptr) aclrtFree(hbm);
  if (stream != nullptr) aclrtDestroyStream(stream);
  if (device_set) aclrtResetDevice(static_cast<int32_t>(device));
  if (acl_initialized) aclFinalize();
  return exit_code;
}
