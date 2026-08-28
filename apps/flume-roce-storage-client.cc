#include <acl/acl.h>

#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "roce_storage/host_ra_session.h"
#include "roce_storage/roce_storage.h"

namespace {

enum class SmokeOperation {
  kSwapIn,
  kSwapOut,
  kRoundTrip,
};

void Usage(const char* name) {
  std::cerr << "usage: " << name
            << " --storage-server <host> --npu-rnic-ip <hccn-ip>"
            << " --device <logical-id> --control-port <port>"
            << " [--physical-device <physical-id>]"
            << " [--operation read|write|roundtrip]"
            << " [--confirm-storage-write]"
            << " [--control-mode tcp|npu-ra] [--bytes N] [--offset N]"
            << " [--transfer-mode push|pull]"
            << " [--gid-index N] [--path-mtu 1024|2048|4096]"
            << " [--timeout-ms N]\n";
}

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

bool ParseTransferMode(const std::string& value,
                       flume::roce::TransferMode* mode) {
  if (value == "push") {
    *mode = flume::roce::TransferMode::kPush;
    return true;
  }
  if (value == "pull") {
    *mode = flume::roce::TransferMode::kPull;
    return true;
  }
  return false;
}

bool ParseOperation(const std::string& value, SmokeOperation* operation) {
  if (value == "read" || value == "swap-in") {
    *operation = SmokeOperation::kSwapIn;
    return true;
  }
  if (value == "write" || value == "swap-out") {
    *operation = SmokeOperation::kSwapOut;
    return true;
  }
  if (value == "roundtrip") {
    *operation = SmokeOperation::kRoundTrip;
    return true;
  }
  return false;
}

bool RunRequest(flume::roce::HostRaSession* session,
                flume::roce::Operation operation, uint64_t request_id,
                uint64_t offset, void* hbm, size_t bytes, aclrtStream stream,
                flume::roce::HostRaResult* result, std::string* error) {
  return session->SubmitAndWait(operation, request_id, 0, offset, hbm, bytes,
                                stream, result, error);
}

const char* OperationName(SmokeOperation operation) {
  switch (operation) {
    case SmokeOperation::kSwapIn:
      return "swap-in";
    case SmokeOperation::kSwapOut:
      return "swap-out";
    case SmokeOperation::kRoundTrip:
      return "roundtrip";
  }
  return "unknown";
}

}  // namespace

int main(int argc, char** argv) {
  std::string storage_server;
  std::string npu_rnic_ip;
  uint64_t parsed_device = 0;
  uint64_t parsed_physical_device = 0;
  uint64_t parsed_control_port = 0;
  uint64_t parsed_gid_index = 0;
  uint64_t parsed_path_mtu = 1024;
  uint64_t parsed_timeout_ms = 30000;
  uint64_t parsed_bytes = 4096;
  uint64_t offset = 0;
  bool parse_ok = true;
  bool storage_write_confirmed = false;
  bool physical_device_set = false;
  SmokeOperation operation = SmokeOperation::kSwapIn;
  flume::roce::ControlMode control_mode = flume::roce::ControlMode::kTcp;
  flume::roce::TransferMode transfer_mode = flume::roce::TransferMode::kPush;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    auto next_u64 = [&](uint64_t* output) {
      return index + 1 < argc && ParseU64(argv[++index], output);
    };
    if (arg == "--storage-server" && index + 1 < argc) {
      storage_server = argv[++index];
    } else if (arg == "--npu-rnic-ip" && index + 1 < argc) {
      npu_rnic_ip = argv[++index];
    } else if (arg == "--device") {
      parse_ok = next_u64(&parsed_device) && parse_ok;
    } else if (arg == "--physical-device") {
      parse_ok = next_u64(&parsed_physical_device) && parse_ok;
      physical_device_set = true;
    } else if (arg == "--control-port") {
      parse_ok = next_u64(&parsed_control_port) && parse_ok;
    } else if (arg == "--gid-index") {
      parse_ok = next_u64(&parsed_gid_index) && parse_ok;
    } else if (arg == "--path-mtu") {
      parse_ok = next_u64(&parsed_path_mtu) && parse_ok;
    } else if (arg == "--timeout-ms") {
      parse_ok = next_u64(&parsed_timeout_ms) && parse_ok;
    } else if (arg == "--bytes") {
      parse_ok = next_u64(&parsed_bytes) && parse_ok;
    } else if (arg == "--offset") {
      parse_ok = next_u64(&offset) && parse_ok;
    } else if (arg == "--operation" && index + 1 < argc) {
      parse_ok = ParseOperation(argv[++index], &operation) && parse_ok;
    } else if (arg == "--confirm-storage-write") {
      storage_write_confirmed = true;
    } else if (arg == "--control-mode" && index + 1 < argc) {
      parse_ok = ParseControlMode(argv[++index], &control_mode) && parse_ok;
    } else if (arg == "--transfer-mode" && index + 1 < argc) {
      parse_ok = ParseTransferMode(argv[++index], &transfer_mode) && parse_ok;
    } else if (arg == "--help") {
      Usage(argv[0]);
      return 0;
    } else {
      parse_ok = false;
    }
  }
  if (!parse_ok || storage_server.empty() || npu_rnic_ip.empty() ||
      parsed_control_port == 0 || parsed_control_port > UINT16_MAX ||
      parsed_device >
          static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ||
      parsed_physical_device >
          static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ||
      parsed_gid_index > UINT8_MAX || parsed_timeout_ms == 0 ||
      parsed_timeout_ms > std::numeric_limits<uint32_t>::max() ||
      parsed_bytes == 0 || parsed_bytes > std::numeric_limits<size_t>::max() ||
      (operation != SmokeOperation::kSwapIn && !storage_write_confirmed)) {
    Usage(argv[0]);
    return 2;
  }

  const uint32_t device = static_cast<uint32_t>(parsed_device);
  const uint32_t control_port = static_cast<uint32_t>(parsed_control_port);
  const uint32_t gid_index = static_cast<uint32_t>(parsed_gid_index);
  uint8_t path_mtu = 0;
  if (parsed_path_mtu > std::numeric_limits<uint32_t>::max() ||
      !flume::roce::PathMtuFromBytes(static_cast<uint32_t>(parsed_path_mtu),
                                     &path_mtu)) {
    Usage(argv[0]);
    return 2;
  }
  const uint32_t timeout_ms = static_cast<uint32_t>(parsed_timeout_ms);
  const size_t bytes = static_cast<size_t>(parsed_bytes);
  void* hbm = nullptr;
  aclrtStream stream = nullptr;
  bool acl_initialized = false;
  bool device_set = false;
  int exit_code = 1;
  flume::roce::HostRaSession session;
  std::string error;
  if (aclInit(nullptr) != ACL_SUCCESS) {
    std::cerr << "flume_swap_smoke=failed step=acl-init\n";
    goto cleanup;
  }
  acl_initialized = true;
  if (aclrtSetDevice(static_cast<int32_t>(device)) != ACL_SUCCESS) {
    std::cerr << "flume_swap_smoke=failed step=device-select\n";
    goto cleanup;
  }
  device_set = true;
  if (aclrtCreateStream(&stream) != ACL_SUCCESS ||
      aclrtMalloc(&hbm, bytes, ACL_MEM_MALLOC_HUGE_FIRST) != ACL_SUCCESS) {
    std::cerr << "flume_swap_smoke=failed step=hbm-allocate\n";
    goto cleanup;
  }
  {
    flume::roce::HostRaConfig config;
    config.storage_server = storage_server;
    config.npu_rnic_ip = npu_rnic_ip;
    config.logical_device = device;
    config.physical_device = physical_device_set
                                 ? static_cast<int32_t>(parsed_physical_device)
                                 : -1;
    config.gid_index = gid_index;
    config.path_mtu = path_mtu;
    config.bootstrap_port = control_port;
    config.timeout_ms = timeout_ms;
    config.control_mode = control_mode;
    config.transfer_mode = transfer_mode;
    if (!session.Open(config, &error)) {
      std::cerr << "flume_swap_smoke="
                << (session.capability_available() ? "failed" : "unsupported")
                << " step="
                << (session.capability_available() ? "host-ra-open" :
                                                     "capability-load")
                << " fallback=none detail=\"" << error << "\"\n";
      goto cleanup;
    }
  }

  {
    const std::vector<uint8_t> expected(bytes, 0xa5);
    flume::roce::HostRaResult swap_out_result;
    flume::roce::HostRaResult swap_in_result;
    uint64_t request_id = 1;
    if (operation != SmokeOperation::kSwapIn) {
      if (aclrtMemset(hbm, bytes, 0xa5, bytes) != ACL_SUCCESS ||
          !RunRequest(&session, flume::roce::Operation::kWrite, request_id++,
                      offset, hbm, bytes, stream, &swap_out_result, &error)) {
        std::cerr << "flume_swap_smoke=failed step=swap-out detail=\""
                  << error << "\"\n";
        goto cleanup;
      }
      const uint32_t expected_checksum =
          flume::roce::Checksum(expected.data(), expected.size());
      if (swap_out_result.checksum != expected_checksum) {
        std::cerr << "flume_swap_smoke=failed step=swap-out-checksum expected="
                  << expected_checksum << " actual="
                  << swap_out_result.checksum << "\n";
        goto cleanup;
      }
    }

    if (operation != SmokeOperation::kSwapOut) {
      if (operation == SmokeOperation::kRoundTrip &&
          aclrtMemset(hbm, bytes, 0, bytes) != ACL_SUCCESS) {
        std::cerr << "flume_swap_smoke=failed step=clear-hbm-before-swap-in\n";
        goto cleanup;
      }
      if (!RunRequest(&session, flume::roce::Operation::kRead, request_id++,
                      offset, hbm, bytes, stream, &swap_in_result, &error)) {
        std::cerr << "flume_swap_smoke=failed step=swap-in detail=\""
                  << error << "\"\n";
        goto cleanup;
      }
      std::vector<uint8_t> actual(bytes);
      if (aclrtMemcpy(actual.data(), actual.size(), hbm, bytes,
                      ACL_MEMCPY_DEVICE_TO_HOST) != ACL_SUCCESS) {
        std::cerr << "flume_swap_smoke=failed step=verification-d2h\n";
        goto cleanup;
      }
      const uint32_t actual_checksum =
          flume::roce::Checksum(actual.data(), actual.size());
      if (actual_checksum != swap_in_result.checksum ||
          (operation == SmokeOperation::kRoundTrip && actual != expected)) {
        std::cerr << "flume_swap_smoke=failed step=swap-in-checksum expected="
                  << swap_in_result.checksum << " actual=" << actual_checksum
                  << "\n";
        goto cleanup;
      }
    }

    if (operation != SmokeOperation::kSwapIn) {
      std::cout << "flume_swap_out_path=passed "
                << swap_out_result.marker << "\n";
    }
    if (operation != SmokeOperation::kSwapOut) {
      std::cout << "flume_swap_in_path=passed "
                << swap_in_result.marker << "\n";
    }
    std::cout << "flume_swap_smoke=passed operation="
              << OperationName(operation) << " bytes=" << bytes
              << " swap_out="
              << (operation == SmokeOperation::kSwapIn ? "not-run" : "passed")
              << " swap_in="
              << (operation == SmokeOperation::kSwapOut ? "not-run" : "passed")
              << " hbm_window=ra-registered compute_host_payload_bytes=0"
                 " storage_server_staging=host-dram fallback=none";
    if (operation != SmokeOperation::kSwapIn) {
      std::cout << " swap_out_checksum=" << swap_out_result.checksum;
    }
    if (operation != SmokeOperation::kSwapOut) {
      std::cout << " swap_in_checksum=" << swap_in_result.checksum
                << " verification_d2h_bytes=" << bytes;
    }
    std::cout << "\n";
    if (operation == SmokeOperation::kSwapIn) {
      std::cout << "roce_hbm_write_smoke=passed bytes="
                << swap_in_result.bytes
                << " checksum=matched verification_d2h_bytes=" << bytes
                << " " << swap_in_result.marker << "\n";
    }
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
