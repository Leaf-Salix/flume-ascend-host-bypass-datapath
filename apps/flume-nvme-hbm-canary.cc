#include <acl/acl.h>

#if !defined(__linux__)
#error flume-nvme-hbm-canary requires Linux
#endif

#include <fcntl.h>
#include <linux/fs.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "nvme_storage/nvme_hbm_canary.h"

namespace {

constexpr int kExitFailed = 1;
constexpr int kExitUsage = 2;
constexpr int kExitUnsupported = 4;

void Usage(const char* program) {
  std::cout
      << "usage: " << program
      << " --namespace <nvme-namespace-block-device> --device <logical-id>"
         " [--direction read|write-roundtrip] [--slba N] [--bytes N]"
         " [--timeout-ms N] [--confirm-scratch-namespace]\n"
         "\n"
         "read is non-destructive and submits NVMe READ directly to NPU HBM.\n"
         "write-roundtrip overwrites the selected range temporarily and is"
         " refused unless the namespace is unmounted and"
         " --confirm-scratch-namespace is present. Use only a dedicated"
         " scratch namespace.\n";
}

bool ParseUnsigned(const std::string& text, uint64_t* value) {
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

uint32_t Checksum(const uint8_t* data, size_t length) {
  uint32_t hash = 2166136261U;
  for (size_t index = 0; index < length; ++index) {
    hash ^= data[index];
    hash *= 16777619U;
  }
  return hash;
}

bool DeviceIsMounted(const struct stat& info) {
  std::ifstream mounts("/proc/self/mountinfo");
  if (!mounts) return true;
  const std::string device = std::to_string(major(info.st_rdev)) + ":" +
      std::to_string(minor(info.st_rdev));
  std::string line;
  while (std::getline(mounts, line)) {
    std::istringstream fields(line);
    std::string mount_id;
    std::string parent_id;
    std::string major_minor;
    if (fields >> mount_id >> parent_id >> major_minor &&
        major_minor == device) {
      return true;
    }
  }
  return false;
}

std::string NamespaceTransport(const std::string& namespace_path) {
  std::error_code error;
  const std::filesystem::path resolved =
      std::filesystem::canonical(namespace_path, error);
  if (error) return {};
  const std::string name = resolved.filename().string();
  if (name.rfind("nvme", 0) != 0) return {};
  size_t end = 4;
  while (end < name.size() && std::isdigit(static_cast<unsigned char>(name[end]))) {
    ++end;
  }
  if (end == 4) return {};
  size_t suffix = end;
  if (suffix < name.size() && name[suffix] == 'c') {
    ++suffix;
    const size_t controller_id = suffix;
    while (suffix < name.size() &&
           std::isdigit(static_cast<unsigned char>(name[suffix]))) {
      ++suffix;
    }
    if (suffix == controller_id) return {};
  }
  if (suffix >= name.size() || name[suffix++] != 'n') return {};
  const size_t namespace_id = suffix;
  while (suffix < name.size() &&
         std::isdigit(static_cast<unsigned char>(name[suffix]))) {
    ++suffix;
  }
  if (suffix == namespace_id || suffix != name.size()) return {};
  std::ifstream input(std::filesystem::path("/sys/class/nvme") /
                      name.substr(0, end) / "transport");
  std::string transport;
  input >> transport;
  return transport;
}

bool ReadExact(int fd, uint64_t offset, std::vector<uint8_t>* output,
               std::string* error) {
  size_t done = 0;
  while (done < output->size()) {
    const ssize_t result = pread(fd, output->data() + done,
                                 output->size() - done,
                                 static_cast<off_t>(offset + done));
    if (result <= 0) {
      if (error != nullptr) {
        *error = std::string("scratch backup read failed: ") +
            (result == 0 ? "unexpected EOF" : std::strerror(errno));
      }
      return false;
    }
    done += static_cast<size_t>(result);
  }
  return true;
}

bool RestoreExact(int fd, uint64_t offset, const std::vector<uint8_t>& input,
                  std::string* error) {
  size_t done = 0;
  while (done < input.size()) {
    const ssize_t result = pwrite(fd, input.data() + done,
                                  input.size() - done,
                                  static_cast<off_t>(offset + done));
    if (result <= 0) {
      if (error != nullptr) {
        *error = std::string("scratch restore write failed: ") +
            (result == 0 ? "zero-byte write" : std::strerror(errno));
      }
      return false;
    }
    done += static_cast<size_t>(result);
  }
  if (fsync(fd) != 0) {
    if (error != nullptr) {
      *error = std::string("scratch restore fsync failed: ") +
          std::strerror(errno);
    }
    return false;
  }
  return true;
}

int SubmitNvme(int fd, const flume::nvme::IoPlan& plan, void* hbm,
               int* saved_errno) {
  nvme_passthru_cmd command{};
  command.opcode = plan.opcode;
  command.nsid = plan.nsid;
  command.addr = reinterpret_cast<uint64_t>(hbm);
  command.data_len = plan.data_len;
  command.cdw10 = plan.cdw10;
  command.cdw11 = plan.cdw11;
  command.cdw12 = plan.cdw12;
  command.timeout_ms = plan.timeout_ms;
  errno = 0;
  const int result = ioctl(fd, NVME_IOCTL_IO_CMD, &command);
  *saved_errno = errno;
  return result;
}

int ReportIoFailure(const char* phase, int result, int saved_errno) {
  const bool unsupported = result < 0 &&
      (saved_errno == EFAULT || saved_errno == ENOTTY ||
       saved_errno == EOPNOTSUPP || saved_errno == ENOSYS);
  std::cerr << "native_nvme_hbm_canary="
            << (unsupported ? "unsupported" : "failed")
            << " step=" << phase << " ioctl_result=" << result
            << " errno=" << saved_errno << " detail=\""
            << (result < 0 ? std::strerror(saved_errno) :
                              "NVMe command returned target status")
            << "\" fallback=none";
  if (saved_errno == EFAULT) {
    std::cerr << " likely_reason=hbm-pointer-rejected-by-kernel";
  }
  std::cerr << "\n";
  return unsupported ? kExitUnsupported : kExitFailed;
}

}  // namespace

int main(int argc, char** argv) {
  flume::nvme::CanaryConfig config;
  uint64_t logical_device = 0;
  bool device_provided = false;
  bool parse_ok = true;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    auto next = [&](uint64_t* output) {
      return index + 1 < argc && ParseUnsigned(argv[++index], output);
    };
    if (arg == "--namespace" && index + 1 < argc) {
      config.namespace_path = argv[++index];
    } else if (arg == "--device") {
      const bool ok = next(&logical_device);
      device_provided = ok;
      parse_ok = parse_ok && ok;
    } else if (arg == "--slba") {
      const bool ok = next(&config.slba);
      parse_ok = parse_ok && ok;
    } else if (arg == "--bytes") {
      const bool ok = next(&config.bytes);
      parse_ok = parse_ok && ok;
    } else if (arg == "--timeout-ms") {
      uint64_t timeout = 0;
      if (!next(&timeout) || timeout > std::numeric_limits<uint32_t>::max()) {
        parse_ok = false;
      } else {
        config.timeout_ms = static_cast<uint32_t>(timeout);
      }
    } else if (arg == "--direction" && index + 1 < argc) {
      const std::string direction = argv[++index];
      if (direction == "read") {
        config.direction = flume::nvme::CanaryDirection::kReadToHbm;
      } else if (direction == "write-roundtrip") {
        config.direction = flume::nvme::CanaryDirection::kWriteRoundTrip;
      } else {
        Usage(argv[0]);
        return kExitUsage;
      }
    } else if (arg == "--confirm-scratch-namespace") {
      config.destructive_write_confirmed = true;
    } else if (arg == "--help") {
      Usage(argv[0]);
      return 0;
    } else {
      Usage(argv[0]);
      return kExitUsage;
    }
  }
  if (!parse_ok || !device_provided ||
      logical_device > std::numeric_limits<int32_t>::max() ||
      config.namespace_path.empty()) {
    Usage(argv[0]);
    return kExitUsage;
  }

  const int open_flags = config.direction ==
      flume::nvme::CanaryDirection::kReadToHbm ? O_RDONLY : O_RDWR | O_EXCL;
  const int fd = open(config.namespace_path.c_str(), open_flags | O_CLOEXEC);
  if (fd < 0) {
    const bool unsupported = errno == ENOENT || errno == ENODEV || errno == ENXIO;
    std::cerr << "native_nvme_hbm_canary="
              << (unsupported ? "unsupported" : "failed")
              << " step=namespace-open errno=" << errno
              << " detail=\"" << std::strerror(errno)
              << "\" fallback=none\n";
    return unsupported ? kExitUnsupported : kExitFailed;
  }

  struct stat info{};
  uint32_t lba_bytes = 0;
  uint64_t namespace_bytes = 0;
  const int nsid_result = ioctl(fd, NVME_IOCTL_ID);
  if (fstat(fd, &info) != 0 || !S_ISBLK(info.st_mode) || nsid_result <= 0 ||
      ioctl(fd, BLKSSZGET, &lba_bytes) != 0 ||
      ioctl(fd, BLKGETSIZE64, &namespace_bytes) != 0) {
    std::cerr << "native_nvme_hbm_canary=unsupported step=namespace-qualify "
                 "reason=path-is-not-an-nvme-namespace-block-device fallback=none\n";
    close(fd);
    return kExitUnsupported;
  }
  const std::string nvme_transport = NamespaceTransport(config.namespace_path);
  if (nvme_transport != "rdma") {
    std::cerr << "native_nvme_hbm_canary=unsupported step=transport-qualify "
                 "required_transport=rdma observed_transport="
              << (nvme_transport.empty() ? "unknown" : nvme_transport)
              << " fallback=none\n";
    close(fd);
    return kExitUnsupported;
  }
  if (config.direction == flume::nvme::CanaryDirection::kWriteRoundTrip &&
      DeviceIsMounted(info)) {
    std::cerr << "native_nvme_hbm_canary=failed step=write-safety "
                 "reason=namespace-is-mounted fallback=none\n";
    close(fd);
    return kExitFailed;
  }
  config.lba_bytes = lba_bytes;
  config.namespace_bytes = namespace_bytes;
  flume::nvme::IoPlan plan;
  std::string error;
  if (!flume::nvme::BuildCanaryPlan(
          config, static_cast<uint32_t>(nsid_result), &plan, &error)) {
    std::cerr << "native_nvme_hbm_canary=failed step=plan detail=\""
              << error << "\" fallback=none\n";
    close(fd);
    return kExitFailed;
  }

  bool acl_initialized = false;
  bool device_set = false;
  void* payload_hbm = nullptr;
  void* readback_hbm = nullptr;
  int exit_code = kExitFailed;
  std::vector<uint8_t> host(config.bytes);
  std::vector<uint8_t> scratch_backup;
  bool scratch_modified = false;

  if (aclInit(nullptr) != ACL_SUCCESS) {
    std::cerr << "native_nvme_hbm_canary=failed step=acl-init\n";
    goto cleanup;
  }
  acl_initialized = true;
  if (aclrtSetDevice(static_cast<int32_t>(logical_device)) != ACL_SUCCESS) {
    std::cerr << "native_nvme_hbm_canary=failed step=device-select\n";
    goto cleanup;
  }
  device_set = true;
  if (aclrtMalloc(&payload_hbm, config.bytes, ACL_MEM_MALLOC_HUGE_FIRST) !=
      ACL_SUCCESS) {
    std::cerr << "native_nvme_hbm_canary=failed step=hbm-allocate\n";
    goto cleanup;
  }

  if (config.direction == flume::nvme::CanaryDirection::kWriteRoundTrip) {
    scratch_backup.resize(config.bytes);
    if (!ReadExact(fd, plan.byte_offset, &scratch_backup, &error)) {
      std::cerr << "native_nvme_hbm_canary=failed step=scratch-backup detail=\""
                << error << "\"\n";
      goto cleanup;
    }
    std::fill(host.begin(), host.end(), static_cast<uint8_t>(0xa5));
    if (aclrtMemset(payload_hbm, config.bytes, 0xa5, config.bytes) !=
        ACL_SUCCESS) {
      std::cerr << "native_nvme_hbm_canary=failed step=prepare-write-pattern\n";
      goto cleanup;
    }
  }

  {
    int saved_errno = 0;
    const int result = SubmitNvme(fd, plan, payload_hbm, &saved_errno);
    if (result != 0) {
      exit_code = ReportIoFailure(
          config.direction == flume::nvme::CanaryDirection::kReadToHbm ?
              "nvme-read-to-hbm" : "nvme-write-from-hbm",
          result, saved_errno);
      goto cleanup;
    }
    scratch_modified = config.direction ==
        flume::nvme::CanaryDirection::kWriteRoundTrip;
  }

  if (config.direction == flume::nvme::CanaryDirection::kWriteRoundTrip) {
    if (aclrtMalloc(&readback_hbm, config.bytes, ACL_MEM_MALLOC_HUGE_FIRST) !=
        ACL_SUCCESS) {
      std::cerr << "native_nvme_hbm_canary=failed step=readback-hbm-allocate\n";
      goto cleanup;
    }
    flume::nvme::IoPlan readback = plan;
    readback.opcode = 0x02;
    int saved_errno = 0;
    const int result = SubmitNvme(fd, readback, readback_hbm, &saved_errno);
    if (result != 0) {
      exit_code = ReportIoFailure("nvme-readback-to-hbm", result, saved_errno);
      goto cleanup;
    }
    std::vector<uint8_t> actual(config.bytes);
    if (aclrtMemcpy(actual.data(), actual.size(), readback_hbm, config.bytes,
                    ACL_MEMCPY_DEVICE_TO_HOST) != ACL_SUCCESS || actual != host) {
      std::cerr << "native_nvme_hbm_canary=failed step=roundtrip-checksum\n";
      goto cleanup;
    }
  } else {
    if (aclrtMemcpy(host.data(), host.size(), payload_hbm, config.bytes,
                    ACL_MEMCPY_DEVICE_TO_HOST) != ACL_SUCCESS) {
      std::cerr << "native_nvme_hbm_canary=failed step=verification-d2h\n";
      goto cleanup;
    }
  }

  exit_code = 0;

cleanup:
  if (scratch_modified) {
    std::string restore_error;
    if (!RestoreExact(fd, plan.byte_offset, scratch_backup, &restore_error)) {
      std::cerr << "native_nvme_hbm_canary=failed step=scratch-restore detail=\""
                << restore_error << "\" manual_recovery_required=yes\n";
      exit_code = kExitFailed;
    } else {
      scratch_modified = false;
    }
  }
  if (readback_hbm != nullptr) (void)aclrtFree(readback_hbm);
  if (payload_hbm != nullptr) (void)aclrtFree(payload_hbm);
  if (device_set) (void)aclrtResetDevice(static_cast<int32_t>(logical_device));
  if (acl_initialized) (void)aclFinalize();
  close(fd);

  if (exit_code == 0) {
    std::cout << "native_nvme_hbm_canary=passed direction="
              << flume::nvme::CanaryDirectionName(config.direction)
              << " bytes=" << config.bytes << " lba_bytes=" << config.lba_bytes
              << " checksum=" << Checksum(host.data(), host.size())
              << " nvme_transport=rdma nvme_ioctl=passed "
                 "hbm_pointer=direct-argument "
                 "flume_nvme_io_host_staging_bytes=0 "
                 "kernel_bounce_buffer=unverified fallback=none";
    if (config.direction == flume::nvme::CanaryDirection::kWriteRoundTrip) {
      std::cout << " scratch_restored=yes"
                   " test_pattern_init=aclrtMemset"
                << " safety_backup_restore_host_bytes=" << config.bytes * 2
                << " verification_d2h_bytes=" << config.bytes
                << " payload_path=npu-hbm->nvme-namespace->npu-hbm";
    } else {
      std::cout << " verification_d2h_bytes=" << config.bytes
                << " payload_path=nvme-namespace->npu-hbm";
    }
    std::cout << "\n";
  }
  return exit_code;
}
