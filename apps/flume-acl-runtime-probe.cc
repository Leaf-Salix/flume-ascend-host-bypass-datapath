#include <acl/acl.h>

#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::vector<int32_t> ParseDevices(const std::string& text) {
  std::vector<int32_t> devices;
  std::stringstream stream(text);
  std::string item;
  while (std::getline(stream, item, ',')) {
    if (item.empty()) {
      continue;
    }
    size_t parsed = 0;
    int value = std::stoi(item, &parsed);
    if (parsed != item.size() || value < 0) {
      throw std::invalid_argument("invalid device id: " + item);
    }
    devices.push_back(static_cast<int32_t>(value));
  }
  return devices;
}

std::string JoinDevices(const std::vector<int32_t>& devices) {
  std::string out;
  for (size_t i = 0; i < devices.size(); ++i) {
    if (i != 0) {
      out += ",";
    }
    out += std::to_string(devices[i]);
  }
  return out.empty() ? "all" : out;
}

}  // namespace

int main(int argc, char** argv) {
  std::vector<int32_t> requested_devices;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--devices=", 0) == 0) {
      try {
        requested_devices = ParseDevices(arg.substr(std::string("--devices=").size()));
      } catch (const std::exception& exc) {
        std::cerr << "acl_runtime_probe=failed step=parse-devices error=\""
                  << exc.what() << "\"\n";
        return 2;
      }
    } else if (arg == "--help") {
      std::cout << "usage: flume-acl-runtime-probe [--devices=<ids>]\n";
      return 0;
    } else {
      std::cerr << "acl_runtime_probe=failed step=parse-args error=\"unknown "
                << "argument " << arg << "\"\n";
      return 2;
    }
  }

  aclError ret = aclInit(nullptr);
  if (ret != ACL_SUCCESS) {
    std::cerr << "acl_runtime_probe=failed step=aclInit ret="
              << static_cast<int>(ret) << "\n";
    return 1;
  }

  uint32_t device_count = 0;
  ret = aclrtGetDeviceCount(&device_count);
  if (ret != ACL_SUCCESS || device_count == 0) {
    std::cerr << "acl_runtime_probe=failed step=aclrtGetDeviceCount ret="
              << static_cast<int>(ret)
              << " device_count=" << device_count << "\n";
    (void)aclFinalize();
    return 1;
  }

  std::vector<int32_t> devices_to_check = requested_devices;
  if (devices_to_check.empty()) {
    for (uint32_t i = 0; i < device_count; ++i) {
      devices_to_check.push_back(static_cast<int32_t>(i));
    }
  }

  for (int32_t device : devices_to_check) {
    if (device < 0 || static_cast<uint32_t>(device) >= device_count) {
      std::cerr << "acl_runtime_probe=failed step=device-range device="
                << device << " device_count=" << device_count << "\n";
      (void)aclFinalize();
      return 1;
    }
    ret = aclrtSetDevice(device);
    if (ret != ACL_SUCCESS) {
      std::cerr << "acl_runtime_probe=failed step=aclrtSetDevice device="
                << device << " ret=" << static_cast<int>(ret) << "\n";
      (void)aclFinalize();
      return 1;
    }
    ret = aclrtResetDevice(device);
    if (ret != ACL_SUCCESS) {
      std::cerr << "acl_runtime_probe=failed step=aclrtResetDevice device="
                << device << " ret=" << static_cast<int>(ret) << "\n";
      (void)aclFinalize();
      return 1;
    }
  }

  ret = aclFinalize();
  if (ret != ACL_SUCCESS) {
    std::cerr << "acl_runtime_probe=failed step=aclFinalize ret="
              << static_cast<int>(ret) << "\n";
    return 1;
  }

  std::cout << "acl_runtime_probe=passed device_count=" << device_count
            << " checked_devices=" << JoinDevices(devices_to_check) << "\n";
  return 0;
}
