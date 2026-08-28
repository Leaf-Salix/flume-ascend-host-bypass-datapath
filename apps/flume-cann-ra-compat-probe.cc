#include <iostream>
#include <string>

#include "roce_storage/cann_ra_loader.h"

int main(int argc, char** argv) {
  bool require_command_posting = false;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--require-command-posting") {
      require_command_posting = true;
    } else if (arg == "--help") {
      std::cout << "usage: flume-cann-ra-compat-probe "
                   "[--require-command-posting]\n";
      return 0;
    } else {
      std::cerr << "unknown argument: " << arg << "\n";
      return 2;
    }
  }

  flume::roce::CannRaApi api;
  std::string error;
  if (!api.Open(require_command_posting, &error)) {
    std::cout << "cann_ra_symbol_probe=unsupported"
              << " cann_ra_compat=unqualified detail=\"" << error
              << "\"\n";
    return 1;
  }
  std::cout << "cann_ra_symbol_probe=passed"
            << " cann_ra_compat=unqualified"
            << " symbol_profile=" << api.symbol_profile_name()
            << " rdev_init=" << api.rdev_init_profile_name()
            << " net_service=" << api.net_service_profile_name()
            << " physical_device_lookup="
            << (api.physical_device_lookup_available() ? "available"
                                                       : "explicit-required")
            << " command_posting="
            << (api.command_posting_available() ? "available" : "not-required")
            << " abi_profile=hccp-reduced-v1"
            << " abi_qualified=no hardware_gate=required\n";
  return 0;
}
