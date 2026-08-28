#include <cstdlib>
#include <iostream>
#include <string>

#include "roce_storage/cann_ra_abi.h"
#include "roce_storage/cann_ra_loader.h"

#define FLUME_TEST_CHECK(expr)                                                   \
  do {                                                                           \
    if (!(expr)) {                                                               \
      std::cerr << "check failed at line " << __LINE__ << ": " #expr << "\n"; \
      return 1;                                                                  \
    }                                                                            \
  } while (0)

int main(int argc, char** argv) {
  FLUME_TEST_CHECK(sizeof(flume::roce::cann::TypicalQp) == 184);
  FLUME_TEST_CHECK(sizeof(flume::roce::cann::MrInfo) == 32);
#if defined(__unix__) || defined(__APPLE__)
  if (argc == 4 || argc == 5) {
    const bool expect_legacy = argc == 5 && std::string(argv[4]) == "legacy";
    setenv("FLUME_CANN_RA_LIBRARY", argv[1], 1);
    setenv("FLUME_CANN_RUNTIME_LIBRARY", argv[2], 1);
    setenv("FLUME_CANN_ACL_LIBRARY", argv[3], 1);
    flume::roce::CannRaApi core_api;
    std::string error;
    FLUME_TEST_CHECK(core_api.Open(false, &error));
    FLUME_TEST_CHECK(core_api.available());
    FLUME_TEST_CHECK(!core_api.command_posting_available());
    FLUME_TEST_CHECK(
        core_api.symbol_profile() ==
        (expect_legacy
             ? flume::roce::CannRaSymbolProfile::kLegacyLowercase
             : flume::roce::CannRaSymbolProfile::kModern));
    FLUME_TEST_CHECK(
        core_api.rdev_init_profile() ==
        (expect_legacy ? flume::roce::CannRaRdevInitProfile::kLegacy
                       : flume::roce::CannRaRdevInitProfile::kV2));
    flume::roce::cann::RdevInitInfo init_info{};
    flume::roce::cann::Rdev rdev{};
    void* rdev_handle = nullptr;
    FLUME_TEST_CHECK(core_api.RdevInit(init_info, rdev, &rdev_handle) == 0);
    FLUME_TEST_CHECK(rdev_handle != nullptr);
    uint32_t physical_device = UINT32_MAX;
    std::string resolve_error;
    if (expect_legacy) {
      FLUME_TEST_CHECK(!core_api.ResolvePhysicalDevice(
          2, -1, &physical_device, &resolve_error));
      FLUME_TEST_CHECK(resolve_error.find("explicit physical device") !=
                       std::string::npos);
      FLUME_TEST_CHECK(core_api.ResolvePhysicalDevice(
          2, 7, &physical_device, &resolve_error));
      FLUME_TEST_CHECK(physical_device == 7);
    } else {
      FLUME_TEST_CHECK(core_api.ResolvePhysicalDevice(
          2, -1, &physical_device, &resolve_error));
      FLUME_TEST_CHECK(physical_device == 2);
    }
    core_api.Close();
    FLUME_TEST_CHECK(!core_api.Open(true, &error));
    FLUME_TEST_CHECK(error.find("RaTypicalSendWr") != std::string::npos);
    return 0;
  }
  setenv("FLUME_CANN_RA_LIBRARY", "/flume/does-not-exist/libra.so", 1);
  flume::roce::CannRaApi api;
  std::string error;
  FLUME_TEST_CHECK(!api.Open(&error));
  FLUME_TEST_CHECK(error.find("failed to load") != std::string::npos);
  FLUME_TEST_CHECK(!api.available());
  unsetenv("FLUME_CANN_RA_LIBRARY");
#endif
  return 0;
}
