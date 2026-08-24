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

int main() {
  FLUME_TEST_CHECK(sizeof(flume::roce::cann::TypicalQp) == 184);
  FLUME_TEST_CHECK(sizeof(flume::roce::cann::MrInfo) == 32);
#if defined(__unix__) || defined(__APPLE__)
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
