#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "roce_storage/npu_ra_push_mover.h"
#include "roce_storage/roce_storage.h"

#define FLUME_TEST_CHECK(expr)                                                   \
  do {                                                                           \
    if (!(expr)) {                                                               \
      std::cerr << "check failed at line " << __LINE__ << ": " #expr << "\n"; \
      return 1;                                                                  \
    }                                                                            \
  } while (0)

int main(int argc, char** argv) {
#if defined(__unix__) || defined(__APPLE__)
  FLUME_TEST_CHECK(argc == 4);
  setenv("FLUME_CANN_RA_LIBRARY", argv[1], 1);
  setenv("FLUME_CANN_RUNTIME_LIBRARY", argv[2], 1);
  setenv("FLUME_CANN_ACL_LIBRARY", argv[3], 1);

  flume::roce::Endpoint peer;
  peer.qpn = 303;
  peer.psn = 404;
  peer.port = 1;
  peer.mtu = 5;
  peer.gid[15] = 2;
  flume::roce::NpuRaPushConfig config;
  config.npu_rnic_ip = "127.0.0.1";
  config.timeout_ms = 5;

  flume::roce::NpuRaPushMover mover;
  flume::roce::Endpoint local;
  std::string error;
  FLUME_TEST_CHECK(mover.Open(config, peer, &local, &error));
  FLUME_TEST_CHECK(local.qpn == 101);
  FLUME_TEST_CHECK(local.psn == 202);
  FLUME_TEST_CHECK(mover.available());

  std::vector<uint8_t> source(64, 7);
  flume::roce::MemoryWindow target{0x1000U, source.size(), 0x1234U,
                                    flume::roce::kMemoryRemoteWrite};
  FLUME_TEST_CHECK(mover.Push(source.data(), source.size(), target,
                              reinterpret_cast<void*>(1), &error));

  setenv("FLUME_RA_FIXTURE_POLL_MODE", "error", 1);
  FLUME_TEST_CHECK(!mover.Push(source.data(), source.size(), target,
                               reinterpret_cast<void*>(1), &error));
  FLUME_TEST_CHECK(error.find("RaPollCq failed") != std::string::npos);
  unsetenv("FLUME_RA_FIXTURE_POLL_MODE");
  FLUME_TEST_CHECK(mover.Close(&error));
  FLUME_TEST_CHECK(!mover.available());
#else
  (void)argc;
  (void)argv;
#endif
  return 0;
}
