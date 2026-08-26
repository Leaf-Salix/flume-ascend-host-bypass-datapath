#include <cstdint>
#include <iostream>
#include <string>

#include "nvme_storage/nvme_hbm_canary.h"
#include "test_util.h"

int main() {
  flume::nvme::CanaryConfig config;
  config.namespace_path = "/dev/nvme-test";
  config.slba = UINT64_C(0x100000002);
  config.bytes = 8192;
  config.lba_bytes = 4096;
  config.namespace_bytes = UINT64_C(0x200000000) * 4096;
  config.timeout_ms = 1000;

  flume::nvme::IoPlan plan;
  std::string error;
  FLUME_TEST_CHECK(flume::nvme::BuildCanaryPlan(config, 7, &plan, &error));
  FLUME_TEST_CHECK(plan.opcode == 0x02);
  FLUME_TEST_CHECK(plan.nsid == 7U);
  FLUME_TEST_CHECK(plan.cdw10 == 2U);
  FLUME_TEST_CHECK(plan.cdw11 == 1U);
  FLUME_TEST_CHECK(plan.cdw12 == 1U);
  FLUME_TEST_CHECK(plan.block_count == 2U);

  config.direction = flume::nvme::CanaryDirection::kWriteRoundTrip;
  FLUME_TEST_CHECK(!flume::nvme::BuildCanaryPlan(config, 7, &plan, &error));
  FLUME_TEST_CHECK(error.find("confirmation") != std::string::npos);
  config.destructive_write_confirmed = true;
  FLUME_TEST_CHECK(flume::nvme::BuildCanaryPlan(config, 7, &plan, &error));
  FLUME_TEST_CHECK(plan.opcode == 0x01);

  config.slba = 0;
  FLUME_TEST_CHECK(!flume::nvme::BuildCanaryPlan(config, 7, &plan, &error));
  FLUME_TEST_CHECK(error.find("first 1 MiB") != std::string::npos);
  config.slba = UINT64_C(0x100000002);

  config.bytes = 4097;
  FLUME_TEST_CHECK(!flume::nvme::BuildCanaryPlan(config, 7, &plan, &error));
  config.bytes = 4096;
  config.slba = config.namespace_bytes / config.lba_bytes;
  FLUME_TEST_CHECK(!flume::nvme::BuildCanaryPlan(config, 7, &plan, &error));
  config.slba = 0;
  config.lba_bytes = 3000;
  FLUME_TEST_CHECK(!flume::nvme::BuildCanaryPlan(config, 7, &plan, &error));

  std::cout << "nvme hbm canary plan tests passed\n";
  return 0;
}
