#include "test_util.h"

#include <cmath>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

namespace {

bool NearlyEqual(float lhs, float rhs) {
  return std::fabs(lhs - rhs) < 1e-6F;
}

void OpenRanks(flume::StorageAgent& agent, uint32_t rank_size,
               std::vector<flume_client_t*>* clients) {
  clients->assign(rank_size, nullptr);
  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  for (uint32_t rank = 0; rank < rank_size; ++rank) {
    FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &(*clients)[rank]) == FLUME_OK);
    FLUME_TEST_CHECK(flume_attach_sim_comm((*clients)[rank], "test-world", rank,
                                rank_size) == FLUME_OK);
  }
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  constexpr uint32_t kRanks = 4;
  fs::path root = fs::temp_directory_path() / "flume-test-sim-collectives";
  FlumeTestRemoveAll(root);
  fs::create_directories(root);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "agent.Start failed: " << error << "\n";
    return 1;
  }

  std::vector<flume_client_t*> clients;
  OpenRanks(agent, kRanks, &clients);

  constexpr uint64_t kReduceCount = 4;
  std::vector<flume_buffer_t*> reduce_src(kRanks, nullptr);
  std::vector<flume_buffer_t*> reduce_dst(kRanks, nullptr);
  std::vector<flume_io_t*> reduce_ios(kRanks, nullptr);
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[rank], kReduceCount * sizeof(float),
                                 FLUME_BUFFER_SIM_HBM,
                                 &reduce_src[rank]) == FLUME_OK);
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[rank], kReduceCount * sizeof(float),
                                 FLUME_BUFFER_SIM_HBM,
                                 &reduce_dst[rank]) == FLUME_OK);
    auto* values = static_cast<float*>(flume_buffer_data(reduce_src[rank]));
    for (uint64_t i = 0; i < kReduceCount; ++i) {
      values[i] = static_cast<float>(rank + 1 + i);
    }
    FLUME_TEST_CHECK(flume_allreduce_async(clients[rank], reduce_dst[rank], 0,
                                reduce_src[rank], 0, kReduceCount,
                                FLUME_DTYPE_FP32, FLUME_REDUCE_SUM, nullptr,
                                &reduce_ios[rank]) == FLUME_OK);
  }
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_wait(reduce_ios[rank], 1000) == FLUME_OK);
    FLUME_TEST_CHECK(flume_io_bytes(reduce_ios[rank]) == kReduceCount * sizeof(float));
    auto* values = static_cast<float*>(flume_buffer_data(reduce_dst[rank]));
    for (uint64_t i = 0; i < kReduceCount; ++i) {
      float expected = 10.0F + static_cast<float>(kRanks * i);
      FLUME_TEST_CHECK(NearlyEqual(values[i], expected));
    }
  }

  constexpr uint64_t kGatherCount = 2;
  std::vector<flume_buffer_t*> gather_src(kRanks, nullptr);
  std::vector<flume_buffer_t*> gather_dst(kRanks, nullptr);
  std::vector<flume_io_t*> gather_ios(kRanks, nullptr);
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[rank], kGatherCount * sizeof(float),
                                 FLUME_BUFFER_SIM_HCCL_COMM,
                                 &gather_src[rank]) == FLUME_OK);
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[rank],
                                 kRanks * kGatherCount * sizeof(float),
                                 FLUME_BUFFER_SIM_HBM,
                                 &gather_dst[rank]) == FLUME_OK);
    auto* values = static_cast<float*>(flume_buffer_data(gather_src[rank]));
    values[0] = static_cast<float>(rank * 10 + 1);
    values[1] = static_cast<float>(rank * 10 + 2);
    FLUME_TEST_CHECK(flume_allgather_async(clients[rank], gather_dst[rank], 0,
                                gather_src[rank], 0, kGatherCount,
                                FLUME_DTYPE_FP32, nullptr,
                                &gather_ios[rank]) == FLUME_OK);
  }
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_wait(gather_ios[rank], 1000) == FLUME_OK);
    FLUME_TEST_CHECK(flume_io_bytes(gather_ios[rank]) ==
           kRanks * kGatherCount * sizeof(float));
    auto* values = static_cast<float*>(flume_buffer_data(gather_dst[rank]));
    for (uint32_t peer = 0; peer < kRanks; ++peer) {
      FLUME_TEST_CHECK(NearlyEqual(values[peer * kGatherCount], peer * 10.0F + 1.0F));
      FLUME_TEST_CHECK(NearlyEqual(values[peer * kGatherCount + 1],
                         peer * 10.0F + 2.0F));
    }
  }

  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    flume_io_release(gather_ios[rank]);
    flume_io_release(reduce_ios[rank]);
    flume_buffer_release(gather_dst[rank]);
    flume_buffer_release(gather_src[rank]);
    flume_buffer_release(reduce_dst[rank]);
    flume_buffer_release(reduce_src[rank]);
    flume_client_close(clients[rank]);
  }
  agent.Stop();
  FlumeTestRemoveAll(root);
  return 0;
}
