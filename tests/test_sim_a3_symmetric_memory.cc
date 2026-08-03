#include "test_util.h"

#include <cstdint>
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
    FLUME_TEST_CHECK(flume_attach_sim_comm((*clients)[rank], "a3-symm-test-world", rank,
                                rank_size) == FLUME_OK);
  }
}

void RegisterWindow(flume_client_t* client,
                    flume_buffer_t* buffer,
                    std::vector<flume_a3_symmetric_window_t*>* windows) {
  flume_a3_symmetric_window_t* window = nullptr;
  FLUME_TEST_CHECK(flume_a3_register_symmetric_memory(client, buffer, 0,
                                           flume_buffer_size(buffer),
                                           &window) == FLUME_OK);
  FLUME_TEST_CHECK(window != nullptr);
  windows->push_back(window);
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  constexpr uint32_t kRanks = 2;
  constexpr uint64_t kReduceCount = 4;
  constexpr uint64_t kGatherCount = 2;
  fs::path root = fs::temp_directory_path() / "flume-test-sim-a3-symmetric";
  fs::remove_all(root);
  fs::create_directories(root);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "agent.Start failed: " << error << "\n";
    return 1;
  }

  std::vector<flume_client_t*> clients;
  OpenRanks(agent, kRanks, &clients);

  std::vector<flume_buffer_t*> reduce_src(kRanks, nullptr);
  std::vector<flume_buffer_t*> reduce_dst(kRanks, nullptr);
  std::vector<flume_buffer_t*> gather_src(kRanks, nullptr);
  std::vector<flume_buffer_t*> gather_dst(kRanks, nullptr);
  std::vector<flume_io_t*> reduce_ios(kRanks, nullptr);
  std::vector<flume_io_t*> gather_ios(kRanks, nullptr);
  std::vector<flume_a3_symmetric_window_t*> windows;

  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[rank], kReduceCount * sizeof(float),
                                 FLUME_BUFFER_SIM_HBM,
                                 &reduce_src[rank]) == FLUME_OK);
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[rank], kReduceCount * sizeof(float),
                                 FLUME_BUFFER_SIM_HBM,
                                 &reduce_dst[rank]) == FLUME_OK);
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[rank], kGatherCount * sizeof(float),
                                 FLUME_BUFFER_SIM_HCCL_COMM,
                                 &gather_src[rank]) == FLUME_OK);
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[rank],
                                 kRanks * kGatherCount * sizeof(float),
                                 FLUME_BUFFER_SIM_HBM,
                                 &gather_dst[rank]) == FLUME_OK);

    auto* reduce_values = static_cast<float*>(flume_buffer_data(reduce_src[rank]));
    for (uint64_t i = 0; i < kReduceCount; ++i) {
      reduce_values[i] = static_cast<float>(rank + 1 + i);
    }
    auto* gather_values = static_cast<float*>(flume_buffer_data(gather_src[rank]));
    gather_values[0] = static_cast<float>(rank * 10 + 1);
    gather_values[1] = static_cast<float>(rank * 10 + 2);

    FLUME_TEST_CHECK(flume_a3_set_memory_range(clients[rank], flume_buffer_data(reduce_src[rank]),
                                    flume_buffer_size(reduce_src[rank]), 1,
                                    0) == FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_a3_set_memory_range(clients[rank], flume_buffer_data(reduce_src[rank]),
                                    flume_buffer_size(reduce_src[rank]), 0,
                                    1) == FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_a3_set_memory_range(clients[rank], flume_buffer_data(reduce_src[rank]),
                                    flume_buffer_size(reduce_src[rank]), 0,
                                    0) == FLUME_OK);
    FLUME_TEST_CHECK(flume_a3_activate_comm_memory(clients[rank],
                                        flume_buffer_data(reduce_src[rank]),
                                        flume_buffer_size(reduce_src[rank]), 0,
                                        nullptr, 0) ==
           FLUME_ERR_INVALID_ARGUMENT);
    void* fake_handle = reinterpret_cast<void*>(
        static_cast<uintptr_t>(rank + 1));
    FLUME_TEST_CHECK(flume_a3_activate_comm_memory(clients[rank],
                                        flume_buffer_data(reduce_src[rank]),
                                        flume_buffer_size(reduce_src[rank]), 1,
                                        fake_handle, 0) ==
           FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_a3_activate_comm_memory(clients[rank],
                                        flume_buffer_data(reduce_src[rank]),
                                        flume_buffer_size(reduce_src[rank]), 0,
                                        fake_handle, 1) ==
           FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_a3_activate_comm_memory(clients[rank],
                                        flume_buffer_data(reduce_src[rank]),
                                        flume_buffer_size(reduce_src[rank]), 0,
                                        fake_handle, 0) == FLUME_OK);
    RegisterWindow(clients[rank], reduce_src[rank], &windows);
    FLUME_TEST_CHECK(flume_buffer_release(reduce_src[rank]) ==
           FLUME_ERR_INVALID_ARGUMENT);
    RegisterWindow(clients[rank], reduce_dst[rank], &windows);
    RegisterWindow(clients[rank], gather_src[rank], &windows);
    RegisterWindow(clients[rank], gather_dst[rank], &windows);
  }

  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_allreduce_async(clients[rank], reduce_dst[rank], 0,
                                reduce_src[rank], 0, kReduceCount,
                                FLUME_DTYPE_FP32, FLUME_REDUCE_SUM, nullptr,
                                &reduce_ios[rank]) == FLUME_OK);
  }
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_wait(reduce_ios[rank], 1000) == FLUME_OK);
    auto* values = static_cast<float*>(flume_buffer_data(reduce_dst[rank]));
    for (uint64_t i = 0; i < kReduceCount; ++i) {
      float expected = 3.0F + static_cast<float>(kRanks * i);
      FLUME_TEST_CHECK(NearlyEqual(values[i], expected));
    }
  }

  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_allgather_async(clients[rank], gather_dst[rank], 0,
                                gather_src[rank], 0, kGatherCount,
                                FLUME_DTYPE_FP32, nullptr,
                                &gather_ios[rank]) == FLUME_OK);
  }
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_wait(gather_ios[rank], 1000) == FLUME_OK);
    auto* values = static_cast<float*>(flume_buffer_data(gather_dst[rank]));
    FLUME_TEST_CHECK(NearlyEqual(values[0], 1.0F));
    FLUME_TEST_CHECK(NearlyEqual(values[1], 2.0F));
    FLUME_TEST_CHECK(NearlyEqual(values[2], 11.0F));
    FLUME_TEST_CHECK(NearlyEqual(values[3], 12.0F));
  }

  for (auto it = windows.rbegin(); it != windows.rend(); ++it) {
    FLUME_TEST_CHECK(flume_a3_deregister_symmetric_memory(*it) == FLUME_OK);
  }
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    FLUME_TEST_CHECK(flume_a3_deactivate_comm_memory(clients[rank],
                                          flume_buffer_data(reduce_src[rank])) ==
           FLUME_OK);
    FLUME_TEST_CHECK(flume_a3_unset_memory_range(clients[rank],
                                      flume_buffer_data(reduce_src[rank])) ==
           FLUME_OK);
    flume_io_release(gather_ios[rank]);
    flume_io_release(reduce_ios[rank]);
    flume_buffer_release(gather_dst[rank]);
    flume_buffer_release(gather_src[rank]);
    flume_buffer_release(reduce_dst[rank]);
    flume_buffer_release(reduce_src[rank]);
    flume_client_close(clients[rank]);
  }

  {
    flume_client_t* rank0 = nullptr;
    flume_client_t* rank1 = nullptr;
    std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
    FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &rank0) == FLUME_OK);
    FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &rank1) == FLUME_OK);
    FLUME_TEST_CHECK(flume_attach_sim_comm(rank0, "a3-mismatch-world", 0, 2) ==
                    FLUME_OK);
    FLUME_TEST_CHECK(flume_attach_sim_comm(rank1, "a3-mismatch-world", 1, 2) ==
                    FLUME_OK);
    flume_buffer_t* buf0 = nullptr;
    flume_buffer_t* buf1 = nullptr;
    flume_a3_symmetric_window_t* win0 = nullptr;
    flume_a3_symmetric_window_t* win1 = nullptr;
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(rank0, 16, FLUME_BUFFER_SIM_HBM,
                                          &buf0) == FLUME_OK);
    FLUME_TEST_CHECK(flume_sim_alloc_buffer(rank1, 32, FLUME_BUFFER_SIM_HBM,
                                          &buf1) == FLUME_OK);
    FLUME_TEST_CHECK(flume_a3_register_symmetric_memory(rank0, buf0, 0, 16,
                                                      &win0) == FLUME_OK);
    FLUME_TEST_CHECK(flume_a3_register_symmetric_memory(rank1, buf1, 0, 32,
                                                      &win1) ==
                    FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(win1 == nullptr);
    FLUME_TEST_CHECK(flume_a3_deregister_symmetric_memory(win0) == FLUME_OK);
    FLUME_TEST_CHECK(flume_buffer_release(buf1) == FLUME_OK);
    FLUME_TEST_CHECK(flume_buffer_release(buf0) == FLUME_OK);
    flume_client_close(rank1);
    flume_client_close(rank0);
  }
  agent.Stop();
  fs::remove_all(root);
  return 0;
}
