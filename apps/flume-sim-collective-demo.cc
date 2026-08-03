#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

namespace {

constexpr uint32_t kRanks = 4;
constexpr uint64_t kReduceCount = 4;
constexpr uint64_t kGatherCount = 2;

bool Check(int status, const char* label) {
  if (status == FLUME_OK) {
    return true;
  }
  std::cerr << label << " failed: " << flume_status_string(status) << "\n";
  return false;
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  fs::path root = fs::temp_directory_path() / "flume-sim-collective-demo";
  fs::remove_all(root);
  fs::create_directories(root);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "agent start failed: " << error << "\n";
    return 1;
  }

  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  std::vector<flume_client_t*> clients(kRanks, nullptr);
  std::vector<flume_buffer_t*> reduce_src(kRanks, nullptr);
  std::vector<flume_buffer_t*> reduce_dst(kRanks, nullptr);
  std::vector<flume_io_t*> reduce_ios(kRanks, nullptr);
  std::vector<flume_buffer_t*> gather_src(kRanks, nullptr);
  std::vector<flume_buffer_t*> gather_dst(kRanks, nullptr);
  std::vector<flume_io_t*> gather_ios(kRanks, nullptr);
  std::vector<flume_a3_symmetric_window_t*> a3_windows;

  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    if (!Check(flume_client_open(endpoint.c_str(), &clients[rank]), "client open") ||
        !Check(flume_attach_sim_comm(clients[rank], "demo-world", rank, kRanks),
               "attach sim comm") ||
        !Check(flume_sim_alloc_buffer(clients[rank], kReduceCount * sizeof(float),
                                     FLUME_BUFFER_SIM_HBM, &reduce_src[rank]),
               "alloc reduce src") ||
        !Check(flume_sim_alloc_buffer(clients[rank], kReduceCount * sizeof(float),
                                     FLUME_BUFFER_SIM_HBM, &reduce_dst[rank]),
               "alloc reduce dst") ||
        !Check(flume_sim_alloc_buffer(clients[rank], kGatherCount * sizeof(float),
                                     FLUME_BUFFER_SIM_HCCL_COMM,
                                     &gather_src[rank]),
               "alloc gather src") ||
        !Check(flume_sim_alloc_buffer(clients[rank],
                                     kRanks * kGatherCount * sizeof(float),
                                     FLUME_BUFFER_SIM_HBM, &gather_dst[rank]),
               "alloc gather dst")) {
      return 1;
    }

    auto* reduce_values = static_cast<float*>(flume_buffer_data(reduce_src[rank]));
    for (uint64_t i = 0; i < kReduceCount; ++i) {
      reduce_values[i] = static_cast<float>(rank + 1 + i);
    }
    auto* gather_values = static_cast<float*>(flume_buffer_data(gather_src[rank]));
    gather_values[0] = static_cast<float>(rank * 10 + 1);
    gather_values[1] = static_cast<float>(rank * 10 + 2);

    flume_a3_symmetric_window_t* reduce_src_window = nullptr;
    flume_a3_symmetric_window_t* reduce_dst_window = nullptr;
    flume_a3_symmetric_window_t* gather_src_window = nullptr;
    flume_a3_symmetric_window_t* gather_dst_window = nullptr;
    if (!Check(flume_a3_register_symmetric_memory(
                   clients[rank], reduce_src[rank], 0,
                   flume_buffer_size(reduce_src[rank]), &reduce_src_window),
               "register reduce src symmetric memory") ||
        !Check(flume_a3_register_symmetric_memory(
                   clients[rank], reduce_dst[rank], 0,
                   flume_buffer_size(reduce_dst[rank]), &reduce_dst_window),
               "register reduce dst symmetric memory") ||
        !Check(flume_a3_register_symmetric_memory(
                   clients[rank], gather_src[rank], 0,
                   flume_buffer_size(gather_src[rank]), &gather_src_window),
               "register gather src symmetric memory") ||
        !Check(flume_a3_register_symmetric_memory(
                   clients[rank], gather_dst[rank], 0,
                   flume_buffer_size(gather_dst[rank]), &gather_dst_window),
               "register gather dst symmetric memory")) {
      return 1;
    }
    a3_windows.push_back(reduce_src_window);
    a3_windows.push_back(reduce_dst_window);
    a3_windows.push_back(gather_src_window);
    a3_windows.push_back(gather_dst_window);
  }

  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    if (!Check(flume_allreduce_async(clients[rank], reduce_dst[rank], 0,
                                    reduce_src[rank], 0, kReduceCount,
                                    FLUME_DTYPE_FP32, FLUME_REDUCE_SUM, nullptr,
                                    &reduce_ios[rank]),
               "allreduce submit")) {
      return 1;
    }
  }
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    if (!Check(flume_wait(reduce_ios[rank], 1000), "allreduce wait")) {
      std::cerr << flume_io_error_message(reduce_ios[rank]) << "\n";
      return 1;
    }
  }

  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    if (!Check(flume_allgather_async(clients[rank], gather_dst[rank], 0,
                                    gather_src[rank], 0, kGatherCount,
                                    FLUME_DTYPE_FP32, nullptr,
                                    &gather_ios[rank]),
               "allgather submit")) {
      return 1;
    }
  }
  for (uint32_t rank = 0; rank < kRanks; ++rank) {
    if (!Check(flume_wait(gather_ios[rank], 1000), "allgather wait")) {
      std::cerr << flume_io_error_message(gather_ios[rank]) << "\n";
      return 1;
    }
  }

  auto* reduce_out = static_cast<float*>(flume_buffer_data(reduce_dst[0]));
  auto* gather_out = static_cast<float*>(flume_buffer_data(gather_dst[0]));
  std::cout << "sim collective complete: allreduce=[";
  for (uint64_t i = 0; i < kReduceCount; ++i) {
    std::cout << (i == 0 ? "" : ", ") << reduce_out[i];
  }
  std::cout << "] allgather=[";
  for (uint64_t i = 0; i < kRanks * kGatherCount; ++i) {
    std::cout << (i == 0 ? "" : ", ") << gather_out[i];
  }
  std::cout << "] a3_symmetric=sim checksum=0x" << std::hex
            << flume_io_checksum(gather_ios[0]) << std::dec << "\n";

  for (auto it = a3_windows.rbegin(); it != a3_windows.rend(); ++it) {
    if (!Check(flume_a3_deregister_symmetric_memory(*it),
               "deregister symmetric memory")) {
      return 1;
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
  fs::remove_all(root);
  return 0;
}
