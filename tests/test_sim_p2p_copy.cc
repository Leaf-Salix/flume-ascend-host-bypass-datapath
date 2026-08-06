#include "test_util.h"

#include <filesystem>
#include <string>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

namespace {

void OpenRanks(flume::StorageAgent& agent, uint32_t rank_size,
               std::vector<flume_client_t*>* clients) {
  clients->assign(rank_size, nullptr);
  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  for (uint32_t rank = 0; rank < rank_size; ++rank) {
    FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &(*clients)[rank]) ==
                     FLUME_OK);
    FLUME_TEST_CHECK(flume_attach_sim_comm((*clients)[rank], "p2p-world",
                                           rank, rank_size) == FLUME_OK);
  }
}

void FillFloats(flume_buffer_t* buffer, float base, uint64_t count) {
  auto* values = static_cast<float*>(flume_buffer_data(buffer));
  for (uint64_t i = 0; i < count; ++i) {
    values[i] = base + static_cast<float>(i);
  }
}

void CheckFloats(flume_buffer_t* buffer, float base, uint64_t count) {
  auto* values = static_cast<float*>(flume_buffer_data(buffer));
  for (uint64_t i = 0; i < count; ++i) {
    FLUME_TEST_CHECK(values[i] == base + static_cast<float>(i));
  }
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  constexpr uint32_t kRanks = 2;
  constexpr uint64_t kCount = 8;
  constexpr size_t kBytes = static_cast<size_t>(kCount) * sizeof(float);

  fs::path root = fs::temp_directory_path() / "flume-test-sim-p2p-copy";
  fs::remove_all(root);
  fs::create_directories(root);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  FLUME_TEST_CHECK(agent.Start(&error));

  std::vector<flume_client_t*> clients;
  OpenRanks(agent, kRanks, &clients);

  flume_io_t* hcomm_probe01 = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_channel_probe(clients[0], 1, nullptr,
                                             &hcomm_probe01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(hcomm_probe01, 0) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_bytes(hcomm_probe01) == 0);
  FLUME_TEST_CHECK(flume_io_release(hcomm_probe01) == FLUME_OK);

  flume_hcomm_channel_probe_options_t hcomm_options = {};
  hcomm_options.size = sizeof(hcomm_options);
  hcomm_options.notify_num = 3;
  hcomm_options.engine = FLUME_HCOMM_ENGINE_CPU_TS;
  hcomm_options.protocol = FLUME_HCOMM_PROTOCOL_HCCS_ONLY;
  hcomm_options.require_thread_export = 1;
  flume_io_t* hcomm_probe10 = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_channel_probe_ex(clients[1], 0, &hcomm_options,
                                                nullptr, &hcomm_probe10) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(hcomm_probe10, 0) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_release(hcomm_probe10) == FLUME_OK);

  hcomm_options.size = 1;
  flume_io_t* bad_options_probe = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_channel_probe_ex(clients[1], 0, &hcomm_options,
                                                nullptr, &bad_options_probe) ==
                   FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(bad_options_probe == nullptr);

  hcomm_options = {};
  hcomm_options.size = sizeof(hcomm_options);
  hcomm_options.protocol = FLUME_HCOMM_PROTOCOL_PCIE;
  flume_io_t* unsupported_hcomm_probe = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_channel_probe_ex(clients[1], 0, &hcomm_options,
                                                nullptr,
                                                &unsupported_hcomm_probe) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(unsupported_hcomm_probe, 0) ==
                   FLUME_ERR_UNSUPPORTED);
  FLUME_TEST_CHECK(flume_io_release(unsupported_hcomm_probe) == FLUME_OK);

  flume_io_t* bad_hcomm_probe = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_channel_probe(clients[0], 0, nullptr,
                                             &bad_hcomm_probe) ==
                   FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(bad_hcomm_probe == nullptr);

  flume_buffer_t* src0 = nullptr;
  flume_buffer_t* dst1 = nullptr;
  flume_io_t* send01 = nullptr;
  flume_io_t* recv01 = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[0], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &src0) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[1], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &dst1) ==
                   FLUME_OK);
  FillFloats(src0, 100.0F, kCount);
  FLUME_TEST_CHECK(flume_p2p_send_async(clients[0], src0, 0, kCount,
                                        FLUME_DTYPE_FP32, 1, nullptr,
                                        &send01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send01, 0) == FLUME_ERR_TIMEOUT);
  FLUME_TEST_CHECK(flume_io_release(send01) == FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(flume_p2p_recv_async(clients[1], dst1, 0, kCount,
                                        FLUME_DTYPE_FP32, 0, nullptr,
                                        &recv01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send01, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv01, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_bytes(send01) == kBytes);
  FLUME_TEST_CHECK(flume_io_bytes(recv01) == kBytes);
  CheckFloats(dst1, 100.0F, kCount);
  FLUME_TEST_CHECK(flume_io_release(send01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_release(recv01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(src0) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(dst1) == FLUME_OK);

  flume_buffer_t* src1 = nullptr;
  flume_buffer_t* dst0 = nullptr;
  flume_io_t* recv10 = nullptr;
  flume_io_t* send10 = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[1], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &src1) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[0], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &dst0) ==
                   FLUME_OK);
  FillFloats(src1, 200.0F, kCount);
  FLUME_TEST_CHECK(flume_p2p_recv_async(clients[0], dst0, 0, kCount,
                                        FLUME_DTYPE_FP32, 1, nullptr,
                                        &recv10) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv10, 0) == FLUME_ERR_TIMEOUT);
  FLUME_TEST_CHECK(flume_buffer_release(dst0) == FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(flume_p2p_send_async(clients[1], src1, 0, kCount,
                                        FLUME_DTYPE_FP32, 0, nullptr,
                                        &send10) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv10, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send10, 1000) == FLUME_OK);
  CheckFloats(dst0, 200.0F, kCount);
  FLUME_TEST_CHECK(flume_io_release(recv10) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_release(send10) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(dst0) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(src1) == FLUME_OK);

  flume_io_t* bad_io = nullptr;
  FLUME_TEST_CHECK(flume_p2p_send_async(clients[0], nullptr, 0, kCount,
                                        FLUME_DTYPE_FP32, 1, nullptr,
                                        &bad_io) ==
                   FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(bad_io == nullptr);

  for (auto* client : clients) {
    flume_client_close(client);
  }
  agent.Stop();
  fs::remove_all(root);
  return 0;
}
