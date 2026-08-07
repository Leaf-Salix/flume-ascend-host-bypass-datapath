#include "test_util.h"

#include <filesystem>
#include <string>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

namespace {

void OpenRanks(flume::StorageAgent& agent,
               const char* comm_name,
               std::vector<flume_client_t*>* clients) {
  clients->assign(2, nullptr);
  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  for (uint32_t rank = 0; rank < 2; ++rank) {
    FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &(*clients)[rank]) ==
                     FLUME_OK);
    FLUME_TEST_CHECK(flume_attach_sim_comm((*clients)[rank], comm_name, rank,
                                           2) == FLUME_OK);
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
  constexpr uint64_t kCount = 16;
  constexpr size_t kBytes = static_cast<size_t>(kCount) * sizeof(float);

  fs::path root = fs::temp_directory_path() / "flume-test-sim-hcomm-payload-copy";
  fs::remove_all(root);
  fs::create_directories(root);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  FLUME_TEST_CHECK(agent.Start(&error));

  std::vector<flume_client_t*> clients;
  OpenRanks(agent, "hcomm-payload-world", &clients);

  flume_io_t* probe = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_payload_probe(clients[0], 1, nullptr, &probe) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(probe, 0) == FLUME_OK);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(probe))
                       .find("scheduler=sim") != std::string::npos);
  FLUME_TEST_CHECK(flume_io_release(probe) == FLUME_OK);

  flume_io_t* custom_op_launch = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_custom_op_launch_smoke(
                       clients[0], 1, nullptr, &custom_op_launch) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(custom_op_launch, 0) == FLUME_OK);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(custom_op_launch))
                       .find("stage3b1_launch_plan=noop-custom-op") !=
                   std::string::npos);
  FLUME_TEST_CHECK(flume_io_release(custom_op_launch) == FLUME_OK);

  flume_io_t* descriptor = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_resource_descriptor_smoke(
                       clients[0], 1, nullptr, &descriptor) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(descriptor, 0) == FLUME_OK);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(descriptor))
                       .find("stage3b2_resource_descriptor=host-packaged") !=
                   std::string::npos);
  FLUME_TEST_CHECK(flume_io_release(descriptor) == FLUME_OK);

  flume_io_t* notify_only = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_notify_only_smoke(
                       clients[0], 1, nullptr, &notify_only) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(notify_only, 0) == FLUME_OK);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(notify_only))
                       .find("stage3b2_notify_only_plan=channel-notify") !=
                   std::string::npos);
  FLUME_TEST_CHECK(flume_io_release(notify_only) == FLUME_OK);

  flume_buffer_t* src0 = nullptr;
  flume_buffer_t* dst1 = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[0], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &src0) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[1], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &dst1) ==
                   FLUME_OK);
  FillFloats(src0, 300.0F, kCount);

  flume_io_t* send01 = nullptr;
  flume_io_t* recv01 = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_payload_send_async(
                       clients[0], src0, 0, kCount, FLUME_DTYPE_FP32, 1,
                       nullptr, &send01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send01, 0) == FLUME_ERR_TIMEOUT);
  FLUME_TEST_CHECK(flume_io_release(send01) == FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(flume_hcomm_payload_recv_async(
                       clients[1], dst1, 0, kCount, FLUME_DTYPE_FP32, 0,
                       nullptr, &recv01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send01, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv01, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_bytes(send01) == kBytes);
  FLUME_TEST_CHECK(flume_io_bytes(recv01) == kBytes);
  CheckFloats(dst1, 300.0F, kCount);
  FLUME_TEST_CHECK(flume_io_release(send01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_release(recv01) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(src0) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(dst1) == FLUME_OK);

  flume_buffer_t* src1 = nullptr;
  flume_buffer_t* dst0 = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[1], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &src1) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[0], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &dst0) ==
                   FLUME_OK);
  FillFloats(src1, 400.0F, kCount);

  flume_io_t* recv10 = nullptr;
  flume_io_t* send10 = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_payload_recv_async(
                       clients[0], dst0, 0, kCount, FLUME_DTYPE_FP32, 1,
                       nullptr, &recv10) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv10, 0) == FLUME_ERR_TIMEOUT);
  FLUME_TEST_CHECK(flume_buffer_release(dst0) == FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(flume_hcomm_payload_send_async(
                       clients[1], src1, 0, kCount, FLUME_DTYPE_FP32, 0,
                       nullptr, &send10) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv10, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send10, 1000) == FLUME_OK);
  CheckFloats(dst0, 400.0F, kCount);
  FLUME_TEST_CHECK(flume_io_release(recv10) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_release(send10) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(dst0) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(src1) == FLUME_OK);

  for (auto* client : clients) {
    flume_client_close(client);
  }
  agent.Stop();
  fs::remove_all(root);
  return 0;
}
