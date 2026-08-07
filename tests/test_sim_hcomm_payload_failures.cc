#include "test_util.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

namespace {

void WriteFixture(const std::filesystem::path& path) {
  std::ofstream out(path, std::ios::binary);
  for (int i = 0; i < 256; ++i) {
    uint8_t value = static_cast<uint8_t>((i * 3) & 0xff);
    out.write(reinterpret_cast<const char*>(&value), 1);
  }
}

}  // namespace

int main() {
  namespace fs = std::filesystem;
  constexpr size_t kBytes = 32;

  fs::path root = fs::temp_directory_path() / "flume-test-sim-hcomm-payload-failures";
  fs::remove_all(root);
  fs::create_directories(root);
  WriteFixture(root / "data.bin");

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  FLUME_TEST_CHECK(agent.Start(&error));

  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  flume_client_t* rank0 = nullptr;
  flume_client_t* rank1 = nullptr;
  flume_client_t* lone_rank = nullptr;
  FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &rank0) == FLUME_OK);
  FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &rank1) == FLUME_OK);
  FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &lone_rank) == FLUME_OK);
  FLUME_TEST_CHECK(flume_attach_sim_comm(rank0, "failure-world", 0, 2) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_attach_sim_comm(rank1, "failure-world", 1, 2) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_attach_sim_comm(lone_rank, "failure-world-3", 0, 3) ==
                   FLUME_OK);

  flume_buffer_t* src = nullptr;
  flume_buffer_t* dst = nullptr;
  flume_buffer_t* lone_dst = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(rank0, kBytes,
                                          FLUME_BUFFER_SIM_HBM, &src) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(rank1, kBytes,
                                          FLUME_BUFFER_SIM_HBM, &dst) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(lone_rank, kBytes,
                                          FLUME_BUFFER_SIM_HBM, &lone_dst) ==
                   FLUME_OK);

  flume_io_t* io = nullptr;
  FLUME_TEST_CHECK(flume_hcomm_payload_send_async(
                       rank0, src, 0, 1, FLUME_DTYPE_FP32, 0, nullptr, &io) ==
                   FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(io == nullptr);
  FLUME_TEST_CHECK(flume_hcomm_payload_send_async(
                       rank0, src, kBytes - 1, 1, FLUME_DTYPE_FP32, 1,
                       nullptr, &io) == FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(io == nullptr);
  FLUME_TEST_CHECK(flume_hcomm_payload_recv_async(
                       rank1, src, 0, 1, FLUME_DTYPE_FP32, 0, nullptr, &io) ==
                   FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(io == nullptr);

  FLUME_TEST_CHECK(flume_hcomm_payload_recv_async(
                       lone_rank, lone_dst, 0, 1, FLUME_DTYPE_FP32, 1,
                       nullptr, &io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(io, 0) == FLUME_ERR_UNSUPPORTED);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(io))
                       .find("exactly two ranks") != std::string::npos);
  FLUME_TEST_CHECK(flume_io_release(io) == FLUME_OK);
  io = nullptr;

  flume_file_t* file = nullptr;
  FLUME_TEST_CHECK(flume_open(rank0, "data.bin", &file) == FLUME_OK);
  flume_storage_block_t* block = nullptr;
  FLUME_TEST_CHECK(flume_prepare_storage_block_async(file, 4, kBytes, &block,
                                                     &io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(io, 0) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_block_size(block) == kBytes);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(io))
                       .find("sim-partial") != std::string::npos);
  FLUME_TEST_CHECK(flume_io_release(io) == FLUME_OK);
  io = nullptr;

  flume_buffer_t* storage_dst = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(rank0, kBytes,
                                          FLUME_BUFFER_SIM_HBM, &storage_dst) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_read_to_hbm_async(rank0, block, storage_dst, 0,
                                           nullptr, &io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(io, 0) == FLUME_OK);
  auto* bytes = static_cast<uint8_t*>(flume_buffer_data(storage_dst));
  for (size_t i = 0; i < kBytes; ++i) {
    FLUME_TEST_CHECK(bytes[i] == static_cast<uint8_t>(((i + 4) * 3) & 0xff));
  }
  FLUME_TEST_CHECK(std::string(flume_io_error_message(io))
                       .find("SIM_HCCL_COMM->SIM_HBM") != std::string::npos);
  FLUME_TEST_CHECK(flume_io_release(io) == FLUME_OK);

  FLUME_TEST_CHECK(flume_storage_block_release(block) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(storage_dst) == FLUME_OK);
  flume_close(file);

  FLUME_TEST_CHECK(flume_buffer_release(lone_dst) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(dst) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(src) == FLUME_OK);
  flume_client_close(lone_rank);
  flume_client_close(rank1);
  flume_client_close(rank0);
  agent.Stop();
  fs::remove_all(root);
  return 0;
}
