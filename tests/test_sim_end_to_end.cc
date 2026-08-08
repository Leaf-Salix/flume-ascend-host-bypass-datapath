#include "test_util.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "agent/storage_agent.h"
#include "flume/flume.h"
#include "protocol/framing.h"

namespace {

void WriteFixture(const std::filesystem::path& path) {
  std::ofstream out(path, std::ios::binary);
  for (int i = 0; i < 8192; ++i) {
    char value = static_cast<char>((i * 11 + 9) % 251);
    out.write(&value, 1);
  }
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  fs::path dir = fs::temp_directory_path() / "flume-test-sim-e2e";
  FlumeTestRemoveAll(dir);
  fs::create_directories(dir);
  WriteFixture(dir / "data.bin");

  flume::StorageAgent agent("127.0.0.1", 0, dir.string());
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "agent.Start failed: " << error << "\n";
    return 1;
  }

  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  flume_client_t* client = nullptr;
  FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &client) == FLUME_OK);

  flume_file_t* file = nullptr;
  FLUME_TEST_CHECK(flume_open(client, "data.bin", &file) == FLUME_OK);

  constexpr size_t kOffset = 321;
  constexpr size_t kLen = 1024;
  flume_buffer_t* storage_hbm = nullptr;
  flume_buffer_t* compute_hbm = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(client, kLen, FLUME_BUFFER_SIM_HCCL_COMM, &storage_hbm) == FLUME_OK);
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(client, kLen, FLUME_BUFFER_SIM_HBM, &compute_hbm) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_type(storage_hbm) == FLUME_BUFFER_SIM_HCCL_COMM);
  FLUME_TEST_CHECK(flume_buffer_type(compute_hbm) == FLUME_BUFFER_SIM_HBM);

  flume_io_t* read_io = nullptr;
  FLUME_TEST_CHECK(flume_pread_async(file, storage_hbm, kLen, kOffset, 0, nullptr, &read_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(read_io, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_bytes(read_io) == kLen);

  flume_io_t* copy_io = nullptr;
  FLUME_TEST_CHECK(flume_hbm_copy_async(client, compute_hbm, 0, storage_hbm, 0, kLen, nullptr, &copy_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(copy_io, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_bytes(copy_io) == kLen);

  auto* data = static_cast<const uint8_t*>(flume_buffer_data(compute_hbm));
  FLUME_TEST_CHECK(data != nullptr);
  FLUME_TEST_CHECK(flume_io_checksum(copy_io) == flume::protocol::Checksum32(data, kLen));
  for (size_t i = 0; i < kLen; ++i) {
    uint8_t expected = static_cast<uint8_t>(((kOffset + i) * 11 + 9) % 251);
    if (data[i] != expected) {
      std::cerr << "mismatch at " << i << " got=" << static_cast<int>(data[i])
                << " expected=" << static_cast<int>(expected) << "\n";
      return 1;
    }
  }

  flume_io_release(copy_io);
  flume_io_release(read_io);
  flume_buffer_release(compute_hbm);
  flume_buffer_release(storage_hbm);
  flume_close(file);
  flume_client_close(client);
  agent.Stop();
  FlumeTestRemoveAll(dir);
  return 0;
}
