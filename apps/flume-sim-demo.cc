#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "agent/storage_agent.h"
#include "flume/flume.h"
#include "protocol/framing.h"

namespace {

void WriteFixture(const std::filesystem::path& path, size_t size) {
  std::ofstream out(path, std::ios::binary);
  for (size_t i = 0; i < size; ++i) {
    char value = static_cast<char>((i * 17 + 3) % 251);
    out.write(&value, 1);
  }
}

bool Verify(const uint8_t* data, size_t len, size_t file_offset) {
  for (size_t i = 0; i < len; ++i) {
    uint8_t expected = static_cast<uint8_t>(((file_offset + i) * 17 + 3) % 251);
    if (data[i] != expected) {
      std::cerr << "verify failed at " << i << ": got=" << static_cast<int>(data[i])
                << " expected=" << static_cast<int>(expected) << "\n";
      return false;
    }
  }
  return true;
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  const size_t file_size = 64 * 1024;
  const size_t offset = 4096 + 123;
  const size_t len = 8192;

  fs::path root = fs::temp_directory_path() / "flume-sim-demo";
  fs::remove_all(root);
  fs::create_directories(root);
  WriteFixture(root / "data.bin", file_size);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "agent start failed: " << error << "\n";
    return 1;
  }

  flume_client_t* client = nullptr;
  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  int ret = flume_client_open(endpoint.c_str(), &client);
  if (ret != FLUME_OK) {
    std::cerr << "client open failed: " << flume_status_string(ret) << "\n";
    return 1;
  }

  flume_file_t* file = nullptr;
  ret = flume_open(client, "data.bin", &file);
  if (ret != FLUME_OK) {
    std::cerr << "file open failed: " << flume_status_string(ret) << "\n";
    return 1;
  }

  flume_buffer_t* storage_hbm = nullptr;
  flume_buffer_t* compute_hbm = nullptr;
  ret = flume_sim_alloc_buffer(client, len, FLUME_BUFFER_SIM_HCCL_COMM, &storage_hbm);
  if (ret != FLUME_OK) {
    std::cerr << "sim storage buffer alloc failed\n";
    return 1;
  }
  ret = flume_sim_alloc_buffer(client, len, FLUME_BUFFER_SIM_HBM, &compute_hbm);
  if (ret != FLUME_OK) {
    std::cerr << "sim compute buffer alloc failed\n";
    return 1;
  }

  flume_io_t* read_io = nullptr;
  ret = flume_pread_async(file, storage_hbm, len, offset, 0, nullptr, &read_io);
  if (ret != FLUME_OK || flume_wait(read_io, 1000) != FLUME_OK) {
    std::cerr << "storage -> sim HCCL comm failed: "
              << (read_io ? flume_io_error_message(read_io) : flume_status_string(ret))
              << "\n";
    return 1;
  }

  flume_io_t* copy_io = nullptr;
  ret = flume_hbm_copy_async(client, compute_hbm, 0, storage_hbm, 0, len, nullptr, &copy_io);
  if (ret != FLUME_OK || flume_wait(copy_io, 1000) != FLUME_OK) {
    std::cerr << "sim HBM -> sim HBM failed: "
              << (copy_io ? flume_io_error_message(copy_io) : flume_status_string(ret))
              << "\n";
    return 1;
  }

  auto* result = static_cast<const uint8_t*>(flume_buffer_data(compute_hbm));
  if (!Verify(result, len, offset)) {
    return 1;
  }

  std::cout << "sim e2e complete: storage->SIM_HCCL_COMM bytes=" << flume_io_bytes(read_io)
            << " checksum=0x" << std::hex << flume_io_checksum(read_io)
            << " hbm->hbm checksum=0x" << flume_io_checksum(copy_io)
            << std::dec << "\n";

  flume_io_release(copy_io);
  flume_io_release(read_io);
  flume_buffer_release(compute_hbm);
  flume_buffer_release(storage_hbm);
  flume_close(file);
  flume_client_close(client);
  agent.Stop();
  fs::remove_all(root);
  return 0;
}
