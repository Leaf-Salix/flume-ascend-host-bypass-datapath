#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

namespace {

void WriteFixture(const std::filesystem::path& path, size_t len) {
  std::ofstream out(path, std::ios::binary);
  for (size_t i = 0; i < len; ++i) {
    char value = static_cast<char>((i * 23 + 5) & 0xff);
    out.write(&value, 1);
  }
}

bool Check(int status, const char* label) {
  if (status == FLUME_OK) {
    return true;
  }
  std::cerr << label << " failed: " << flume_status_string(status) << "\n";
  return false;
}

bool Verify(const uint8_t* data, size_t len, size_t file_offset) {
  for (size_t i = 0; i < len; ++i) {
    uint8_t expected = static_cast<uint8_t>(((file_offset + i) * 23 + 5) & 0xff);
    if (data[i] != expected) {
      std::cerr << "verify failed at " << i << ": got="
                << static_cast<int>(data[i]) << " expected="
                << static_cast<int>(expected) << "\n";
      return false;
    }
  }
  return true;
}

flume_storage_block_t* PrepareBlock(flume_client_t* client,
                                    const char* path,
                                    size_t offset,
                                    size_t len) {
  flume_file_t* file = nullptr;
  flume_storage_block_t* block = nullptr;
  flume_io_t* io = nullptr;
  if (!Check(flume_open(client, path, &file), "open storage file") ||
      !Check(flume_prepare_storage_block_async(file, offset, len, &block, &io),
             "prepare storage block") ||
      !Check(flume_wait(io, 1000), "prepare storage block wait")) {
    return nullptr;
  }
  flume_io_release(io);
  flume_close(file);
  return block;
}

}  // namespace

int main() {
  namespace fs = std::filesystem;
  constexpr size_t kFileSize = 64 * 1024;
  constexpr size_t kOffset = 123;
  constexpr size_t kBytes = 4096;

  fs::path root = fs::temp_directory_path() / "flume-storage-direct-sim-smoke";
  fs::remove_all(root);
  fs::create_directories(root);
  WriteFixture(root / "data.bin", kFileSize);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "agent start failed: " << error << "\n";
    return 1;
  }

  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  std::vector<flume_client_t*> clients(2, nullptr);
  for (uint32_t rank = 0; rank < 2; ++rank) {
    if (!Check(flume_client_open(endpoint.c_str(), &clients[rank]),
               "client open") ||
        !Check(flume_attach_sim_comm(clients[rank],
                                     "storage-direct-smoke-world", rank, 2),
               "attach sim comm")) {
      return 1;
    }
  }

  flume_storage_block_t* block =
      PrepareBlock(clients[0], "data.bin", kOffset, kBytes);
  flume_buffer_t* dst = nullptr;
  if (block == nullptr ||
      !Check(flume_sim_alloc_buffer(clients[1], kBytes, FLUME_BUFFER_SIM_HBM,
                                    &dst),
             "alloc target HBM")) {
    return 1;
  }

  flume_storage_direct_options_t send_options = {};
  send_options.size = sizeof(send_options);
  send_options.path = FLUME_STORAGE_TRANSFER_SIM_DIRECT;
  send_options.role = FLUME_STORAGE_DIRECT_ROLE_SOURCE_SEND;
  send_options.peer_rank = 1;
  send_options.require_direct = 1;
  send_options.allow_host_staging = 0;
  send_options.len = kBytes;
  flume_storage_direct_options_t recv_options = send_options;
  recv_options.role = FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV;
  recv_options.peer_rank = 0;

  flume_storage_direct_plan_t* send_plan = nullptr;
  flume_storage_direct_plan_t* recv_plan = nullptr;
  if (!Check(flume_storage_direct_plan_create(
                 clients[0], block, nullptr, 0, &send_options, &send_plan),
             "create source direct plan") ||
      !Check(flume_storage_direct_plan_create(
                 clients[1], nullptr, dst, 0, &recv_options, &recv_plan),
             "create target direct plan")) {
    return 1;
  }

  flume_io_t* send_io = nullptr;
  flume_io_t* recv_io = nullptr;
  if (!Check(flume_read_storage_to_hbm_async(clients[0], send_plan, nullptr,
                                             &send_io),
             "submit source direct plan") ||
      !Check(flume_read_storage_to_hbm_async(clients[1], recv_plan, nullptr,
                                             &recv_io),
             "submit target direct plan") ||
      !Check(flume_wait(send_io, 1000), "wait source direct plan") ||
      !Check(flume_wait(recv_io, 1000), "wait target direct plan")) {
    return 1;
  }
  if (!Verify(static_cast<const uint8_t*>(flume_buffer_data(dst)), kBytes,
              kOffset)) {
    return 1;
  }
  std::cout << "storage direct sim smoke passed: "
            << flume_io_error_message(recv_io)
            << " bytes=" << flume_io_bytes(recv_io)
            << " checksum=" << flume_io_checksum(recv_io) << "\n";

  flume_io_release(send_io);
  flume_io_release(recv_io);
  flume_storage_direct_plan_release(send_plan);
  flume_storage_direct_plan_release(recv_plan);
  flume_storage_block_release(block);
  flume_buffer_release(dst);

  flume_storage_block_t* fallback_block =
      PrepareBlock(clients[0], "data.bin", kOffset + 17, kBytes);
  flume_buffer_t* fallback_dst = nullptr;
  if (fallback_block == nullptr ||
      !Check(flume_sim_alloc_buffer(clients[0], kBytes, FLUME_BUFFER_SIM_HBM,
                                    &fallback_dst),
             "alloc fallback HBM")) {
    return 1;
  }
  flume_storage_direct_options_t fallback_options = {};
  fallback_options.size = sizeof(fallback_options);
  fallback_options.path = FLUME_STORAGE_TRANSFER_HOST_STAGING;
  fallback_options.role = FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING;
  fallback_options.allow_host_staging = 1;
  fallback_options.len = kBytes;
  flume_storage_direct_plan_t* fallback_plan = nullptr;
  flume_io_t* fallback_io = nullptr;
  if (!Check(flume_storage_direct_plan_create(
                 clients[0], fallback_block, fallback_dst, 0,
                 &fallback_options, &fallback_plan),
             "create fallback plan") ||
      !Check(flume_read_storage_to_hbm_async(clients[0], fallback_plan, nullptr,
                                             &fallback_io),
             "submit fallback plan") ||
      !Check(flume_wait(fallback_io, 1000), "wait fallback plan")) {
    return 1;
  }
  if (!Verify(static_cast<const uint8_t*>(flume_buffer_data(fallback_dst)),
              kBytes, kOffset + 17)) {
    return 1;
  }
  std::cout << "storage host-staging fallback smoke passed: "
            << flume_io_error_message(fallback_io)
            << " bytes=" << flume_io_bytes(fallback_io)
            << " checksum=" << flume_io_checksum(fallback_io) << "\n";

  flume_io_release(fallback_io);
  flume_storage_direct_plan_release(fallback_plan);
  flume_storage_block_release(fallback_block);
  flume_buffer_release(fallback_dst);

  for (auto* client : clients) {
    flume_client_close(client);
  }
  agent.Stop();
  fs::remove_all(root);
  return 0;
}
