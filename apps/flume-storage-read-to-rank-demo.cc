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
    char value = static_cast<char>((i * 29 + 11) & 0xff);
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
    uint8_t expected =
        static_cast<uint8_t>(((file_offset + i) * 29 + 11) & 0xff);
    if (data[i] != expected) {
      std::cerr << "verify failed at " << i << ": got="
                << static_cast<int>(data[i]) << " expected="
                << static_cast<int>(expected) << "\n";
      return false;
    }
  }
  return true;
}

}  // namespace

int main() {
  namespace fs = std::filesystem;
  constexpr size_t kFileSize = 32 * 1024;
  constexpr size_t kOffset = 211;
  constexpr size_t kBytes = 2048;
  constexpr uint32_t kTargetRank = 1;

  fs::path root = fs::temp_directory_path() / "flume-storage-read-to-rank-demo";
  fs::remove_all(root);
  fs::create_directories(root);
  WriteFixture(root / "tensor.bin", kFileSize);

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
                                     "read-to-rank-demo-world", rank, 2),
               "attach sim comm")) {
      return 1;
    }
  }

  flume_file_t* file = nullptr;
  flume_storage_block_t* block = nullptr;
  flume_io_t* prepare_io = nullptr;
  if (!Check(flume_open(clients[0], "tensor.bin", &file), "open file") ||
      !Check(flume_prepare_storage_block_async(file, kOffset, kBytes, &block,
                                               &prepare_io),
             "prepare storage block") ||
      !Check(flume_wait(prepare_io, 1000), "prepare wait")) {
    return 1;
  }
  flume_io_release(prepare_io);
  flume_close(file);

  flume_buffer_t* target_hbm = nullptr;
  flume_storage_target_window_t* target_window = nullptr;
  if (!Check(flume_sim_alloc_buffer(clients[kTargetRank], kBytes,
                                    FLUME_BUFFER_SIM_HBM, &target_hbm),
             "alloc target HBM") ||
      !Check(flume_register_storage_target_memory(clients[kTargetRank],
                                                  target_hbm, 0, kBytes,
                                                  &target_window),
             "register target window")) {
    return 1;
  }

  flume_storage_direct_options_t source = {};
  source.size = sizeof(source);
  source.path = FLUME_STORAGE_TRANSFER_SIM_DIRECT;
  source.role = FLUME_STORAGE_DIRECT_ROLE_SOURCE_SEND;
  source.peer_rank = kTargetRank;
  source.require_direct = 1;
  source.allow_host_staging = 0;
  source.len = kBytes;

  flume_storage_direct_options_t target = source;
  target.role = FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV;
  target.peer_rank = 0;
  target.target_window = target_window;

  flume_storage_direct_plan_t* source_plan = nullptr;
  flume_storage_direct_plan_t* target_plan = nullptr;
  flume_io_t* source_io = nullptr;
  flume_io_t* target_io = nullptr;
  if (!Check(flume_storage_direct_plan_create(clients[0], block, nullptr, 0,
                                              &source, &source_plan),
             "create source plan") ||
      !Check(flume_storage_direct_plan_create(clients[kTargetRank], nullptr,
                                              target_hbm, 0, &target,
                                              &target_plan),
             "create target plan") ||
      !Check(flume_read_storage_to_hbm_async(clients[0], source_plan, nullptr,
                                             &source_io),
             "submit source plan") ||
      !Check(flume_read_storage_to_hbm_async(clients[kTargetRank], target_plan,
                                             nullptr, &target_io),
             "submit target plan") ||
      !Check(flume_wait(source_io, 1000), "source wait") ||
      !Check(flume_wait(target_io, 1000), "target wait")) {
    return 1;
  }

  if (!Verify(static_cast<const uint8_t*>(flume_buffer_data(target_hbm)),
              kBytes, kOffset)) {
    return 1;
  }

  std::cout << "app read-to-rank-HBM demo passed: "
            << "app_request=read_to_rank_hbm "
            << "selected_path=sim-direct "
            << "host_payload_copy=not-used "
            << "storage_fabric=sim-rdma "
            << "storage_memory_registration=sim-hbm-window "
            << "target_rank=" << kTargetRank << " "
            << "bytes=" << flume_io_bytes(target_io) << " "
            << "checksum=" << flume_io_checksum(target_io) << " "
            << "verdict=passed "
            << "detail=\"" << flume_io_error_message(target_io) << "\"\n";

  flume_io_release(source_io);
  flume_io_release(target_io);
  flume_storage_direct_plan_release(source_plan);
  flume_storage_direct_plan_release(target_plan);
  flume_storage_target_window_release(target_window);
  flume_buffer_release(target_hbm);
  flume_storage_block_release(block);
  for (auto* client : clients) {
    flume_client_close(client);
  }
  agent.Stop();
  fs::remove_all(root);
  return 0;
}
