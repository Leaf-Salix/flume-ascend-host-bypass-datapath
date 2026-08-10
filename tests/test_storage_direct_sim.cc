#include "test_util.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

namespace {

void WriteFixture(const std::filesystem::path& path, size_t len) {
  std::ofstream out(path, std::ios::binary);
  for (size_t i = 0; i < len; ++i) {
    uint8_t value = static_cast<uint8_t>((i * 19 + 7) & 0xff);
    out.write(reinterpret_cast<const char*>(&value), 1);
  }
}

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

void VerifyPattern(flume_buffer_t* buffer, size_t len, size_t file_offset) {
  auto* bytes = static_cast<const uint8_t*>(flume_buffer_data(buffer));
  for (size_t i = 0; i < len; ++i) {
    uint8_t expected = static_cast<uint8_t>(((file_offset + i) * 19 + 7) & 0xff);
    FLUME_TEST_CHECK(bytes[i] == expected);
  }
}

flume_storage_block_t* PrepareBlock(flume_client_t* client,
                                    const char* path,
                                    size_t offset,
                                    size_t len) {
  flume_file_t* file = nullptr;
  FLUME_TEST_CHECK(flume_open(client, path, &file) == FLUME_OK);
  flume_storage_block_t* block = nullptr;
  flume_io_t* io = nullptr;
  FLUME_TEST_CHECK(flume_prepare_storage_block_async(file, offset, len, &block,
                                                     &io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(io, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_block_size(block) == len);
  FLUME_TEST_CHECK(flume_io_release(io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_close(file) == FLUME_OK);
  return block;
}

}  // namespace

int main() {
  namespace fs = std::filesystem;
  constexpr size_t kFileSize = 4096;
  constexpr size_t kOffset = 37;
  constexpr size_t kBytes = 512;

  fs::path root = fs::temp_directory_path() / "flume-test-storage-direct-sim";
  FlumeTestRemoveAll(root);
  fs::create_directories(root);
  WriteFixture(root / "data.bin", kFileSize);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  FLUME_TEST_CHECK(agent.Start(&error));

  std::vector<flume_client_t*> clients;
  OpenRanks(agent, "storage-direct-world", &clients);

  flume_storage_transfer_caps_t caps = {};
  FLUME_TEST_CHECK(flume_get_storage_transfer_caps(clients[0], &caps) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(caps.storage_direct_sim == 1);
  FLUME_TEST_CHECK(caps.storage_host_staging == 1);
  FLUME_TEST_CHECK(caps.hcomm_payload_sim == 1);
  FLUME_TEST_CHECK(caps.default_path == FLUME_STORAGE_TRANSFER_SIM_DIRECT);

  flume_storage_block_t* block =
      PrepareBlock(clients[0], "data.bin", kOffset, kBytes);
  flume_buffer_t* dst = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[1], kBytes,
                                          FLUME_BUFFER_SIM_HBM, &dst) ==
                   FLUME_OK);

  flume_storage_direct_options_t send_options = {};
  send_options.size = sizeof(send_options);
  send_options.path = FLUME_STORAGE_TRANSFER_SIM_DIRECT;
  send_options.role = FLUME_STORAGE_DIRECT_ROLE_SOURCE_SEND;
  send_options.peer_rank = 1;
  send_options.require_direct = 1;
  send_options.allow_host_staging = 0;
  send_options.len = kBytes;
  flume_storage_direct_plan_t* send_plan = nullptr;
  FLUME_TEST_CHECK(flume_storage_direct_plan_create(
                       clients[0], block, nullptr, 0, &send_options,
                       &send_plan) == FLUME_OK);

  flume_storage_direct_options_t recv_options = {};
  recv_options.size = sizeof(recv_options);
  recv_options.path = FLUME_STORAGE_TRANSFER_SIM_DIRECT;
  recv_options.role = FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV;
  recv_options.peer_rank = 0;
  recv_options.require_direct = 1;
  recv_options.allow_host_staging = 0;
  recv_options.len = kBytes;
  flume_storage_direct_plan_t* recv_plan = nullptr;
  FLUME_TEST_CHECK(flume_storage_direct_plan_create(
                       clients[1], nullptr, dst, 0, &recv_options,
                       &recv_plan) == FLUME_OK);

  flume_io_t* send_io = nullptr;
  FLUME_TEST_CHECK(flume_read_storage_to_hbm_async(
                       clients[0], send_plan, nullptr, &send_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send_io, 0) == FLUME_ERR_TIMEOUT);
  FLUME_TEST_CHECK(flume_io_release(send_io) == FLUME_ERR_INVALID_ARGUMENT);

  flume_io_t* recv_io = nullptr;
  FLUME_TEST_CHECK(flume_read_storage_to_hbm_async(
                       clients[1], recv_plan, nullptr, &recv_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send_io, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv_io, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_bytes(send_io) == kBytes);
  FLUME_TEST_CHECK(flume_io_bytes(recv_io) == kBytes);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(send_io))
                       .find("storage_hbm_path=sim-direct") !=
                   std::string::npos);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(send_io))
                       .find("storage_host_payload_copy=not-used") !=
                   std::string::npos);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(recv_io))
                       .find("fallback=none") != std::string::npos);
  VerifyPattern(dst, kBytes, kOffset);

  FLUME_TEST_CHECK(flume_io_release(send_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_release(recv_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_direct_plan_release(send_plan) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_direct_plan_release(recv_plan) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_block_release(block) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(dst) == FLUME_OK);

  flume_storage_block_t* recv_first_block =
      PrepareBlock(clients[0], "data.bin", kOffset + 5, kBytes);
  flume_buffer_t* recv_first_dst = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[1], kBytes,
                                          FLUME_BUFFER_SIM_HBM,
                                          &recv_first_dst) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_direct_plan_create(
                       clients[0], recv_first_block, nullptr, 0, &send_options,
                       &send_plan) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_direct_plan_create(
                       clients[1], nullptr, recv_first_dst, 0, &recv_options,
                       &recv_plan) == FLUME_OK);
  FLUME_TEST_CHECK(flume_read_storage_to_hbm_async(
                       clients[1], recv_plan, nullptr, &recv_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv_io, 0) == FLUME_ERR_TIMEOUT);
  FLUME_TEST_CHECK(flume_buffer_release(recv_first_dst) ==
                   FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(flume_read_storage_to_hbm_async(
                       clients[0], send_plan, nullptr, &send_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(send_io, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(recv_io, 1000) == FLUME_OK);
  VerifyPattern(recv_first_dst, kBytes, kOffset + 5);
  FLUME_TEST_CHECK(flume_io_release(send_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_release(recv_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_direct_plan_release(send_plan) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_direct_plan_release(recv_plan) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_block_release(recv_first_block) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(recv_first_dst) == FLUME_OK);

  flume_storage_block_t* fallback_block =
      PrepareBlock(clients[0], "data.bin", kOffset + 11, kBytes);
  flume_buffer_t* fallback_dst = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[0], kBytes,
                                          FLUME_BUFFER_SIM_HBM,
                                          &fallback_dst) == FLUME_OK);
  flume_storage_direct_options_t fallback_options = {};
  fallback_options.size = sizeof(fallback_options);
  fallback_options.path = FLUME_STORAGE_TRANSFER_HOST_STAGING;
  fallback_options.role = FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING;
  fallback_options.allow_host_staging = 1;
  fallback_options.len = kBytes;
  flume_storage_direct_plan_t* fallback_plan = nullptr;
  FLUME_TEST_CHECK(flume_storage_direct_plan_create(
                       clients[0], fallback_block, fallback_dst, 0,
                       &fallback_options, &fallback_plan) == FLUME_OK);
  flume_io_t* fallback_io = nullptr;
  FLUME_TEST_CHECK(flume_read_storage_to_hbm_async(
                       clients[0], fallback_plan, nullptr, &fallback_io) ==
                   FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(fallback_io, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(fallback_io))
                       .find("storage_hbm_path=host-staging") !=
                   std::string::npos);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(fallback_io))
                       .find("storage_host_payload_copy=used") !=
                   std::string::npos);
  FLUME_TEST_CHECK(std::string(flume_io_error_message(fallback_io))
                       .find("fallback=host-staging") != std::string::npos);
  VerifyPattern(fallback_dst, kBytes, kOffset + 11);
  FLUME_TEST_CHECK(flume_io_release(fallback_io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_direct_plan_release(fallback_plan) == FLUME_OK);
  FLUME_TEST_CHECK(flume_storage_block_release(fallback_block) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(fallback_dst) == FLUME_OK);

  flume_storage_block_t* strict_block =
      PrepareBlock(clients[0], "data.bin", 0, kBytes);
  flume_buffer_t* strict_dst = nullptr;
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(clients[0], kBytes,
                                          FLUME_BUFFER_SIM_HBM,
                                          &strict_dst) == FLUME_OK);
  flume_storage_direct_options_t strict_fallback = {};
  strict_fallback.size = sizeof(strict_fallback);
  strict_fallback.path = FLUME_STORAGE_TRANSFER_HOST_STAGING;
  strict_fallback.role = FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING;
  strict_fallback.require_direct = 1;
  strict_fallback.allow_host_staging = 0;
  strict_fallback.len = kBytes;
  flume_storage_direct_plan_t* strict_plan = nullptr;
  FLUME_TEST_CHECK(flume_storage_direct_plan_create(
                       clients[0], strict_block, strict_dst, 0,
                       &strict_fallback, &strict_plan) == FLUME_ERR_UNSUPPORTED);
  FLUME_TEST_CHECK(strict_plan == nullptr);
  FLUME_TEST_CHECK(flume_storage_block_release(strict_block) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(strict_dst) == FLUME_OK);

  flume_storage_block_t* invalid_block =
      PrepareBlock(clients[0], "data.bin", kOffset, kBytes);
  flume_storage_direct_options_t bad_peer = send_options;
  bad_peer.peer_rank = 0;
  FLUME_TEST_CHECK(flume_storage_direct_plan_create(
                       clients[0], invalid_block, nullptr, 0, &bad_peer,
                       &send_plan) == FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(send_plan == nullptr);

  flume_storage_direct_options_t bad_len = send_options;
  bad_len.peer_rank = 1;
  bad_len.len = kBytes + 1;
  FLUME_TEST_CHECK(flume_storage_direct_plan_create(
                       clients[0], invalid_block, nullptr, 0, &bad_len,
                       &send_plan) == FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(send_plan == nullptr);
  FLUME_TEST_CHECK(flume_storage_block_release(invalid_block) == FLUME_OK);

  for (auto* client : clients) {
    flume_client_close(client);
  }
  agent.Stop();
  FlumeTestRemoveAll(root);
  return 0;
}
