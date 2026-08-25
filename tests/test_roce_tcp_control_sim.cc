#include <sys/socket.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "roce_storage/control_channel.h"
#include "roce_storage/storage_backend.h"
#include "test_util.h"

namespace {

constexpr uint32_t kSimRkey = 0x51a7U;

void FillPattern(std::vector<uint8_t>* data) {
  for (size_t index = 0; index < data->size(); ++index) {
    (*data)[index] = static_cast<uint8_t>((index * 29U + 11U) & 0xffU);
  }
}

void RunServer(int fd, flume::roce::StorageBackend* storage,
               std::vector<uint8_t>* registered_hbm) {
  flume::roce::Command command;
  std::string error;
  FLUME_TEST_CHECK(flume::roce::ReceiveCommand(fd, &command, &error) ==
                   flume::roce::ControlReadResult::kSuccess);
  flume::roce::Completion completion;
  completion.request_id = command.request_id;
  if (command.operation != flume::roce::Operation::kRead ||
      command.npu_address != reinterpret_cast<uint64_t>(registered_hbm->data()) ||
      command.npu_rkey != kSimRkey || command.length > registered_hbm->size()) {
    completion.status = 1;
  } else {
    std::vector<uint8_t> staging(static_cast<size_t>(command.length));
    const bool ok = storage->Read(command.storage_offset, staging.data(),
                                  staging.size(), &error);
    if (ok) {
      std::memcpy(registered_hbm->data(), staging.data(), staging.size());
      completion.bytes = staging.size();
      completion.checksum = flume::roce::Checksum(staging.data(), staging.size());
    } else {
      completion.status = 2;
    }
  }
  FLUME_TEST_CHECK(flume::roce::SendCompletion(fd, completion, &error));
  close(fd);
}

void RunCase(flume::roce::StorageBackend* storage, bool server_first) {
  constexpr size_t kOffset = 17;
  constexpr size_t kBytes = 1024;
  int sockets[2] = {-1, -1};
  FLUME_TEST_CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
  std::vector<uint8_t> registered_hbm(kBytes, 0);
  std::thread server;
  if (server_first) {
    server = std::thread(RunServer, sockets[1], storage, &registered_hbm);
  }

  flume::roce::Command command;
  command.request_id = server_first ? 1 : 2;
  command.operation = flume::roce::Operation::kRead;
  command.storage_offset = kOffset;
  command.length = kBytes;
  command.npu_address = reinterpret_cast<uint64_t>(registered_hbm.data());
  command.npu_rkey = kSimRkey;
  command.npu_access = flume::roce::kMemoryRemoteWrite;
  std::string error;
  FLUME_TEST_CHECK(flume::roce::SendCommand(sockets[0], command, &error));
  if (!server_first) {
    server = std::thread(RunServer, sockets[1], storage, &registered_hbm);
  }
  flume::roce::Completion completion;
  FLUME_TEST_CHECK(flume::roce::ReceiveCompletion(sockets[0], &completion, &error) ==
                   flume::roce::ControlReadResult::kSuccess);
  server.join();
  close(sockets[0]);
  FLUME_TEST_CHECK(completion.status == 0);
  FLUME_TEST_CHECK(completion.bytes == kBytes);
  FLUME_TEST_CHECK(completion.checksum ==
                   flume::roce::Checksum(registered_hbm.data(), registered_hbm.size()));
}

void RunRejectedWindowCase(flume::roce::StorageBackend* storage) {
  int sockets[2] = {-1, -1};
  FLUME_TEST_CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
  std::vector<uint8_t> registered_hbm(64, 0);
  std::thread server(RunServer, sockets[1], storage, &registered_hbm);
  flume::roce::Command command;
  command.request_id = 3;
  command.operation = flume::roce::Operation::kRead;
  command.length = registered_hbm.size();
  command.npu_address = reinterpret_cast<uint64_t>(registered_hbm.data());
  command.npu_rkey = kSimRkey + 1;
  command.npu_access = flume::roce::kMemoryRemoteWrite;
  std::string error;
  FLUME_TEST_CHECK(flume::roce::SendCommand(sockets[0], command, &error));
  flume::roce::Completion completion;
  FLUME_TEST_CHECK(flume::roce::ReceiveCompletion(sockets[0], &completion, &error) ==
                   flume::roce::ControlReadResult::kSuccess);
  server.join();
  close(sockets[0]);
  FLUME_TEST_CHECK(completion.request_id == command.request_id);
  FLUME_TEST_CHECK(completion.status != 0);
  FLUME_TEST_CHECK(completion.bytes == 0);
}

}  // namespace

int main() {
  namespace fs = std::filesystem;
  constexpr size_t kStorageBytes = 4096;
  std::vector<uint8_t> fixture(kStorageBytes);
  FillPattern(&fixture);

  flume::roce::MemoryStorageBackend memory(kStorageBytes);
  std::string error;
  FLUME_TEST_CHECK(memory.Write(0, fixture.data(), fixture.size(), &error));
  RunCase(&memory, false);
  RunCase(&memory, true);
  RunRejectedWindowCase(&memory);

  const fs::path root = fs::temp_directory_path() / "flume-roce-tcp-control-sim";
  FlumeTestRemoveAll(root);
  fs::create_directories(root);
  const fs::path file = root / "namespace.bin";
  {
    std::ofstream out(file, std::ios::binary);
    out.write(reinterpret_cast<const char*>(fixture.data()), fixture.size());
  }
  flume::roce::PosixStorageBackend posix(file.string());
  RunCase(&posix, true);

  int sockets[2] = {-1, -1};
  FLUME_TEST_CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
  const uint8_t partial[3] = {1, 2, 3};
  FLUME_TEST_CHECK(flume::roce::ControlWriteAll(sockets[0], partial,
                                                sizeof(partial), &error));
  close(sockets[0]);
  flume::roce::Command truncated;
  FLUME_TEST_CHECK(flume::roce::ReceiveCommand(sockets[1], &truncated, &error) ==
                   flume::roce::ControlReadResult::kError);
  FLUME_TEST_CHECK(error.find("mid-frame") != std::string::npos);
  close(sockets[1]);

  FlumeTestRemoveAll(root);
  return 0;
}
