#include "test_util.h"

#include <arpa/inet.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"
#include "protocol/framing.h"

namespace {

void WriteFixture(const std::filesystem::path& path) {
  std::ofstream out(path, std::ios::binary);
  for (int i = 0; i < 4096; ++i) {
    char value = static_cast<char>(i % 251);
    out.write(&value, 1);
  }
}

int ConnectRaw(uint16_t port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  FLUME_TEST_CHECK(fd >= 0);
  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  FLUME_TEST_CHECK(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);
  FLUME_TEST_CHECK(connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
  return fd;
}

uint64_t OpenRawFile(int fd, const std::string& path) {
  flume::protocol::Frame req;
  req.type = flume::protocol::FrameType::kOpenReq;
  req.request_id = 100;
  flume::protocol::AppendString(&req.body, path);
  std::string error;
  FLUME_TEST_CHECK(flume::protocol::WriteFrame(fd, req, &error));

  flume::protocol::Frame resp;
  FLUME_TEST_CHECK(flume::protocol::ReadFrame(fd, &resp, &error));
  FLUME_TEST_CHECK(resp.type == flume::protocol::FrameType::kOpenResp);
  flume::protocol::Reader reader(resp.body);
  uint32_t status = 0;
  uint64_t file_id = 0;
  uint64_t file_size = 0;
  std::string remote_error;
  FLUME_TEST_CHECK(reader.ReadU32(&status));
  FLUME_TEST_CHECK(reader.ReadU64(&file_id));
  FLUME_TEST_CHECK(reader.ReadU64(&file_size));
  FLUME_TEST_CHECK(reader.ReadString(&remote_error));
  FLUME_TEST_CHECK(status == FLUME_OK);
  FLUME_TEST_CHECK(file_size == 4096);
  return file_id;
}

void ExpectOversizedReadRejected(uint16_t port) {
  int fd = ConnectRaw(port);
  uint64_t file_id = OpenRawFile(fd, "data.bin");

  flume::protocol::Frame req;
  req.type = flume::protocol::FrameType::kReadReq;
  req.request_id = 101;
  flume::protocol::AppendU64(&req.body, file_id);
  flume::protocol::AppendU64(&req.body, 0);
  flume::protocol::AppendU64(&req.body, flume::protocol::kMaxBodySize);
  std::string error;
  FLUME_TEST_CHECK(flume::protocol::WriteFrame(fd, req, &error));

  flume::protocol::Frame resp;
  FLUME_TEST_CHECK(flume::protocol::ReadFrame(fd, &resp, &error));
  FLUME_TEST_CHECK(resp.type == flume::protocol::FrameType::kReadResp);
  flume::protocol::Reader reader(resp.body);
  uint32_t status = 0;
  uint64_t bytes = 0;
  uint32_t checksum = 0;
  std::string remote_error;
  FLUME_TEST_CHECK(reader.ReadU32(&status));
  FLUME_TEST_CHECK(reader.ReadU64(&bytes));
  FLUME_TEST_CHECK(reader.ReadU32(&checksum));
  FLUME_TEST_CHECK(reader.ReadString(&remote_error));
  FLUME_TEST_CHECK(status == FLUME_ERR_INVALID_ARGUMENT);
  FLUME_TEST_CHECK(bytes == 0);
  FLUME_TEST_CHECK(checksum == 0);
  FLUME_TEST_CHECK(remote_error.find("READ_REQ length") != std::string::npos);
  close(fd);
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  fs::path dir = fs::temp_directory_path() / "flume-test-mock-pread";
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
  flume_file_t* escaped = nullptr;
  FLUME_TEST_CHECK(flume_open(client, "missing/../../etc/passwd", &escaped) ==
                  FLUME_ERR_REMOTE);
  FLUME_TEST_CHECK(escaped == nullptr);

  std::vector<uint8_t> dst(128, 0);
  flume_buffer_t* buffer = nullptr;
  FLUME_TEST_CHECK(flume_register_buffer(client, dst.data(), dst.size(), FLUME_BUFFER_HOST, &buffer) == FLUME_OK);

  flume_io_t* io = nullptr;
  FLUME_TEST_CHECK(flume_pread_async(file, buffer, dst.size(), 100, 0, nullptr, &io) == FLUME_OK);
  FLUME_TEST_CHECK(flume_wait(io, 1000) == FLUME_OK);
  FLUME_TEST_CHECK(flume_io_bytes(io) == dst.size());
  FLUME_TEST_CHECK(flume_io_checksum(io) == flume::protocol::Checksum32(dst.data(), dst.size()));

  for (size_t i = 0; i < dst.size(); ++i) {
    uint8_t expected = static_cast<uint8_t>((100 + i) % 251);
    if (dst[i] != expected) {
      std::cerr << "mismatch at " << i << " got=" << static_cast<int>(dst[i])
                << " expected=" << static_cast<int>(expected) << "\n";
      return 1;
    }
  }

  flume_io_t* unsupported_io = nullptr;
  FLUME_TEST_CHECK(flume_hbm_copy_async(client, buffer, 0, buffer, 0, 1,
                                      nullptr, &unsupported_io) == FLUME_OK);
  const char* message = flume_io_error_message(unsupported_io);
  FLUME_TEST_CHECK(std::string(message).find("HBM copy") != std::string::npos);
  FLUME_TEST_CHECK(flume_io_release(unsupported_io) == FLUME_OK);
  FLUME_TEST_CHECK(std::string(message).find("HBM copy") != std::string::npos);

  ExpectOversizedReadRejected(agent.port());

  flume_io_release(io);
  flume_buffer_release(buffer);
  flume_close(file);
  flume_client_close(client);
  agent.Stop();
  FlumeTestRemoveAll(dir);
  return 0;
}
