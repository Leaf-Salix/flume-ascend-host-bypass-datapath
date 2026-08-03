#include <stdint.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "flume/flume.h"

namespace {

void Usage(const char* argv0) {
  std::cerr << "Usage: " << argv0
            << " --endpoint 127.0.0.1:18080 --file PATH"
            << " [--offset N] [--size N]\n";
}

uint64_t ParseU64(const std::string& value) {
  return static_cast<uint64_t>(std::stoull(value));
}

}  // namespace

int main(int argc, char** argv) {
  std::string endpoint;
  std::string file;
  uint64_t offset = 0;
  size_t size = 1024 * 1024;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--endpoint" && i + 1 < argc) {
      endpoint = argv[++i];
    } else if (arg == "--file" && i + 1 < argc) {
      file = argv[++i];
    } else if (arg == "--offset" && i + 1 < argc) {
      offset = ParseU64(argv[++i]);
    } else if (arg == "--size" && i + 1 < argc) {
      size = static_cast<size_t>(ParseU64(argv[++i]));
    } else if (arg == "--help" || arg == "-h") {
      Usage(argv[0]);
      return 0;
    } else {
      Usage(argv[0]);
      return 2;
    }
  }

  if (endpoint.empty() || file.empty()) {
    Usage(argv[0]);
    return 2;
  }

  flume_client_t* client = nullptr;
  int ret = flume_client_open(endpoint.c_str(), &client);
  if (ret != FLUME_OK) {
    std::cerr << "flume_client_open failed: " << flume_status_string(ret) << "\n";
    return 1;
  }

  flume_file_t* remote_file = nullptr;
  ret = flume_open(client, file.c_str(), &remote_file);
  if (ret != FLUME_OK) {
    std::cerr << "flume_open failed: " << flume_status_string(ret) << "\n";
    flume_client_close(client);
    return 1;
  }

  std::vector<uint8_t> data(size);
  flume_buffer_t* buffer = nullptr;
  ret = flume_register_buffer(client, data.data(), data.size(), FLUME_BUFFER_HOST, &buffer);
  if (ret != FLUME_OK) {
    std::cerr << "flume_register_buffer failed: " << flume_status_string(ret) << "\n";
    flume_close(remote_file);
    flume_client_close(client);
    return 1;
  }

  flume_io_t* io = nullptr;
  ret = flume_pread_async(remote_file, buffer, size, offset, 0, nullptr, &io);
  if (ret != FLUME_OK) {
    std::cerr << "flume_pread_async submit failed: " << flume_status_string(ret) << "\n";
    flume_buffer_release(buffer);
    flume_close(remote_file);
    flume_client_close(client);
    return 1;
  }
  ret = flume_wait(io, 5000);
  if (ret != FLUME_OK) {
    std::cerr << "flume_wait failed: " << flume_status_string(ret)
              << " message=" << flume_io_error_message(io) << "\n";
    flume_io_release(io);
    flume_buffer_release(buffer);
    flume_close(remote_file);
    flume_client_close(client);
    return 1;
  }

  std::cout << "read complete: bytes=" << flume_io_bytes(io)
            << " checksum=0x" << std::hex << flume_io_checksum(io) << std::dec
            << "\n";

  flume_io_release(io);
  flume_buffer_release(buffer);
  flume_close(remote_file);
  flume_client_close(client);
  return 0;
}
