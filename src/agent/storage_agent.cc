#include "agent/storage_agent.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <vector>

#include "flume/flume.h"
#include "protocol/framing.h"

namespace flume {
namespace {

constexpr uint64_t kReadRespFixedBytes = 4 + 8 + 4 + 4;
constexpr uint64_t kMaxReadPayloadSize =
    protocol::kMaxBodySize - kReadRespFixedBytes;

int StatusFromBool(bool ok) {
  return ok ? FLUME_OK : FLUME_ERR_REMOTE;
}

bool ConnectTo(const std::string& host, uint16_t port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return false;
  }
  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    close(fd);
    return false;
  }
  bool ok = connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
  close(fd);
  return ok;
}

}  // namespace

StorageAgent::StorageAgent(std::string host, uint16_t port, std::string root)
    : host_(std::move(host)), requested_port_(port), store_(std::move(root)) {}

StorageAgent::~StorageAgent() {
  Stop();
}

bool StorageAgent::Start(std::string* error) {
  if (running_.load()) {
    return true;
  }

  listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    if (error != nullptr) {
      *error = std::string("socket failed: ") + strerror(errno);
    }
    return false;
  }

  int one = 1;
  setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(requested_port_);
  if (inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) {
    if (error != nullptr) {
      *error = "invalid listen host";
    }
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    if (error != nullptr) {
      *error = std::string("bind failed: ") + strerror(errno);
    }
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  socklen_t addr_len = sizeof(addr);
  if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &addr_len) != 0) {
    if (error != nullptr) {
      *error = std::string("getsockname failed: ") + strerror(errno);
    }
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }
  bound_port_ = ntohs(addr.sin_port);

  if (listen(listen_fd_, 16) != 0) {
    if (error != nullptr) {
      *error = std::string("listen failed: ") + strerror(errno);
    }
    close(listen_fd_);
    listen_fd_ = -1;
    return false;
  }

  running_.store(true);
  thread_ = std::thread(&StorageAgent::ServeLoop, this);
  return true;
}

void StorageAgent::Stop() {
  if (!running_.exchange(false)) {
    return;
  }

  if (bound_port_ != 0) {
    (void)ConnectTo(host_, bound_port_);
  }
  if (thread_.joinable()) {
    thread_.join();
  }
  {
    std::lock_guard<std::mutex> lock(client_threads_mu_);
    for (int fd : client_fds_) {
      if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
      }
    }
  }
  std::vector<std::thread> client_threads;
  {
    std::lock_guard<std::mutex> lock(client_threads_mu_);
    client_threads.swap(client_threads_);
  }
  for (auto& client_thread : client_threads) {
    if (client_thread.joinable()) {
      client_thread.join();
    }
  }
  {
    std::lock_guard<std::mutex> lock(client_threads_mu_);
    client_fds_.clear();
  }
  if (listen_fd_ >= 0) {
    close(listen_fd_);
    listen_fd_ = -1;
  }

  std::lock_guard<std::mutex> lock(files_mu_);
  for (auto& item : files_) {
    store_.Close(item.second.fd);
  }
  files_.clear();
}

void StorageAgent::ServeLoop() {
  while (running_.load()) {
    sockaddr_in peer = {};
    socklen_t peer_len = sizeof(peer);
    int fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    if (fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (!running_.load()) {
        break;
      }
      continue;
    }
    if (!running_.load()) {
      close(fd);
      break;
    }
    std::lock_guard<std::mutex> lock(client_threads_mu_);
    client_fds_.push_back(fd);
    client_threads_.emplace_back([this, fd] {
      HandleClient(fd);
      close(fd);
      std::lock_guard<std::mutex> lock(client_threads_mu_);
      auto it = std::find(client_fds_.begin(), client_fds_.end(), fd);
      if (it != client_fds_.end()) {
        client_fds_.erase(it);
      }
    });
  }
}

void StorageAgent::HandleClient(int fd) {
  using protocol::AppendBytes;
  using protocol::AppendString;
  using protocol::AppendU32;
  using protocol::AppendU64;
  using protocol::Checksum32;
  using protocol::Frame;
  using protocol::FrameType;
  using protocol::ReadFrame;
  using protocol::Reader;
  using protocol::WriteFrame;

  std::string io_error;
  Frame frame;
  while (running_.load() && ReadFrame(fd, &frame, &io_error)) {
    if (frame.type == FrameType::kHello) {
      Frame resp;
      resp.type = FrameType::kHelloAck;
      resp.request_id = frame.request_id;
      AppendU32(&resp.body, FLUME_OK);
      AppendString(&resp.body, "mock,hccl-hcomm-planned");
      if (!WriteFrame(fd, resp, nullptr)) {
        break;
      }
      continue;
    }

    if (frame.type == FrameType::kOpenReq) {
      Reader reader(frame.body);
      std::string path;
      if (!reader.ReadString(&path) || !reader.Done()) {
        if (!SendStatus(fd, FrameType::kOpenResp, frame.request_id,
                        FLUME_ERR_PROTOCOL, "malformed OPEN_REQ")) {
          break;
        }
        continue;
      }

      OpenedFile file;
      std::string error;
      bool ok = store_.Open(path, &file, &error);
      Frame resp;
      resp.type = FrameType::kOpenResp;
      resp.request_id = frame.request_id;
      AppendU32(&resp.body, StatusFromBool(ok));
      if (ok) {
        uint64_t file_id = 0;
        uint64_t file_size = file.size;
        {
          std::lock_guard<std::mutex> lock(files_mu_);
          file_id = next_file_id_++;
          files_.emplace(file_id, std::move(file));
        }
        AppendU64(&resp.body, file_id);
        AppendU64(&resp.body, file_size);
        AppendString(&resp.body, "");
      } else {
        AppendU64(&resp.body, 0);
        AppendU64(&resp.body, 0);
        AppendString(&resp.body, error);
      }
      if (!WriteFrame(fd, resp, nullptr)) {
        break;
      }
      continue;
    }

    if (frame.type == FrameType::kReadReq) {
      Reader reader(frame.body);
      uint64_t file_id = 0;
      uint64_t offset = 0;
      uint64_t length = 0;
      if (!reader.ReadU64(&file_id) || !reader.ReadU64(&offset) ||
          !reader.ReadU64(&length) || !reader.Done()) {
        if (!SendStatus(fd, FrameType::kReadResp, frame.request_id,
                        FLUME_ERR_PROTOCOL, "malformed READ_REQ")) {
          break;
        }
        continue;
      }
      if (length > kMaxReadPayloadSize) {
        if (!SendStatus(fd, FrameType::kReadResp, frame.request_id,
                        FLUME_ERR_INVALID_ARGUMENT,
                        "READ_REQ length exceeds maximum response payload")) {
          break;
        }
        continue;
      }

      bool found_file = false;
      int file_fd = -1;
      {
        std::lock_guard<std::mutex> lock(files_mu_);
        auto it = files_.find(file_id);
        if (it != files_.end()) {
          found_file = true;
          file_fd = dup(it->second.fd);
        }
      }
      if (!found_file) {
        if (!SendStatus(fd, FrameType::kReadResp, frame.request_id,
                        FLUME_ERR_REMOTE, "unknown file id")) {
          break;
        }
        continue;
      }
      if (file_fd < 0) {
        if (!SendStatus(fd, FrameType::kReadResp, frame.request_id,
                        FLUME_ERR_IO, std::string("dup failed: ") + strerror(errno))) {
          break;
        }
        continue;
      }

      std::vector<uint8_t> payload;
      std::string error;
      bool ok = store_.Read(file_fd, offset, length, &payload, &error);
      store_.Close(file_fd);
      Frame resp;
      resp.type = FrameType::kReadResp;
      resp.request_id = frame.request_id;
      AppendU32(&resp.body, StatusFromBool(ok));
      if (ok) {
        AppendU64(&resp.body, payload.size());
        AppendU32(&resp.body, Checksum32(payload.data(), payload.size()));
        AppendString(&resp.body, "");
        AppendBytes(&resp.body, payload.data(), payload.size());
      } else {
        AppendU64(&resp.body, 0);
        AppendU32(&resp.body, 0);
        AppendString(&resp.body, error);
      }
      if (!WriteFrame(fd, resp, nullptr)) {
        break;
      }
      continue;
    }

    if (frame.type == FrameType::kCloseReq) {
      Reader reader(frame.body);
      uint64_t file_id = 0;
      if (!reader.ReadU64(&file_id) || !reader.Done()) {
        if (!SendStatus(fd, FrameType::kCloseResp, frame.request_id,
                        FLUME_ERR_PROTOCOL, "malformed CLOSE_REQ")) {
          break;
        }
        continue;
      }
      {
        std::lock_guard<std::mutex> lock(files_mu_);
        auto it = files_.find(file_id);
        if (it != files_.end()) {
          store_.Close(it->second.fd);
          files_.erase(it);
        }
      }
      if (!SendStatus(fd, FrameType::kCloseResp, frame.request_id, FLUME_OK, "")) {
        break;
      }
      continue;
    }

    if (!SendStatus(fd, FrameType::kError, frame.request_id, FLUME_ERR_PROTOCOL,
                    "unknown frame type")) {
      break;
    }
  }
}

bool StorageAgent::SendStatus(int fd, protocol::FrameType type, uint64_t request_id,
                              int status, const std::string& message) {
  protocol::Frame resp;
  resp.type = type;
  resp.request_id = request_id;
  protocol::AppendU32(&resp.body, static_cast<uint32_t>(status));
  if (type == protocol::FrameType::kOpenResp) {
    protocol::AppendU64(&resp.body, 0);
    protocol::AppendU64(&resp.body, 0);
  } else if (type == protocol::FrameType::kReadResp) {
    protocol::AppendU64(&resp.body, 0);
    protocol::AppendU32(&resp.body, 0);
  }
  protocol::AppendString(&resp.body, message);
  return protocol::WriteFrame(fd, resp, nullptr);
}

}  // namespace flume
