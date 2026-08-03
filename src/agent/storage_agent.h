#ifndef FLUME_AGENT_STORAGE_AGENT_H_
#define FLUME_AGENT_STORAGE_AGENT_H_

#include <atomic>
#include <stdint.h>

#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "protocol/framing.h"
#include "storage/posix_file.h"

namespace flume {

class StorageAgent {
 public:
  StorageAgent(std::string host, uint16_t port, std::string root);
  ~StorageAgent();

  bool Start(std::string* error);
  void Stop();

  uint16_t port() const { return bound_port_; }
  const std::string& host() const { return host_; }

 private:
  void ServeLoop();
  void HandleClient(int fd);
  bool SendStatus(int fd, protocol::FrameType type, uint64_t request_id,
                  int status, const std::string& message);

  std::string host_;
  uint16_t requested_port_ = 0;
  uint16_t bound_port_ = 0;
  PosixFileStore store_;
  int listen_fd_ = -1;
  std::atomic<bool> running_{false};
  std::thread thread_;
  std::mutex client_threads_mu_;
  std::vector<std::thread> client_threads_;
  std::vector<int> client_fds_;
  std::mutex files_mu_;
  uint64_t next_file_id_ = 1;
  std::map<uint64_t, OpenedFile> files_;
};

}  // namespace flume

#endif  // FLUME_AGENT_STORAGE_AGENT_H_
