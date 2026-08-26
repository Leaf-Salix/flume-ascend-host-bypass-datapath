#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <string>

#include "roce_storage/control_proxy.h"

namespace {

int Listen(const std::string& ip, uint16_t port, std::string* error) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    *error = "failed to create proxy listener";
    return -1;
  }
  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (inet_pton(AF_INET, ip.c_str(), &address.sin_addr) != 1 ||
      bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(fd, 8) != 0) {
    close(fd);
    *error = "failed to bind/listen on proxy control endpoint";
    return -1;
  }
  return fd;
}

int Connect(const std::string& host, uint16_t port, std::string* error) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* addresses = nullptr;
  const std::string service = std::to_string(port);
  if (getaddrinfo(host.c_str(), service.c_str(), &hints, &addresses) != 0) {
    *error = "failed to resolve upstream storage data node";
    return -1;
  }
  int fd = -1;
  for (addrinfo* address = addresses; address != nullptr;
       address = address->ai_next) {
    fd = socket(address->ai_family, address->ai_socktype,
                address->ai_protocol);
    if (fd < 0) continue;
    if (connect(fd, address->ai_addr, address->ai_addrlen) == 0) break;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(addresses);
  if (fd < 0) *error = "failed to connect upstream storage data node";
  return fd;
}

}  // namespace

int main(int argc, char** argv) {
  std::string listen_ip = "0.0.0.0";
  std::string upstream;
  uint32_t control_port = 0;
  uint32_t upstream_port = 0;
  for (int index = 1; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--listen" && index + 1 < argc) {
      listen_ip = argv[++index];
    } else if (arg == "--control-port" && index + 1 < argc) {
      control_port = std::strtoul(argv[++index], nullptr, 10);
    } else if (arg == "--upstream" && index + 1 < argc) {
      upstream = argv[++index];
    } else if (arg == "--upstream-port" && index + 1 < argc) {
      upstream_port = std::strtoul(argv[++index], nullptr, 10);
    } else {
      std::cerr << "usage: flume-roce-storage-proxy [--listen ip] "
                   "--control-port port --upstream host "
                   "--upstream-port port\n";
      return arg == "--help" ? 0 : 2;
    }
  }
  if (control_port == 0 || control_port > UINT16_MAX || upstream.empty() ||
      upstream_port == 0 || upstream_port > UINT16_MAX) {
    std::cerr << "proxy requires valid control and upstream endpoints\n";
    return 2;
  }

  std::string error;
  const int listener = Listen(listen_ip, static_cast<uint16_t>(control_port),
                              &error);
  if (listener < 0) {
    std::cerr << error << "\n";
    return 1;
  }
  std::cout << "flume storage control proxy waiting: transfer_mode=push "
               "upstream_protocol=flume-roce-v2 "
               "requires_upstream_daemon=yes control_proxy=used "
               "payload_proxy_bytes=0\n";
  const int downstream = accept(listener, nullptr, nullptr);
  close(listener);
  if (downstream < 0) {
    std::cerr << "failed to accept downstream NPU client\n";
    return 1;
  }
  const int upstream_fd = Connect(upstream, static_cast<uint16_t>(upstream_port),
                                  &error);
  if (upstream_fd < 0) {
    close(downstream);
    std::cerr << error << "\n";
    return 1;
  }

  flume::roce::ControlProxyStats stats;
  const bool ok = flume::roce::ProxyPushControlSession(
      downstream, upstream_fd, &stats, &error);
  close(upstream_fd);
  close(downstream);
  if (!ok) {
    std::cerr << "storage control proxy failed: " << error << "\n";
    return 1;
  }
  std::cout << "storage control proxy passed: transfer_mode=push "
            << "upstream_protocol=flume-roce-v2 "
               "requires_upstream_daemon=yes data_plane=remote-rnic "
               "control_proxy=used requests="
            << stats.requests << " control_bytes="
            << (stats.session_bytes + stats.command_bytes +
                stats.completion_bytes)
            << " payload_proxy_bytes=" << stats.payload_bytes
            << " fallback=none\n";
  return 0;
}
