#ifndef FLUME_ROCE_STORAGE_CONTROL_PROXY_H_
#define FLUME_ROCE_STORAGE_CONTROL_PROXY_H_

#include <cstddef>
#include <string>

namespace flume::roce {

struct ControlProxyStats {
  size_t session_bytes = 0;
  size_t command_bytes = 0;
  size_t completion_bytes = 0;
  size_t requests = 0;
  size_t payload_bytes = 0;
};

// Proxies one TCP-controlled push session. RDMA endpoints and target HBM
// descriptors pass through unchanged, so payload moves directly between the
// upstream data node and the downstream NPU client.
bool ProxyPushControlSession(int downstream_fd, int upstream_fd,
                             ControlProxyStats* stats, std::string* error);

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_CONTROL_PROXY_H_
