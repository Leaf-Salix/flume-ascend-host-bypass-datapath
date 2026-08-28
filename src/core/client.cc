#include "flume/flume.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if FLUME_ENABLE_HCCL
#if __has_include(<acl/acl.h>)
#include <acl/acl.h>
#else
#error "FLUME_ENABLE_HCCL=1 requires acl/acl.h"
#endif
#if __has_include(<hccl/hccl.h>)
#include <hccl/hccl.h>
#elif __has_include(<hccl.h>)
#include <hccl.h>
#else
#error "FLUME_ENABLE_HCCL=1 requires hccl/hccl.h or hccl.h"
#endif
#endif

#ifndef FLUME_HAVE_HCCL_SYM_WINDOW
#define FLUME_HAVE_HCCL_SYM_WINDOW 0
#endif
#ifndef FLUME_HAVE_HCCL_COMM_MEMORY
#define FLUME_HAVE_HCCL_COMM_MEMORY 0
#endif
#ifndef FLUME_HAVE_HCCL_P2P
#define FLUME_HAVE_HCCL_P2P 0
#endif
#ifndef FLUME_HAVE_HCCL_COMM_NAME
#define FLUME_HAVE_HCCL_COMM_NAME 0
#endif
#ifndef FLUME_HAVE_HCCL_ROOT_INFO
#define FLUME_HAVE_HCCL_ROOT_INFO 0
#endif
#ifndef FLUME_HAVE_HCCL_COMM_INIT_ALL
#define FLUME_HAVE_HCCL_COMM_INIT_ALL 0
#endif
#ifndef FLUME_HAVE_HCOMM_CHANNEL_RES
#define FLUME_HAVE_HCOMM_CHANNEL_RES 0
#endif
#ifndef FLUME_HAVE_HCOMM_THREAD_EXPORT
#define FLUME_HAVE_HCOMM_THREAD_EXPORT 0
#endif
#ifndef FLUME_HAVE_HCOMM_PRIMITIVES
#define FLUME_HAVE_HCOMM_PRIMITIVES 0
#endif
#ifndef FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD
#define FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD 0
#endif
#ifndef FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY
#define FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY 0
#endif
#ifndef FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY
#define FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY 0
#endif
#ifndef FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI
#define FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI 0
#endif
#ifndef FLUME_HAVE_HCOMM_RANK_GRAPH
#define FLUME_HAVE_HCOMM_RANK_GRAPH 0
#endif
#ifndef FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH
#define FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH 0
#endif
#ifndef FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH
#define FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH 0
#endif
#ifndef FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS
#define FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS 0
#endif
#ifndef FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT
#define FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT 0
#endif

#if FLUME_ENABLE_HCCL && \
    (FLUME_HAVE_HCOMM_CHANNEL_RES || FLUME_HAVE_HCOMM_THREAD_EXPORT)
#if __has_include(<hccl/hccl_res.h>)
#include <hccl/hccl_res.h>
#else
#error "HCOMM resource features require hccl/hccl_res.h"
#endif
#endif

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_COMM_NAME
#if __has_include(<hccl/hccl_comm.h>)
#include <hccl/hccl_comm.h>
#else
#error "FLUME_HAVE_HCCL_COMM_NAME=1 requires hccl/hccl_comm.h"
#endif
#endif

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_RANK_GRAPH
#if __has_include(<hccl/hccl_rank_graph.h>)
#include <hccl/hccl_rank_graph.h>
#else
#error "FLUME_HAVE_HCOMM_RANK_GRAPH=1 requires hccl/hccl_rank_graph.h"
#endif
#endif

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_THREAD_EXPORT
#if __has_include(<hccl/hccl_res_expt.h>)
#include <hccl/hccl_res_expt.h>
#endif
#endif

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_PRIMITIVES
#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#elif __has_include(<hcomm_primitives.h>)
#include <hcomm_primitives.h>
#else
#error "FLUME_HAVE_HCOMM_PRIMITIVES=1 requires hcomm_primitives.h"
#endif
#endif

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH
#if __has_include(<hccl/hccl_launch.h>)
#include <hccl/hccl_launch.h>
#else
#error "FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH=1 requires hccl/hccl_launch.h"
#endif
#endif

#include "protocol/framing.h"
#include "flume_hcomm_notify_only_abi.h"
#include "hcomm_payload/payload_backend.h"
#include "roce_storage/host_ra_session.h"
#include "roce_storage/roce_storage.h"

struct flume_client {
  int fd = -1;
  uint64_t next_request_id = 1;
  std::mutex mu;
  bool hccl_attached = false;
  void* hccl_comm = nullptr;
  bool sim_comm_attached = false;
  std::string sim_comm_name;
  uint32_t rank = 0;
  uint32_t rank_size = 0;
  uint64_t sim_allreduce_seq = 0;
  uint64_t sim_allgather_seq = 0;
  uint64_t sim_p2p_send_seq = 0;
  uint64_t sim_p2p_recv_seq = 0;
  uint64_t sim_hcomm_payload_send_seq = 0;
  uint64_t sim_hcomm_payload_recv_seq = 0;
  uint64_t sim_a3_register_seq = 0;
};

struct flume_file {
  flume_client_t* client = nullptr;
  uint64_t file_id = 0;
  uint64_t size = 0;
  std::string path;
};

struct flume_buffer {
  flume_client_t* client = nullptr;
  void* ptr = nullptr;
  size_t len = 0;
  flume_buffer_type_t type = FLUME_BUFFER_HOST;
  std::vector<uint8_t> owned;
  std::atomic<size_t> a3_symmetric_ref_count{0};
  std::atomic<size_t> pending_ref_count{0};
};

struct flume_a3_symmetric_window {
  flume_client_t* client = nullptr;
  flume_buffer_t* buffer = nullptr;
  void* hccl_window = nullptr;
  size_t offset = 0;
  size_t len = 0;
  bool sim = false;
};

struct flume_storage_block {
  flume_client_t* client = nullptr;
  std::vector<uint8_t> payload;
  uint64_t file_offset = 0;
  bool sim_partial = true;
};

struct flume_storage_target_window {
  flume_client_t* client = nullptr;
  flume_buffer_t* buffer = nullptr;
  size_t offset = 0;
  size_t len = 0;
  uint64_t sim_lkey = 0;
  uint64_t sim_rkey = 0;
  std::atomic<size_t> plan_ref_count{0};
};

struct flume_storage_direct_plan {
  flume_client_t* client = nullptr;
  flume_storage_block_t* block = nullptr;
  flume_buffer_t* dst = nullptr;
  flume_storage_target_window_t* target_window = nullptr;
  size_t dst_offset = 0;
  flume_storage_transfer_path_t path = FLUME_STORAGE_TRANSFER_AUTO;
  flume_storage_direct_role_t role = FLUME_STORAGE_DIRECT_ROLE_AUTO;
  uint32_t peer_rank = 0;
  bool require_direct = false;
  bool allow_host_staging = true;
  size_t bytes = 0;
};

struct flume_roce_storage_session {
  static constexpr uint64_t kIdle = 0;
  static constexpr uint64_t kActive = 1;
  static constexpr uint64_t kClosing = std::numeric_limits<uint64_t>::max();

  flume_client_t* client = nullptr;
  std::string storage_server;
  std::string npu_rnic_ip;
  std::string storage_rnic_ip;
  uint32_t npu_device = 0;
  int32_t npu_physical_device = -1;
  uint32_t gid_index = 0;
  uint8_t path_mtu = flume::roce::kDefaultPathMtu;
  uint32_t bootstrap_port = 0;
  uint32_t timeout_ms = 0;
  flume_roce_post_mode_t post_mode = FLUME_ROCE_POST_AUTO;
  flume_roce_control_mode_t control_mode = FLUME_ROCE_CONTROL_TCP;
  flume_roce_transfer_mode_t transfer_mode = FLUME_ROCE_TRANSFER_PUSH;
  flume_roce_storage_backend_t storage_backend = FLUME_ROCE_STORAGE_MEMORY;
  bool require_compute_host_bypass = true;
  std::shared_ptr<flume::roce::HostRaSession> host_ra;
  std::string native_open_error;
  std::atomic<uint64_t> next_request_id{1};
  std::atomic<uint64_t> request_state{kIdle};
};

struct flume_io {
  mutable std::mutex mu;
  std::condition_variable cv;
  bool done = true;
  int status = FLUME_OK;
  size_t bytes = 0;
  uint32_t checksum = 0;
  std::string error;
  std::atomic<size_t> pending_ref_count{0};
  bool sync_acl_stream = false;
  bool sync_started = false;
  void* acl_stream = nullptr;
};

namespace {

using flume::protocol::AppendString;
using flume::protocol::AppendU32;
using flume::protocol::AppendU64;
using flume::protocol::Frame;
using flume::protocol::FrameType;
using flume::protocol::ReadFrame;
using flume::protocol::Reader;
using flume::protocol::WriteFrame;

constexpr int kSocketTimeoutSeconds = 30;
constexpr uint32_t kDefaultHcommTimeoutSeconds = 60;
constexpr const char* kDefaultHcommPayloadBatchTag =
    FLUME_HCOMM_PAYLOAD_DEFAULT_BATCH_TAG;

constexpr bool HcommWriteWithNotifyUsesNbiBackend() {
#if FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY
  return false;
#elif FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI
  return true;
#else
  return false;
#endif
}

constexpr const char* HcommWriteWithNotifyBackendName() {
#if FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY
  return "blocking";
#elif FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI
  return "nbi";
#else
  return "missing";
#endif
}

struct CommState {
  bool hccl_attached = false;
  void* hccl_comm = nullptr;
  bool sim_comm_attached = false;
  std::string sim_comm_name;
  uint32_t rank = 0;
  uint32_t rank_size = 0;
};

struct HcommProbeOptions {
  uint32_t notify_num = 2;
  flume_hcomm_engine_t engine = FLUME_HCOMM_ENGINE_AUTO;
  flume_hcomm_protocol_t protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  bool require_thread_export = false;
  uint32_t timeout_sec = kDefaultHcommTimeoutSeconds;
  bool disable_payload_batch_mode = false;
  std::string payload_batch_tag = kDefaultHcommPayloadBatchTag;
  bool payload_recv_direct_output = false;
  bool payload_skip_comm_acquire = false;
  flume_hcomm_payload_comm_binding_t payload_comm_binding =
      FLUME_HCOMM_PAYLOAD_COMM_BINDING_COMM_NAME;
  bool payload_force_channel_fence = false;
  bool payload_write_path = false;
  bool payload_write_with_notify = false;
};

struct HcommChannelResourceInfo {
  size_t usable_buffer_bytes = 0;
  uint64_t local_buffer_bytes = 0;
  uint64_t remote_buffer_bytes = 0;
  void* local_buffer = nullptr;
  void* remote_buffer = nullptr;
  uint64_t channel_handle = 0;
  uint64_t cpu_ts_thread = 0;
  uint64_t aicpu_ts_thread = 0;
  uint64_t cpu_thread_on_aicpu = 0;
  uint64_t aicpu_thread_on_cpu = 0;
  uint32_t channel_count = 0;
  uint32_t notify_num = 0;
  flume_hcomm_engine_t resolved_engine = FLUME_HCOMM_ENGINE_AUTO;
  flume_hcomm_protocol_t resolved_protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  bool thread_export_required = false;
  bool host_thread_notify_ready = false;
  uint32_t timeout_sec = kDefaultHcommTimeoutSeconds;
  std::string channel_desc_source;
};

bool SetSocketTimeouts(int fd) {
  timeval timeout = {};
  timeout.tv_sec = kSocketTimeoutSeconds;
  return setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0 &&
         setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0;
}

CommState SnapshotCommState(flume_client_t* client) {
  std::lock_guard<std::mutex> lock(client->mu);
  CommState state;
  state.hccl_attached = client->hccl_attached;
  state.hccl_comm = client->hccl_comm;
  state.sim_comm_attached = client->sim_comm_attached;
  state.sim_comm_name = client->sim_comm_name;
  state.rank = client->rank;
  state.rank_size = client->rank_size;
  return state;
}

#if FLUME_ENABLE_HCCL && FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT
bool IsAclStreamSyncTimeout(aclError ret) {
#ifdef ACL_ERROR_RT_STREAM_SYNC_TIMEOUT
  return ret == ACL_ERROR_RT_STREAM_SYNC_TIMEOUT;
#else
  (void)ret;
  return false;
#endif
}
#endif

int ConnectEndpoint(const char* endpoint, int* out_fd) {
  if (endpoint == nullptr || out_fd == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  std::string spec(endpoint);
  const std::string prefix = "tcp://";
  if (spec.compare(0, prefix.size(), prefix) == 0) {
    spec.erase(0, prefix.size());
  }
  auto colon = spec.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= spec.size()) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  std::string host = spec.substr(0, colon);
  int port_int = 0;
  try {
    port_int = std::stoi(spec.substr(colon + 1));
  } catch (...) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (port_int <= 0 || port_int > 65535) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return FLUME_ERR_IO;
  }

  sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port_int));
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    close(fd);
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    close(fd);
    return FLUME_ERR_IO;
  }
  if (!SetSocketTimeouts(fd)) {
    close(fd);
    return FLUME_ERR_IO;
  }
  *out_fd = fd;
  return FLUME_OK;
}

int ReadStatusString(const Frame& frame, int* status, std::string* message) {
  Reader reader(frame.body);
  uint32_t raw_status = 0;
  if (!reader.ReadU32(&raw_status)) {
    return FLUME_ERR_PROTOCOL;
  }
  *status = static_cast<int>(raw_status);
  if (message != nullptr) {
    *message = "";
  }
  return FLUME_OK;
}

uint64_t NextRequestId(flume_client_t* client) {
  return client->next_request_id++;
}

bool IsSimBufferType(flume_buffer_type_t type) {
  return type == FLUME_BUFFER_SIM_HBM || type == FLUME_BUFFER_SIM_HCCL_COMM;
}

bool IsReadableLocalType(flume_buffer_type_t type) {
  return type == FLUME_BUFFER_HOST || IsSimBufferType(type);
}

bool IsSimHbmCopyType(flume_buffer_type_t type) {
  return IsSimBufferType(type);
}

bool IsRealHcclBufferType(flume_buffer_type_t type) {
  return type == FLUME_BUFFER_ASCEND_HBM || type == FLUME_BUFFER_HCCL_COMM;
}

bool IsRegisterableBufferType(flume_buffer_type_t type) {
#if FLUME_ENABLE_HCCL
  return type == FLUME_BUFFER_HOST || IsSimBufferType(type) || IsRealHcclBufferType(type);
#else
  return type == FLUME_BUFFER_HOST || IsSimBufferType(type);
#endif
}

bool IsCollectiveBufferType(flume_buffer_type_t type) {
  return IsSimBufferType(type) || IsRealHcclBufferType(type);
}

bool IsSupportedReduceOp(flume_reduce_op_t op) {
  switch (op) {
    case FLUME_REDUCE_SUM:
    case FLUME_REDUCE_PROD:
    case FLUME_REDUCE_MAX:
    case FLUME_REDUCE_MIN:
      return true;
    default:
      return false;
  }
}

const char* FlumeHcommEngineName(flume_hcomm_engine_t engine) {
  switch (engine) {
    case FLUME_HCOMM_ENGINE_AUTO:
      return "auto";
    case FLUME_HCOMM_ENGINE_AICPU:
      return "aicpu";
    case FLUME_HCOMM_ENGINE_AICPU_TS:
      return "aicpu-ts";
    case FLUME_HCOMM_ENGINE_CPU:
      return "cpu";
    case FLUME_HCOMM_ENGINE_CPU_TS:
      return "cpu-ts";
  }
  return "unknown";
}

const char* FlumeHcommProtocolName(flume_hcomm_protocol_t protocol) {
  switch (protocol) {
    case FLUME_HCOMM_PROTOCOL_AUTO:
      return "auto";
    case FLUME_HCOMM_PROTOCOL_HCCS:
      return "hccs";
    case FLUME_HCOMM_PROTOCOL_ROCE:
      return "roce";
    case FLUME_HCOMM_PROTOCOL_PCIE:
      return "pcie";
    case FLUME_HCOMM_PROTOCOL_SIO:
      return "sio";
    case FLUME_HCOMM_PROTOCOL_HCCS_ONLY:
      return "hccs-only";
  }
  return "unknown";
}

size_t DataTypeBytes(flume_data_type_t data_type) {
  switch (data_type) {
    case FLUME_DTYPE_INT8:
    case FLUME_DTYPE_UINT8:
      return 1;
    case FLUME_DTYPE_INT16:
    case FLUME_DTYPE_UINT16:
    case FLUME_DTYPE_FP16:
    case FLUME_DTYPE_BFP16:
      return 2;
    case FLUME_DTYPE_INT32:
    case FLUME_DTYPE_UINT32:
    case FLUME_DTYPE_FP32:
      return 4;
    case FLUME_DTYPE_INT64:
    case FLUME_DTYPE_UINT64:
    case FLUME_DTYPE_FP64:
      return 8;
    default:
      return 0;
  }
}

bool CheckedBytes(uint64_t count, flume_data_type_t data_type, size_t* out) {
  if (count == 0 || out == nullptr) {
    return false;
  }
  size_t elem = DataTypeBytes(data_type);
  if (elem == 0 || count > std::numeric_limits<size_t>::max() / elem) {
    return false;
  }
  *out = static_cast<size_t>(count) * elem;
  return true;
}

bool CheckedMul(size_t lhs, size_t rhs, size_t* out) {
  if (out == nullptr || (rhs != 0 && lhs > std::numeric_limits<size_t>::max() / rhs)) {
    return false;
  }
  *out = lhs * rhs;
  return true;
}

const char* StorageDirectRoleName(flume_storage_direct_role_t role) {
  switch (role) {
    case FLUME_STORAGE_DIRECT_ROLE_AUTO:
      return "auto";
    case FLUME_STORAGE_DIRECT_ROLE_SOURCE_SEND:
      return "source-send";
    case FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV:
      return "target-recv";
    case FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING:
      return "host-staging";
  }
  return "unknown";
}

bool ValidateRange(const flume_buffer_t* buffer, size_t offset, size_t len) {
  return buffer != nullptr && offset <= buffer->len && len <= buffer->len - offset;
}

bool NormalizeHcommProbeOptions(
    const flume_hcomm_channel_probe_options_t* options,
    HcommProbeOptions* out,
    std::string* error) {
  if (out == nullptr) {
    return false;
  }
  HcommProbeOptions normalized;
  if (options != nullptr && options->size != 0) {
    const size_t min_size =
        offsetof(flume_hcomm_channel_probe_options_t, notify_num) +
        sizeof(options->notify_num);
    if (options->size < min_size) {
      if (error != nullptr) {
        *error = "HCOMM channel probe options size is too small";
      }
      return false;
    }
    normalized.notify_num = options->notify_num == 0 ? 2 : options->notify_num;
    if (options->size >= offsetof(flume_hcomm_channel_probe_options_t, engine) +
                             sizeof(options->engine)) {
      normalized.engine = options->engine;
    }
    if (options->size >= offsetof(flume_hcomm_channel_probe_options_t, protocol) +
                             sizeof(options->protocol)) {
      normalized.protocol = options->protocol == FLUME_HCOMM_PROTOCOL_AUTO ?
                                FLUME_HCOMM_PROTOCOL_HCCS :
                                options->protocol;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t, require_thread_export) +
            sizeof(options->require_thread_export)) {
      normalized.require_thread_export = options->require_thread_export != 0;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t, timeout_sec) +
            sizeof(options->timeout_sec)) {
      normalized.timeout_sec =
          options->timeout_sec == 0 ? kDefaultHcommTimeoutSeconds :
                                      options->timeout_sec;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t,
                 disable_payload_batch_mode) +
            sizeof(options->disable_payload_batch_mode)) {
      normalized.disable_payload_batch_mode =
          options->disable_payload_batch_mode != 0;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t, payload_batch_tag) +
            sizeof(options->payload_batch_tag) &&
        options->payload_batch_tag != nullptr) {
      normalized.payload_batch_tag = options->payload_batch_tag;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t,
                 payload_recv_direct_output) +
            sizeof(options->payload_recv_direct_output)) {
      normalized.payload_recv_direct_output =
          options->payload_recv_direct_output != 0;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t,
                 payload_skip_comm_acquire) +
            sizeof(options->payload_skip_comm_acquire)) {
      normalized.payload_skip_comm_acquire =
          options->payload_skip_comm_acquire != 0;
      if (normalized.payload_skip_comm_acquire) {
        normalized.payload_comm_binding =
            FLUME_HCOMM_PAYLOAD_COMM_BINDING_DIAGNOSTIC_SKIP;
      }
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t,
                 payload_comm_binding) +
            sizeof(options->payload_comm_binding)) {
      normalized.payload_comm_binding = options->payload_comm_binding;
      normalized.payload_skip_comm_acquire =
          normalized.payload_comm_binding !=
          FLUME_HCOMM_PAYLOAD_COMM_BINDING_COMM_NAME;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t,
                 payload_force_channel_fence) +
            sizeof(options->payload_force_channel_fence)) {
      normalized.payload_force_channel_fence =
          options->payload_force_channel_fence != 0;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t,
                 payload_write_path) +
            sizeof(options->payload_write_path)) {
      normalized.payload_write_path = options->payload_write_path != 0;
    }
    if (options->size >=
        offsetof(flume_hcomm_channel_probe_options_t,
                 payload_write_with_notify) +
            sizeof(options->payload_write_with_notify)) {
      normalized.payload_write_with_notify =
          options->payload_write_with_notify != 0;
      if (normalized.payload_write_with_notify) {
        normalized.payload_write_path = true;
      }
    }
  }

  if (normalized.notify_num == 0 || normalized.notify_num > 64) {
    if (error != nullptr) {
      *error = "HCOMM channel probe notify_num must be in [1, 64]";
    }
    return false;
  }
  switch (normalized.engine) {
    case FLUME_HCOMM_ENGINE_AUTO:
    case FLUME_HCOMM_ENGINE_AICPU:
    case FLUME_HCOMM_ENGINE_AICPU_TS:
    case FLUME_HCOMM_ENGINE_CPU:
    case FLUME_HCOMM_ENGINE_CPU_TS:
      break;
    default:
      if (error != nullptr) {
        *error = "unsupported HCOMM channel probe engine";
      }
      return false;
  }
  switch (normalized.protocol) {
    case FLUME_HCOMM_PROTOCOL_HCCS:
    case FLUME_HCOMM_PROTOCOL_ROCE:
    case FLUME_HCOMM_PROTOCOL_PCIE:
    case FLUME_HCOMM_PROTOCOL_SIO:
    case FLUME_HCOMM_PROTOCOL_HCCS_ONLY:
      break;
    default:
      if (error != nullptr) {
        *error = "unsupported HCOMM channel probe protocol";
      }
      return false;
  }
  if (normalized.timeout_sec == 0 || normalized.timeout_sec > 86400) {
    if (error != nullptr) {
      *error = "HCOMM channel probe timeout_sec must be in [1, 86400]";
    }
    return false;
  }
  if (normalized.payload_batch_tag.size() >=
      FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES) {
    if (error != nullptr) {
      *error = "HCOMM payload batch tag is too long";
    }
    return false;
  }
  if (normalized.payload_comm_binding !=
          FLUME_HCOMM_PAYLOAD_COMM_BINDING_COMM_NAME &&
      normalized.payload_comm_binding !=
          FLUME_HCOMM_PAYLOAD_COMM_BINDING_DIAGNOSTIC_SKIP &&
      normalized.payload_comm_binding !=
          FLUME_HCOMM_PAYLOAD_COMM_BINDING_CHANNEL_HANDLE) {
    if (error != nullptr) {
      *error = "unsupported HCOMM payload comm binding";
    }
    return false;
  }
  *out = normalized;
  return true;
}

flume_io* MakeIo(int status = FLUME_OK, size_t bytes = 0, uint32_t checksum = 0,
                std::string error = "") {
  auto* io = new flume_io;
  io->done = true;
  io->status = status;
  io->bytes = bytes;
  io->checksum = checksum;
  io->error = std::move(error);
  return io;
}

flume_io* MakePendingIo() {
  auto* io = new flume_io;
  io->done = false;
  io->status = FLUME_PENDING;
  return io;
}

flume_io* MakeAclStreamIo(void* acl_stream, size_t bytes) {
  auto* io = MakePendingIo();
  io->sync_acl_stream = true;
  io->acl_stream = acl_stream;
  io->bytes = bytes;
  return io;
}

void CompleteIo(flume_io* io, int status, size_t bytes, uint32_t checksum,
                std::string error = "") {
  if (io == nullptr) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(io->mu);
    io->status = status;
    io->bytes = bytes;
    io->checksum = checksum;
    io->error = std::move(error);
    io->done = true;
  }
  io->cv.notify_all();
}

void RetainPendingBuffer(flume_buffer_t* buffer) {
  if (buffer != nullptr) {
    buffer->pending_ref_count.fetch_add(1, std::memory_order_relaxed);
  }
}

void ReleasePendingBuffer(flume_buffer_t* buffer) {
  if (buffer != nullptr) {
    buffer->pending_ref_count.fetch_sub(1, std::memory_order_relaxed);
  }
}

void RetainPendingIo(flume_io_t* io) {
  if (io != nullptr) {
    io->pending_ref_count.fetch_add(1, std::memory_order_relaxed);
  }
}

void CompletePendingIo(flume_io* io, int status, size_t bytes,
                       uint32_t checksum, std::string error = "") {
  if (io == nullptr) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(io->mu);
    io->status = status;
    io->bytes = bytes;
    io->checksum = checksum;
    io->error = std::move(error);
    io->done = true;
    io->pending_ref_count.fetch_sub(1, std::memory_order_relaxed);
  }
  io->cv.notify_all();
}

template <typename T>
T ReadValue(const std::vector<uint8_t>& payload, size_t index) {
  T value;
  memcpy(&value, payload.data() + index * sizeof(T), sizeof(T));
  return value;
}

template <typename T>
void WriteValue(std::vector<uint8_t>* payload, size_t index, T value) {
  memcpy(payload->data() + index * sizeof(T), &value, sizeof(T));
}

template <typename T>
T ApplyReduce(T lhs, T rhs, flume_reduce_op_t op) {
  switch (op) {
    case FLUME_REDUCE_SUM:
      return lhs + rhs;
    case FLUME_REDUCE_PROD:
      return lhs * rhs;
    case FLUME_REDUCE_MAX:
      return std::max(lhs, rhs);
    case FLUME_REDUCE_MIN:
      return std::min(lhs, rhs);
    default:
      return lhs;
  }
}

enum class SimCollectiveKind {
  kAllReduce = 0,
  kAllGather = 1,
};

struct SimCollectiveKey {
  std::string comm_name;
  uint32_t rank_size = 0;
  uint64_t seq = 0;
  SimCollectiveKind kind = SimCollectiveKind::kAllReduce;

  bool operator<(const SimCollectiveKey& other) const {
    if (comm_name != other.comm_name) {
      return comm_name < other.comm_name;
    }
    if (rank_size != other.rank_size) {
      return rank_size < other.rank_size;
    }
    if (seq != other.seq) {
      return seq < other.seq;
    }
    return static_cast<int>(kind) < static_cast<int>(other.kind);
  }
};

struct PendingSimCollective {
  SimCollectiveKind kind = SimCollectiveKind::kAllReduce;
  uint32_t rank_size = 0;
  uint64_t count = 0;
  flume_data_type_t data_type = FLUME_DTYPE_INT8;
  flume_reduce_op_t reduce_op = FLUME_REDUCE_SUM;
  size_t input_bytes = 0;
  size_t output_bytes = 0;
  uint32_t arrivals = 0;
  std::vector<bool> arrived;
  std::vector<std::vector<uint8_t>> payloads;
  std::vector<flume_buffer_t*> dsts;
  std::vector<size_t> dst_offsets;
  std::vector<flume_io_t*> ios;
};

struct SimA3RegistrationKey {
  std::string comm_name;
  uint32_t rank_size = 0;
  uint64_t seq = 0;

  bool operator<(const SimA3RegistrationKey& other) const {
    if (comm_name != other.comm_name) {
      return comm_name < other.comm_name;
    }
    if (rank_size != other.rank_size) {
      return rank_size < other.rank_size;
    }
    return seq < other.seq;
  }
};

struct PendingSimA3Registration {
  uint32_t rank_size = 0;
  size_t offset = 0;
  size_t len = 0;
  uint32_t arrivals = 0;
  std::vector<bool> arrived;
};

struct SimP2pKey {
  std::string comm_name;
  uint32_t rank_size = 0;
  uint32_t src_rank = 0;
  uint32_t dst_rank = 0;
  uint64_t seq = 0;

  bool operator<(const SimP2pKey& other) const {
    if (comm_name != other.comm_name) {
      return comm_name < other.comm_name;
    }
    if (rank_size != other.rank_size) {
      return rank_size < other.rank_size;
    }
    if (src_rank != other.src_rank) {
      return src_rank < other.src_rank;
    }
    if (dst_rank != other.dst_rank) {
      return dst_rank < other.dst_rank;
    }
    return seq < other.seq;
  }
};

struct PendingSimP2p {
  bool metadata_set = false;
  uint64_t count = 0;
  flume_data_type_t data_type = FLUME_DTYPE_INT8;
  size_t bytes = 0;
  bool has_send = false;
  bool has_recv = false;
  std::vector<uint8_t> payload;
  flume_buffer_t* dst = nullptr;
  size_t dst_offset = 0;
  flume_io_t* send_io = nullptr;
  flume_io_t* recv_io = nullptr;
};

std::mutex& SimCollectiveMutex() {
  static auto* mu = new std::mutex;
  return *mu;
}

std::map<SimCollectiveKey, std::unique_ptr<PendingSimCollective>>&
SimCollectives() {
  static auto* collectives =
      new std::map<SimCollectiveKey, std::unique_ptr<PendingSimCollective>>;
  return *collectives;
}

std::mutex& SimA3RegistrationMutex() {
  static auto* mu = new std::mutex;
  return *mu;
}

std::map<SimA3RegistrationKey, std::unique_ptr<PendingSimA3Registration>>&
SimA3Registrations() {
  static auto* registrations =
      new std::map<SimA3RegistrationKey, std::unique_ptr<PendingSimA3Registration>>;
  return *registrations;
}

std::mutex& SimP2pMutex() {
  static auto* mu = new std::mutex;
  return *mu;
}

std::map<SimP2pKey, std::unique_ptr<PendingSimP2p>>& SimP2ps() {
  static auto* p2ps = new std::map<SimP2pKey, std::unique_ptr<PendingSimP2p>>;
  return *p2ps;
}

std::mutex& SimHcommPayloadMutex() {
  static auto* mu = new std::mutex;
  return *mu;
}

std::map<SimP2pKey, std::unique_ptr<PendingSimP2p>>& SimHcommPayloads() {
  static auto* payloads =
      new std::map<SimP2pKey, std::unique_ptr<PendingSimP2p>>;
  return *payloads;
}

bool PendingSimP2pMatches(const PendingSimP2p& pending,
                          uint64_t count,
                          flume_data_type_t data_type,
                          size_t bytes) {
  return !pending.metadata_set ||
         (pending.count == count && pending.data_type == data_type &&
          pending.bytes == bytes);
}

void MaybeSetPendingSimP2pMetadata(PendingSimP2p* pending,
                                   uint64_t count,
                                   flume_data_type_t data_type,
                                   size_t bytes) {
  if (pending == nullptr || pending->metadata_set) {
    return;
  }
  pending->metadata_set = true;
  pending->count = count;
  pending->data_type = data_type;
  pending->bytes = bytes;
}

void FailPendingSimP2p(PendingSimP2p& pending, int status,
                       const std::string& error) {
  if (pending.has_recv && pending.dst != nullptr) {
    ReleasePendingBuffer(pending.dst);
  }
  if (pending.send_io != nullptr) {
    CompletePendingIo(pending.send_io, status, 0, 0, error);
  }
  if (pending.recv_io != nullptr) {
    CompletePendingIo(pending.recv_io, status, 0, 0, error);
  }
}

void CompletePendingSimP2p(PendingSimP2p& pending) {
  auto* dst = static_cast<uint8_t*>(pending.dst->ptr) + pending.dst_offset;
  memcpy(dst, pending.payload.data(), pending.bytes);
  uint32_t checksum = flume::protocol::Checksum32(dst, pending.bytes);
  ReleasePendingBuffer(pending.dst);
  CompletePendingIo(pending.recv_io, FLUME_OK, pending.bytes, checksum);
  CompletePendingIo(pending.send_io, FLUME_OK, pending.bytes, checksum);
}

int SubmitSimA3Registration(const std::string& comm_name,
                            uint32_t rank,
                            uint32_t rank_size,
                            uint64_t seq,
                            size_t offset,
                            size_t len) {
  SimA3RegistrationKey key{comm_name, rank_size, seq};
  auto& map = SimA3Registrations();
  std::lock_guard<std::mutex> lock(SimA3RegistrationMutex());
  auto inserted = map.emplace(key, nullptr);
  if (inserted.second) {
    inserted.first->second = std::make_unique<PendingSimA3Registration>();
    inserted.first->second->rank_size = rank_size;
    inserted.first->second->offset = offset;
    inserted.first->second->len = len;
    inserted.first->second->arrived.assign(rank_size, false);
  }

  PendingSimA3Registration& pending = *inserted.first->second;
  if (rank >= pending.rank_size || pending.arrived[rank] ||
      pending.offset != offset || pending.len != len) {
    map.erase(inserted.first);
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  pending.arrived[rank] = true;
  ++pending.arrivals;
  if (pending.arrivals == pending.rank_size) {
    map.erase(inserted.first);
  }
  return FLUME_OK;
}

template <typename T>
void SimReduceTyped(const PendingSimCollective& pending, std::vector<uint8_t>* output) {
  output->assign(pending.input_bytes, 0);
  const size_t elements = static_cast<size_t>(pending.count);
  for (size_t i = 0; i < elements; ++i) {
    T acc = ReadValue<T>(pending.payloads[0], i);
    for (uint32_t rank = 1; rank < pending.rank_size; ++rank) {
      acc = ApplyReduce(acc, ReadValue<T>(pending.payloads[rank], i),
                        pending.reduce_op);
    }
    WriteValue(output, i, acc);
  }
}

bool SimReducePayloads(const PendingSimCollective& pending,
                       std::vector<uint8_t>* output,
                       std::string* error) {
  if (pending.rank_size == 1) {
    *output = pending.payloads[0];
    return true;
  }

  switch (pending.data_type) {
    case FLUME_DTYPE_INT8:
      SimReduceTyped<int8_t>(pending, output);
      return true;
    case FLUME_DTYPE_UINT8:
      SimReduceTyped<uint8_t>(pending, output);
      return true;
    case FLUME_DTYPE_INT16:
      SimReduceTyped<int16_t>(pending, output);
      return true;
    case FLUME_DTYPE_UINT16:
      SimReduceTyped<uint16_t>(pending, output);
      return true;
    case FLUME_DTYPE_INT32:
      SimReduceTyped<int32_t>(pending, output);
      return true;
    case FLUME_DTYPE_UINT32:
      SimReduceTyped<uint32_t>(pending, output);
      return true;
    case FLUME_DTYPE_INT64:
      SimReduceTyped<int64_t>(pending, output);
      return true;
    case FLUME_DTYPE_UINT64:
      SimReduceTyped<uint64_t>(pending, output);
      return true;
    case FLUME_DTYPE_FP32:
      SimReduceTyped<float>(pending, output);
      return true;
    case FLUME_DTYPE_FP64:
      SimReduceTyped<double>(pending, output);
      return true;
    case FLUME_DTYPE_FP16:
    case FLUME_DTYPE_BFP16:
      if (error != nullptr) {
        *error = "sim allreduce for fp16/bfp16 with more than one rank is not implemented";
      }
      return false;
    default:
      if (error != nullptr) {
        *error = "unsupported sim allreduce data type";
      }
      return false;
  }
}

void FailPendingSimCollective(PendingSimCollective& pending, int status,
                              const std::string& error) {
  for (uint32_t rank = 0; rank < pending.rank_size; ++rank) {
    if (pending.ios[rank] != nullptr) {
      ReleasePendingBuffer(pending.dsts[rank]);
      CompletePendingIo(pending.ios[rank], status, 0, 0, error);
    }
  }
}

bool CompletePendingSimCollective(PendingSimCollective& pending,
                                  std::string* error) {
  if (pending.kind == SimCollectiveKind::kAllReduce) {
    std::vector<uint8_t> output;
    if (!SimReducePayloads(pending, &output, error)) {
      return false;
    }
    for (uint32_t rank = 0; rank < pending.rank_size; ++rank) {
      auto* dst = static_cast<uint8_t*>(pending.dsts[rank]->ptr) +
                  pending.dst_offsets[rank];
      memcpy(dst, output.data(), output.size());
      uint32_t checksum = flume::protocol::Checksum32(dst, output.size());
      ReleasePendingBuffer(pending.dsts[rank]);
      CompletePendingIo(pending.ios[rank], FLUME_OK, output.size(), checksum);
    }
    return true;
  }

  std::vector<uint8_t> output(pending.output_bytes, 0);
  for (uint32_t rank = 0; rank < pending.rank_size; ++rank) {
    memcpy(output.data() + static_cast<size_t>(rank) * pending.input_bytes,
           pending.payloads[rank].data(), pending.input_bytes);
  }
  for (uint32_t rank = 0; rank < pending.rank_size; ++rank) {
    auto* dst = static_cast<uint8_t*>(pending.dsts[rank]->ptr) +
                pending.dst_offsets[rank];
    memcpy(dst, output.data(), output.size());
    uint32_t checksum = flume::protocol::Checksum32(dst, output.size());
    ReleasePendingBuffer(pending.dsts[rank]);
    CompletePendingIo(pending.ios[rank], FLUME_OK, output.size(), checksum);
  }
  return true;
}

void InitPendingSimCollective(PendingSimCollective* pending,
                              SimCollectiveKind kind,
                              uint32_t rank_size,
                              uint64_t count,
                              flume_data_type_t data_type,
                              flume_reduce_op_t reduce_op,
                              size_t input_bytes,
                              size_t output_bytes) {
  pending->kind = kind;
  pending->rank_size = rank_size;
  pending->count = count;
  pending->data_type = data_type;
  pending->reduce_op = reduce_op;
  pending->input_bytes = input_bytes;
  pending->output_bytes = output_bytes;
  pending->arrived.assign(rank_size, false);
  pending->payloads.resize(rank_size);
  pending->dsts.assign(rank_size, nullptr);
  pending->dst_offsets.assign(rank_size, 0);
  pending->ios.assign(rank_size, nullptr);
}

bool PendingMatches(const PendingSimCollective& pending,
                    SimCollectiveKind kind,
                    uint64_t count,
                    flume_data_type_t data_type,
                    flume_reduce_op_t reduce_op,
                    size_t input_bytes,
                    size_t output_bytes) {
  return pending.kind == kind && pending.count == count &&
         pending.data_type == data_type && pending.reduce_op == reduce_op &&
         pending.input_bytes == input_bytes && pending.output_bytes == output_bytes;
}

int SubmitSimCollective(flume_client_t* client,
                        SimCollectiveKind kind,
                        flume_buffer_t* dst,
                        size_t dst_offset,
                        flume_buffer_t* src,
                        size_t src_offset,
                        uint64_t count,
                        flume_data_type_t data_type,
                        flume_reduce_op_t reduce_op,
                        flume_io_t** out) {
  std::string comm_name;
  uint32_t rank = 0;
  uint32_t rank_size = 0;
  {
    std::lock_guard<std::mutex> lock(client->mu);
    if (!client->sim_comm_attached || client->rank_size == 0) {
      *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                    "sim collective requires flume_attach_sim_comm");
      return FLUME_OK;
    }
    comm_name = client->sim_comm_name;
    rank = client->rank;
    rank_size = client->rank_size;
  }

  size_t input_bytes = 0;
  if (kind == SimCollectiveKind::kAllReduce &&
      !IsSupportedReduceOp(reduce_op)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (!CheckedBytes(count, data_type, &input_bytes)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  size_t output_bytes = input_bytes;
  if (kind == SimCollectiveKind::kAllGather &&
      !CheckedMul(input_bytes, rank_size, &output_bytes)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (!ValidateRange(src, src_offset, input_bytes) ||
      !ValidateRange(dst, dst_offset, output_bytes)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (!IsSimBufferType(src->type) || !IsSimBufferType(dst->type)) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "sim collective requires simulated HBM buffers");
    return FLUME_OK;
  }

  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(client->mu);
    seq = (kind == SimCollectiveKind::kAllReduce) ?
              client->sim_allreduce_seq++ :
              client->sim_allgather_seq++;
  }

  std::vector<uint8_t> payload(input_bytes);
  memcpy(payload.data(), static_cast<const uint8_t*>(src->ptr) + src_offset,
         input_bytes);

  auto* io = MakePendingIo();
  *out = io;

  SimCollectiveKey key{comm_name, rank_size, seq, kind};
  auto& map = SimCollectives();
  std::lock_guard<std::mutex> lock(SimCollectiveMutex());
  auto inserted = map.emplace(key, nullptr);
  if (inserted.second) {
    inserted.first->second = std::make_unique<PendingSimCollective>();
    InitPendingSimCollective(inserted.first->second.get(), kind, rank_size, count,
                             data_type, reduce_op, input_bytes, output_bytes);
  }

  PendingSimCollective& pending = *inserted.first->second;
  if (!PendingMatches(pending, kind, count, data_type, reduce_op,
                      input_bytes, output_bytes)) {
    FailPendingSimCollective(pending, FLUME_ERR_INVALID_ARGUMENT,
                             "sim collective parameters do not match prior rank submission");
    map.erase(inserted.first);
    CompleteIo(io, FLUME_ERR_INVALID_ARGUMENT, 0, 0,
               "sim collective parameters do not match prior rank submission");
    return FLUME_OK;
  }
  if (rank >= pending.rank_size || pending.arrived[rank]) {
    FailPendingSimCollective(pending, FLUME_ERR_INVALID_ARGUMENT,
                             "duplicate or invalid sim collective rank submission");
    map.erase(inserted.first);
    CompleteIo(io, FLUME_ERR_INVALID_ARGUMENT, 0, 0,
               "duplicate or invalid sim collective rank submission");
    return FLUME_OK;
  }

  RetainPendingBuffer(dst);
  RetainPendingIo(io);
  pending.arrived[rank] = true;
  pending.payloads[rank] = std::move(payload);
  pending.dsts[rank] = dst;
  pending.dst_offsets[rank] = dst_offset;
  pending.ios[rank] = io;
  ++pending.arrivals;

  if (pending.arrivals == pending.rank_size) {
    std::string error;
    if (!CompletePendingSimCollective(pending, &error)) {
      FailPendingSimCollective(pending, FLUME_ERR_UNSUPPORTED, error);
    }
    map.erase(inserted.first);
  }
  return FLUME_OK;
}

enum class SimP2pRole {
  kSend = 0,
  kRecv = 1,
};

int SubmitSimP2p(flume_client_t* client,
                 SimP2pRole role,
                 flume_buffer_t* buffer,
                 size_t offset,
                 uint64_t count,
                 flume_data_type_t data_type,
                 uint32_t peer_rank,
                 flume_io_t** out) {
  std::string comm_name;
  uint32_t rank = 0;
  uint32_t rank_size = 0;
  {
    std::lock_guard<std::mutex> lock(client->mu);
    if (!client->sim_comm_attached || client->rank_size == 0) {
      *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                    "sim p2p requires flume_attach_sim_comm");
      return FLUME_OK;
    }
    comm_name = client->sim_comm_name;
    rank = client->rank;
    rank_size = client->rank_size;
  }

  if (peer_rank >= rank_size || peer_rank == rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  size_t bytes = 0;
  if (!CheckedBytes(count, data_type, &bytes) ||
      !ValidateRange(buffer, offset, bytes)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (buffer->client != client || !IsSimBufferType(buffer->type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(client->mu);
    seq = role == SimP2pRole::kSend ? client->sim_p2p_send_seq++
                                    : client->sim_p2p_recv_seq++;
  }

  std::vector<uint8_t> payload;
  if (role == SimP2pRole::kSend) {
    payload.resize(bytes);
    memcpy(payload.data(), static_cast<const uint8_t*>(buffer->ptr) + offset,
           bytes);
  }

  auto* io = MakePendingIo();
  *out = io;

  SimP2pKey key{
      comm_name,
      rank_size,
      role == SimP2pRole::kSend ? rank : peer_rank,
      role == SimP2pRole::kSend ? peer_rank : rank,
      seq,
  };
  auto& map = SimP2ps();
  std::lock_guard<std::mutex> lock(SimP2pMutex());
  auto inserted = map.emplace(key, nullptr);
  if (inserted.second) {
    inserted.first->second = std::make_unique<PendingSimP2p>();
  }

  PendingSimP2p& pending = *inserted.first->second;
  if (!PendingSimP2pMatches(pending, count, data_type, bytes)) {
    FailPendingSimP2p(pending, FLUME_ERR_INVALID_ARGUMENT,
                      "sim p2p parameters do not match peer submission");
    map.erase(inserted.first);
    CompleteIo(io, FLUME_ERR_INVALID_ARGUMENT, 0, 0,
               "sim p2p parameters do not match peer submission");
    return FLUME_OK;
  }
  MaybeSetPendingSimP2pMetadata(&pending, count, data_type, bytes);

  if (role == SimP2pRole::kSend) {
    if (pending.has_send) {
      FailPendingSimP2p(pending, FLUME_ERR_INVALID_ARGUMENT,
                        "duplicate sim p2p send submission");
      map.erase(inserted.first);
      CompleteIo(io, FLUME_ERR_INVALID_ARGUMENT, 0, 0,
                 "duplicate sim p2p send submission");
      return FLUME_OK;
    }
    RetainPendingIo(io);
    pending.has_send = true;
    pending.payload = std::move(payload);
    pending.send_io = io;
  } else {
    if (pending.has_recv) {
      FailPendingSimP2p(pending, FLUME_ERR_INVALID_ARGUMENT,
                        "duplicate sim p2p recv submission");
      map.erase(inserted.first);
      CompleteIo(io, FLUME_ERR_INVALID_ARGUMENT, 0, 0,
                 "duplicate sim p2p recv submission");
      return FLUME_OK;
    }
    RetainPendingBuffer(buffer);
    RetainPendingIo(io);
    pending.has_recv = true;
    pending.dst = buffer;
    pending.dst_offset = offset;
    pending.recv_io = io;
  }

  if (pending.has_send && pending.has_recv) {
    CompletePendingSimP2p(pending);
    map.erase(inserted.first);
  }
  return FLUME_OK;
}

int SubmitSimHcommPayload(flume_client_t* client,
                          SimP2pRole role,
                          flume_buffer_t* buffer,
                          size_t offset,
                          uint64_t count,
                          flume_data_type_t data_type,
                          uint32_t peer_rank,
                          flume_io_t** out) {
  std::string comm_name;
  uint32_t rank = 0;
  uint32_t rank_size = 0;
  {
    std::lock_guard<std::mutex> lock(client->mu);
    if (!client->sim_comm_attached || client->rank_size == 0) {
      *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                    "sim HCOMM payload requires flume_attach_sim_comm");
      return FLUME_OK;
    }
    comm_name = client->sim_comm_name;
    rank = client->rank;
    rank_size = client->rank_size;
  }

  if (rank_size != 2) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "sim HCOMM payload copy is pair-only and requires exactly two ranks");
    return FLUME_OK;
  }
  if (peer_rank >= rank_size || peer_rank == rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  size_t bytes = 0;
  if (!CheckedBytes(count, data_type, &bytes) ||
      !ValidateRange(buffer, offset, bytes)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (buffer->client != client || buffer->type != FLUME_BUFFER_SIM_HBM) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  uint64_t seq = 0;
  {
    std::lock_guard<std::mutex> lock(client->mu);
    seq = role == SimP2pRole::kSend ?
              client->sim_hcomm_payload_send_seq++ :
              client->sim_hcomm_payload_recv_seq++;
  }

  std::vector<uint8_t> staging;
  if (role == SimP2pRole::kSend) {
    staging.resize(bytes);
    memcpy(staging.data(), static_cast<const uint8_t*>(buffer->ptr) + offset,
           bytes);
  }

  auto* io = MakePendingIo();
  *out = io;

  SimP2pKey key{
      comm_name,
      rank_size,
      role == SimP2pRole::kSend ? rank : peer_rank,
      role == SimP2pRole::kSend ? peer_rank : rank,
      seq,
  };
  auto& map = SimHcommPayloads();
  std::lock_guard<std::mutex> lock(SimHcommPayloadMutex());
  auto inserted = map.emplace(key, nullptr);
  if (inserted.second) {
    inserted.first->second = std::make_unique<PendingSimP2p>();
  }

  PendingSimP2p& pending = *inserted.first->second;
  if (!PendingSimP2pMatches(pending, count, data_type, bytes)) {
    FailPendingSimP2p(pending, FLUME_ERR_INVALID_ARGUMENT,
                      "sim HCOMM payload parameters do not match peer submission");
    map.erase(inserted.first);
    CompleteIo(io, FLUME_ERR_INVALID_ARGUMENT, 0, 0,
               "sim HCOMM payload parameters do not match peer submission");
    return FLUME_OK;
  }
  MaybeSetPendingSimP2pMetadata(&pending, count, data_type, bytes);

  if (role == SimP2pRole::kSend) {
    if (pending.has_send) {
      FailPendingSimP2p(pending, FLUME_ERR_INVALID_ARGUMENT,
                        "duplicate sim HCOMM payload send submission");
      map.erase(inserted.first);
      CompleteIo(io, FLUME_ERR_INVALID_ARGUMENT, 0, 0,
                 "duplicate sim HCOMM payload send submission");
      return FLUME_OK;
    }
    RetainPendingIo(io);
    pending.has_send = true;
    pending.payload = std::move(staging);
    pending.send_io = io;
  } else {
    if (pending.has_recv) {
      FailPendingSimP2p(pending, FLUME_ERR_INVALID_ARGUMENT,
                        "duplicate sim HCOMM payload recv submission");
      map.erase(inserted.first);
      CompleteIo(io, FLUME_ERR_INVALID_ARGUMENT, 0, 0,
                 "duplicate sim HCOMM payload recv submission");
      return FLUME_OK;
    }
    RetainPendingBuffer(buffer);
    RetainPendingIo(io);
    pending.has_recv = true;
    pending.dst = buffer;
    pending.dst_offset = offset;
    pending.recv_io = io;
  }

  if (pending.has_send && pending.has_recv) {
    CompletePendingSimP2p(pending);
    map.erase(inserted.first);
  }
  return FLUME_OK;
}

#if FLUME_ENABLE_HCCL
bool ToHcclDataType(flume_data_type_t data_type, HcclDataType* out) {
  if (out == nullptr) {
    return false;
  }
  switch (data_type) {
    case FLUME_DTYPE_INT8:
      *out = HCCL_DATA_TYPE_INT8;
      return true;
    case FLUME_DTYPE_INT16:
      *out = HCCL_DATA_TYPE_INT16;
      return true;
    case FLUME_DTYPE_INT32:
      *out = HCCL_DATA_TYPE_INT32;
      return true;
    case FLUME_DTYPE_FP16:
      *out = HCCL_DATA_TYPE_FP16;
      return true;
    case FLUME_DTYPE_FP32:
      *out = HCCL_DATA_TYPE_FP32;
      return true;
    case FLUME_DTYPE_INT64:
      *out = HCCL_DATA_TYPE_INT64;
      return true;
    case FLUME_DTYPE_UINT64:
      *out = HCCL_DATA_TYPE_UINT64;
      return true;
    case FLUME_DTYPE_UINT8:
      *out = HCCL_DATA_TYPE_UINT8;
      return true;
    case FLUME_DTYPE_UINT16:
      *out = HCCL_DATA_TYPE_UINT16;
      return true;
    case FLUME_DTYPE_UINT32:
      *out = HCCL_DATA_TYPE_UINT32;
      return true;
    case FLUME_DTYPE_FP64:
      *out = HCCL_DATA_TYPE_FP64;
      return true;
    case FLUME_DTYPE_BFP16:
      *out = HCCL_DATA_TYPE_BFP16;
      return true;
    default:
      return false;
  }
}

bool ToHcclReduceOp(flume_reduce_op_t op, HcclReduceOp* out) {
  if (out == nullptr) {
    return false;
  }
  switch (op) {
    case FLUME_REDUCE_SUM:
      *out = HCCL_REDUCE_SUM;
      return true;
    case FLUME_REDUCE_PROD:
      *out = HCCL_REDUCE_PROD;
      return true;
    case FLUME_REDUCE_MAX:
      *out = HCCL_REDUCE_MAX;
      return true;
    case FLUME_REDUCE_MIN:
      *out = HCCL_REDUCE_MIN;
      return true;
    default:
      return false;
  }
}

std::string HcclErrorMessage(HcclResult result) {
  const char* text = HcclGetErrorString(result);
  if (text != nullptr && text[0] != '\0') {
    return text;
  }
  return "HCCL error code " + std::to_string(static_cast<int>(result));
}

bool CheckHcclResource(HcclResult result, const char* label,
                       std::string* error) {
  if (result == HCCL_SUCCESS) {
    return true;
  }
  if (error != nullptr) {
    *error = std::string(label) + " failed: " + HcclErrorMessage(result);
  }
  return false;
}

#if FLUME_HAVE_HCOMM_CHANNEL_RES
const char* HcommProbeEngineName(flume_hcomm_engine_t engine) {
  switch (engine) {
    case FLUME_HCOMM_ENGINE_AUTO:
      return "auto";
    case FLUME_HCOMM_ENGINE_AICPU:
      return "aicpu";
    case FLUME_HCOMM_ENGINE_AICPU_TS:
      return "aicpu-ts";
    case FLUME_HCOMM_ENGINE_CPU:
      return "cpu";
    case FLUME_HCOMM_ENGINE_CPU_TS:
      return "cpu-ts";
  }
  return "unknown";
}

const char* HcommProbeProtocolName(flume_hcomm_protocol_t protocol) {
  switch (protocol) {
    case FLUME_HCOMM_PROTOCOL_AUTO:
      return "auto";
    case FLUME_HCOMM_PROTOCOL_HCCS:
      return "hccs";
    case FLUME_HCOMM_PROTOCOL_ROCE:
      return "roce";
    case FLUME_HCOMM_PROTOCOL_PCIE:
      return "pcie";
    case FLUME_HCOMM_PROTOCOL_SIO:
      return "sio";
    case FLUME_HCOMM_PROTOCOL_HCCS_ONLY:
      return "hccs-only";
  }
  return "unknown";
}

flume_hcomm_engine_t ResolveHcommProbeEngine(flume_hcomm_engine_t engine) {
  if (engine != FLUME_HCOMM_ENGINE_AUTO) {
    return engine;
  }
#if FLUME_HAVE_HCOMM_THREAD_EXPORT
  return FLUME_HCOMM_ENGINE_AICPU_TS;
#else
  return FLUME_HCOMM_ENGINE_CPU_TS;
#endif
}

bool IsAicpuProbeEngine(flume_hcomm_engine_t engine) {
  return engine == FLUME_HCOMM_ENGINE_AICPU ||
         engine == FLUME_HCOMM_ENGINE_AICPU_TS;
}

bool ToHcommCommEngine(flume_hcomm_engine_t engine, CommEngine* out) {
  if (out == nullptr) {
    return false;
  }
  switch (engine) {
    case FLUME_HCOMM_ENGINE_AICPU:
      *out = COMM_ENGINE_AICPU;
      return true;
    case FLUME_HCOMM_ENGINE_AICPU_TS:
      *out = COMM_ENGINE_AICPU_TS;
      return true;
    case FLUME_HCOMM_ENGINE_CPU:
      *out = COMM_ENGINE_CPU;
      return true;
    case FLUME_HCOMM_ENGINE_CPU_TS:
      *out = COMM_ENGINE_CPU_TS;
      return true;
    default:
      return false;
  }
}

bool ToHcommProtocol(flume_hcomm_protocol_t protocol, CommProtocol* out) {
  if (out == nullptr) {
    return false;
  }
  switch (protocol) {
    case FLUME_HCOMM_PROTOCOL_HCCS:
      *out = COMM_PROTOCOL_HCCS;
      return true;
    case FLUME_HCOMM_PROTOCOL_ROCE:
      *out = COMM_PROTOCOL_ROCE;
      return true;
    case FLUME_HCOMM_PROTOCOL_PCIE:
      *out = COMM_PROTOCOL_PCIE;
      return true;
    case FLUME_HCOMM_PROTOCOL_SIO:
      *out = COMM_PROTOCOL_SIO;
      return true;
    case FLUME_HCOMM_PROTOCOL_HCCS_ONLY:
      *out = COMM_PROTOCOL_HCCS;
      return true;
    default:
      return false;
  }
}

const char* HcommCommProtocolName(CommProtocol protocol) {
  switch (protocol) {
    case COMM_PROTOCOL_HCCS:
      return "hccs";
    case COMM_PROTOCOL_ROCE:
      return "roce";
    case COMM_PROTOCOL_PCIE:
      return "pcie";
    case COMM_PROTOCOL_SIO:
      return "sio";
    default:
      return "unknown";
  }
}

#if FLUME_HAVE_HCOMM_RANK_GRAPH
bool TryBuildRankGraphChannelDescs(HcclComm comm,
                                   uint32_t src_rank,
                                   uint32_t peer_rank,
                                   CommProtocol protocol,
                                   uint32_t notify_num,
                                   std::vector<HcclChannelDesc>* descs,
                                   int* status,
                                   std::string* error) {
  if (descs == nullptr || status == nullptr || error == nullptr) {
    return false;
  }
  uint32_t* net_layers = nullptr;
  uint32_t net_layer_num = 0;
  HcclResult layers_ret = HcclRankGraphGetLayers(comm, &net_layers, &net_layer_num);
  if (layers_ret != HCCL_SUCCESS || net_layers == nullptr || net_layer_num == 0) {
    return false;
  }

  bool saw_links = false;
  for (uint32_t layer_idx = 0; layer_idx < net_layer_num; ++layer_idx) {
    CommLink* links = nullptr;
    uint32_t link_num = 0;
    HcclResult links_ret =
        HcclRankGraphGetLinks(comm, net_layers[layer_idx], src_rank, peer_rank,
                              &links, &link_num);
    if (links_ret != HCCL_SUCCESS || links == nullptr || link_num == 0) {
      continue;
    }
    saw_links = true;
    for (uint32_t link_idx = 0; link_idx < link_num; ++link_idx) {
      const CommLink& link = links[link_idx];
      if (link.linkAttr.linkProtocol != protocol) {
        continue;
      }
      HcclChannelDesc desc;
      HcclChannelDescInit(&desc, 1);
      desc.remoteRank = peer_rank;
      desc.localEndpoint = link.srcEndpointDesc;
      desc.remoteEndpoint = link.dstEndpointDesc;
      desc.channelProtocol = link.linkAttr.linkProtocol;
      desc.notifyNum = notify_num;
      descs->push_back(desc);
    }
  }
  if (!descs->empty()) {
    return true;
  }
  if (saw_links) {
    *status = FLUME_ERR_UNSUPPORTED;
    *error = std::string("rank graph returned no link matching protocol ") +
             HcommCommProtocolName(protocol);
  }
  return false;
}
#endif

bool ProbeHcommChannelResources(const CommState& state,
                                uint32_t peer_rank,
                                const HcommProbeOptions& options,
                                void* acl_stream,
                                size_t* usable_buffer_bytes,
                                int* status,
                                std::string* detail,
                                std::string* error,
                                HcommChannelResourceInfo* resource_info =
                                    nullptr) {
  if (usable_buffer_bytes == nullptr || status == nullptr ||
      detail == nullptr || error == nullptr) {
    return false;
  }
  *usable_buffer_bytes = 0;
  *status = FLUME_ERR_BACKEND;
  detail->clear();
  flume_hcomm_engine_t resolved_engine =
      ResolveHcommProbeEngine(options.engine);
  if (options.protocol == FLUME_HCOMM_PROTOCOL_PCIE) {
    *status = FLUME_ERR_UNSUPPORTED;
    *error = "HCOMM channel resource probe does not support pcie protocol";
    return false;
  }
  if (options.require_thread_export) {
#if !FLUME_HAVE_HCOMM_THREAD_EXPORT
    *status = FLUME_ERR_UNSUPPORTED;
    *error = "HCOMM thread export is unavailable in this CANN build";
    return false;
#else
    if (!IsAicpuProbeEngine(resolved_engine)) {
      *status = FLUME_ERR_UNSUPPORTED;
      *error = "HCOMM thread export probe requires aicpu or aicpu-ts engine";
      return false;
    }
#endif
  }
  CommEngine channel_engine = COMM_ENGINE_RESERVED;
  if (!ToHcommCommEngine(resolved_engine, &channel_engine)) {
    *status = FLUME_ERR_UNSUPPORTED;
    *error = "unsupported HCOMM channel probe engine";
    return false;
  }
  CommProtocol channel_protocol = COMM_PROTOCOL_RESERVED;
  if (!ToHcommProtocol(options.protocol, &channel_protocol)) {
    *status = FLUME_ERR_UNSUPPORTED;
    *error = "unsupported HCOMM channel probe protocol";
    return false;
  }

  auto comm = static_cast<HcclComm>(state.hccl_comm);
  void* local_buffer = nullptr;
  uint64_t local_size = 0;
  if (!CheckHcclResource(HcclGetHcclBuffer(comm, &local_buffer, &local_size),
                         "HcclGetHcclBuffer", error)) {
    *status = FLUME_ERR_BACKEND;
    return false;
  }
  if (local_buffer == nullptr || local_size == 0) {
    *status = FLUME_ERR_BACKEND;
    *error = "HcclGetHcclBuffer returned an empty HCCL buffer";
    return false;
  }

  ThreadHandle cpu_ts_thread = 0;
  bool needs_cpu_ts_thread =
      resolved_engine == FLUME_HCOMM_ENGINE_CPU_TS ||
      IsAicpuProbeEngine(resolved_engine);
  if (needs_cpu_ts_thread) {
    if (!CheckHcclResource(
            HcclThreadAcquireWithStream(
                comm, COMM_ENGINE_CPU_TS, static_cast<aclrtStream>(acl_stream),
                1, &cpu_ts_thread),
            "HcclThreadAcquireWithStream(CPU_TS)", error)) {
      *status = FLUME_ERR_BACKEND;
      return false;
    }
  }

  ThreadHandle aicpu_ts_thread = 0;
  bool needs_aicpu_thread = IsAicpuProbeEngine(resolved_engine);
  if (needs_aicpu_thread) {
    if (!CheckHcclResource(
            HcclThreadAcquire(comm, COMM_ENGINE_AICPU_TS, 1,
                              1, &aicpu_ts_thread),
            "HcclThreadAcquire(AICPU_TS)", error)) {
      *status = FLUME_ERR_BACKEND;
      return false;
    }
  }

  ThreadHandle cpu_thread_on_aicpu = 0;
  ThreadHandle aicpu_thread_on_cpu = 0;
  bool host_thread_notify_ready = false;
  std::string thread_notify_detail =
      "payload_thread_notify=unavailable reason=thread-export-off";
#if FLUME_HAVE_HCOMM_THREAD_EXPORT
  thread_notify_detail = "payload_thread_notify=unavailable reason=not-aicpu-engine";
  if (needs_aicpu_thread) {
    HcclResult export_ret =
        HcclThreadExportToCommEngine(comm, 1, &cpu_ts_thread,
                                     COMM_ENGINE_AICPU_TS,
                                     &cpu_thread_on_aicpu);
    if (export_ret == HCCL_SUCCESS) {
      export_ret =
          HcclThreadExportToCommEngine(comm, 1, &aicpu_ts_thread,
                                       COMM_ENGINE_CPU_TS,
                                       &aicpu_thread_on_cpu);
    }
    if (export_ret == HCCL_SUCCESS) {
      host_thread_notify_ready = cpu_thread_on_aicpu != 0 &&
                                 aicpu_thread_on_cpu != 0 &&
                                 cpu_ts_thread != 0 && aicpu_ts_thread != 0;
      thread_notify_detail = host_thread_notify_ready ?
          "payload_thread_notify=host-aicpu" :
          "payload_thread_notify=unavailable reason=zero-thread-handle";
    } else if (options.require_thread_export) {
      if (!CheckHcclResource(
              export_ret, "HcclThreadExportToCommEngine", error)) {
        *status = FLUME_ERR_BACKEND;
        return false;
      }
    } else {
      thread_notify_detail =
          std::string("payload_thread_notify=unavailable hcomm_ret=") +
          std::to_string(static_cast<int>(export_ret));
    }
  }
#else
  (void)cpu_ts_thread;
  (void)aicpu_ts_thread;
#endif

  std::vector<HcclChannelDesc> descs;
  const char* desc_source = "legacy-desc";
#if FLUME_HAVE_HCOMM_RANK_GRAPH
  int graph_status = FLUME_OK;
  std::string graph_error;
  if (TryBuildRankGraphChannelDescs(comm, state.rank, peer_rank,
                                    channel_protocol, options.notify_num,
                                    &descs, &graph_status, &graph_error)) {
    desc_source = "rank-graph";
  } else if (graph_status != FLUME_OK) {
    *status = graph_status;
    *error = graph_error;
    return false;
  }
#endif
  if (descs.empty()) {
    HcclChannelDesc desc;
    HcclChannelDescInit(&desc, 1);
    desc.remoteRank = peer_rank;
    desc.channelProtocol = channel_protocol;
    desc.notifyNum = options.notify_num;
    descs.push_back(desc);
  }

  std::vector<ChannelHandle> channels(descs.size(), 0);
  if (!CheckHcclResource(
          HcclChannelAcquire(comm, channel_engine, descs.data(),
                             static_cast<uint32_t>(descs.size()),
                             channels.data()),
          "HcclChannelAcquire", error)) {
    *status = FLUME_ERR_BACKEND;
    return false;
  }

  void* remote_buffer = nullptr;
  uint64_t remote_size = 0;
  if (!CheckHcclResource(
          HcclChannelGetHcclBuffer(comm, channels[0], &remote_buffer,
                                   &remote_size),
          "HcclChannelGetHcclBuffer", error)) {
    *status = FLUME_ERR_BACKEND;
    return false;
  }
  if (remote_buffer == nullptr || remote_size == 0) {
    *status = FLUME_ERR_BACKEND;
    *error = "HcclChannelGetHcclBuffer returned an empty remote HCCL buffer";
    return false;
  }

  uint64_t usable = std::min(local_size, remote_size);
  if (usable > std::numeric_limits<size_t>::max()) {
    usable = std::numeric_limits<size_t>::max();
  }
  *usable_buffer_bytes = static_cast<size_t>(usable);
  if (resource_info != nullptr) {
    resource_info->usable_buffer_bytes = *usable_buffer_bytes;
    resource_info->local_buffer_bytes = local_size;
    resource_info->remote_buffer_bytes = remote_size;
    resource_info->local_buffer = local_buffer;
    resource_info->remote_buffer = remote_buffer;
    resource_info->channel_handle = channels.empty() ? 0 : channels[0];
    resource_info->cpu_ts_thread = cpu_ts_thread;
    resource_info->aicpu_ts_thread = aicpu_ts_thread;
    resource_info->cpu_thread_on_aicpu = cpu_thread_on_aicpu;
    resource_info->aicpu_thread_on_cpu = aicpu_thread_on_cpu;
    resource_info->channel_count = static_cast<uint32_t>(descs.size());
    resource_info->notify_num = options.notify_num;
    resource_info->resolved_engine = resolved_engine;
    resource_info->resolved_protocol = options.protocol;
    resource_info->thread_export_required = options.require_thread_export;
    resource_info->host_thread_notify_ready = host_thread_notify_ready;
    resource_info->timeout_sec = options.timeout_sec;
    resource_info->channel_desc_source = desc_source;
  }
  *status = FLUME_OK;
  *detail = std::string("resolved_engine=") +
            HcommProbeEngineName(resolved_engine) +
            " resolved_protocol=" + HcommProbeProtocolName(options.protocol) +
            " channel_desc=" + desc_source +
            " channel_num=" + std::to_string(descs.size()) +
            " thread_export=" +
            (options.require_thread_export ? "required" : "not-required") +
            " hcomm_timeout_sec=" + std::to_string(options.timeout_sec) +
            " " + thread_notify_detail;
  return true;
}
#endif
#endif

std::string MakeHcommPayloadPlanDetail(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    uint64_t bytes,
    const HcommProbeOptions& options,
    flume_hcomm_protocol_t resolved_protocol,
    const std::string& channel_detail) {
  flume::hcomm_payload::PayloadPlan plan;
  flume::hcomm_payload::PayloadPlanOptions plan_options;
  if (options.payload_write_with_notify) {
    plan_options.transfer_mode =
        flume::hcomm_payload::PayloadTransferMode::kWriteWithNotify;
  } else if (options.payload_write_path) {
    plan_options.transfer_mode =
        flume::hcomm_payload::PayloadTransferMode::kWrite;
  }
  if (options.payload_recv_direct_output && !options.payload_write_path &&
      !options.payload_write_with_notify) {
    plan_options.recv_path =
        flume::hcomm_payload::PayloadRecvPath::kDirectOutput;
  }
  if (options.payload_comm_binding ==
      FLUME_HCOMM_PAYLOAD_COMM_BINDING_CHANNEL_HANDLE) {
    plan_options.comm_binding =
        flume::hcomm_payload::PayloadCommBinding::kChannelHandle;
  } else if (options.payload_comm_binding ==
             FLUME_HCOMM_PAYLOAD_COMM_BINDING_DIAGNOSTIC_SKIP) {
    plan_options.comm_binding =
        flume::hcomm_payload::PayloadCommBinding::kDiagnosticSkip;
  }
  if (options.payload_force_channel_fence ||
      (options.payload_write_with_notify &&
       HcommWriteWithNotifyUsesNbiBackend()) ||
      resolved_protocol == FLUME_HCOMM_PROTOCOL_ROCE) {
    plan_options.completion_mode =
        flume::hcomm_payload::PayloadCompletionMode::kChannelFence;
  }
  plan_options.batch_enabled = !options.disable_payload_batch_mode;
  std::string error;
  if (!flume::hcomm_payload::BuildPairCopyPlanWithOptions(
          role, state.rank, peer_rank, state.rank_size, bytes, plan_options,
          &plan, &error)) {
    return std::string("stage3b_plan=invalid error=\"") + error +
           "\" channel_detail=\"" + channel_detail + "\"";
  }
  return flume::hcomm_payload::DescribePlan(plan) + " channel_detail=\"" +
         channel_detail + "\"";
}

std::string DetailQuote(const std::string& text) {
  std::string out;
  out.reserve(text.size());
  for (char ch : text) {
    if (ch == '"') {
      out.push_back('\'');
    } else if (ch == '\n' || ch == '\r' || ch == '\t') {
      out.push_back(' ');
    } else {
      out.push_back(ch);
    }
  }
  return out;
}

std::string PayloadResourceStepFromError(const std::string& error) {
  auto has = [&error](const char* needle) {
    return error.find(needle) != std::string::npos;
  };
  if (has("HcclGetHcclBuffer returned an empty HCCL buffer")) {
    return "local-hccl-buffer-empty";
  }
  if (has("HcclGetHcclBuffer returned an empty remote HCCL buffer")) {
    return "remote-hccl-buffer-empty";
  }
  if (has("HcclGetHcclBuffer")) {
    return "local-hccl-buffer";
  }
  if (has("HcclThreadAcquireWithStream(CPU_TS)")) {
    return "cpu-ts-thread";
  }
  if (has("HcclThreadAcquire(AICPU_TS)")) {
    return "aicpu-ts-thread";
  }
  if (has("HcclThreadExportToCommEngine")) {
    return "thread-export";
  }
  if (has("rank graph")) {
    return "rank-graph";
  }
  if (has("HcclChannelAcquire")) {
    return "channel-acquire";
  }
  if (has("HcclChannelGetHcclBuffer")) {
    return "remote-hccl-buffer";
  }
  if (has("protocol")) {
    return "protocol-select";
  }
  if (has("engine")) {
    return "engine-select";
  }
  return "unknown";
}

const char* PayloadRoleMarkerName(flume::hcomm_payload::PayloadRole role) {
  return role == flume::hcomm_payload::PayloadRole::kSend ? "send" : "recv";
}

std::string MakeHcommPayloadResourceAcquireFailedDetail(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    uint64_t bytes,
    int probe_status,
    const std::string& error) {
  const bool unsupported = probe_status == FLUME_ERR_UNSUPPORTED;
  return std::string("stage3b3e_payload_copy=") +
         (unsupported ? "unsupported" : "failed") +
         " stage3b3e_direct_aclrt_payload_loader=not-attempted "
         "stage3b3e_payload_descriptor_handoff=blocked "
         "stage3b3e_direct_aclrt_payload_launch=not-attempted "
         "stage3b3e_payload_sync=not-attempted "
         "payload_resource_acquire=failed payload_resource_step=" +
         PayloadResourceStepFromError(error) +
         " payload_resource_status=" +
         (unsupported ? "unsupported" : "backend-error") +
         " payload_role=" + PayloadRoleMarkerName(role) +
         " payload_local_rank=" + std::to_string(state.rank) +
         " payload_peer_rank=" + std::to_string(peer_rank) +
         " payload_bytes=" + std::to_string(bytes) +
         " error=\"" + DetailQuote(error) + "\"";
}

std::string MakeHcommCustomOpLaunchSmokeDetail(
    const CommState& state,
    uint32_t peer_rank,
    const std::string& channel_detail) {
  flume::hcomm_payload::CustomOpLaunchSmokePlan plan;
  std::string error;
  if (!flume::hcomm_payload::BuildCustomOpLaunchSmokePlan(
          state.rank, peer_rank, state.rank_size, &plan, &error)) {
    return std::string("stage3b1_launch=invalid error=\"") + error +
           "\" channel_detail=\"" + channel_detail + "\"";
  }
  return flume::hcomm_payload::DescribeCustomOpLaunchSmokePlan(plan) +
         " channel_detail=\"" + channel_detail + "\"";
}

std::string MakeHcommResourceDescriptorDetail(
    const CommState& state,
    uint32_t peer_rank,
    const HcommChannelResourceInfo& resource_info,
    const std::string& channel_detail) {
  flume::hcomm_payload::ResourceDescriptor descriptor;
  std::string error;
  if (!flume::hcomm_payload::BuildResourceDescriptor(
          state.rank, peer_rank, state.rank_size, resource_info.channel_count,
          resource_info.notify_num, resource_info.local_buffer_bytes,
          resource_info.remote_buffer_bytes,
          resource_info.thread_export_required,
          FlumeHcommEngineName(resource_info.resolved_engine),
          FlumeHcommProtocolName(resource_info.resolved_protocol),
          resource_info.channel_desc_source, &descriptor, &error)) {
    return std::string("stage3b2_resource_descriptor=invalid error=\"") +
           error + "\" channel_detail=\"" + channel_detail + "\"";
  }
  return flume::hcomm_payload::DescribeResourceDescriptor(descriptor) +
         " channel_detail=\"" + channel_detail + "\"";
}

std::string MakeHcommNotifyOnlyDetail(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    const HcommChannelResourceInfo& resource_info,
    const std::string& descriptor_detail) {
  flume::hcomm_payload::NotifyOnlyPlan plan;
  std::string error;
  if (!flume::hcomm_payload::BuildNotifyOnlyPlan(
          role, state.rank, peer_rank, state.rank_size, 0, 1, &plan, &error)) {
    return std::string("stage3b2_notify_only=invalid error=\"") + error +
           "\" descriptor_detail=\"" + descriptor_detail + "\"";
  }
  if (resource_info.notify_num <= plan.done_notify_idx) {
    return std::string("stage3b2_notify_only=invalid error=\"notify index "
                       "exceeds descriptor notify_num\" descriptor_detail=\"") +
           descriptor_detail + "\"";
  }
  return flume::hcomm_payload::DescribeNotifyOnlyPlan(plan) +
         " descriptor_detail=\"" + descriptor_detail + "\"";
}

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
void FillFlumeNotifyOnlyDesc(flume::hcomm_payload::PayloadRole role,
                             const CommState& state,
                             uint32_t peer_rank,
                             const HcommChannelResourceInfo& resource_info,
                             flume_hcomm_notify_only_desc_v1* desc) {
  flume_hcomm_notify_only_desc_init(desc);
  desc->role = role == flume::hcomm_payload::PayloadRole::kSend ?
                   FLUME_HCOMM_NOTIFY_ROLE_SEND :
                   FLUME_HCOMM_NOTIFY_ROLE_RECV;
  desc->local_rank = state.rank;
  desc->peer_rank = peer_rank;
  desc->rank_size = state.rank_size;
  desc->ready_notify_idx = 0;
  desc->done_notify_idx = 1;
  desc->timeout_sec = resource_info.timeout_sec;
  desc->aicpu_thread = resource_info.aicpu_ts_thread;
  desc->channel_handle = resource_info.channel_handle;
  desc->local_hccl_buffer =
      reinterpret_cast<uint64_t>(resource_info.local_buffer);
  desc->remote_hccl_buffer =
      reinterpret_cast<uint64_t>(resource_info.remote_buffer);
  desc->local_hccl_buffer_bytes = resource_info.local_buffer_bytes;
  desc->remote_hccl_buffer_bytes = resource_info.remote_buffer_bytes;
}

void FillFlumePayloadCopyDesc(flume::hcomm_payload::PayloadRole role,
                              const CommState& state,
                              uint32_t peer_rank,
                              const HcommChannelResourceInfo& resource_info,
                              void* user_buffer,
                              uint64_t bytes,
                              const char* comm_name,
                              bool disable_payload_batch_mode,
                              const std::string& payload_batch_tag,
                              bool payload_recv_direct_output,
                              bool payload_force_channel_fence,
                              bool payload_write_path,
                              bool payload_write_with_notify,
                              flume_hcomm_payload_comm_binding_t
                                  payload_comm_binding,
                              flume_hcomm_payload_copy_desc_v1* desc) {
  flume_hcomm_payload_copy_desc_init(desc);
  desc->role = role == flume::hcomm_payload::PayloadRole::kSend ?
                   FLUME_HCOMM_NOTIFY_ROLE_SEND :
                   FLUME_HCOMM_NOTIFY_ROLE_RECV;
  desc->local_rank = state.rank;
  desc->peer_rank = peer_rank;
  desc->rank_size = state.rank_size;
  desc->ready_notify_idx = 0;
  desc->done_notify_idx = 1;
  desc->timeout_sec = resource_info.timeout_sec;
  desc->bytes = bytes;
  desc->thread_notify_mode = resource_info.host_thread_notify_ready ?
      FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU :
      FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE;
  desc->completion_mode =
      (payload_force_channel_fence ||
       (payload_write_with_notify && HcommWriteWithNotifyUsesNbiBackend()) ||
       resource_info.resolved_protocol == FLUME_HCOMM_PROTOCOL_ROCE) ?
          FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN :
          FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY;
  if (payload_comm_binding ==
      FLUME_HCOMM_PAYLOAD_COMM_BINDING_DIAGNOSTIC_SKIP) {
    desc->completion_mode |=
        FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE;
  } else if (payload_comm_binding ==
             FLUME_HCOMM_PAYLOAD_COMM_BINDING_CHANNEL_HANDLE) {
    desc->completion_mode |=
        FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE |
        FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING;
  }
  if (payload_write_path) {
    desc->completion_mode |= FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH;
  }
  if (payload_write_with_notify) {
    desc->completion_mode |=
        FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH |
        FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY;
  }
  desc->aicpu_thread = resource_info.aicpu_ts_thread;
  desc->channel_handle = resource_info.channel_handle;
  desc->user_buffer = reinterpret_cast<uint64_t>(user_buffer);
  desc->local_hccl_buffer =
      reinterpret_cast<uint64_t>(resource_info.local_buffer);
  desc->remote_hccl_buffer =
      reinterpret_cast<uint64_t>(resource_info.remote_buffer);
  desc->local_hccl_buffer_bytes = resource_info.local_buffer_bytes;
  desc->remote_hccl_buffer_bytes = resource_info.remote_buffer_bytes;
  const char* batch_tag = payload_batch_tag.empty() ?
      kDefaultHcommPayloadBatchTag : payload_batch_tag.c_str();
  const size_t batch_tag_size = payload_batch_tag.empty() ?
      strlen(kDefaultHcommPayloadBatchTag) : payload_batch_tag.size();
  const size_t tag_len = std::min(
      batch_tag_size,
      static_cast<size_t>(FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES - 1U));
  memcpy(desc->batch_tag, batch_tag, tag_len);
  desc->batch_tag[tag_len] = '\0';
  desc->reserved2[0] = disable_payload_batch_mode ?
      FLUME_HCOMM_PAYLOAD_BATCH_MODE_DISABLED :
      FLUME_HCOMM_PAYLOAD_BATCH_MODE_DEFAULT;
  desc->reserved2[1] = (payload_recv_direct_output && !payload_write_path) ?
      FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT :
      FLUME_HCOMM_PAYLOAD_RECV_PATH_LOCAL_BUFFER;
  desc->cpu_thread_on_aicpu = resource_info.cpu_thread_on_aicpu;
  if (comm_name != nullptr) {
    strncpy(desc->comm_name, comm_name, sizeof(desc->comm_name) - 1);
  }
}

bool IsUnsupportedHcclLaunchResult(HcclResult result) {
  return result == HCCL_E_NOT_SUPPORT || result == HCCL_E_NOT_FOUND ||
         result == HCCL_E_UNAVAIL;
}

enum class HcommLauncherBackend {
  kPublicHcclLaunch,
  kDirectAclrtPending,
  kUnsupported,
};

struct HcommCustomOpPackageProbe {
  bool installed = false;
  bool payload_ready = false;
  bool aicpu_tar_present = false;
  bool aicpu_tar_readable = false;
  std::string vendor = "none";
  std::string json_path;
  std::string aicpu_tar_path;
  std::string source = "none";
  std::string payload_reason = "not-checked";
};

struct HcommLauncherDecision {
  HcommLauncherBackend backend = HcommLauncherBackend::kUnsupported;
  bool custom_op_build = FLUME_BUILD_HCOMM_CUSTOM_OP;
  bool public_hccl_launch = FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH;
  bool direct_aclrt_launch = FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH;
  bool thread_export = FLUME_HAVE_HCOMM_THREAD_EXPORT;
  bool hcomm_primitives = FLUME_HAVE_HCOMM_PRIMITIVES;
  HcommCustomOpPackageProbe package;
  std::vector<std::string> missing;
};

void AppendMissing(std::vector<std::string>* missing, const std::string& item) {
  if (missing != nullptr) {
    missing->push_back(item);
  }
}

bool FileExists(const std::string& path) {
  return !path.empty() && access(path.c_str(), F_OK) == 0;
}

bool FileReadable(const std::string& path) {
  return !path.empty() && access(path.c_str(), R_OK) == 0;
}

std::string ReadTextFile(const std::string& path) {
  std::ifstream input(path);
  if (!input) {
    return "";
  }
  std::ostringstream out;
  out << input.rdbuf();
  return out.str();
}

bool TextContains(const std::string& text, const char* needle) {
  return needle != nullptr && text.find(needle) != std::string::npos;
}

bool JsonLooksPayloadReady(const std::string& json_text,
                           std::string* reason) {
  if (TextContains(json_text,
                   "\"packageProvenance\": \"host-diagnostic\"")) {
    if (reason != nullptr) {
      *reason = "host direct-build package is not AICPU device-qualified";
    }
    return false;
  }
  const char* required[] = {
      FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V4_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V9_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V10_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V11_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V12_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V13_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V14_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V15_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V16_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V17_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V18_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V19_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SUPPORTS_OFFICIAL_P2P_LAYOUT_FUNC,
      FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION_FUNC,
      FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT_FUNC,
      FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION_FUNC,
      FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT_FUNC,
      FLUME_HCOMM_PAYLOAD_PRIMITIVE_DEPS_FUNC,
      FLUME_HCOMM_PAYLOAD_NO_HCCL_SENDRECV_DEPS_FUNC,
      FLUME_HCOMM_PAYLOAD_NO_HCCL_PAYLOAD_API_DEPS_FUNC,
      FLUME_HCOMM_PAYLOAD_BUILD_MODE_INTERNAL_PAYLOAD_FUNC,
  };
  if (json_text.empty()) {
    if (reason != nullptr) {
      *reason = "payload JSON unreadable or empty";
    }
    return false;
  }
  for (const char* item : required) {
    if (!TextContains(json_text, item)) {
      if (reason != nullptr) {
        *reason = std::string("payload JSON missing ") + item;
      }
      return false;
    }
  }
  if (reason != nullptr) {
    *reason = "payload-ready JSON markers present";
  }
  return true;
}

const char kFlumeHcommPayloadAicpuTar[] = "aicpu_flume_hcomm_payload.tar.gz";

std::string NormalizeAscendRoot(std::string root) {
  const std::string suffix = "/opp";
  if (root.size() > suffix.size() &&
      root.compare(root.size() - suffix.size(), suffix.size(), suffix) == 0) {
    root.resize(root.size() - suffix.size());
  }
  return root;
}

void AppendUnique(std::vector<std::string>* values, const std::string& value) {
  if (values == nullptr || value.empty()) {
    return;
  }
  if (std::find(values->begin(), values->end(), value) == values->end()) {
    values->push_back(value);
  }
}

std::vector<std::string> SplitCommaList(const char* text) {
  std::vector<std::string> items;
  if (text == nullptr || text[0] == '\0') {
    return items;
  }
  std::string value(text);
  size_t start = 0;
  while (start <= value.size()) {
    size_t end = value.find(',', start);
    std::string item = value.substr(
        start, end == std::string::npos ? std::string::npos : end - start);
    if (!item.empty()) {
      items.push_back(item);
    }
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return items;
}

std::string DirName(const std::string& path) {
  size_t pos = path.find_last_of('/');
  if (pos == std::string::npos) {
    return ".";
  }
  if (pos == 0) {
    return "/";
  }
  return path.substr(0, pos);
}

std::string ReplacePathSegment(std::string path,
                               const std::string& from,
                               const std::string& to) {
  size_t pos = path.find(from);
  if (pos == std::string::npos) {
    return "";
  }
  path.replace(pos, from.size(), to);
  return path;
}

std::vector<std::string> AicpuTarCandidatesForJson(
    const std::string& json_path) {
  std::vector<std::string> candidates;
  AppendUnique(&candidates, DirName(json_path) + "/" +
                            kFlumeHcommPayloadAicpuTar);
  std::string opp_candidate = ReplacePathSegment(
      json_path, "/aicpu/config/", "/aicpu/kernel/");
  if (!opp_candidate.empty()) {
    AppendUnique(&candidates, DirName(opp_candidate) + "/" +
                              kFlumeHcommPayloadAicpuTar);
  }
  return candidates;
}

std::string FindAicpuTarForJson(const std::string& json_path) {
  for (const std::string& candidate : AicpuTarCandidatesForJson(json_path)) {
    if (FileExists(candidate)) {
      return candidate;
    }
  }
  return "";
}

std::vector<std::string> AscendHomeCandidates() {
  std::vector<std::string> roots;
  const char* flume_root = std::getenv("FLUME_HCOMM_CUSTOM_OP_ROOT");
  if (flume_root != nullptr && flume_root[0] != '\0') {
    AppendUnique(&roots, NormalizeAscendRoot(flume_root));
  }
  const char* ascend_home = std::getenv("ASCEND_HOME_PATH");
  if (ascend_home != nullptr && ascend_home[0] != '\0') {
    AppendUnique(&roots, NormalizeAscendRoot(ascend_home));
  }
  const char* custom_opp = std::getenv("ASCEND_CUSTOM_OPP_PATH");
  if (custom_opp != nullptr && custom_opp[0] != '\0') {
    AppendUnique(&roots, NormalizeAscendRoot(custom_opp));
  }
  const char* opp_path = std::getenv("ASCEND_OPP_PATH");
  if (opp_path != nullptr && opp_path[0] != '\0') {
    AppendUnique(&roots, NormalizeAscendRoot(opp_path));
  }
  AppendUnique(&roots, "/usr/local/Ascend/cann");
  return roots;
}

HcommCustomOpPackageProbe ProbeHcommCustomOpPackage() {
  HcommCustomOpPackageProbe probe;
  const char* explicit_json = std::getenv("FLUME_HCOMM_CUSTOM_OP_JSON");
  const char* explicit_tar = std::getenv("FLUME_HCOMM_CUSTOM_OP_AICPU_TAR");
  if (explicit_json != nullptr && explicit_json[0] != '\0') {
    probe.installed = FileExists(explicit_json);
    probe.vendor = "explicit";
    probe.json_path = explicit_json;
    if (explicit_tar != nullptr && explicit_tar[0] != '\0') {
      probe.aicpu_tar_path = explicit_tar;
    } else {
      probe.aicpu_tar_path = FindAicpuTarForJson(probe.json_path);
    }
    probe.aicpu_tar_present = FileExists(probe.aicpu_tar_path);
    probe.aicpu_tar_readable = FileReadable(probe.aicpu_tar_path);
    probe.source = probe.installed ? "explicit-json" : "explicit-json-missing";
    if (probe.installed) {
      probe.payload_ready =
          JsonLooksPayloadReady(ReadTextFile(probe.json_path),
                                &probe.payload_reason);
      if (probe.payload_ready && !probe.aicpu_tar_present) {
        probe.payload_ready = false;
        probe.payload_reason = "payload AICPU tar missing beside JSON";
      } else if (probe.payload_ready && !probe.aicpu_tar_readable) {
        probe.payload_ready = false;
        probe.payload_reason = "payload AICPU tar is not readable";
      }
    } else {
      probe.payload_reason = "custom-op JSON missing";
    }
    return probe;
  }
  std::vector<std::string> vendors =
      SplitCommaList(std::getenv("FLUME_HCOMM_CUSTOM_OP_VENDOR"));
  if (vendors.empty()) {
    vendors.push_back("flume");
    vendors.push_back("cust");
  }
  const char* json_name = "libflume_hcomm_payload_aicpu_kernel.json";
  for (const std::string& root : AscendHomeCandidates()) {
    for (const std::string& vendor : vendors) {
      std::string json_path = root + "/opp/vendors/" + vendor +
                              "/aicpu/config/" + json_name;
      if (FileExists(json_path)) {
        probe.installed = true;
        probe.vendor = vendor;
        probe.json_path = json_path;
        probe.aicpu_tar_path = root + "/opp/vendors/" + vendor +
                               "/aicpu/kernel/" +
                               kFlumeHcommPayloadAicpuTar;
        probe.aicpu_tar_present = FileExists(probe.aicpu_tar_path);
        probe.aicpu_tar_readable = FileReadable(probe.aicpu_tar_path);
        probe.source = "root-scan";
        probe.payload_ready =
            JsonLooksPayloadReady(ReadTextFile(probe.json_path),
                                  &probe.payload_reason);
        if (probe.payload_ready && !probe.aicpu_tar_present) {
          probe.payload_ready = false;
          probe.payload_reason = "payload AICPU tar missing in OPP layout";
        } else if (probe.payload_ready && !probe.aicpu_tar_readable) {
          probe.payload_ready = false;
          probe.payload_reason = "payload AICPU tar is not readable";
        }
        return probe;
      }
    }
  }
  return probe;
}

HcommLauncherDecision DecideHcommLauncherBackend() {
  HcommLauncherDecision decision;
  decision.package = ProbeHcommCustomOpPackage();
  if (!decision.custom_op_build) {
    AppendMissing(&decision.missing, "custom-op build disabled");
  }
  if (!decision.public_hccl_launch) {
    AppendMissing(&decision.missing, "public HcclAicpuKernelLaunch unavailable");
  }
  if (!decision.direct_aclrt_launch) {
    AppendMissing(&decision.missing, "ACL runtime custom-op launch APIs unavailable");
  }
  if (!decision.hcomm_primitives) {
    AppendMissing(&decision.missing,
                  "host HCOMM primitive APIs unavailable");
  }
  if (!decision.package.installed) {
    AppendMissing(&decision.missing, "Flume custom-op package not installed");
  }

  if (decision.custom_op_build && decision.public_hccl_launch &&
      decision.package.installed) {
    decision.backend = HcommLauncherBackend::kPublicHcclLaunch;
  } else if (decision.custom_op_build && decision.direct_aclrt_launch &&
             decision.package.installed) {
    decision.backend = HcommLauncherBackend::kDirectAclrtPending;
  }
  return decision;
}

std::string JoinReasons(const std::vector<std::string>& reasons) {
  if (reasons.empty()) {
    return "none";
  }
  std::string joined;
  for (size_t i = 0; i < reasons.size(); ++i) {
    if (i != 0) {
      joined += ",";
    }
    joined += reasons[i];
  }
  return joined;
}

std::string DescribeHcommLauncherDecision(
    const HcommLauncherDecision& decision) {
  const char* selected = "unsupported";
  if (decision.backend == HcommLauncherBackend::kPublicHcclLaunch) {
    selected = "public_hccl_launch";
  } else if (decision.backend == HcommLauncherBackend::kDirectAclrtPending) {
    selected = "direct_aclrt";
  }
  std::string direct_candidate =
      decision.custom_op_build && decision.direct_aclrt_launch &&
              decision.package.installed ?
          "available" :
          "blocked";
  std::string direct_canary_candidate =
      decision.custom_op_build && decision.direct_aclrt_launch &&
              decision.package.installed ?
          "available" :
          "blocked";
  return std::string("stage3b3b_launcher_router=selected:") + selected +
         " custom_op_build=" + (decision.custom_op_build ? "on" : "off") +
         " public_hccl_launch=" +
         (decision.public_hccl_launch ? "on" : "off") +
         " direct_aclrt=" + (decision.direct_aclrt_launch ? "on" : "off") +
         " direct_aclrt_candidate=" + direct_candidate +
         " stage3b3d_no_internal_headers=on direct_aclrt_canary_candidate=" +
         direct_canary_candidate +
         " thread_export=" + (decision.thread_export ? "on" : "off") +
         " hcomm_primitives=" + (decision.hcomm_primitives ? "on" : "off") +
         " custom_op_package=" +
         (decision.package.installed ? "present" : "missing") +
         " payload_package=" +
         (decision.package.payload_ready ? "ready" : "not-ready") +
         " package_aicpu_tar=" +
         (decision.package.aicpu_tar_present ? "present" : "missing") +
         " package_aicpu_tar_readable=" +
         (decision.package.aicpu_tar_readable ? "yes" : "no") +
         " package_vendor=" + decision.package.vendor +
         " package_source=" + decision.package.source +
         " payload_package_reason=\"" + decision.package.payload_reason + "\"" +
         " reason=\"" + JoinReasons(decision.missing) + "\"";
}

std::string AclErrorMessage(aclError ret) {
  return std::string("ACL_ERROR(") + std::to_string(static_cast<int>(ret)) + ")";
}

const char* AclStreamSyncApiName() {
#if FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT
  return "aclrtSynchronizeStreamWithTimeout";
#else
  return "aclrtSynchronizeStream";
#endif
}

aclError SyncAclStreamForHcomm(aclrtStream stream, uint32_t timeout_sec) {
#if FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT
  const uint64_t timeout_ms = static_cast<uint64_t>(timeout_sec) * 1000U;
  const int32_t acl_timeout =
      timeout_ms > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) ?
          std::numeric_limits<int32_t>::max() :
          static_cast<int32_t>(timeout_ms);
  return aclrtSynchronizeStreamWithTimeout(stream, acl_timeout);
#else
  (void)timeout_sec;
  return aclrtSynchronizeStream(stream);
#endif
}

uint8_t HcommPayloadLocalPrimeByte(flume::hcomm_payload::PayloadRole role,
                                   uint32_t local_rank,
                                   uint32_t peer_rank,
                                   uint64_t offset) {
  const uint8_t role_seed =
      role == flume::hcomm_payload::PayloadRole::kSend ? 0x5AU : 0xC3U;
  return static_cast<uint8_t>(
      0xA5U ^ role_seed ^ ((local_rank * 17U) & 0xFFU) ^
      ((peer_rank * 31U) & 0xFFU) ^ (offset & 0xFFU) ^
      ((offset >> 8U) & 0xFFU));
}

aclError PrimeHcommPayloadLocalBuffer(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    const HcommChannelResourceInfo& resource_info,
    uint64_t bytes) {
  constexpr size_t kChunkBytes = 64U * 1024U;
  std::vector<uint8_t> scratch(kChunkBytes);
  uint64_t copied = 0;
  while (copied < bytes) {
    const uint64_t remaining = bytes - copied;
    const size_t chunk =
        static_cast<size_t>(std::min<uint64_t>(remaining, scratch.size()));
    for (size_t i = 0; i < chunk; ++i) {
      scratch[i] =
          HcommPayloadLocalPrimeByte(role, state.rank, peer_rank, copied + i);
    }
    uint8_t* dst = static_cast<uint8_t*>(resource_info.local_buffer) + copied;
    const size_t dst_max =
        static_cast<size_t>(resource_info.local_buffer_bytes - copied);
    aclError ret = aclrtMemcpy(dst, dst_max, scratch.data(), chunk,
                               ACL_MEMCPY_HOST_TO_DEVICE);
    if (ret != ACL_SUCCESS) {
      return ret;
    }
    copied += chunk;
  }
  return ACL_SUCCESS;
}

std::string PayloadLocalPrimeDetail(uint64_t bytes) {
  return std::string(" payload_local_buffer_prime=passed"
                     " payload_local_buffer_prime_pattern=strict-sentinel-v1"
                     " payload_local_buffer_prime_source=host-sentinel-not-payload"
                     " payload_local_buffer_prime_bytes=") +
         std::to_string(bytes);
}

std::string PayloadKernelStatusName(uint32_t status) {
  switch (status) {
    case 0xFFFFFFFFU:
      return "not-written";
    case FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS:
      return "success";
    case FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT:
      return "invalid-argument";
    case FLUME_HCOMM_PAYLOAD_STATUS_HCOMM_ERROR:
      return "hcomm-error";
    case FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED:
      return "thread-notify-wait-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED:
      return "batch-start-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED:
      return "local-copy-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED:
      return "ready-notify-record-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED:
      return "done-notify-wait-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED:
      return "ready-notify-wait-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED:
      return "remote-read-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED:
      return "done-notify-record-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED:
      return "batch-end-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED:
      return "thread-notify-record-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED:
      return "channel-fence-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED:
      return "comm-acquire-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED:
      return "comm-release-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED:
      return "output-copy-failed";
    case FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_WRITE_FAILED:
      return "remote-write-failed";
    default:
      return std::string("unknown-") + std::to_string(status);
  }
}

std::string PayloadFailureStepName(uint32_t status) {
  switch (status) {
    case 0xFFFFFFFFU:
      return "kernel-not-written";
    case FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS:
      return "none";
    case FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT:
      return "validate-descriptor";
    case FLUME_HCOMM_PAYLOAD_STATUS_HCOMM_ERROR:
      return "pre-dispatch";
    case FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED:
      return "host-aicpu-thread-notify-wait";
    case FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED:
      return "batch-start";
    case FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED:
      return "local-copy";
    case FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED:
      return "ready-notify-record";
    case FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED:
      return "done-notify-wait";
    case FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED:
      return "ready-notify-wait";
    case FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED:
      return "remote-read";
    case FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED:
      return "done-notify-record";
    case FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED:
      return "batch-end";
    case FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED:
      return "host-aicpu-thread-notify-record";
    case FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED:
      return "channel-fence";
    case FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED:
      return "comm-acquire";
    case FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED:
      return "comm-release";
    case FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED:
      return "output-copy";
    case FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_WRITE_FAILED:
      return "remote-write";
    default:
      return std::string("unknown-") + std::to_string(status);
  }
}

const char* PayloadCommBindingName(
    flume_hcomm_payload_comm_binding_t binding) {
  switch (binding) {
    case FLUME_HCOMM_PAYLOAD_COMM_BINDING_COMM_NAME:
      return "comm-name";
    case FLUME_HCOMM_PAYLOAD_COMM_BINDING_DIAGNOSTIC_SKIP:
      return "diagnostic-skip";
    case FLUME_HCOMM_PAYLOAD_COMM_BINDING_CHANNEL_HANDLE:
      return "channel-handle";
    default:
      return "unknown";
  }
}

flume_hcomm_payload_comm_binding_t PayloadDescCommBinding(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  if ((desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING) != 0) {
    return FLUME_HCOMM_PAYLOAD_COMM_BINDING_CHANNEL_HANDLE;
  }
  if ((desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE) != 0) {
    return FLUME_HCOMM_PAYLOAD_COMM_BINDING_DIAGNOSTIC_SKIP;
  }
  return FLUME_HCOMM_PAYLOAD_COMM_BINDING_COMM_NAME;
}

const char* PayloadLayoutName(const flume_hcomm_payload_copy_desc_v1& desc) {
  const bool batch_disabled =
      desc.reserved2[0] == FLUME_HCOMM_PAYLOAD_BATCH_MODE_DISABLED;
  const bool recv_direct_output =
      desc.reserved2[1] == FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT;
  const bool write_path =
      (desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0;
  const bool write_with_notify =
      (desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY) != 0;
  const flume_hcomm_payload_comm_binding_t comm_binding =
      PayloadDescCommBinding(desc);
  if (write_with_notify) {
    return "write-with-notify";
  }
  if (write_path) {
    return "write";
  }
  if (batch_disabled &&
      comm_binding == FLUME_HCOMM_PAYLOAD_COMM_BINDING_CHANNEL_HANDLE &&
      recv_direct_output) {
    return "official-p2p";
  }
  return recv_direct_output ? "read-direct-output" : "read-default";
}

const char* PayloadDescPrimitivePath(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  const bool send_role = desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND;
  const bool recv_role = desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV;
  const bool recv_direct_output =
      desc.reserved2[1] == FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT;
  const bool write_path =
      (desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0;
  const bool write_with_notify =
      (desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY) != 0;
  if (send_role) {
    if (write_with_notify) {
      return "send-write-with-notify";
    }
    return write_path ? "send-write" : "send-local-copy";
  }
  if (recv_role) {
    if (write_path) {
      return write_with_notify ? "recv-write-notify-local-copy" :
                                 "recv-write-local-copy";
    }
    return recv_direct_output ? "recv-read-direct-output" :
                                "recv-read-local-copy";
  }
  return "unknown";
}

const char* PayloadDescOperandLayout(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  const char* primitive_path = PayloadDescPrimitivePath(desc);
  if (strcmp(primitive_path, "send-local-copy") == 0) {
    return "input-hbm->local-hccl-buffer";
  }
  if (strcmp(primitive_path, "send-write") == 0) {
    return "input-hbm->local-hccl-buffer->remote-hccl-buffer";
  }
  if (strcmp(primitive_path, "send-write-with-notify") == 0) {
    return "input-hbm->local-hccl-buffer->remote-hccl-buffer+ready-notify";
  }
  if (strcmp(primitive_path, "recv-read-local-copy") == 0) {
    return "remote-hccl-buffer->local-hccl-buffer->output-hbm";
  }
  if (strcmp(primitive_path, "recv-read-direct-output") == 0) {
    return "remote-hccl-buffer->output-hbm";
  }
  if (strcmp(primitive_path, "recv-write-local-copy") == 0 ||
      strcmp(primitive_path, "recv-write-notify-local-copy") == 0) {
    return "local-hccl-buffer->output-hbm";
  }
  return "unknown";
}

const char* PayloadPrimitivePlan(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  const char* primitive_path = PayloadDescPrimitivePath(desc);
  if (strcmp(primitive_path, "send-local-copy") == 0) {
    return "hcomm-local-copy+notify-record+notify-wait";
  }
  if (strcmp(primitive_path, "send-write") == 0) {
    return "hcomm-local-copy+hcomm-write+notify-record+notify-wait";
  }
  if (strcmp(primitive_path, "send-write-with-notify") == 0) {
    return "hcomm-local-copy+hcomm-write-with-notify+notify-wait";
  }
  if (strcmp(primitive_path, "recv-read-local-copy") == 0) {
    return "notify-wait+hcomm-read+hcomm-local-copy+notify-record";
  }
  if (strcmp(primitive_path, "recv-read-direct-output") == 0) {
    return "notify-wait+hcomm-read+notify-record";
  }
  if (strcmp(primitive_path, "recv-write-local-copy") == 0 ||
      strcmp(primitive_path, "recv-write-notify-local-copy") == 0) {
    return "notify-wait+hcomm-local-copy+notify-record";
  }
  return "unknown";
}

uint64_t PayloadEchoBytes(const uint32_t* status_words) {
  return static_cast<uint64_t>(status_words[4]) |
         (static_cast<uint64_t>(status_words[5]) << 32U);
}

uint64_t PayloadEchoDescriptorFingerprint(const uint32_t* status_words) {
  return static_cast<uint64_t>(status_words[8]) |
         (static_cast<uint64_t>(status_words[9]) << 32U);
}

std::string PayloadStatusSchemaDetail() {
  return std::string(" payload_status_schema=v") +
         std::to_string(FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION) +
         " payload_status_word_count=" +
         std::to_string(FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT);
}

std::string PayloadPrimitiveStateDetail(const uint32_t* status_words) {
  if (status_words == nullptr) {
    return "";
  }
  std::string state = "returned";
  if (status_words[0] == 0xFFFFFFFFU) {
    state = "not-written";
  } else if (status_words[0] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS &&
             status_words[1] == 0U) {
    state = "completed";
  } else if (status_words[1] == 0xFFFFFFFFU) {
    state = "pending";
  }
  return " payload_primitive_state=" + state;
}

const char* PayloadValidationReasonName(uint32_t reason) {
  switch (reason) {
    case FLUME_HCOMM_PAYLOAD_VALIDATE_OK:
      return "ok";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_HEADER:
      return "header";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_RANK_SIZE:
      return "rank-size";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_ROLE:
      return "role";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_LOCAL_RANK:
      return "local-rank";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_PEER_RANK:
      return "peer-rank";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_RANK_PAIR:
      return "rank-pair";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_NOTIFY_INDEX:
      return "notify-index";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_BYTES:
      return "bytes";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_THREAD_NOTIFY_MODE:
      return "thread-notify-mode";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_COMPLETION_MODE:
      return "completion-mode";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_AICPU_THREAD:
      return "aicpu-thread";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_CHANNEL_HANDLE:
      return "channel-handle";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_USER_BUFFER:
      return "user-buffer";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_LOCAL_HCCL_BUFFER:
      return "local-hccl-buffer";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_REMOTE_HCCL_BUFFER:
      return "remote-hccl-buffer";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_LOCAL_HCCL_BUFFER_BYTES:
      return "local-hccl-buffer-bytes";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_REMOTE_HCCL_BUFFER_BYTES:
      return "remote-hccl-buffer-bytes";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_STATUS_WORD:
      return "status-word";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_STATUS_WORD_COUNT:
      return "status-word-count";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_STATUS_SCHEMA:
      return "status-schema";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_COMM_NAME:
      return "comm-name";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_BATCH_TAG:
      return "batch-tag";
    case FLUME_HCOMM_PAYLOAD_VALIDATE_CPU_THREAD_ON_AICPU:
      return "cpu-thread-on-aicpu";
    default:
      return "unknown";
  }
}

std::string PayloadValidationReasonDetail(const uint32_t* status_words) {
  if (status_words == nullptr ||
      status_words[0] != FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT) {
    return " payload_validation_reason=not-applicable";
  }
  return std::string(" payload_validation_reason=") +
         PayloadValidationReasonName(status_words[1]) +
         " payload_validation_reason_code=" +
         std::to_string(status_words[1]);
}

std::string HostPayloadValidationDetail(uint32_t reason) {
  return std::string(" payload_host_descriptor_validation=") +
         (reason == FLUME_HCOMM_PAYLOAD_VALIDATE_OK ? "passed" : "failed") +
         " payload_host_validation_reason=" +
         PayloadValidationReasonName(reason) +
         " payload_host_validation_reason_code=" +
         std::to_string(reason);
}

std::string PayloadDescriptorDetail(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  const bool batch_disabled =
      desc.reserved2[0] == FLUME_HCOMM_PAYLOAD_BATCH_MODE_DISABLED;
  const char* batch_tag_state = "custom";
  if (desc.batch_tag[0] == '\0') {
    batch_tag_state = "empty";
  } else if (strncmp(desc.batch_tag, kDefaultHcommPayloadBatchTag,
                    FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES) == 0) {
    batch_tag_state = "default";
  }
  const bool recv_direct_output =
      desc.reserved2[1] == FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT;
  const bool write_path =
      (desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0;
  const bool write_with_notify =
      (desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY) != 0;
  const flume_hcomm_payload_comm_binding_t comm_binding =
      PayloadDescCommBinding(desc);
  const bool skip_comm_acquire =
      (desc.completion_mode &
       (FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE |
        FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING)) != 0;
  return std::string(" payload_desc_role=") + std::to_string(desc.role) +
         " payload_desc_local_rank=" + std::to_string(desc.local_rank) +
         " payload_desc_peer_rank=" + std::to_string(desc.peer_rank) +
         " payload_desc_rank_size=" + std::to_string(desc.rank_size) +
         " payload_desc_bytes=" + std::to_string(desc.bytes) +
         " payload_desc_ready_notify_idx=" +
         std::to_string(desc.ready_notify_idx) +
         " payload_desc_done_notify_idx=" +
         std::to_string(desc.done_notify_idx) +
         " payload_desc_thread_notify_mode=" +
         std::to_string(desc.thread_notify_mode) +
         " payload_desc_completion_mode=" +
         std::to_string(desc.completion_mode) +
         " payload_comm_acquire=" +
         (skip_comm_acquire ? "skipped" : "default") +
         " payload_comm_binding=" + PayloadCommBindingName(comm_binding) +
         " payload_desc_timeout_sec=" + std::to_string(desc.timeout_sec) +
         " payload_desc_status_schema=v" +
         std::to_string(desc.status_schema_version) +
         " payload_desc_status_word_count=" +
         std::to_string(desc.status_word_count) +
         " payload_desc_fingerprint=" +
         std::to_string(flume_hcomm_payload_copy_desc_fingerprint(&desc)) +
         " payload_desc_batch_mode=" + (batch_disabled ? "off" : "on") +
         " payload_desc_batch_tag=" + batch_tag_state +
         " payload_transfer_mode=" +
         (write_with_notify ? "write-with-notify" :
                              (write_path ? "write" : "read")) +
         " payload_layout=" + PayloadLayoutName(desc) +
         " payload_write_notify_backend=" +
         (write_with_notify ? HcommWriteWithNotifyBackendName() : "none") +
         " payload_recv_path=" +
         (recv_direct_output ? "direct-output" : "local-buffer") +
         " payload_desc_primitive_path=" + PayloadDescPrimitivePath(desc) +
         " payload_desc_operand_layout=" + PayloadDescOperandLayout(desc) +
         " payload_primitive_plan=" + PayloadPrimitivePlan(desc) +
         " payload_desc_local_hccl_buffer_bytes=" +
         std::to_string(desc.local_hccl_buffer_bytes) +
         " payload_desc_remote_hccl_buffer_bytes=" +
         std::to_string(desc.remote_hccl_buffer_bytes);
}

std::string PayloadBatchModeDetail(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  return desc.reserved2[0] == FLUME_HCOMM_PAYLOAD_BATCH_MODE_DISABLED ?
      "payload_batch_mode=off" : "payload_batch_mode=on";
}

std::string PayloadEchoWordsDetail(const uint32_t* status_words) {
  if (status_words == nullptr) {
    return "";
  }
  return std::string(" payload_echo_role=") +
         std::to_string(status_words[2]) +
         " payload_echo_peer_rank=" + std::to_string(status_words[3]) +
         " payload_echo_bytes=" +
         std::to_string(PayloadEchoBytes(status_words)) +
         " payload_echo_local_rank=" + std::to_string(status_words[6]) +
         " payload_echo_completion_mode=" +
         std::to_string(status_words[7]) +
         " payload_echo_desc_fingerprint=" +
         std::to_string(PayloadEchoDescriptorFingerprint(status_words));
}

std::string PayloadDataProbeDetail(const uint32_t* status_words) {
  if (status_words == nullptr) {
    return "";
  }
  const bool observed =
      status_words[10] != 0xFFFFFFFFU &&
      status_words[11] != 0xFFFFFFFFU &&
      status_words[12] != 0xFFFFFFFFU &&
      status_words[13] != 0xFFFFFFFFU &&
      status_words[14] != 0xFFFFFFFFU &&
      status_words[15] != 0xFFFFFFFFU &&
      status_words[16] != 0xFFFFFFFFU;
  auto fingerprint = [](uint32_t word) {
    return word == FLUME_HCOMM_PAYLOAD_DATA_FINGERPRINT_NOT_SAMPLED ?
        std::string("not-sampled") : std::to_string(word);
  };
  return std::string(" payload_data_probe=") +
         (observed ? "observed" : "missing") +
         " payload_data_user_entry_fingerprint=" +
         fingerprint(status_words[10]) +
         " payload_data_local_entry_fingerprint=" +
         fingerprint(status_words[11]) +
         " payload_data_local_exit_fingerprint=" +
         fingerprint(status_words[12]) +
         " payload_data_user_exit_fingerprint=" +
         fingerprint(status_words[13]) +
         " payload_data_sample_bytes=" +
         std::to_string(status_words[14]) +
         " payload_data_remote_entry_fingerprint=" +
         fingerprint(status_words[15]) +
         " payload_data_transfer_exit_fingerprint=" +
         fingerprint(status_words[16]);
}

std::string PayloadDeviceDataSideDetail(
    const flume_hcomm_payload_copy_desc_v1& desc,
    const uint32_t* status_words) {
  if (status_words == nullptr) {
    return " payload_device_data_side=missing"
           " payload_device_data_side_reason=missing-status";
  }
  const uint32_t missing = 0xFFFFFFFFU;
  for (uint32_t index = 10; index <= 16; ++index) {
    if (status_words[index] == missing) {
      return " payload_device_data_side=missing"
             " payload_device_data_side_reason=missing-fingerprint";
    }
  }
  const uint32_t user_entry = status_words[10];
  const uint32_t remote_entry = status_words[15];
  const uint32_t transfer_exit = status_words[16];
  const uint32_t local_exit = status_words[12];
  const uint32_t user_exit = status_words[13];
  const bool write_path =
      (desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0;
  const bool recv_direct_output =
      desc.reserved2[1] == FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT;
  auto result = [](const char* state, const char* reason) {
    return std::string(" payload_device_data_side=") + state +
           " payload_device_data_side_reason=" + reason;
  };
  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND) {
    if (user_entry != local_exit) {
      return result("failed", "send-user-local-mismatch");
    }
    if (user_entry != user_exit) {
      return result("failed", "send-user-exit-mismatch");
    }
    if (write_path && transfer_exit !=
                          FLUME_HCOMM_PAYLOAD_DATA_FINGERPRINT_NOT_SAMPLED &&
        transfer_exit != user_entry) {
      return result("failed", "send-transfer-exit-mismatch");
    }
    return result("passed", write_path ? "send-write-side" :
                                       "send-read-side");
  }
  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    if (remote_entry != transfer_exit) {
      return result("failed", "recv-transfer-exit-mismatch");
    }
    if (recv_direct_output && !write_path) {
      if (remote_entry != user_exit) {
        return result("failed", "recv-direct-output-mismatch");
      }
      return result("passed", "recv-read-direct-output-side");
    }
    if (remote_entry != local_exit) {
      return result("failed", "recv-local-exit-mismatch");
    }
    if (remote_entry != user_exit) {
      return result("failed", "recv-user-exit-mismatch");
    }
    return result("passed", write_path ? "recv-write-side" :
                                       "recv-read-local-buffer-side");
  }
  return result("missing", "unknown-role");
}

void InitPayloadTraceWords(uint32_t* trace_words) {
  if (trace_words == nullptr) {
    return;
  }
  for (uint32_t i = 0; i < FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT; ++i) {
    trace_words[i] = 0xFFFFFFFFU;
  }
}

std::string PayloadTraceEventName(uint32_t event) {
  switch (event) {
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_NONE:
      return "none";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_ENTER:
      return "kernel-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_ENTER:
      return "comm-acquire-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_DONE:
      return "comm-acquire-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_ENTER:
      return "batch-start-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_DONE:
      return "batch-start-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_ENTER:
      return "thread-notify-wait-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_DONE:
      return "thread-notify-wait-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_ENTER:
      return "send-local-copy-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_DONE:
      return "send-local-copy-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_ENTER:
      return "send-ready-record-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_DONE:
      return "send-ready-record-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_ENTER:
      return "send-done-wait-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_DONE:
      return "send-done-wait-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_ENTER:
      return "recv-ready-wait-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_DONE:
      return "recv-ready-wait-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_ENTER:
      return "recv-remote-read-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_DONE:
      return "recv-remote-read-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_ENTER:
      return "recv-channel-fence-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_DONE:
      return "recv-channel-fence-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_ENTER:
      return "recv-output-copy-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_DONE:
      return "recv-output-copy-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_ENTER:
      return "recv-done-record-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_DONE:
      return "recv-done-record-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_ENTER:
      return "thread-notify-record-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_DONE:
      return "thread-notify-record-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_ENTER:
      return "batch-end-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_DONE:
      return "batch-end-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_ENTER:
      return "comm-release-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_DONE:
      return "comm-release-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_ENTER:
      return "send-remote-write-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_DONE:
      return "send-remote-write-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_ENTER:
      return "send-channel-fence-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_DONE:
      return "send-channel-fence-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT:
      return "kernel-exit";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_ENTER:
      return "send-remote-write-notify-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_DONE:
      return "send-remote-write-notify-done";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_ENTER:
      return "send-remote-write-notify-nbi-enter";
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_DONE:
      return "send-remote-write-notify-nbi-done";
    default:
      return std::string("unknown-") + std::to_string(event);
  }
}

std::vector<uint32_t> PayloadTraceEvents(const uint32_t* trace_words) {
  std::vector<uint32_t> events;
  if (trace_words == nullptr ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return events;
  }
  const uint32_t count = trace_words[4];
  const uint32_t capacity = FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY;
  const uint32_t event_base = FLUME_HCOMM_PAYLOAD_TRACE_HEADER_WORD_COUNT;
  if (count == 0 || capacity == 0) {
    return events;
  }
  if (count <= capacity) {
    events.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      events.push_back(trace_words[event_base + i]);
    }
    return events;
  }
  events.reserve(capacity);
  const uint32_t first = count % capacity;
  for (uint32_t i = 0; i < capacity; ++i) {
    events.push_back(trace_words[event_base + ((first + i) % capacity)]);
  }
  return events;
}

std::vector<uint32_t> PayloadTraceReturns(const uint32_t* trace_words) {
  std::vector<uint32_t> returns;
  if (trace_words == nullptr ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return returns;
  }
  const uint32_t count = trace_words[4];
  const uint32_t capacity = FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY;
  const uint32_t ret_base =
      FLUME_HCOMM_PAYLOAD_TRACE_HEADER_WORD_COUNT +
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY;
  if (count == 0 || capacity == 0) {
    return returns;
  }
  if (count <= capacity) {
    returns.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      returns.push_back(trace_words[ret_base + i]);
    }
    return returns;
  }
  returns.reserve(capacity);
  const uint32_t first = count % capacity;
  for (uint32_t i = 0; i < capacity; ++i) {
    returns.push_back(trace_words[ret_base + ((first + i) % capacity)]);
  }
  return returns;
}

bool PayloadTraceContainsEvent(const uint32_t* trace_words, uint32_t event) {
  if (trace_words == nullptr ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return false;
  }
  const uint32_t count = trace_words[4];
  const uint32_t capacity = FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY;
  const uint32_t event_base = FLUME_HCOMM_PAYLOAD_TRACE_HEADER_WORD_COUNT;
  if (count == 0 || count > capacity) {
    return false;
  }
  for (uint32_t i = 0; i < count; ++i) {
    if (trace_words[event_base + i] == event) {
      return true;
    }
  }
  return false;
}

bool PayloadTraceUsesWriteNotifyNbi(const uint32_t* trace_words) {
  return PayloadTraceContainsEvent(
             trace_words,
             FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_ENTER) ||
         PayloadTraceContainsEvent(
             trace_words,
             FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_DONE);
}

std::string PayloadTraceWriteNotifyBackend(const uint32_t* trace_words) {
  if (trace_words == nullptr ||
      trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return "missing";
  }
  const bool write_with_notify =
      (trace_words[12] &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY) != 0;
  if (!write_with_notify) {
    return "none";
  }
  if (trace_words[5] == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    return "peer";
  }
  if (PayloadTraceUsesWriteNotifyNbi(trace_words)) {
    return "nbi";
  }
  if (PayloadTraceContainsEvent(
          trace_words,
          FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_ENTER) ||
      PayloadTraceContainsEvent(
          trace_words,
          FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_DONE)) {
    return "blocking";
  }
  return "missing";
}

std::string PayloadTraceEventSequence(const std::vector<uint32_t>& events) {
  if (events.empty()) {
    return "empty";
  }
  std::string sequence;
  for (size_t i = 0; i < events.size(); ++i) {
    if (i != 0) {
      sequence += ">";
    }
    sequence += PayloadTraceEventName(events[i]);
  }
  return sequence;
}

std::string PayloadTraceReturnSequence(const std::vector<uint32_t>& returns) {
  if (returns.empty()) {
    return "empty";
  }
  std::string sequence;
  for (size_t i = 0; i < returns.size(); ++i) {
    if (i != 0) {
      sequence += ">";
    }
    sequence += std::to_string(static_cast<int32_t>(returns[i]));
  }
  return sequence;
}

std::string PayloadTraceFirstErrorDetail(
    const std::vector<uint32_t>& events,
    const std::vector<uint32_t>& returns) {
  if (events.empty() || events.size() != returns.size()) {
    return " payload_trace_first_error_event=missing"
           " payload_trace_first_error_ret=missing"
           " payload_trace_first_error_index=missing";
  }
  for (size_t i = 0; i < events.size(); ++i) {
    if (returns[i] == 0U || returns[i] == 0xFFFFFFFFU) {
      continue;
    }
    return std::string(" payload_trace_first_error_event=") +
           PayloadTraceEventName(events[i]) +
           " payload_trace_first_error_ret=" +
           std::to_string(static_cast<int32_t>(returns[i])) +
           " payload_trace_first_error_index=" + std::to_string(i);
  }
  return " payload_trace_first_error_event=none"
         " payload_trace_first_error_ret=0"
         " payload_trace_first_error_index=-1";
}

std::vector<uint32_t> ExpectedPayloadTraceEvents(const uint32_t* trace_words,
                                                bool include_thread_notify) {
  std::vector<uint32_t> expected;
  auto append = [&expected](uint32_t enter, uint32_t done) {
    expected.push_back(enter);
    expected.push_back(done);
  };
  expected.push_back(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_ENTER);
  const bool batch_enabled =
      trace_words[10] != FLUME_HCOMM_PAYLOAD_BATCH_MODE_DISABLED;
  const bool skip_comm_acquire =
      (trace_words[12] &
       (FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE |
        FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING)) != 0;
  const bool write_path =
      (trace_words[12] & FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0;
  const bool write_with_notify =
      (trace_words[12] &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY) != 0;
  if (!skip_comm_acquire) {
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_DONE);
  }
  if (batch_enabled) {
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_DONE);
  }
  if (include_thread_notify) {
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_DONE);
  }
  if (trace_words[5] == FLUME_HCOMM_NOTIFY_ROLE_SEND) {
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_DONE);
    if (write_path) {
      if (write_with_notify) {
        if (PayloadTraceUsesWriteNotifyNbi(trace_words)) {
          append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_ENTER,
                 FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_DONE);
        } else {
          append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_ENTER,
                 FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_DONE);
        }
      } else {
        append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_ENTER,
               FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_DONE);
      }
      if ((trace_words[12] & FLUME_HCOMM_PAYLOAD_COMPLETION_MODE_MASK) ==
          FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN) {
        append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_ENTER,
               FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_DONE);
      }
    }
    if (!write_with_notify) {
      append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_ENTER,
             FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_DONE);
    }
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_DONE);
  } else if (trace_words[5] == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_DONE);
    if (!write_path) {
      append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_ENTER,
             FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_DONE);
      if ((trace_words[12] & FLUME_HCOMM_PAYLOAD_COMPLETION_MODE_MASK) ==
          FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN) {
        append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_ENTER,
               FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_DONE);
      }
    }
    if (write_path ||
        trace_words[11] != FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT) {
      append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_ENTER,
             FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_DONE);
    }
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_DONE);
  }
  if (include_thread_notify) {
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_DONE);
  }
  if (batch_enabled) {
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_DONE);
  }
  if (!skip_comm_acquire) {
    append(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_ENTER,
           FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_DONE);
  }
  expected.push_back(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT);
  return expected;
}

std::string PayloadTraceOrderState(const uint32_t* trace_words,
                                   const std::vector<uint32_t>& events) {
  if (trace_words == nullptr ||
      trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return "missing";
  }
  if (trace_words[4] > FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY) {
    return "truncated";
  }
  if (events == ExpectedPayloadTraceEvents(trace_words, false) ||
      events == ExpectedPayloadTraceEvents(trace_words, true)) {
    return "passed";
  }
  return "observed";
}

bool PayloadTraceOrderMatches(const uint32_t* trace_words,
                              const std::vector<uint32_t>& events,
                              bool include_thread_notify) {
  if (trace_words == nullptr ||
      trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return false;
  }
  if (trace_words[4] > FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY) {
    return false;
  }
  return events == ExpectedPayloadTraceEvents(trace_words,
                                             include_thread_notify);
}

bool PayloadTraceEventIsEnter(uint32_t event) {
  switch (event) {
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_ENTER:
    case FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_ENTER:
      return true;
    default:
      return false;
  }
}

std::string PayloadTraceReturnOrderState(
    const uint32_t* trace_words,
    const std::vector<uint32_t>& events,
    const std::vector<uint32_t>& returns) {
  if (trace_words == nullptr ||
      trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return "missing";
  }
  if (events.empty() || events.size() != returns.size()) {
    return "missing";
  }
  if (trace_words[4] > FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY) {
    return "truncated";
  }
  if (PayloadTraceOrderState(trace_words, events) != "passed") {
    return "observed";
  }
  for (size_t i = 0; i < events.size(); ++i) {
    const uint32_t expected_ret =
        PayloadTraceEventIsEnter(events[i]) ? 0xFFFFFFFFU : 0U;
    if (returns[i] != expected_ret) {
      return "observed";
    }
  }
  return "passed";
}

bool PayloadTraceReturnOrderMatches(const uint32_t* trace_words,
                                    const std::vector<uint32_t>& events,
                                    const std::vector<uint32_t>& returns,
                                    bool include_thread_notify) {
  if (trace_words == nullptr ||
      trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return false;
  }
  if (events.empty() || events.size() != returns.size()) {
    return false;
  }
  if (trace_words[4] > FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY) {
    return false;
  }
  if (!PayloadTraceOrderMatches(trace_words, events,
                                include_thread_notify)) {
    return false;
  }
  for (size_t i = 0; i < events.size(); ++i) {
    const uint32_t expected_ret =
        PayloadTraceEventIsEnter(events[i]) ? 0xFFFFFFFFU : 0U;
    if (returns[i] != expected_ret) {
      return false;
    }
  }
  return true;
}

std::string PayloadTracePrimitivePath(const uint32_t* trace_words) {
  if (trace_words == nullptr ||
      trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return "missing";
  }
  const bool write_path =
      (trace_words[12] & FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0;
  const bool write_with_notify =
      (trace_words[12] &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY) != 0;
  if (trace_words[5] == FLUME_HCOMM_NOTIFY_ROLE_SEND) {
    if (write_with_notify) {
      return "send-write-with-notify";
    }
    return write_path ? "send-write" : "send-local-copy";
  }
  if (trace_words[5] == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    if (write_path) {
      return write_with_notify ? "recv-write-notify-local-copy" :
                                 "recv-write-local-copy";
    }
    return trace_words[11] == FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT ?
        "recv-read-direct-output" : "recv-read-local-copy";
  }
  return "unknown";
}

std::string PayloadTraceOperandLayout(const uint32_t* trace_words) {
  const std::string primitive_path = PayloadTracePrimitivePath(trace_words);
  if (primitive_path == "send-local-copy") {
    return "input-hbm->local-hccl-buffer";
  }
  if (primitive_path == "send-write") {
    return "input-hbm->local-hccl-buffer->remote-hccl-buffer";
  }
  if (primitive_path == "send-write-with-notify") {
    return "input-hbm->local-hccl-buffer->remote-hccl-buffer+ready-notify";
  }
  if (primitive_path == "recv-read-local-copy") {
    return "remote-hccl-buffer->local-hccl-buffer->output-hbm";
  }
  if (primitive_path == "recv-read-direct-output") {
    return "remote-hccl-buffer->output-hbm";
  }
  if (primitive_path == "recv-write-local-copy") {
    return "local-hccl-buffer->output-hbm";
  }
  if (primitive_path == "recv-write-notify-local-copy") {
    return "local-hccl-buffer->output-hbm";
  }
  return "unknown";
}

struct PayloadTracePrimitiveCounts {
  uint32_t local_copy = 0;
  uint32_t read = 0;
  uint32_t write = 0;
  uint32_t write_notify = 0;
  uint32_t notify_record = 0;
  uint32_t notify_wait = 0;
  uint32_t channel_fence = 0;
  uint32_t comm_acquire = 0;
  uint32_t comm_release = 0;
  uint32_t batch = 0;
  uint32_t thread_notify = 0;
};

uint32_t CountPayloadTraceEvent(const std::vector<uint32_t>& events,
                                uint32_t event) {
  return static_cast<uint32_t>(
      std::count(events.begin(), events.end(), event));
}

PayloadTracePrimitiveCounts CountPayloadTracePrimitives(
    const std::vector<uint32_t>& events) {
  PayloadTracePrimitiveCounts counts;
  counts.local_copy =
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_DONE) +
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_DONE);
  counts.read = CountPayloadTraceEvent(
      events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_DONE);
  counts.write = CountPayloadTraceEvent(
      events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_DONE);
  counts.write_notify =
      CountPayloadTraceEvent(
          events,
          FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_DONE) +
      CountPayloadTraceEvent(
          events,
          FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_DONE);
  counts.notify_record =
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_DONE) +
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_DONE);
  counts.notify_wait =
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_DONE) +
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_DONE);
  counts.channel_fence =
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_DONE) +
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_DONE);
  counts.comm_acquire = CountPayloadTraceEvent(
      events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_DONE);
  counts.comm_release = CountPayloadTraceEvent(
      events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_DONE);
  counts.batch =
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_DONE) +
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_DONE);
  counts.thread_notify =
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_DONE) +
      CountPayloadTraceEvent(
          events, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_DONE);
  return counts;
}

bool PayloadTracePrimitiveCountsEqual(
    const PayloadTracePrimitiveCounts& lhs,
    const PayloadTracePrimitiveCounts& rhs) {
  return lhs.local_copy == rhs.local_copy &&
         lhs.read == rhs.read &&
         lhs.write == rhs.write &&
         lhs.write_notify == rhs.write_notify &&
         lhs.notify_record == rhs.notify_record &&
         lhs.notify_wait == rhs.notify_wait &&
         lhs.channel_fence == rhs.channel_fence &&
         lhs.comm_acquire == rhs.comm_acquire &&
         lhs.comm_release == rhs.comm_release &&
         lhs.batch == rhs.batch &&
         lhs.thread_notify == rhs.thread_notify;
}

std::string PayloadTracePrimitiveCountsDetail(
    const uint32_t* trace_words,
    const std::vector<uint32_t>& events) {
  if (trace_words == nullptr ||
      trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return " payload_trace_primitive_counts=missing";
  }
  const PayloadTracePrimitiveCounts observed =
      CountPayloadTracePrimitives(events);
  const bool include_thread_notify =
      observed.thread_notify != 0 ||
      PayloadTraceContainsEvent(
          trace_words,
          FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_ENTER) ||
      PayloadTraceContainsEvent(
          trace_words,
          FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_ENTER);
  const PayloadTracePrimitiveCounts expected = CountPayloadTracePrimitives(
      ExpectedPayloadTraceEvents(trace_words, include_thread_notify));
  return std::string(" payload_trace_primitive_counts=") +
         (PayloadTracePrimitiveCountsEqual(observed, expected) ?
              "passed" : "observed") +
         " payload_trace_local_copy_count=" +
         std::to_string(observed.local_copy) +
         " payload_trace_read_count=" + std::to_string(observed.read) +
         " payload_trace_write_count=" + std::to_string(observed.write) +
         " payload_trace_write_notify_count=" +
         std::to_string(observed.write_notify) +
         " payload_trace_notify_record_count=" +
         std::to_string(observed.notify_record) +
         " payload_trace_notify_wait_count=" +
         std::to_string(observed.notify_wait) +
         " payload_trace_channel_fence_count=" +
         std::to_string(observed.channel_fence) +
         " payload_trace_comm_acquire_count=" +
         std::to_string(observed.comm_acquire) +
         " payload_trace_comm_release_count=" +
         std::to_string(observed.comm_release) +
         " payload_trace_batch_count=" + std::to_string(observed.batch) +
         " payload_trace_thread_notify_count=" +
         std::to_string(observed.thread_notify);
}

std::string PayloadTraceWordsDetail(const uint32_t* trace_words,
                                    aclError read_status = ACL_SUCCESS) {
  if (trace_words == nullptr) {
    return "";
  }
  const bool schema_ok =
      trace_words[0] == FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION &&
      trace_words[1] == FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT;
  const std::vector<uint32_t> events = PayloadTraceEvents(trace_words);
  const std::vector<uint32_t> returns = PayloadTraceReturns(trace_words);
  std::string state = schema_ok ? "observed" : "missing";
  if (schema_ok &&
      trace_words[2] == FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT &&
      trace_words[3] == 0U &&
      trace_words[15] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS &&
      trace_words[16] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS &&
      trace_words[17] == 0U &&
      PayloadTraceOrderState(trace_words, events) == "passed" &&
      PayloadTraceReturnOrderState(trace_words, events, returns) == "passed") {
    state = "passed";
  }
  return std::string(" payload_trace=") + state +
         " payload_trace_read=\"" + AclErrorMessage(read_status) + "\"" +
         " payload_trace_schema=v" + std::to_string(trace_words[0]) +
         " payload_trace_word_count=" + std::to_string(trace_words[1]) +
         " payload_trace_event=" + PayloadTraceEventName(trace_words[2]) +
         " payload_trace_event_code=" + std::to_string(trace_words[2]) +
         " payload_trace_ret=" + std::to_string(trace_words[3]) +
         " payload_trace_count=" + std::to_string(trace_words[4]) +
         " payload_trace_role=" + std::to_string(trace_words[5]) +
         " payload_trace_local_rank=" + std::to_string(trace_words[6]) +
         " payload_trace_peer_rank=" + std::to_string(trace_words[7]) +
         " payload_trace_bytes=" +
         std::to_string(static_cast<uint64_t>(trace_words[8]) |
                        (static_cast<uint64_t>(trace_words[9]) << 32U)) +
         " payload_trace_batch_mode=" + std::to_string(trace_words[10]) +
         " payload_trace_recv_path=" + std::to_string(trace_words[11]) +
         " payload_trace_completion_mode=" + std::to_string(trace_words[12]) +
         " payload_trace_comm_acquire=" +
         (((trace_words[12] &
            (FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE |
             FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING)) != 0) ?
              "skipped" : "default") +
         " payload_trace_comm_binding=" +
         (((trace_words[12] &
            FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING) != 0) ?
              "channel-handle" :
              (((trace_words[12] &
                 FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE) != 0) ?
                   "diagnostic-skip" : "comm-name")) +
         " payload_trace_transfer_mode=" +
         (((trace_words[12] &
            FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY) != 0) ?
              "write-with-notify" :
              (((trace_words[12] &
                 FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0) ?
                   "write" : "read")) +
         " payload_trace_write_notify_backend=" +
         PayloadTraceWriteNotifyBackend(trace_words) +
         " payload_trace_ready_notify_idx=" + std::to_string(trace_words[13]) +
         " payload_trace_done_notify_idx=" + std::to_string(trace_words[14]) +
         " payload_trace_result=" + PayloadKernelStatusName(trace_words[15]) +
         " payload_trace_status_word=" + std::to_string(trace_words[16]) +
         " payload_trace_hcomm_ret=" +
         std::to_string(static_cast<int32_t>(trace_words[17])) +
         " payload_trace_order=" + PayloadTraceOrderState(trace_words, events) +
         " payload_trace_ret_order=" +
         PayloadTraceReturnOrderState(trace_words, events, returns) +
         PayloadTracePrimitiveCountsDetail(trace_words, events) +
         " payload_trace_primitive_path=" +
         PayloadTracePrimitivePath(trace_words) +
         " payload_trace_operand_layout=" +
         PayloadTraceOperandLayout(trace_words) +
         " payload_trace_capacity=" +
         std::to_string(FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY) +
         " payload_trace_sequence=\"" + PayloadTraceEventSequence(events) +
         "\" payload_trace_ret_sequence=\"" +
         PayloadTraceReturnSequence(returns) + "\"" +
         PayloadTraceFirstErrorDetail(events, returns);
}

bool PayloadTracePassed(const uint32_t* trace_words,
                        aclError read_status,
                        bool include_thread_notify) {
  if (read_status != ACL_SUCCESS || trace_words == nullptr) {
    return false;
  }
  const std::vector<uint32_t> events = PayloadTraceEvents(trace_words);
  const std::vector<uint32_t> returns = PayloadTraceReturns(trace_words);
  return trace_words[0] == FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION &&
         trace_words[1] == FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT &&
         trace_words[2] == FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT &&
         trace_words[3] == 0U &&
         trace_words[15] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS &&
         trace_words[16] == FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS &&
         trace_words[17] == 0U &&
         PayloadTraceOrderMatches(trace_words, events,
                                  include_thread_notify) &&
         PayloadTraceReturnOrderMatches(trace_words, events, returns,
                                        include_thread_notify);
}

bool PayloadTraceHeaderMatchesDesc(
    const flume_hcomm_payload_copy_desc_v1& desc,
    const uint32_t* trace_words,
    aclError read_status) {
  if (read_status != ACL_SUCCESS || trace_words == nullptr ||
      trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    return false;
  }
  const uint64_t trace_bytes =
      static_cast<uint64_t>(trace_words[8]) |
      (static_cast<uint64_t>(trace_words[9]) << 32U);
  return trace_words[5] == desc.role &&
         trace_words[6] == desc.local_rank &&
         trace_words[7] == desc.peer_rank &&
         trace_bytes == desc.bytes &&
         trace_words[10] == static_cast<uint32_t>(desc.reserved2[0]) &&
         trace_words[11] == static_cast<uint32_t>(desc.reserved2[1]) &&
         trace_words[12] == desc.completion_mode &&
         trace_words[13] == desc.ready_notify_idx &&
         trace_words[14] == desc.done_notify_idx;
}

std::string NotifyKernelStatusName(uint32_t status) {
  switch (status) {
    case 0xFFFFFFFFU:
      return "not-written";
    case FLUME_HCOMM_NOTIFY_STATUS_SUCCESS:
      return "success";
    case FLUME_HCOMM_NOTIFY_STATUS_INVALID_ARGUMENT:
      return "invalid-argument";
    case FLUME_HCOMM_NOTIFY_STATUS_HCOMM_ERROR:
      return "hcomm-error";
    default:
      return std::string("unknown-") + std::to_string(status);
  }
}

std::string HcommPackageDetail(const HcommLauncherDecision& decision) {
  return std::string(" package_vendor=") + decision.package.vendor +
         " package_source=" + decision.package.source +
         " package_aicpu_tar=" +
         (decision.package.aicpu_tar_present ? "present" : "missing") +
         " package_aicpu_tar_readable=" +
         (decision.package.aicpu_tar_readable ? "yes" : "no") +
         " package_json_path=\"" + decision.package.json_path + "\"" +
         " package_aicpu_tar_path=\"" + decision.package.aicpu_tar_path + "\"" +
         " payload_package=" +
         (decision.package.payload_ready ? "ready" : "not-ready") +
         " payload_package_reason=\"" + decision.package.payload_reason + "\"";
}

std::string MakeDirectAclrtBlockedDetail(
    const HcommLauncherDecision& decision,
    const std::string& reason) {
  return std::string("stage3b3c_direct_aclrt_loader=unsupported "
                     "stage3b3c_descriptor_handoff=blocked "
                     "stage3b3c_direct_aclrt_launch=not-attempted "
                     "reason=\"") +
         reason + "\" custom_op_package=" +
         (decision.package.installed ? "present" : "missing") +
         HcommPackageDetail(decision);
}

std::string MakeDirectAclrtCanaryBlockedDetail(
    const HcommLauncherDecision& decision,
    const std::string& reason) {
  return std::string("stage3b3d_no_internal_headers=on "
                     "stage3b3d_direct_aclrt_canary_loader=unsupported "
                     "stage3b3d_direct_aclrt_canary_handoff=blocked "
                     "stage3b3d_direct_aclrt_canary_launch=not-attempted "
                     "reason=\"") +
         reason + "\" custom_op_package=" +
         (decision.package.installed ? "present" : "missing") +
         HcommPackageDetail(decision);
}

std::string MakeDirectAclrtPayloadBlockedDetail(
    const HcommLauncherDecision& decision,
    const std::string& reason) {
  return std::string("stage3b3e_payload_copy=unsupported "
                     "stage3b3e_direct_aclrt_payload_loader=unsupported "
                     "stage3b3e_payload_descriptor_handoff=blocked "
                     "stage3b3e_direct_aclrt_payload_launch=not-attempted "
                     "reason=\"") +
         reason + "\" custom_op_package=" +
         (decision.package.installed ? "present" : "missing") +
         HcommPackageDetail(decision);
}

std::string HcommPayloadCompletionDetail(
    const flume_hcomm_payload_copy_desc_v1& desc,
    const HcommChannelResourceInfo& resource_info) {
  std::string detail = resource_info.host_thread_notify_ready ?
      " payload_thread_notify=host-aicpu" :
      " payload_thread_notify=unavailable";
  detail += std::string(" payload_sync_api=") + AclStreamSyncApiName();
  detail += " payload_sync_timeout_sec=" +
      std::to_string(resource_info.timeout_sec);
  detail += (desc.completion_mode &
             FLUME_HCOMM_PAYLOAD_COMPLETION_MODE_MASK) ==
                FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN ?
      " payload_completion_mode=channel-fence" :
      " payload_completion_mode=ordered-notify";
  const bool write_path =
      (desc.completion_mode &
       FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0;
  if (write_path) {
    detail += desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND ?
        " payload_transfer=send-write" :
        " payload_transfer=recv-write-local-copy";
  } else {
    detail += desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND ?
        " payload_transfer=send-read-source" : " payload_transfer=recv-read";
  }
  detail += resource_info.host_thread_notify_ready ?
      " payload_completion=thread-notify+stream-sync+status-word" :
      " payload_completion=stream-sync+status-word";
  detail += resource_info.host_thread_notify_ready ?
      " payload_thread_notify_order=host-notify-before-batch-end" :
      " payload_thread_notify_order=not-used";
  detail += std::string(" payload_resolved_engine=") +
      FlumeHcommEngineName(resource_info.resolved_engine) +
      " payload_resolved_protocol=" +
      FlumeHcommProtocolName(resource_info.resolved_protocol) +
      " payload_channel_desc=" + resource_info.channel_desc_source +
      " payload_channel_count=" +
      std::to_string(resource_info.channel_count) +
      " payload_notify_num=" + std::to_string(resource_info.notify_num) +
      " payload_usable_hccl_buffer_bytes=" +
      std::to_string(resource_info.usable_buffer_bytes) +
      " payload_local_hccl_buffer_bytes=" +
      std::to_string(resource_info.local_buffer_bytes) +
      " payload_remote_hccl_buffer_bytes=" +
      std::to_string(resource_info.remote_buffer_bytes);
  return detail;
}

std::string HcommPayloadRuntimeDetail(
    const flume_hcomm_payload_copy_desc_v1& desc,
    const HcommChannelResourceInfo& resource_info,
    const HcommLauncherDecision& decision) {
  return PayloadDescriptorDetail(desc) +
         HostPayloadValidationDetail(flume_hcomm_payload_copy_desc_validate_reason(&desc)) +
         HcommPayloadCompletionDetail(desc, resource_info) +
         " payload_semantic=present payload_semantic_v5=present "
         "payload_semantic_v6=present payload_semantic_v7=present "
         "payload_semantic_v8=present payload_semantic_v9=present "
         "payload_semantic_v10=present payload_semantic_v11=present "
         "payload_semantic_v12=present payload_semantic_v13=present "
         "payload_semantic_v14=present payload_semantic_v15=present "
         "payload_semantic_v16=present payload_semantic_v17=present "
         "payload_semantic_v18=present payload_semantic_v19=present "
         "payload_official_p2p_layout=present "
         "payload_copy_api=hcomm-direct-aclrt "
         "payload_hccl_p2p_api=not-used "
         "payload_no_hccl_sendrecv=passed "
         "payload_no_hccl_payload_collective=passed "
         "payload_build_mode=internal" +
         " custom_op_package=present" + HcommPackageDetail(decision);
}

#if FLUME_BUILD_HCOMM_CUSTOM_OP && FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH
struct PayloadAclrtRequiredFunction {
  const char* function_name;
  const char* missing_detail;
};

std::string MakePayloadAclrtFunctionMissingDetail(
    aclError acl_ret,
    const char* function_name,
    const char* missing_detail,
    const flume_hcomm_payload_copy_desc_v1& desc,
    const HcommLauncherDecision& decision,
    const std::string& local_prime_detail) {
  return std::string("stage3b3e_payload_copy=unsupported "
                     "stage3b3e_direct_aclrt_payload_loader=unsupported "
                     "api=aclrtBinaryGetFunction error=\"") +
         AclErrorMessage(acl_ret) +
         "\" stage3b3e_payload_descriptor_handoff=blocked "
         "stage3b3e_direct_aclrt_payload_launch=not-attempted " +
         missing_detail + " kernel_func=" + function_name +
         local_prime_detail + PayloadDescriptorDetail(desc) +
         " custom_op_package=present" + HcommPackageDetail(decision);
}

bool LookupPayloadAclrtRequiredFunction(
    aclrtBinHandle bin_handle,
    const PayloadAclrtRequiredFunction& required_function,
    const flume_hcomm_payload_copy_desc_v1& desc,
    const HcommLauncherDecision& decision,
    const std::string& local_prime_detail,
    void* kernel_status_dev,
    int* status,
    std::string* error_detail,
    aclrtFuncHandle* out_handle) {
  aclrtFuncHandle handle = nullptr;
  aclError acl_ret = aclrtBinaryGetFunction(
      bin_handle, required_function.function_name, &handle);
  if (acl_ret == ACL_SUCCESS) {
    if (out_handle != nullptr) {
      *out_handle = handle;
    }
    return true;
  }
  (void)aclrtBinaryUnLoad(bin_handle);
  (void)aclrtFree(kernel_status_dev);
  if (status != nullptr) {
    *status = FLUME_ERR_UNSUPPORTED;
  }
  if (error_detail != nullptr) {
    *error_detail = MakePayloadAclrtFunctionMissingDetail(
        acl_ret, required_function.function_name,
        required_function.missing_detail, desc, decision, local_prime_detail);
  }
  return false;
}

std::string TryLaunchHcommPayloadCopyDirectAclrt(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    void* acl_stream,
    void* user_buffer,
    uint64_t bytes,
    bool disable_payload_batch_mode,
    const std::string& payload_batch_tag,
    bool payload_recv_direct_output,
    bool payload_force_channel_fence,
    bool payload_write_path,
    bool payload_write_with_notify,
    flume_hcomm_payload_comm_binding_t payload_comm_binding,
    const HcommChannelResourceInfo& resource_info,
    const HcommLauncherDecision& decision,
    int* status) {
  if (status == nullptr) {
    return "stage3b3e_payload_copy=failed "
           "stage3b3e_direct_aclrt_payload_loader=failed "
           "reason=\"status pointer null\"";
  }
  if (!decision.package.installed) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtPayloadBlockedDetail(decision,
                                               "custom_op_package missing");
  }
  if (!decision.package.payload_ready) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtPayloadBlockedDetail(
        decision, std::string("payload package not ready: ") +
                      decision.package.payload_reason);
  }
  if (acl_stream == nullptr || user_buffer == nullptr || bytes == 0) {
    *status = FLUME_ERR_INVALID_ARGUMENT;
    return MakeDirectAclrtPayloadBlockedDetail(
        decision, "invalid stream, buffer, or byte count");
  }
  if (resource_info.aicpu_ts_thread == 0 || resource_info.channel_handle == 0) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtPayloadBlockedDetail(
        decision, "missing AICPU_TS thread or HCOMM channel handle");
  }
  if (resource_info.host_thread_notify_ready &&
      (resource_info.cpu_ts_thread == 0 ||
       resource_info.cpu_thread_on_aicpu == 0 ||
       resource_info.aicpu_thread_on_cpu == 0)) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtPayloadBlockedDetail(
        decision, "incomplete host/AICPU thread notify handles");
  }
#if !FLUME_HAVE_HCOMM_PRIMITIVES
  if (resource_info.host_thread_notify_ready) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtPayloadBlockedDetail(
        decision, "host/AICPU thread notify primitives are unavailable");
  }
#endif
  if (bytes > resource_info.usable_buffer_bytes) {
    *status = FLUME_ERR_INVALID_ARGUMENT;
    return MakeDirectAclrtPayloadBlockedDetail(
        decision, "payload bytes exceed usable HCCL buffer size");
  }
#if !FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY && !FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI
  if (payload_write_with_notify) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtPayloadBlockedDetail(
        decision,
        "HcommWriteWithNotifyOnThread and HcommWriteWithNotifyNbiOnThread "
        "are unavailable in this CANN build");
  }
#endif

  char comm_name[FLUME_HCOMM_PAYLOAD_COMM_NAME_BYTES] = {};
  const bool require_comm_name =
      payload_comm_binding == FLUME_HCOMM_PAYLOAD_COMM_BINDING_COMM_NAME;
  if (!require_comm_name) {
    strncpy(comm_name, "flume_channel_handle", sizeof(comm_name) - 1);
  }
  if (require_comm_name) {
#if FLUME_HAVE_HCCL_COMM_NAME
    HcclResult comm_name_ret =
        HcclGetCommName(static_cast<HcclComm>(state.hccl_comm), comm_name);
    if (comm_name_ret != HCCL_SUCCESS) {
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3e_payload_copy=failed "
                         "stage3b3e_direct_aclrt_payload_loader=not-attempted "
                         "stage3b3e_payload_descriptor_handoff=blocked "
                         "api=HcclGetCommName error=\"") +
             HcclErrorMessage(comm_name_ret) +
             "\" stage3b3e_direct_aclrt_payload_launch=not-attempted";
    }
    if (comm_name[0] == '\0') {
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3e_payload_copy=failed "
                         "stage3b3e_direct_aclrt_payload_loader=not-attempted "
                         "stage3b3e_payload_descriptor_handoff=blocked "
                         "api=HcclGetCommName error=\"empty comm name\" "
                         "stage3b3e_direct_aclrt_payload_launch=not-attempted");
    }
#else
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtPayloadBlockedDetail(
        decision, "HcclGetCommName is unavailable in this CANN build");
#endif
  }

  void* kernel_status_dev = nullptr;
  uint32_t kernel_status_words[FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT];
  for (uint32_t& word : kernel_status_words) {
    word = 0xFFFFFFFFU;
  }
  aclError acl_ret = aclrtMalloc(&kernel_status_dev,
                                 sizeof(kernel_status_words),
                                 ACL_MEM_MALLOC_HUGE_FIRST);
  if (acl_ret != ACL_SUCCESS) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=not-attempted "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtMalloc(payload_status) error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }
  acl_ret = aclrtMemcpy(kernel_status_dev, sizeof(kernel_status_words),
                        kernel_status_words, sizeof(kernel_status_words),
                        ACL_MEMCPY_HOST_TO_DEVICE);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=not-attempted "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtMemcpy(payload_status_h2d) error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }

  flume_hcomm_payload_copy_desc_v1 desc = {};
  FillFlumePayloadCopyDesc(role, state, peer_rank, resource_info, user_buffer,
                           bytes, comm_name, disable_payload_batch_mode,
                           payload_batch_tag, payload_recv_direct_output,
                           payload_force_channel_fence,
                           payload_write_path, payload_write_with_notify,
                           payload_comm_binding,
                           &desc);
  desc.status_word = reinterpret_cast<uint64_t>(kernel_status_dev);
  desc.status_word_count = FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT;
  desc.status_schema_version = FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION;
  const uint32_t host_validation_reason =
      flume_hcomm_payload_copy_desc_validate_reason(&desc);
  if (host_validation_reason != FLUME_HCOMM_PAYLOAD_VALIDATE_OK) {
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_INVALID_ARGUMENT;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=not-attempted "
                       "stage3b3e_payload_descriptor_handoff=blocked "
                       "stage3b3e_direct_aclrt_payload_launch=not-attempted "
                       "payload_kernel_status=not-run "
                       "payload_failure_step=host-validate-descriptor") +
           HostPayloadValidationDetail(host_validation_reason) +
           " payload_validation_reason=" +
           PayloadValidationReasonName(host_validation_reason) +
           " payload_validation_reason_code=" +
           std::to_string(host_validation_reason) +
           PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }
  acl_ret = PrimeHcommPayloadLocalBuffer(role, state, peer_rank,
                                         resource_info, bytes);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=not-attempted "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "payload_local_buffer_prime=failed "
                       "api=aclrtMemcpy(local_hccl_buffer_prime) error=\"") +
           AclErrorMessage(acl_ret) + "\"" +
           HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
  const std::string local_prime_detail = PayloadLocalPrimeDetail(bytes);

  aclrtBinHandle bin_handle = nullptr;
  aclrtBinaryLoadOption option = {};
  option.type = ACL_RT_BINARY_LOAD_OPT_CPU_KERNEL_MODE;
  option.value.cpuKernelMode = 0;
  aclrtBinaryLoadOptions load_options = {};
  load_options.options = &option;
  load_options.numOpt = 1;
  acl_ret = aclrtBinaryLoadFromFile(decision.package.json_path.c_str(),
                                    &load_options, &bin_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=failed "
           "api=aclrtBinaryLoadFromFile error=\"") +
           AclErrorMessage(acl_ret) +
           "\"" + local_prime_detail + PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle func_handle = nullptr;
  const PayloadAclrtRequiredFunction required_functions[] = {
      {FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V4_FUNC,
       "payload_abi=v4-missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_FUNC,
       "payload_semantic=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5_FUNC,
       "payload_semantic=present payload_semantic_v5=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V9_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V10_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V11_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V12_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=present "
       "payload_semantic_v12=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V13_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=present "
       "payload_semantic_v12=present payload_semantic_v13=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V14_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=present "
       "payload_semantic_v12=present payload_semantic_v13=present "
       "payload_semantic_v14=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V15_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=present "
       "payload_semantic_v12=present payload_semantic_v13=present "
       "payload_semantic_v14=present payload_semantic_v15=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V16_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=present "
       "payload_semantic_v12=present payload_semantic_v13=present "
       "payload_semantic_v14=present payload_semantic_v15=present "
       "payload_semantic_v16=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V17_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=present "
       "payload_semantic_v12=present payload_semantic_v13=present "
       "payload_semantic_v14=present payload_semantic_v15=present "
       "payload_semantic_v16=present payload_semantic_v17=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V18_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=present "
       "payload_semantic_v12=present payload_semantic_v13=present "
       "payload_semantic_v14=present payload_semantic_v15=present "
       "payload_semantic_v16=present payload_semantic_v17=present "
       "payload_semantic_v18=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V19_FUNC,
       "payload_semantic=present payload_semantic_v5=present "
       "payload_semantic_v6=present payload_semantic_v7=present "
       "payload_semantic_v8=present payload_semantic_v9=present "
       "payload_semantic_v10=present payload_semantic_v11=present "
       "payload_semantic_v12=present payload_semantic_v13=present "
       "payload_semantic_v14=present payload_semantic_v15=present "
       "payload_semantic_v16=present payload_semantic_v17=present "
       "payload_semantic_v18=present payload_semantic_v19=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE_FUNC,
       "payload_requires_comm_acquire=missing"},
      {FLUME_HCOMM_PAYLOAD_COPY_SUPPORTS_OFFICIAL_P2P_LAYOUT_FUNC,
       "payload_official_p2p_layout=missing"},
      {FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION_FUNC,
       "payload_status_schema_marker=missing"},
      {FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT_FUNC,
       "payload_status_word_count_marker=missing"},
      {FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION_FUNC,
       "payload_trace_schema_marker=missing"},
      {FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT_FUNC,
       "payload_trace_word_count_marker=missing"},
      {FLUME_HCOMM_PAYLOAD_PRIMITIVE_DEPS_FUNC,
       "payload_primitive_deps=missing"},
      {FLUME_HCOMM_PAYLOAD_NO_HCCL_SENDRECV_DEPS_FUNC,
       "payload_no_hccl_sendrecv_deps=missing"},
      {FLUME_HCOMM_PAYLOAD_NO_HCCL_PAYLOAD_API_DEPS_FUNC,
       "payload_no_hccl_payload_api_deps=missing"},
      {FLUME_HCOMM_PAYLOAD_BUILD_MODE_INTERNAL_PAYLOAD_FUNC,
       "payload_build_mode=not-internal"},
      {FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC,
       "payload_kernel=missing"},
  };
  std::string function_error_detail;
  for (const PayloadAclrtRequiredFunction& required_function :
       required_functions) {
    const bool is_payload_kernel =
        strcmp(required_function.function_name,
               FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC) == 0;
    if (!LookupPayloadAclrtRequiredFunction(
            bin_handle, required_function, desc, decision, local_prime_detail,
            kernel_status_dev, status, &function_error_detail,
            is_payload_kernel ? &func_handle : nullptr)) {
      return function_error_detail;
    }
  }

  void* kernel_trace_dev = nullptr;
  uint32_t kernel_trace_words[FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT];
  InitPayloadTraceWords(kernel_trace_words);
  acl_ret = aclrtMalloc(&kernel_trace_dev, sizeof(kernel_trace_words),
                        ACL_MEM_MALLOC_HUGE_FIRST);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtMalloc(payload_trace) error=\"") +
           AclErrorMessage(acl_ret) + "\"" +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
  acl_ret = aclrtMemcpy(kernel_trace_dev, sizeof(kernel_trace_words),
                        kernel_trace_words, sizeof(kernel_trace_words),
                        ACL_MEMCPY_HOST_TO_DEVICE);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(kernel_trace_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtMemcpy(payload_trace_h2d) error=\"") +
           AclErrorMessage(acl_ret) + "\"" +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
  desc.reserved2[2] = reinterpret_cast<uint64_t>(kernel_trace_dev);

  aclrtLaunchKernelAttr attr = {};
  attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
  attr.value.timeout = desc.timeout_sec;
  aclrtLaunchKernelCfg cfg = {};
  cfg.attrs = &attr;
  cfg.numAttrs = 1;

#if FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS
  const char* payload_launch_api = "host-args";
#else
  const char* payload_launch_api = "args-handle";
  aclrtArgsHandle args_handle = nullptr;
  acl_ret = aclrtKernelArgsInit(func_handle, &args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(kernel_trace_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtKernelArgsInit error=\"") +
           AclErrorMessage(acl_ret) + "\"" +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }

  aclrtParamHandle param_handle = nullptr;
  acl_ret =
      aclrtKernelArgsAppend(args_handle, &desc, sizeof(desc), &param_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(kernel_trace_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtKernelArgsAppend error=\"") +
           AclErrorMessage(acl_ret) + "\"" +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }

  acl_ret = aclrtKernelArgsFinalize(args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(kernel_trace_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtKernelArgsFinalize error=\"") +
           AclErrorMessage(acl_ret) + "\"" +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
#endif

#if FLUME_HAVE_HCOMM_PRIMITIVES
  if (resource_info.host_thread_notify_ready) {
    int32_t notify_ret = HcommThreadNotifyRecordOnThread(
        static_cast<ThreadHandle>(resource_info.cpu_ts_thread),
        static_cast<ThreadHandle>(resource_info.aicpu_thread_on_cpu), 0);
    if (notify_ret != 0) {
      (void)aclrtBinaryUnLoad(bin_handle);
      (void)aclrtFree(kernel_trace_dev);
      (void)aclrtFree(kernel_status_dev);
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3e_payload_copy=failed "
                         "stage3b3e_direct_aclrt_payload_loader=passed "
                         "stage3b3e_payload_descriptor_handoff=passed "
                         "stage3b3e_direct_aclrt_payload_launch=not-attempted "
                         "payload_thread_notify=host-aicpu "
                         "api=HcommThreadNotifyRecordOnThread "
                         "hcomm_ret=") +
             std::to_string(notify_ret) + " kernel_func=" +
             FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
             local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
    }
  }
#endif

#if FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS
  acl_ret = aclrtLaunchKernelWithHostArgs(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, &desc,
      sizeof(desc), nullptr, 0);
  const char* acl_launch_api = "aclrtLaunchKernelWithHostArgs";
#else
  acl_ret = aclrtLaunchKernelWithConfig(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, args_handle,
      nullptr);
  const char* acl_launch_api = "aclrtLaunchKernelWithConfig";
#endif
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_trace_dev);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=failed "
                       "api=") + acl_launch_api + " payload_launch_api=" +
           payload_launch_api + " error=\"" +
           AclErrorMessage(acl_ret) + "\" kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }

#if FLUME_HAVE_HCOMM_PRIMITIVES
  if (resource_info.host_thread_notify_ready) {
    int32_t notify_ret = HcommThreadNotifyWaitOnThread(
        static_cast<ThreadHandle>(resource_info.cpu_ts_thread), 0,
        desc.timeout_sec);
    if (notify_ret != 0) {
      aclError sync_ret = SyncAclStreamForHcomm(
          static_cast<aclrtStream>(acl_stream), desc.timeout_sec);
      uint32_t observed_status_words[FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT];
      for (uint32_t& word : observed_status_words) {
        word = 0xFFFFFFFFU;
      }
      aclError status_ret = aclrtMemcpy(
          observed_status_words, sizeof(observed_status_words),
          kernel_status_dev, sizeof(observed_status_words),
          ACL_MEMCPY_DEVICE_TO_HOST);
      uint32_t observed_trace_words[FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT];
      InitPayloadTraceWords(observed_trace_words);
      aclError trace_ret = aclrtMemcpy(
          observed_trace_words, sizeof(observed_trace_words),
          kernel_trace_dev, sizeof(observed_trace_words),
          ACL_MEMCPY_DEVICE_TO_HOST);
      (void)aclrtBinaryUnLoad(bin_handle);
      (void)aclrtFree(kernel_trace_dev);
      (void)aclrtFree(kernel_status_dev);
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3e_payload_copy=failed "
                         "stage3b3e_direct_aclrt_payload_loader=passed "
                         "stage3b3e_payload_descriptor_handoff=passed "
                         "stage3b3e_direct_aclrt_payload_launch=passed "
                         "stage3b3e_payload_sync=failed "
                         "payload_launch_api=") +
             payload_launch_api +
             " "
                         "payload_thread_notify=host-aicpu "
                         "api=HcommThreadNotifyWaitOnThread hcomm_ret=" +
             std::to_string(notify_ret) + " post_notify_stream_sync=\"" +
             AclErrorMessage(sync_ret) + "\" post_notify_stream_sync_api=" +
             AclStreamSyncApiName() +
             " payload_kernel_status=" +
             PayloadKernelStatusName(observed_status_words[0]) +
             " payload_failure_step=" +
             PayloadFailureStepName(observed_status_words[0]) +
             " payload_status_word=" +
             std::to_string(observed_status_words[0]) +
             " payload_kernel_hcomm_ret=" +
             std::to_string(observed_status_words[1]) +
             " payload_status_read=\"" + AclErrorMessage(status_ret) +
             "\"" + PayloadStatusSchemaDetail() +
             PayloadPrimitiveStateDetail(observed_status_words) +
             PayloadValidationReasonDetail(observed_status_words) +
             PayloadEchoWordsDetail(observed_status_words) +
             PayloadDataProbeDetail(observed_status_words) +
             PayloadTraceWordsDetail(observed_trace_words, trace_ret) +
             " kernel_func=" +
             FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
             local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
    }
  }
#endif

  acl_ret = SyncAclStreamForHcomm(static_cast<aclrtStream>(acl_stream),
                                  desc.timeout_sec);
  if (acl_ret != ACL_SUCCESS) {
    uint32_t observed_status_words[FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT];
    for (uint32_t& word : observed_status_words) {
      word = 0xFFFFFFFFU;
    }
    aclError status_ret = aclrtMemcpy(
        observed_status_words, sizeof(observed_status_words),
        kernel_status_dev, sizeof(observed_status_words),
        ACL_MEMCPY_DEVICE_TO_HOST);
    uint32_t observed_trace_words[FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT];
    InitPayloadTraceWords(observed_trace_words);
    aclError trace_ret = aclrtMemcpy(
        observed_trace_words, sizeof(observed_trace_words),
        kernel_trace_dev, sizeof(observed_trace_words),
        ACL_MEMCPY_DEVICE_TO_HOST);
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_trace_dev);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=failed "
                       "payload_launch_api=") +
           payload_launch_api + " api=" + AclStreamSyncApiName() + " error=\"" +
           AclErrorMessage(acl_ret) + "\" payload_status_read=\"" +
           AclErrorMessage(status_ret) + "\" payload_kernel_status=" +
           PayloadKernelStatusName(observed_status_words[0]) +
           " payload_failure_step=" +
           PayloadFailureStepName(observed_status_words[0]) +
           " payload_status_word=" +
           std::to_string(observed_status_words[0]) +
           " payload_kernel_hcomm_ret=" +
           std::to_string(observed_status_words[1]) +
           PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(observed_status_words) +
           PayloadValidationReasonDetail(observed_status_words) +
           PayloadEchoWordsDetail(observed_status_words) +
           PayloadDataProbeDetail(observed_status_words) +
           PayloadTraceWordsDetail(observed_trace_words, trace_ret) +
           " kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }

  acl_ret = aclrtMemcpy(kernel_status_words, sizeof(kernel_status_words),
                        kernel_status_dev, sizeof(kernel_status_words),
                        ACL_MEMCPY_DEVICE_TO_HOST);
  if (acl_ret != ACL_SUCCESS) {
    aclError trace_ret = aclrtMemcpy(
        kernel_trace_words, sizeof(kernel_trace_words),
        kernel_trace_dev, sizeof(kernel_trace_words),
        ACL_MEMCPY_DEVICE_TO_HOST);
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_trace_dev);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=failed "
                       "payload_launch_api=") +
           payload_launch_api + " api=aclrtMemcpy(payload_status_d2h) error=\"" +
           AclErrorMessage(acl_ret) + "\" kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           PayloadTraceWordsDetail(kernel_trace_words, trace_ret) +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }

  aclError trace_ret = aclrtMemcpy(kernel_trace_words,
                                   sizeof(kernel_trace_words),
                                   kernel_trace_dev,
                                   sizeof(kernel_trace_words),
                                   ACL_MEMCPY_DEVICE_TO_HOST);
  (void)aclrtBinaryUnLoad(bin_handle);
  (void)aclrtFree(kernel_trace_dev);
  (void)aclrtFree(kernel_status_dev);
  const uint32_t kernel_status = kernel_status_words[0];
  const uint32_t kernel_hcomm_ret = kernel_status_words[1];
  const uint64_t echo_bytes = PayloadEchoBytes(kernel_status_words);
  if (kernel_status != 0) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=passed "
                       "payload_launch_api=") +
           payload_launch_api + " " +
           PayloadBatchModeDetail(desc) + " payload_kernel_status=" +
           PayloadKernelStatusName(kernel_status) +
           " payload_failure_step=" +
           PayloadFailureStepName(kernel_status) +
           " payload_status_word=" +
           std::to_string(kernel_status) +
           " payload_kernel_hcomm_ret=" +
           std::to_string(kernel_hcomm_ret) +
           " payload_echo=observed" + PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(kernel_status_words) +
           PayloadValidationReasonDetail(kernel_status_words) +
           PayloadEchoWordsDetail(kernel_status_words) +
           PayloadDataProbeDetail(kernel_status_words) +
           PayloadDeviceDataSideDetail(desc, kernel_status_words) +
           PayloadTraceWordsDetail(kernel_trace_words, trace_ret) +
           " kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
  if (kernel_hcomm_ret != 0) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=passed "
                       "payload_launch_api=") +
           payload_launch_api + " " +
           PayloadBatchModeDetail(desc) +
           " payload_kernel_status=success "
           "payload_failure_step=primitive-return "
           "payload_status_word=0 payload_kernel_hcomm_ret=" +
           std::to_string(kernel_hcomm_ret) +
           " payload_echo=observed" + PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(kernel_status_words) +
           PayloadValidationReasonDetail(kernel_status_words) +
           PayloadEchoWordsDetail(kernel_status_words) +
           PayloadDataProbeDetail(kernel_status_words) +
           PayloadDeviceDataSideDetail(desc, kernel_status_words) +
           PayloadTraceWordsDetail(kernel_trace_words, trace_ret) +
           " kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
  const uint32_t expected_role =
      role == flume::hcomm_payload::PayloadRole::kSend ?
          FLUME_HCOMM_NOTIFY_ROLE_SEND :
          FLUME_HCOMM_NOTIFY_ROLE_RECV;
  const uint32_t expected_completion_mode =
      desc.completion_mode;
  const uint64_t expected_desc_fingerprint =
      flume_hcomm_payload_copy_desc_fingerprint(&desc);
  const uint64_t echo_desc_fingerprint =
      PayloadEchoDescriptorFingerprint(kernel_status_words);
  if (kernel_status_words[2] != expected_role ||
      kernel_status_words[3] != peer_rank || echo_bytes != bytes ||
      kernel_status_words[6] != state.rank ||
      kernel_status_words[7] != expected_completion_mode ||
      echo_desc_fingerprint != expected_desc_fingerprint) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=passed "
                       "payload_launch_api=") +
           payload_launch_api + " " +
           PayloadBatchModeDetail(desc) +
           " payload_kernel_status=success "
           "payload_failure_step=none "
           "payload_status_word=0 payload_kernel_hcomm_ret=0 "
           "payload_echo=failed" +
           PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(kernel_status_words) +
           PayloadValidationReasonDetail(kernel_status_words) +
           PayloadEchoWordsDetail(kernel_status_words) +
           PayloadDataProbeDetail(kernel_status_words) +
           PayloadDeviceDataSideDetail(desc, kernel_status_words) +
           PayloadTraceWordsDetail(kernel_trace_words, trace_ret) +
           " expected_role=" + std::to_string(expected_role) +
           " expected_peer_rank=" + std::to_string(peer_rank) +
           " expected_bytes=" + std::to_string(bytes) +
           " expected_local_rank=" + std::to_string(state.rank) +
           " expected_completion_mode=" +
           std::to_string(expected_completion_mode) +
           " expected_desc_fingerprint=" +
           std::to_string(expected_desc_fingerprint) +
           " kernel_func=" + FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
  if (!PayloadTraceHeaderMatchesDesc(desc, kernel_trace_words, trace_ret)) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=passed "
                       "payload_launch_api=") +
           payload_launch_api + " " +
           PayloadBatchModeDetail(desc) +
           " payload_kernel_status=success "
           "payload_failure_step=none "
           "payload_status_word=0 payload_kernel_hcomm_ret=0 "
           "payload_echo=passed "
           "payload_descriptor_fingerprint=passed "
           "payload_trace_header=failed "
           "expected_trace_role=" + std::to_string(desc.role) +
           " expected_trace_local_rank=" + std::to_string(desc.local_rank) +
           " expected_trace_peer_rank=" + std::to_string(desc.peer_rank) +
           " expected_trace_bytes=" + std::to_string(desc.bytes) +
           " expected_trace_batch_mode=" +
           std::to_string(static_cast<uint32_t>(desc.reserved2[0])) +
           " expected_trace_recv_path=" +
           std::to_string(static_cast<uint32_t>(desc.reserved2[1])) +
           " expected_trace_completion_mode=" +
           std::to_string(desc.completion_mode) +
           " expected_trace_ready_notify_idx=" +
           std::to_string(desc.ready_notify_idx) +
           " expected_trace_done_notify_idx=" +
           std::to_string(desc.done_notify_idx) +
           PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(kernel_status_words) +
           PayloadValidationReasonDetail(kernel_status_words) +
           PayloadEchoWordsDetail(kernel_status_words) +
           PayloadDataProbeDetail(kernel_status_words) +
           PayloadDeviceDataSideDetail(desc, kernel_status_words) +
           PayloadTraceWordsDetail(kernel_trace_words, trace_ret) +
           " kernel_func=" + FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
  if (!PayloadTracePassed(kernel_trace_words, trace_ret,
                          resource_info.host_thread_notify_ready)) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=passed "
                       "payload_launch_api=") +
           payload_launch_api + " " +
           PayloadBatchModeDetail(desc) +
           " payload_kernel_status=success "
           "payload_failure_step=none "
           "payload_status_word=0 payload_kernel_hcomm_ret=0 "
           "payload_echo=passed "
           "payload_descriptor_fingerprint=passed "
           "payload_trace_gate=failed "
           "payload_trace_expected_thread_notify=" +
           (resource_info.host_thread_notify_ready ? "on" : "off") +
           PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(kernel_status_words) +
           PayloadValidationReasonDetail(kernel_status_words) +
           PayloadEchoWordsDetail(kernel_status_words) +
           PayloadDataProbeDetail(kernel_status_words) +
           PayloadDeviceDataSideDetail(desc, kernel_status_words) +
           PayloadTraceWordsDetail(kernel_trace_words, trace_ret) +
           " kernel_func=" + FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
  }
  *status = FLUME_OK;
  return std::string("stage3b3e_payload_copy=passed "
                     "stage3b3e_direct_aclrt_payload_loader=passed "
                     "stage3b3e_payload_descriptor_handoff=passed "
                     "stage3b3e_direct_aclrt_payload_launch=passed "
                     "stage3b3e_payload_sync=passed "
                     "payload_launch_api=") +
         payload_launch_api + " " +
         PayloadBatchModeDetail(desc) +
         " payload_kernel_status=success "
         "payload_failure_step=none "
         "payload_status_word=0 "
         "payload_kernel_hcomm_ret=" +
         std::to_string(kernel_hcomm_ret) + " " +
         "payload_echo=passed payload_role=" + PayloadRoleMarkerName(role) +
         " payload_descriptor_fingerprint=passed "
         "payload_expected_desc_fingerprint=" +
         std::to_string(expected_desc_fingerprint) +
         " payload_trace_expected_thread_notify=" +
         (resource_info.host_thread_notify_ready ? "on" : "off") +
         " payload_trace_header=passed" +
         PayloadStatusSchemaDetail() +
         PayloadPrimitiveStateDetail(kernel_status_words) +
         PayloadValidationReasonDetail(kernel_status_words) +
         PayloadEchoWordsDetail(kernel_status_words) +
         PayloadDataProbeDetail(kernel_status_words) +
         PayloadDeviceDataSideDetail(desc, kernel_status_words) +
         PayloadTraceWordsDetail(kernel_trace_words, trace_ret) + " " +
         "kernel_func=" +
         FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
         local_prime_detail + HcommPayloadRuntimeDetail(desc, resource_info, decision);
}

std::string TryLaunchHcommDirectAclrtCanary(
    const CommState& state,
    uint32_t peer_rank,
    void* acl_stream,
    const HcommLauncherDecision& decision,
    int* status) {
  if (status == nullptr) {
    return "stage3b3d_no_internal_headers=on "
           "stage3b3d_direct_aclrt_canary_loader=failed "
           "reason=\"status pointer null\"";
  }
  if (!decision.package.installed) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtCanaryBlockedDetail(decision,
                                              "custom_op_package missing");
  }
  if (acl_stream == nullptr) {
    *status = FLUME_ERR_INVALID_ARGUMENT;
    return MakeDirectAclrtCanaryBlockedDetail(decision, "acl stream is null");
  }

  flume_hcomm_canary_desc_v1 desc = {};
  flume_hcomm_canary_desc_init(&desc);
  desc.local_rank = state.rank;
  desc.peer_rank = peer_rank;
  desc.rank_size = state.rank_size;

  aclrtBinHandle bin_handle = nullptr;
  aclrtBinaryLoadOption option = {};
  option.type = ACL_RT_BINARY_LOAD_OPT_CPU_KERNEL_MODE;
  option.value.cpuKernelMode = 0;
  aclrtBinaryLoadOptions load_options = {};
  load_options.options = &option;
  load_options.numOpt = 1;
  aclError acl_ret =
      aclrtBinaryLoadFromFile(decision.package.json_path.c_str(), &load_options,
                              &bin_handle);
  if (acl_ret != ACL_SUCCESS) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=failed "
           "api=aclrtBinaryLoadFromFile error=\"") +
           AclErrorMessage(acl_ret) +
           "\" custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC, &func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3d_direct_aclrt_canary_handoff=blocked "
           "stage3b3d_direct_aclrt_canary_launch=not-attempted kernel_func=" +
           FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  void* canary_status_dev = nullptr;
  uint32_t canary_status_words[2] = {0xFFFFFFFFU, 0U};
  acl_ret = aclrtMalloc(&canary_status_dev, sizeof(canary_status_words),
                        ACL_MEM_MALLOC_HUGE_FIRST);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=passed "
                       "stage3b3d_direct_aclrt_canary_handoff=failed "
                       "api=aclrtMalloc(canary_status) error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }
  acl_ret = aclrtMemcpy(canary_status_dev, sizeof(canary_status_words),
                        canary_status_words, sizeof(canary_status_words),
                        ACL_MEMCPY_HOST_TO_DEVICE);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(canary_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=passed "
                       "stage3b3d_direct_aclrt_canary_handoff=failed "
                       "api=aclrtMemcpy(canary_status_h2d) error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }
  desc.status_word = reinterpret_cast<uint64_t>(canary_status_dev);
  desc.observed_token_word = reinterpret_cast<uint64_t>(
      static_cast<uint8_t*>(canary_status_dev) + sizeof(uint32_t));

#if FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS
  const char* canary_launch_api = "host-args";
#else
  const char* canary_launch_api = "args-handle";
  aclrtArgsHandle args_handle = nullptr;
  acl_ret = aclrtKernelArgsInit(func_handle, &args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(canary_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=passed "
                       "stage3b3d_direct_aclrt_canary_handoff=failed "
                       "canary_launch_api=args-handle "
                       "api=aclrtKernelArgsInit error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }

  aclrtParamHandle param_handle = nullptr;
  acl_ret =
      aclrtKernelArgsAppend(args_handle, &desc, sizeof(desc), &param_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(canary_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=passed "
                       "stage3b3d_direct_aclrt_canary_handoff=failed "
                       "canary_launch_api=args-handle "
                       "api=aclrtKernelArgsAppend error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }

  acl_ret = aclrtKernelArgsFinalize(args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(canary_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=passed "
                       "stage3b3d_direct_aclrt_canary_handoff=failed "
                       "canary_launch_api=args-handle "
                       "api=aclrtKernelArgsFinalize error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }
#endif

  aclrtLaunchKernelAttr attr = {};
  attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
  attr.value.timeout = kDefaultHcommTimeoutSeconds;
  aclrtLaunchKernelCfg cfg = {};
  cfg.attrs = &attr;
  cfg.numAttrs = 1;
#if FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS
  acl_ret = aclrtLaunchKernelWithHostArgs(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, &desc,
      sizeof(desc), nullptr, 0);
  const char* acl_launch_api = "aclrtLaunchKernelWithHostArgs";
#else
  acl_ret = aclrtLaunchKernelWithConfig(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, args_handle,
      nullptr);
  const char* acl_launch_api = "aclrtLaunchKernelWithConfig";
#endif
  if (acl_ret == ACL_SUCCESS) {
    acl_ret = SyncAclStreamForHcomm(static_cast<aclrtStream>(acl_stream),
                                    kDefaultHcommTimeoutSeconds);
    if (acl_ret != ACL_SUCCESS) {
      (void)aclrtFree(canary_status_dev);
      (void)aclrtBinaryUnLoad(bin_handle);
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3d_no_internal_headers=on "
                         "stage3b3d_direct_aclrt_canary_loader=passed "
                         "stage3b3d_direct_aclrt_canary_handoff=passed "
                         "stage3b3d_direct_aclrt_canary_launch=passed "
                         "stage3b3d_direct_aclrt_canary_sync=failed "
                         "canary_launch_api=") +
             canary_launch_api + " api=" + AclStreamSyncApiName() + " error=\"" +
             AclErrorMessage(acl_ret) + "\" kernel_func=" +
             FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC;
    }
    acl_ret = aclrtMemcpy(canary_status_words, sizeof(canary_status_words),
                          canary_status_dev, sizeof(canary_status_words),
                          ACL_MEMCPY_DEVICE_TO_HOST);
    if (acl_ret != ACL_SUCCESS) {
      (void)aclrtFree(canary_status_dev);
      (void)aclrtBinaryUnLoad(bin_handle);
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3d_no_internal_headers=on "
                         "stage3b3d_direct_aclrt_canary_loader=passed "
                         "stage3b3d_direct_aclrt_canary_handoff=passed "
                         "stage3b3d_direct_aclrt_canary_launch=passed "
                         "stage3b3d_direct_aclrt_canary_sync=failed "
                         "canary_launch_api=") +
             canary_launch_api +
             " api=aclrtMemcpy(canary_status_d2h) error=\"" +
             AclErrorMessage(acl_ret) + "\" kernel_func=" +
             FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC;
    }
    (void)aclrtFree(canary_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    if (canary_status_words[0] != 0 ||
        canary_status_words[1] != FLUME_HCOMM_CANARY_TOKEN) {
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3d_no_internal_headers=on "
                         "stage3b3d_direct_aclrt_canary_loader=passed "
                         "stage3b3d_direct_aclrt_canary_handoff=passed "
                         "stage3b3d_direct_aclrt_canary_launch=passed "
                         "stage3b3d_direct_aclrt_canary_sync=passed "
                         "stage3b3d_direct_aclrt_canary=failed "
                         "canary_launch_api=") +
             canary_launch_api + " canary_status_word=" +
             std::to_string(canary_status_words[0]) +
             " canary_observed_token=" +
             std::to_string(canary_status_words[1]) + " kernel_func=" +
             FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC;
    }
    *status = FLUME_OK;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=passed "
                       "stage3b3d_direct_aclrt_canary_handoff=passed "
                       "stage3b3d_direct_aclrt_canary_launch=passed "
                       "stage3b3d_direct_aclrt_canary_sync=passed "
                       "stage3b3d_direct_aclrt_canary=passed "
                       "canary_launch_api=") +
           canary_launch_api +
           " canary_status_word=0 canary_observed_token=" +
           std::to_string(FLUME_HCOMM_CANARY_TOKEN) + " kernel_func=" +
           FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC;
  }

  (void)aclrtFree(canary_status_dev);
  (void)aclrtBinaryUnLoad(bin_handle);
  *status = FLUME_ERR_BACKEND;
  return std::string("stage3b3d_no_internal_headers=on "
                     "stage3b3d_direct_aclrt_canary_loader=passed "
                     "stage3b3d_direct_aclrt_canary_handoff=passed "
                     "stage3b3d_direct_aclrt_canary_launch=failed "
                     "api=") +
         acl_launch_api + " canary_launch_api=" + canary_launch_api +
         " error=\"" +
         AclErrorMessage(acl_ret) + "\" kernel_func=" +
         FLUME_HCOMM_CANARY_DIRECT_ACLRT_KERNEL_FUNC;
}

std::string TryLaunchHcommNotifyOnlyDirectAclrt(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    void* acl_stream,
    const HcommChannelResourceInfo& resource_info,
    const HcommLauncherDecision& decision,
    int* status) {
  if (status == nullptr) {
    return "stage3b3c_direct_aclrt_loader=failed reason=\"status pointer null\"";
  }
  if (!decision.package.installed) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtBlockedDetail(decision, "custom_op_package missing");
  }
  if (acl_stream == nullptr) {
    *status = FLUME_ERR_INVALID_ARGUMENT;
    return MakeDirectAclrtBlockedDetail(decision, "acl stream is null");
  }
  if (resource_info.aicpu_ts_thread == 0 || resource_info.channel_handle == 0) {
    *status = FLUME_ERR_UNSUPPORTED;
    return MakeDirectAclrtBlockedDetail(
        decision, "missing AICPU_TS thread or HCOMM channel handle");
  }

  flume_hcomm_notify_only_desc_v1 desc = {};
  FillFlumeNotifyOnlyDesc(role, state, peer_rank, resource_info, &desc);

  aclrtBinHandle bin_handle = nullptr;
  aclrtBinaryLoadOption option = {};
  option.type = ACL_RT_BINARY_LOAD_OPT_CPU_KERNEL_MODE;
  option.value.cpuKernelMode = 0;
  aclrtBinaryLoadOptions load_options = {};
  load_options.options = &option;
  load_options.numOpt = 1;
  aclError acl_ret =
      aclrtBinaryLoadFromFile(decision.package.json_path.c_str(), &load_options,
                              &bin_handle);
  if (acl_ret != ACL_SUCCESS) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3c_direct_aclrt_loader=failed "
           "api=aclrtBinaryLoadFromFile error=\"") +
           AclErrorMessage(acl_ret) +
           "\" custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC, &func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3c_direct_aclrt_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3c_descriptor_handoff=blocked "
           "stage3b3c_direct_aclrt_launch=not-attempted kernel_func=" +
           FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  void* notify_status_dev = nullptr;
  uint32_t notify_status_words[2] = {0xFFFFFFFFU, 0xFFFFFFFFU};
  acl_ret = aclrtMalloc(&notify_status_dev, sizeof(notify_status_words),
                        ACL_MEM_MALLOC_HUGE_FIRST);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3c_direct_aclrt_loader=passed "
                       "stage3b3c_descriptor_handoff=failed "
                       "api=aclrtMalloc(notify_status) error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }
  acl_ret = aclrtMemcpy(notify_status_dev, sizeof(notify_status_words),
                        notify_status_words, sizeof(notify_status_words),
                        ACL_MEMCPY_HOST_TO_DEVICE);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(notify_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3c_direct_aclrt_loader=passed "
                       "stage3b3c_descriptor_handoff=failed "
                       "api=aclrtMemcpy(notify_status_h2d) error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }
  desc.status_word = reinterpret_cast<uint64_t>(notify_status_dev);

#if FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS
  const char* notify_launch_api = "host-args";
#else
  const char* notify_launch_api = "args-handle";
  aclrtArgsHandle args_handle = nullptr;
  acl_ret = aclrtKernelArgsInit(func_handle, &args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(notify_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3c_direct_aclrt_loader=passed "
                       "stage3b3c_descriptor_handoff=failed "
                       "notify_launch_api=args-handle "
                       "api=aclrtKernelArgsInit error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }

  aclrtParamHandle param_handle = nullptr;
  acl_ret =
      aclrtKernelArgsAppend(args_handle, &desc, sizeof(desc), &param_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(notify_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3c_direct_aclrt_loader=passed "
                       "stage3b3c_descriptor_handoff=failed "
                       "notify_launch_api=args-handle "
                       "api=aclrtKernelArgsAppend error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }

  acl_ret = aclrtKernelArgsFinalize(args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(notify_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3c_direct_aclrt_loader=passed "
                       "stage3b3c_descriptor_handoff=failed "
                       "notify_launch_api=args-handle "
                       "api=aclrtKernelArgsFinalize error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }
#endif

  aclrtLaunchKernelAttr attr = {};
  attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
  attr.value.timeout = desc.timeout_sec;
  aclrtLaunchKernelCfg cfg = {};
  cfg.attrs = &attr;
  cfg.numAttrs = 1;
#if FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS
  acl_ret = aclrtLaunchKernelWithHostArgs(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, &desc,
      sizeof(desc), nullptr, 0);
  const char* acl_launch_api = "aclrtLaunchKernelWithHostArgs";
#else
  acl_ret = aclrtLaunchKernelWithConfig(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, args_handle,
      nullptr);
  const char* acl_launch_api = "aclrtLaunchKernelWithConfig";
#endif
  if (acl_ret == ACL_SUCCESS) {
    acl_ret = SyncAclStreamForHcomm(static_cast<aclrtStream>(acl_stream),
                                    desc.timeout_sec);
    if (acl_ret != ACL_SUCCESS) {
      (void)aclrtFree(notify_status_dev);
      (void)aclrtBinaryUnLoad(bin_handle);
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3c_direct_aclrt_loader=passed "
                         "stage3b3c_descriptor_handoff=passed "
                         "stage3b3c_direct_aclrt_launch=passed "
                         "stage3b3c_direct_aclrt_sync=failed "
                         "notify_launch_api=") +
             notify_launch_api + " api=" + AclStreamSyncApiName() +
             " error=\"" +
             AclErrorMessage(acl_ret) + "\" kernel_func=" +
             FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC;
    }
    acl_ret = aclrtMemcpy(notify_status_words, sizeof(notify_status_words),
                          notify_status_dev, sizeof(notify_status_words),
                          ACL_MEMCPY_DEVICE_TO_HOST);
    if (acl_ret != ACL_SUCCESS) {
      (void)aclrtFree(notify_status_dev);
      (void)aclrtBinaryUnLoad(bin_handle);
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3c_direct_aclrt_loader=passed "
                         "stage3b3c_descriptor_handoff=passed "
                         "stage3b3c_direct_aclrt_launch=passed "
                         "stage3b3c_direct_aclrt_sync=failed "
                         "notify_launch_api=") +
             notify_launch_api +
             " api=aclrtMemcpy(notify_status_d2h) error=\"" +
             AclErrorMessage(acl_ret) + "\" kernel_func=" +
             FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC;
    }
    (void)aclrtFree(notify_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    const uint32_t notify_status = notify_status_words[0];
    const uint32_t notify_hcomm_ret = notify_status_words[1];
    if (notify_status != 0) {
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3c_direct_aclrt_loader=passed "
                         "stage3b3c_descriptor_handoff=passed "
                         "stage3b3c_direct_aclrt_launch=passed "
                         "stage3b3c_direct_aclrt_sync=passed "
                         "stage3b2_kernel_consume=failed "
                         "notify_launch_api=") +
             notify_launch_api + " notify_kernel_status=" +
             NotifyKernelStatusName(notify_status) +
             " notify_status_word=" +
             std::to_string(notify_status) +
             " notify_kernel_hcomm_ret=" +
             std::to_string(notify_hcomm_ret) + " kernel_func=" +
             FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC;
    }
    *status = FLUME_OK;
    return std::string("stage3b3c_direct_aclrt_loader=passed "
                       "stage3b3c_descriptor_handoff=passed "
                       "stage3b3c_direct_aclrt_launch=passed "
                       "stage3b3c_direct_aclrt_sync=passed "
                       "stage3b2_kernel_consume=passed "
                       "notify_launch_api=") +
           notify_launch_api +
           " "
                       "notify_kernel_status=success "
                       "notify_status_word=0 kernel_func=" +
           FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC;
  }

  (void)aclrtFree(notify_status_dev);
  (void)aclrtBinaryUnLoad(bin_handle);
  *status = FLUME_ERR_BACKEND;
  return std::string("stage3b3c_direct_aclrt_loader=passed "
                     "stage3b3c_descriptor_handoff=passed "
                     "stage3b3c_direct_aclrt_launch=failed "
                     "api=") +
         acl_launch_api + " notify_launch_api=" + notify_launch_api +
         " error=\"" +
         AclErrorMessage(acl_ret) + "\" kernel_func=" +
         FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC;
}
#else
std::string TryLaunchHcommPayloadCopyDirectAclrt(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    void* acl_stream,
    void* user_buffer,
    uint64_t bytes,
    bool disable_payload_batch_mode,
    const std::string& payload_batch_tag,
    bool payload_recv_direct_output,
    bool payload_force_channel_fence,
    bool payload_write_path,
    bool payload_write_with_notify,
    flume_hcomm_payload_comm_binding_t payload_comm_binding,
    const HcommChannelResourceInfo& resource_info,
    const HcommLauncherDecision& decision,
    int* status) {
  (void)role;
  (void)state;
  (void)peer_rank;
  (void)acl_stream;
  (void)user_buffer;
  (void)bytes;
  (void)disable_payload_batch_mode;
  (void)payload_batch_tag;
  (void)payload_recv_direct_output;
  (void)payload_force_channel_fence;
  (void)payload_write_path;
  (void)payload_write_with_notify;
  (void)payload_comm_binding;
  (void)resource_info;
  if (status != nullptr) {
    *status = FLUME_ERR_UNSUPPORTED;
  }
#if FLUME_BUILD_HCOMM_CUSTOM_OP
  return MakeDirectAclrtPayloadBlockedDetail(
      decision, "ACL runtime custom-op launch APIs unavailable");
#else
  return MakeDirectAclrtPayloadBlockedDetail(decision,
                                             "custom-op build disabled");
#endif
}

std::string TryLaunchHcommDirectAclrtCanary(
    const CommState& state,
    uint32_t peer_rank,
    void* acl_stream,
    const HcommLauncherDecision& decision,
    int* status) {
  (void)state;
  (void)peer_rank;
  (void)acl_stream;
  if (status != nullptr) {
    *status = FLUME_ERR_UNSUPPORTED;
  }
#if FLUME_BUILD_HCOMM_CUSTOM_OP
  return MakeDirectAclrtCanaryBlockedDetail(
      decision, "ACL runtime custom-op launch APIs unavailable");
#else
  return MakeDirectAclrtCanaryBlockedDetail(decision,
                                            "custom-op build disabled");
#endif
}

std::string TryLaunchHcommNotifyOnlyDirectAclrt(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    void* acl_stream,
    const HcommChannelResourceInfo& resource_info,
    const HcommLauncherDecision& decision,
    int* status) {
  (void)role;
  (void)state;
  (void)peer_rank;
  (void)acl_stream;
  (void)resource_info;
  if (status != nullptr) {
    *status = FLUME_ERR_UNSUPPORTED;
  }
#if FLUME_BUILD_HCOMM_CUSTOM_OP
  return MakeDirectAclrtBlockedDetail(
      decision, "ACL runtime custom-op launch APIs unavailable");
#else
  return MakeDirectAclrtBlockedDetail(decision, "custom-op build disabled");
#endif
}
#endif

bool TryLaunchHcommNotifyOnlyKernel(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    void* acl_stream,
    const HcommChannelResourceInfo& resource_info,
    int* status,
    std::string* detail) {
  if (status == nullptr || detail == nullptr) {
    return false;
  }
  *status = FLUME_ERR_BACKEND;
  detail->clear();
  HcommLauncherDecision launcher = DecideHcommLauncherBackend();
  std::string router_detail = DescribeHcommLauncherDecision(launcher);
  if (launcher.backend == HcommLauncherBackend::kUnsupported) {
    int canary_status = FLUME_ERR_UNSUPPORTED;
    std::string canary_detail = TryLaunchHcommDirectAclrtCanary(
        state, peer_rank, acl_stream, launcher, &canary_status);
    if (canary_status != FLUME_OK) {
      *status = canary_status;
      std::string direct_detail = MakeDirectAclrtBlockedDetail(
          launcher, "standalone canary gate did not pass");
      *detail = std::string("stage3b3a_kernel_launch=") +
                (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
                " stage3b3d_canary_gate=" +
                (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
                " " + router_detail + " " + canary_detail + " " +
                direct_detail;
      return false;
    }
    int direct_status = FLUME_ERR_UNSUPPORTED;
    std::string direct_detail = TryLaunchHcommNotifyOnlyDirectAclrt(
        role, state, peer_rank, acl_stream, resource_info, launcher,
        &direct_status);
    if (direct_status == FLUME_OK) {
      *status = FLUME_OK;
      *detail = std::string("stage3b3a_kernel_launch=passed ") +
                "stage3b3d_canary_gate=passed " + router_detail + " " +
                canary_detail + " " + direct_detail;
      return true;
    }
    *status = direct_status;
    *detail = std::string("stage3b3a_kernel_launch=") +
              (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
              " stage3b3d_canary_gate=passed " + router_detail + " " +
              canary_detail + " " + direct_detail;
    return false;
  }
  if (launcher.backend == HcommLauncherBackend::kDirectAclrtPending) {
    int canary_status = FLUME_ERR_UNSUPPORTED;
    std::string canary_detail = TryLaunchHcommDirectAclrtCanary(
        state, peer_rank, acl_stream, launcher, &canary_status);
    if (canary_status != FLUME_OK) {
      *status = canary_status;
      std::string direct_detail = MakeDirectAclrtBlockedDetail(
          launcher, "standalone canary gate did not pass");
      *detail = std::string("stage3b3a_kernel_launch=") +
                (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
                " stage3b3d_canary_gate=" +
                (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
                " " + router_detail + " " + canary_detail + " " +
                direct_detail;
      return false;
    }
    std::string direct_detail = TryLaunchHcommNotifyOnlyDirectAclrt(
        role, state, peer_rank, acl_stream, resource_info, launcher, status);
    if (*status == FLUME_OK) {
      *detail = std::string("stage3b3a_kernel_launch=passed ") +
                "stage3b3d_canary_gate=passed " + router_detail + " " +
                canary_detail + " " + direct_detail;
      return true;
    }
    *detail = std::string("stage3b3a_kernel_launch=") +
              (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
              " stage3b3d_canary_gate=passed " + router_detail + " " +
              canary_detail + " " + direct_detail;
    return false;
  }
#if FLUME_BUILD_HCOMM_CUSTOM_OP && FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH
  if (resource_info.aicpu_ts_thread == 0 || resource_info.channel_handle == 0) {
    *status = FLUME_ERR_UNSUPPORTED;
    *detail = "stage3b3a_kernel_launch=unsupported reason=\"missing "
              "AICPU_TS thread or HCOMM channel handle\" " +
              router_detail;
    return false;
  }

  flume_hcomm_notify_only_desc_v1 desc = {};
  FillFlumeNotifyOnlyDesc(role, state, peer_rank, resource_info, &desc);

  auto comm = static_cast<HcclComm>(state.hccl_comm);
  HcclOpDesc op_desc;
  HcclResult ret = HcclOpDescInit(&op_desc);
  if (ret != HCCL_SUCCESS) {
    *detail = std::string("stage3b3a_kernel_launch=failed api=HcclOpDescInit "
                          "error=\"") +
              HcclErrorMessage(ret) + "\" " + router_detail;
    return false;
  }
  op_desc.opDescType = 1;
  const char* op_name = role == flume::hcomm_payload::PayloadRole::kSend ?
                            "FlumeNotifyOnlySend" :
                            "FlumeNotifyOnlyRecv";
  std::snprintf(op_desc.opName, sizeof(op_desc.opName), "%s", op_name);
  op_desc.p2p.buffer = resource_info.local_buffer;
  op_desc.p2p.cmdType = role == flume::hcomm_payload::PayloadRole::kSend ?
                            HCCL_CMD_SEND :
                            HCCL_CMD_RECEIVE;
  op_desc.p2p.dataType = HCCL_DATA_TYPE_UINT8;
  op_desc.p2p.count = 1;
  op_desc.p2p.remoteRank = peer_rank;
  op_desc.p2p.unfoldStream = acl_stream;

  HcclKernelFuncInfo func_info = {};
  std::snprintf(func_info.kernelSoName, sizeof(func_info.kernelSoName), "%s",
                FLUME_HCOMM_NOTIFY_ONLY_KERNEL_SO);
  std::snprintf(func_info.kernelFuncName, sizeof(func_info.kernelFuncName),
                "%s", FLUME_HCOMM_NOTIFY_ONLY_KERNEL_FUNC);
  func_info.args = &desc;
  func_info.argSize = sizeof(desc);

  HcclKernelLaunchCfg launch_cfg;
  ret = HcclKernelLaunchCfgInit(&launch_cfg);
  if (ret != HCCL_SUCCESS) {
    *detail = std::string("stage3b3a_kernel_launch=failed "
                          "api=HcclKernelLaunchCfgInit error=\"") +
              HcclErrorMessage(ret) + "\" " + router_detail;
    return false;
  }
  launch_cfg.timeOut = desc.timeout_sec;

  ret = HcclAicpuKernelLaunch(
      comm, &op_desc, &func_info,
      static_cast<ThreadHandle>(resource_info.aicpu_ts_thread),
      static_cast<aclrtStream>(acl_stream), &launch_cfg);
  if (ret == HCCL_SUCCESS) {
    *status = FLUME_OK;
    *detail = std::string("stage3b3a_kernel_launch=passed "
                          "stage3b2_kernel_consume=passed kernel_so=") +
              FLUME_HCOMM_NOTIFY_ONLY_KERNEL_SO +
              " kernel_func=" + FLUME_HCOMM_NOTIFY_ONLY_KERNEL_FUNC +
              " " + router_detail;
    return true;
  }
  *status = IsUnsupportedHcclLaunchResult(ret) ? FLUME_ERR_UNSUPPORTED :
                                                 FLUME_ERR_BACKEND;
  *detail = std::string("stage3b3a_kernel_launch=") +
            (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
            " api=HcclAicpuKernelLaunch error=\"" + HcclErrorMessage(ret) +
            "\" kernel_so=" + FLUME_HCOMM_NOTIFY_ONLY_KERNEL_SO +
            " kernel_func=" + FLUME_HCOMM_NOTIFY_ONLY_KERNEL_FUNC +
            " " + router_detail;
  return false;
#else
  *status = FLUME_ERR_UNSUPPORTED;
  *detail = std::string("stage3b3a_kernel_launch=unsupported ") +
            router_detail;
  return false;
#endif
}
#endif

}  // namespace

const char* flume_status_string(int status) {
  switch (status) {
    case FLUME_OK:
      return "ok";
    case FLUME_ERR_INVALID_ARGUMENT:
      return "invalid argument";
    case FLUME_ERR_IO:
      return "io error";
    case FLUME_ERR_TIMEOUT:
      return "timeout";
    case FLUME_ERR_UNSUPPORTED:
      return "unsupported";
    case FLUME_ERR_BACKEND:
      return "backend error";
    case FLUME_ERR_REMOTE:
      return "remote error";
    case FLUME_ERR_PROTOCOL:
      return "protocol error";
    case FLUME_PENDING:
      return "pending";
    default:
      return "unknown";
  }
}

int flume_client_open(const char* endpoint, flume_client_t** out) {
  if (out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

  int fd = -1;
  int ret = ConnectEndpoint(endpoint, &fd);
  if (ret != FLUME_OK) {
    return ret;
  }

  auto* client = new flume_client;
  client->fd = fd;

  Frame hello;
  hello.type = FrameType::kHello;
  hello.request_id = NextRequestId(client);
  AppendString(&hello.body, "flume-client");
  std::string error;
  if (!WriteFrame(fd, hello, &error)) {
    flume_client_close(client);
    return FLUME_ERR_IO;
  }
  Frame ack;
  if (!ReadFrame(fd, &ack, &error) || ack.type != FrameType::kHelloAck) {
    flume_client_close(client);
    return FLUME_ERR_PROTOCOL;
  }
  Reader reader(ack.body);
  uint32_t status = 0;
  std::string capabilities;
  if (!reader.ReadU32(&status) || !reader.ReadString(&capabilities)) {
    flume_client_close(client);
    return FLUME_ERR_PROTOCOL;
  }
  if (status != FLUME_OK) {
    flume_client_close(client);
    return static_cast<int>(status);
  }

  *out = client;
  return FLUME_OK;
}

int flume_client_close(flume_client_t* client) {
  if (client == nullptr) {
    return FLUME_OK;
  }
  if (client->fd >= 0) {
    close(client->fd);
    client->fd = -1;
  }
  delete client;
  return FLUME_OK;
}

int flume_attach_hccl_comm(flume_client_t* client, void* hccl_comm,
                          uint32_t rank, uint32_t rank_size) {
  if (client == nullptr || hccl_comm == nullptr || rank_size == 0 || rank >= rank_size) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
#if FLUME_ENABLE_HCCL
  std::lock_guard<std::mutex> lock(client->mu);
  client->hccl_attached = true;
  client->hccl_comm = hccl_comm;
  client->sim_comm_attached = false;
  client->sim_comm_name.clear();
  client->rank = rank;
  client->rank_size = rank_size;
  return FLUME_OK;
#else
  (void)hccl_comm;
  (void)rank;
  (void)rank_size;
  return FLUME_ERR_UNSUPPORTED;
#endif
}

int flume_attach_sim_comm(flume_client_t* client, const char* comm_name,
                         uint32_t rank, uint32_t rank_size) {
  if (client == nullptr || comm_name == nullptr || comm_name[0] == '\0' ||
      rank_size == 0 || rank >= rank_size) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> lock(client->mu);
  client->hccl_attached = false;
  client->hccl_comm = nullptr;
  client->sim_comm_attached = true;
  client->sim_comm_name = comm_name;
  client->rank = rank;
  client->rank_size = rank_size;
  client->sim_allreduce_seq = 0;
  client->sim_allgather_seq = 0;
  client->sim_p2p_send_seq = 0;
  client->sim_p2p_recv_seq = 0;
  client->sim_hcomm_payload_send_seq = 0;
  client->sim_hcomm_payload_recv_seq = 0;
  client->sim_a3_register_seq = 0;
  return FLUME_OK;
}

int flume_get_backend_caps(flume_client_t* client, flume_backend_caps_t* out) {
  if (out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (out->size != 0 && out->size < sizeof(flume_backend_caps_t)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  bool sim_attached = false;
  bool hccl_attached = false;
  if (client != nullptr) {
    CommState state = SnapshotCommState(client);
    sim_attached = state.sim_comm_attached;
    hccl_attached = state.hccl_attached;
  }

  flume_backend_caps_t caps = {};
  caps.size = sizeof(caps);
  caps.hccl_root_info = FLUME_HAVE_HCCL_ROOT_INFO ? 1U : 0U;
  caps.hccl_init_all = FLUME_HAVE_HCCL_COMM_INIT_ALL ? 1U : 0U;
  caps.hccl_p2p = (FLUME_HAVE_HCCL_P2P || sim_attached) ? 1U : 0U;
  caps.hcomm_channel_res = (FLUME_HAVE_HCOMM_CHANNEL_RES || sim_attached) ? 1U : 0U;
  caps.hcomm_primitives = FLUME_HAVE_HCOMM_PRIMITIVES ? 1U : 0U;
  caps.hcomm_thread_export = FLUME_HAVE_HCOMM_THREAD_EXPORT ? 1U : 0U;
  caps.hcomm_rank_graph = FLUME_HAVE_HCOMM_RANK_GRAPH ? 1U : 0U;
  caps.hcomm_payload_probe =
      ((FLUME_HAVE_HCOMM_CHANNEL_RES && FLUME_HAVE_HCOMM_PRIMITIVES &&
        (FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD ||
         FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY)) ||
       sim_attached) ? 1U : 0U;
  caps.hcomm_payload_scheduler = sim_attached ? 1U : 0U;
  caps.storage_hbm = sim_attached ? 1U : 0U;
  caps.fallback_hccl_p2p = (FLUME_HAVE_HCCL_P2P || sim_attached) ? 1U : 0U;
  caps.fallback_runtime_staging = (!sim_attached && hccl_attached) ? 1U : 0U;
  caps.hcomm_payload_direct_aclrt =
      (FLUME_BUILD_HCOMM_CUSTOM_OP && FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH) ? 1U : 0U;
  caps.hcomm_payload_direct_aclrt_host_args =
      (FLUME_BUILD_HCOMM_CUSTOM_OP && FLUME_HAVE_ACLRT_CUSTOM_OP_HOST_ARGS) ? 1U : 0U;
  caps.hcomm_payload_thread_notify =
      (FLUME_HAVE_HCOMM_THREAD_EXPORT && FLUME_HAVE_HCOMM_PRIMITIVES) ? 1U : 0U;
  caps.hcomm_write_with_notify =
      (FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY ||
       FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI) ? 1U : 0U;
  caps.roce_storage_protocol = 1U;
  caps.roce_storage_native = flume::roce::NativeTransportCompiled() ? 1U : 0U;
  caps.hcomm_payload_scheduler_candidate =
      (sim_attached ||
       (FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES &&
        caps.hcomm_payload_direct_aclrt)) ? 1U : 0U;
#if FLUME_HAVE_HCOMM_THREAD_EXPORT
  caps.hcomm_default_engine = FLUME_HCOMM_ENGINE_AICPU_TS;
#else
  caps.hcomm_default_engine = FLUME_HCOMM_ENGINE_CPU_TS;
#endif
  *out = caps;
  return FLUME_OK;
}

int flume_open(flume_client_t* client, const char* path, flume_file_t** out) {
  if (client == nullptr || path == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  std::lock_guard<std::mutex> lock(client->mu);

  Frame req;
  req.type = FrameType::kOpenReq;
  req.request_id = NextRequestId(client);
  AppendString(&req.body, path);
  std::string error;
  if (!WriteFrame(client->fd, req, &error)) {
    return FLUME_ERR_IO;
  }
  Frame resp;
  if (!ReadFrame(client->fd, &resp, &error) || resp.type != FrameType::kOpenResp) {
    return FLUME_ERR_PROTOCOL;
  }

  Reader reader(resp.body);
  uint32_t status = 0;
  uint64_t file_id = 0;
  uint64_t size = 0;
  std::string remote_error;
  if (!reader.ReadU32(&status) || !reader.ReadU64(&file_id) ||
      !reader.ReadU64(&size) || !reader.ReadString(&remote_error)) {
    return FLUME_ERR_PROTOCOL;
  }
  if (status != FLUME_OK) {
    return static_cast<int>(status);
  }

  auto* file = new flume_file;
  file->client = client;
  file->file_id = file_id;
  file->size = size;
  file->path = path;
  *out = file;
  return FLUME_OK;
}

int flume_close(flume_file_t* file) {
  if (file == nullptr) {
    return FLUME_OK;
  }
  flume_client_t* client = file->client;
  if (client != nullptr && client->fd >= 0) {
    std::lock_guard<std::mutex> lock(client->mu);
    Frame req;
    req.type = FrameType::kCloseReq;
    req.request_id = NextRequestId(client);
    AppendU64(&req.body, file->file_id);
    std::string error;
    (void)WriteFrame(client->fd, req, &error);
    Frame resp;
    (void)ReadFrame(client->fd, &resp, &error);
  }
  delete file;
  return FLUME_OK;
}

int flume_register_buffer(flume_client_t* client, void* ptr, size_t len,
                         flume_buffer_type_t type, flume_buffer_t** out) {
  if (client == nullptr || ptr == nullptr || len == 0 || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (!IsRegisterableBufferType(type)) {
    return FLUME_ERR_UNSUPPORTED;
  }
  auto* buffer = new flume_buffer;
  buffer->client = client;
  buffer->ptr = ptr;
  buffer->len = len;
  buffer->type = type;
  *out = buffer;
  return FLUME_OK;
}

int flume_sim_alloc_buffer(flume_client_t* client, size_t len,
                          flume_buffer_type_t type, flume_buffer_t** out) {
  if (client == nullptr || len == 0 || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (!IsSimBufferType(type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  auto* buffer = new flume_buffer;
  buffer->client = client;
  buffer->owned.assign(len, 0);
  buffer->ptr = buffer->owned.data();
  buffer->len = buffer->owned.size();
  buffer->type = type;
  *out = buffer;
  return FLUME_OK;
}

void* flume_buffer_data(flume_buffer_t* buffer) {
  return buffer == nullptr ? nullptr : buffer->ptr;
}

size_t flume_buffer_size(flume_buffer_t* buffer) {
  return buffer == nullptr ? 0 : buffer->len;
}

flume_buffer_type_t flume_buffer_type(flume_buffer_t* buffer) {
  return buffer == nullptr ? FLUME_BUFFER_HOST : buffer->type;
}

int flume_buffer_release(flume_buffer_t* buffer) {
  if (buffer == nullptr) {
    return FLUME_OK;
  }
  if (buffer->a3_symmetric_ref_count.load(std::memory_order_relaxed) != 0 ||
      buffer->pending_ref_count.load(std::memory_order_relaxed) != 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  delete buffer;
  return FLUME_OK;
}

int flume_a3_register_symmetric_memory(flume_client_t* client,
                                      flume_buffer_t* buffer,
                                      size_t offset,
                                      size_t len,
                                      flume_a3_symmetric_window_t** out) {
  if (client == nullptr || buffer == nullptr || len == 0 || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  if (buffer->client != client || !ValidateRange(buffer, offset, len) ||
      !IsCollectiveBufferType(buffer->type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (IsSimBufferType(buffer->type)) {
    std::string comm_name;
    uint32_t rank = 0;
    uint32_t rank_size = 0;
    uint64_t seq = 0;
    {
      std::lock_guard<std::mutex> lock(client->mu);
      if (!client->sim_comm_attached || client->rank_size == 0) {
        return FLUME_ERR_UNSUPPORTED;
      }
      comm_name = client->sim_comm_name;
      rank = client->rank;
      rank_size = client->rank_size;
      seq = client->sim_a3_register_seq++;
    }
    int ret = SubmitSimA3Registration(comm_name, rank, rank_size, seq, offset, len);
    if (ret != FLUME_OK) {
      return ret;
    }
    auto* window = new flume_a3_symmetric_window;
    window->client = client;
    window->buffer = buffer;
    window->offset = offset;
    window->len = len;
    window->sim = true;
    buffer->a3_symmetric_ref_count.fetch_add(1, std::memory_order_relaxed);
    *out = window;
    return FLUME_OK;
  }

  if (!IsRealHcclBufferType(buffer->type)) {
    return FLUME_ERR_UNSUPPORTED;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_SYM_WINDOW
  CommState state = SnapshotCommState(client);
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_UNSUPPORTED;
  }
  auto* addr = static_cast<uint8_t*>(buffer->ptr) + offset;
  HcclCommSymWindow hccl_window = nullptr;
  HcclResult result =
      HcclCommSymWinRegister(static_cast<HcclComm>(state.hccl_comm), addr,
                             static_cast<uint64_t>(len), &hccl_window, 1);
  if (result != HCCL_SUCCESS) {
    return FLUME_ERR_BACKEND;
  }

  auto* window = new flume_a3_symmetric_window;
  window->client = client;
  window->buffer = buffer;
  window->hccl_window = static_cast<void*>(hccl_window);
  window->offset = offset;
  window->len = len;
  window->sim = false;
  buffer->a3_symmetric_ref_count.fetch_add(1, std::memory_order_relaxed);
  *out = window;
  return FLUME_OK;
#else
  return FLUME_ERR_UNSUPPORTED;
#endif
}

int flume_a3_deregister_symmetric_memory(flume_a3_symmetric_window_t* window) {
  if (window == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  int status = FLUME_OK;
#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_SYM_WINDOW
  if (!window->sim && window->hccl_window != nullptr) {
    HcclResult result =
        HcclCommSymWinDeregister(static_cast<HcclCommSymWindow>(window->hccl_window));
    if (result != HCCL_SUCCESS) {
      status = FLUME_ERR_BACKEND;
    }
  }
#else
  if (!window->sim) {
    return FLUME_ERR_UNSUPPORTED;
  }
#endif

  if (window->buffer != nullptr &&
      window->buffer->a3_symmetric_ref_count.load(std::memory_order_relaxed) > 0) {
    window->buffer->a3_symmetric_ref_count.fetch_sub(1, std::memory_order_relaxed);
  }
  delete window;
  return status;
}

int flume_a3_set_memory_range(flume_client_t* client,
                             void* base_vir_ptr,
                             size_t size,
                             size_t alignment,
                             uint64_t flags) {
  if (client == nullptr || base_vir_ptr == nullptr || size == 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (alignment != 0 || flags != 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  CommState state = SnapshotCommState(client);
  if (state.sim_comm_attached && !state.hccl_attached) {
    return FLUME_OK;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_COMM_MEMORY
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_UNSUPPORTED;
  }
  HcclResult result =
      HcclCommSetMemoryRange(static_cast<HcclComm>(state.hccl_comm),
                             base_vir_ptr, size, alignment, flags);
  return result == HCCL_SUCCESS ? FLUME_OK : FLUME_ERR_BACKEND;
#else
  return FLUME_ERR_UNSUPPORTED;
#endif
}

int flume_a3_unset_memory_range(flume_client_t* client, void* base_vir_ptr) {
  if (client == nullptr || base_vir_ptr == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  CommState state = SnapshotCommState(client);
  if (state.sim_comm_attached && !state.hccl_attached) {
    return FLUME_OK;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_COMM_MEMORY
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_UNSUPPORTED;
  }
  HcclResult result =
      HcclCommUnsetMemoryRange(static_cast<HcclComm>(state.hccl_comm),
                               base_vir_ptr);
  return result == HCCL_SUCCESS ? FLUME_OK : FLUME_ERR_BACKEND;
#else
  return FLUME_ERR_UNSUPPORTED;
#endif
}

int flume_a3_activate_comm_memory(flume_client_t* client,
                                 void* vir_ptr,
                                 size_t size,
                                 size_t offset,
                                 void* drv_mem_handle,
                                 uint64_t flags) {
  if (client == nullptr || vir_ptr == nullptr || size == 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (offset != 0 || flags != 0 || drv_mem_handle == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  CommState state = SnapshotCommState(client);
  if (state.sim_comm_attached && !state.hccl_attached) {
    return FLUME_OK;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_COMM_MEMORY
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_UNSUPPORTED;
  }
  HcclResult result =
      HcclCommActivateCommMemory(static_cast<HcclComm>(state.hccl_comm),
                                 vir_ptr, size, offset,
                                 static_cast<aclrtDrvMemHandle>(drv_mem_handle),
                                 flags);
  return result == HCCL_SUCCESS ? FLUME_OK : FLUME_ERR_BACKEND;
#else
  return FLUME_ERR_UNSUPPORTED;
#endif
}

int flume_a3_deactivate_comm_memory(flume_client_t* client, void* vir_ptr) {
  if (client == nullptr || vir_ptr == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  CommState state = SnapshotCommState(client);
  if (state.sim_comm_attached && !state.hccl_attached) {
    return FLUME_OK;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_COMM_MEMORY
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_UNSUPPORTED;
  }
  HcclResult result =
      HcclCommDeactivateCommMemory(static_cast<HcclComm>(state.hccl_comm),
                                   vir_ptr);
  return result == HCCL_SUCCESS ? FLUME_OK : FLUME_ERR_BACKEND;
#else
  return FLUME_ERR_UNSUPPORTED;
#endif
}

int flume_pread_async(flume_file_t* file, flume_buffer_t* dst, size_t len,
                     uint64_t file_offset, size_t buffer_offset,
                     void* acl_stream, flume_io_t** out) {
  (void)acl_stream;
  if (file == nullptr || dst == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  if (buffer_offset > dst->len || len > dst->len - buffer_offset) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (!IsReadableLocalType(dst->type)) {
    return FLUME_ERR_UNSUPPORTED;
  }

  flume_client_t* client = file->client;
  std::lock_guard<std::mutex> lock(client->mu);

  Frame req;
  req.type = FrameType::kReadReq;
  req.request_id = NextRequestId(client);
  AppendU64(&req.body, file->file_id);
  AppendU64(&req.body, file_offset);
  AppendU64(&req.body, len);

  auto* io = new flume_io;
  std::string error;
  if (!WriteFrame(client->fd, req, &error)) {
    io->status = FLUME_ERR_IO;
    io->error = error;
    *out = io;
    return FLUME_OK;
  }
  Frame resp;
  if (!ReadFrame(client->fd, &resp, &error) || resp.type != FrameType::kReadResp) {
    io->status = FLUME_ERR_PROTOCOL;
    io->error = error.empty() ? "invalid READ_RESP" : error;
    *out = io;
    return FLUME_OK;
  }

  Reader reader(resp.body);
  uint32_t status = 0;
  uint64_t bytes = 0;
  uint32_t checksum = 0;
  std::string remote_error;
  if (!reader.ReadU32(&status) || !reader.ReadU64(&bytes) ||
      !reader.ReadU32(&checksum) || !reader.ReadString(&remote_error)) {
    io->status = FLUME_ERR_PROTOCOL;
    io->error = "malformed READ_RESP";
    *out = io;
    return FLUME_OK;
  }
  io->status = static_cast<int>(status);
  io->bytes = static_cast<size_t>(std::min<uint64_t>(bytes, len));
  io->checksum = checksum;
  io->error = remote_error;
  if (status == FLUME_OK) {
    const uint8_t* payload = nullptr;
    if (!reader.ReadRaw(io->bytes, &payload)) {
      io->status = FLUME_ERR_PROTOCOL;
      io->error = "READ_RESP payload length mismatch";
    } else {
      memcpy(static_cast<uint8_t*>(dst->ptr) + buffer_offset, payload, io->bytes);
    }
  }
  *out = io;
  return FLUME_OK;
}

int flume_hbm_copy_async(flume_client_t* client, flume_buffer_t* dst,
                        size_t dst_offset, flume_buffer_t* src,
                        size_t src_offset, size_t len, void* acl_stream,
                        flume_io_t** out) {
  (void)client;
  (void)acl_stream;
  if (dst == nullptr || src == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  if (dst_offset > dst->len || len > dst->len - dst_offset ||
      src_offset > src->len || len > src->len - src_offset) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  auto* io = new flume_io;
  if (IsSimHbmCopyType(dst->type) && IsSimHbmCopyType(src->type)) {
    auto* dst_ptr = static_cast<uint8_t*>(dst->ptr) + dst_offset;
    auto* src_ptr = static_cast<const uint8_t*>(src->ptr) + src_offset;
    memmove(dst_ptr, src_ptr, len);
    io->status = FLUME_OK;
    io->bytes = len;
    io->checksum = flume::protocol::Checksum32(dst_ptr, len);
  } else {
#if FLUME_ENABLE_HCCL
    io->status = FLUME_ERR_UNSUPPORTED;
    io->error = "HCCL/HCOMM HBM copy is not implemented yet";
#else
    io->status = FLUME_ERR_UNSUPPORTED;
    io->error = "HBM copy requires simulated buffers on this build";
#endif
  }
  *out = io;
  return FLUME_OK;
}

int flume_p2p_send_async(flume_client_t* client,
                        flume_buffer_t* src,
                        size_t src_offset,
                        uint64_t count,
                        flume_data_type_t data_type,
                        uint32_t dest_rank,
                        void* acl_stream,
                        flume_io_t** out) {
  if (client == nullptr || src == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

  size_t bytes = 0;
  if (!CheckedBytes(count, data_type, &bytes) ||
      !ValidateRange(src, src_offset, bytes) || src->client != client) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (IsSimBufferType(src->type)) {
    return SubmitSimP2p(client, SimP2pRole::kSend, src, src_offset, count,
                        data_type, dest_rank, out);
  }
  if (!IsRealHcclBufferType(src->type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_P2P
  CommState state = SnapshotCommState(client);
  if (!state.hccl_attached || state.hccl_comm == nullptr ||
      state.rank_size == 0) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCCL P2P send requires flume_attach_hccl_comm");
    return FLUME_OK;
  }
  if (dest_rank >= state.rank_size || dest_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  HcclDataType hccl_data_type;
  if (!ToHcclDataType(data_type, &hccl_data_type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  auto* send_ptr = static_cast<uint8_t*>(src->ptr) + src_offset;
  HcclResult result =
      HcclSend(send_ptr, count, hccl_data_type, dest_rank,
               static_cast<HcclComm>(state.hccl_comm),
               static_cast<aclrtStream>(acl_stream));
  if (result != HCCL_SUCCESS) {
    *out = MakeIo(FLUME_ERR_BACKEND, 0, 0, HcclErrorMessage(result));
    return FLUME_OK;
  }

  *out = MakeAclStreamIo(acl_stream, bytes);
  return FLUME_OK;
#else
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCCL P2P send is unavailable in this build");
  return FLUME_OK;
#endif
}

int flume_p2p_recv_async(flume_client_t* client,
                        flume_buffer_t* dst,
                        size_t dst_offset,
                        uint64_t count,
                        flume_data_type_t data_type,
                        uint32_t src_rank,
                        void* acl_stream,
                        flume_io_t** out) {
  if (client == nullptr || dst == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

  size_t bytes = 0;
  if (!CheckedBytes(count, data_type, &bytes) ||
      !ValidateRange(dst, dst_offset, bytes) || dst->client != client) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (IsSimBufferType(dst->type)) {
    return SubmitSimP2p(client, SimP2pRole::kRecv, dst, dst_offset, count,
                        data_type, src_rank, out);
  }
  if (!IsRealHcclBufferType(dst->type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCCL_P2P
  CommState state = SnapshotCommState(client);
  if (!state.hccl_attached || state.hccl_comm == nullptr ||
      state.rank_size == 0) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCCL P2P recv requires flume_attach_hccl_comm");
    return FLUME_OK;
  }
  if (src_rank >= state.rank_size || src_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  HcclDataType hccl_data_type;
  if (!ToHcclDataType(data_type, &hccl_data_type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  auto* recv_ptr = static_cast<uint8_t*>(dst->ptr) + dst_offset;
  HcclResult result =
      HcclRecv(recv_ptr, count, hccl_data_type, src_rank,
               static_cast<HcclComm>(state.hccl_comm),
               static_cast<aclrtStream>(acl_stream));
  if (result != HCCL_SUCCESS) {
    *out = MakeIo(FLUME_ERR_BACKEND, 0, 0, HcclErrorMessage(result));
    return FLUME_OK;
  }

  *out = MakeAclStreamIo(acl_stream, bytes);
  return FLUME_OK;
#else
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCCL P2P recv is unavailable in this build");
  return FLUME_OK;
#endif
}

int flume_hcomm_channel_probe(flume_client_t* client,
                              uint32_t peer_rank,
                              void* acl_stream,
                              flume_io_t** out) {
  flume_hcomm_channel_probe_options_t options = {};
  options.size = sizeof(options);
  options.notify_num = 2;
  options.engine = FLUME_HCOMM_ENGINE_AUTO;
  options.protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  options.require_thread_export = 0;
  return flume_hcomm_channel_probe_ex(client, peer_rank, &options, acl_stream,
                                      out);
}

int flume_hcomm_channel_probe_ex(
    flume_client_t* client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t* options,
    void* acl_stream,
    flume_io_t** out) {
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  HcommProbeOptions normalized_options;
  std::string option_error;
  if (!NormalizeHcommProbeOptions(options, &normalized_options,
                                  &option_error)) {
    (void)option_error;
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (normalized_options.protocol == FLUME_HCOMM_PROTOCOL_PCIE) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCOMM channel resource probe does not support pcie protocol");
    return FLUME_OK;
  }

  CommState state = SnapshotCommState(client);
  if (state.sim_comm_attached) {
    if (state.rank_size == 0 || peer_rank >= state.rank_size ||
        peer_rank == state.rank) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    (void)acl_stream;
    *out = MakeIo(FLUME_OK, 0, 0);
    return FLUME_OK;
  }
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (state.rank_size == 0 || peer_rank >= state.rank_size ||
      peer_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string detail;
  std::string error;
  if (!ProbeHcommChannelResources(state, peer_rank, normalized_options,
                                  acl_stream, &usable_buffer_bytes,
                                  &probe_status, &detail, &error)) {
    *out = MakeIo(probe_status, 0, 0, error);
    return FLUME_OK;
  }
  *out = MakeIo(FLUME_OK, usable_buffer_bytes, 0, detail);
  return FLUME_OK;
#else
  (void)peer_rank;
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCOMM channel resources are unavailable in this build");
  return FLUME_OK;
#endif
}

int flume_hcomm_payload_probe(flume_client_t* client,
                              uint32_t peer_rank,
                              void* acl_stream,
                              flume_io_t** out) {
  flume_hcomm_channel_probe_options_t options = {};
  options.size = sizeof(options);
  options.notify_num = 2;
  options.engine = FLUME_HCOMM_ENGINE_AUTO;
  options.protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  options.require_thread_export = 0;
  return flume_hcomm_payload_probe_ex(client, peer_rank, &options, acl_stream,
                                      out);
}

int flume_hcomm_payload_probe_ex(
    flume_client_t* client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t* options,
    void* acl_stream,
    flume_io_t** out) {
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  HcommProbeOptions normalized_options;
  std::string option_error;
  if (!NormalizeHcommProbeOptions(options, &normalized_options,
                                  &option_error)) {
    (void)option_error;
    return FLUME_ERR_INVALID_ARGUMENT;
  }
#if FLUME_HAVE_HCCL_P2P
  const char* fallback_path = "hccl-p2p";
#else
  const char* fallback_path = "none";
#endif
  if (normalized_options.protocol == FLUME_HCOMM_PROTOCOL_PCIE) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  std::string("HCOMM payload probe does not support pcie "
                              "protocol; fallback=") +
                      fallback_path);
    return FLUME_OK;
  }

  CommState state = SnapshotCommState(client);
  if (state.sim_comm_attached) {
    if (state.rank_size == 0 || peer_rank >= state.rank_size ||
        peer_rank == state.rank) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    (void)acl_stream;
    *out = MakeIo(FLUME_OK, 0, 0,
                  "sim HCOMM payload backend ready; scheduler=sim");
    return FLUME_OK;
  }
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (state.rank_size == 0 || peer_rank >= state.rank_size ||
      peer_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string detail;
  std::string error;
  if (!ProbeHcommChannelResources(state, peer_rank, normalized_options,
                                  acl_stream, &usable_buffer_bytes,
                                  &probe_status, &detail, &error)) {
    *out = MakeIo(probe_status, 0, 0, error);
    return FLUME_OK;
  }
  std::string plan_detail = MakeHcommPayloadPlanDetail(
      state.rank == 0 ? flume::hcomm_payload::PayloadRole::kSend :
                        flume::hcomm_payload::PayloadRole::kRecv,
      state, peer_rank, usable_buffer_bytes == 0 ? 1 : usable_buffer_bytes,
      normalized_options, normalized_options.protocol, detail);
#if FLUME_HAVE_HCOMM_PRIMITIVES
  *out = MakeIo(
      FLUME_ERR_UNSUPPORTED, usable_buffer_bytes, 0,
      std::string("host HCOMM payload base primitive symbols are available; "
                  "channel_fence=") +
          (FLUME_HAVE_HCOMM_CHANNEL_FENCE_ON_THREAD ? "on-thread" :
           (FLUME_HAVE_HCOMM_CHANNEL_FENCE_LEGACY ? "legacy" : "missing")) +
          "; " +
          std::string(
                  "run strict payload smoke to verify the direct ACL "
                  "custom-op payload scheduler; "
                  "fallback=") +
          fallback_path + "; " + plan_detail);
  return FLUME_OK;
#else
  *out = MakeIo(
      FLUME_ERR_UNSUPPORTED, usable_buffer_bytes, 0,
      std::string("host HCOMM payload primitive symbols are unavailable in "
                  "this build; direct ACL payload may still run through an "
                  "installed payload-ready custom-op package; fallback=") +
          fallback_path + "; " + plan_detail);
  return FLUME_OK;
#endif
#else
  (void)peer_rank;
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                std::string("HCOMM channel resources are unavailable in this "
                            "build; fallback=") +
                    fallback_path);
  return FLUME_OK;
#endif
}

int flume_hcomm_custom_op_launch_smoke(flume_client_t* client,
                                       uint32_t peer_rank,
                                       void* acl_stream,
                                       flume_io_t** out) {
  flume_hcomm_channel_probe_options_t options = {};
  options.size = sizeof(options);
  options.notify_num = 2;
  options.engine = FLUME_HCOMM_ENGINE_AUTO;
  options.protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  options.require_thread_export = 0;
  return flume_hcomm_custom_op_launch_smoke_ex(client, peer_rank, &options,
                                               acl_stream, out);
}

int flume_hcomm_custom_op_launch_smoke_ex(
    flume_client_t* client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t* options,
    void* acl_stream,
    flume_io_t** out) {
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  HcommProbeOptions normalized_options;
  std::string option_error;
  if (!NormalizeHcommProbeOptions(options, &normalized_options,
                                  &option_error)) {
    (void)option_error;
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (normalized_options.protocol == FLUME_HCOMM_PROTOCOL_PCIE) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCOMM custom-op launch smoke does not support pcie protocol");
    return FLUME_OK;
  }

  CommState state = SnapshotCommState(client);
  if (state.sim_comm_attached) {
    if (state.rank_size == 0 || peer_rank >= state.rank_size ||
        peer_rank == state.rank) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    (void)acl_stream;
    flume::hcomm_payload::CustomOpLaunchSmokePlan plan;
    std::string error;
    if (!flume::hcomm_payload::BuildCustomOpLaunchSmokePlan(
            state.rank, peer_rank, state.rank_size, &plan, &error)) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    *out = MakeIo(FLUME_OK, 0, 0,
                  std::string("sim custom-op launch smoke passed; "
                              "stage3b1_launch=passed ") +
                      flume::hcomm_payload::DescribeCustomOpLaunchSmokePlan(
                          plan));
    return FLUME_OK;
  }
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (state.rank_size == 0 || peer_rank >= state.rank_size ||
      peer_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string detail;
  std::string error;
  if (!ProbeHcommChannelResources(state, peer_rank, normalized_options,
                                  acl_stream, &usable_buffer_bytes,
                                  &probe_status, &detail, &error)) {
    *out = MakeIo(probe_status, 0, 0, error);
    return FLUME_OK;
  }
  std::string launch_detail =
      MakeHcommCustomOpLaunchSmokeDetail(state, peer_rank, detail);
  flume::hcomm_payload::SchedulerStatus scheduler_status =
      flume::hcomm_payload::CurrentSchedulerStatus();
  if (scheduler_status == flume::hcomm_payload::SchedulerStatus::kReady) {
    *out = MakeIo(FLUME_OK, usable_buffer_bytes, 0,
                  std::string("HCOMM custom-op no-op launch smoke passed; "
                              "stage3b1_launch=passed ") +
                      launch_detail);
    return FLUME_OK;
  }
  *out = MakeIo(
      FLUME_ERR_UNSUPPORTED, usable_buffer_bytes, 0,
      std::string("HCOMM custom-op no-op launch smoke unavailable: ") +
          flume::hcomm_payload::SchedulerStatusMessage(scheduler_status) +
          "; stage3b1_launch=unsupported " + launch_detail);
  return FLUME_OK;
#else
  (void)peer_rank;
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCOMM channel resources are unavailable in this build; "
                "custom-op launch smoke cannot run");
  return FLUME_OK;
#endif
}

int flume_hcomm_resource_descriptor_smoke(flume_client_t* client,
                                          uint32_t peer_rank,
                                          void* acl_stream,
                                          flume_io_t** out) {
  flume_hcomm_channel_probe_options_t options = {};
  options.size = sizeof(options);
  options.notify_num = 2;
  options.engine = FLUME_HCOMM_ENGINE_AUTO;
  options.protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  options.require_thread_export = 0;
  return flume_hcomm_resource_descriptor_smoke_ex(client, peer_rank, &options,
                                                  acl_stream, out);
}

int flume_hcomm_resource_descriptor_smoke_ex(
    flume_client_t* client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t* options,
    void* acl_stream,
    flume_io_t** out) {
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  HcommProbeOptions normalized_options;
  std::string option_error;
  if (!NormalizeHcommProbeOptions(options, &normalized_options,
                                  &option_error)) {
    (void)option_error;
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (normalized_options.protocol == FLUME_HCOMM_PROTOCOL_PCIE) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCOMM resource descriptor smoke does not support pcie "
                  "protocol");
    return FLUME_OK;
  }

  CommState state = SnapshotCommState(client);
  if (state.sim_comm_attached) {
    if (state.rank_size == 0 || peer_rank >= state.rank_size ||
        peer_rank == state.rank) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    (void)acl_stream;
    flume::hcomm_payload::ResourceDescriptor descriptor;
    std::string error;
    if (!flume::hcomm_payload::BuildResourceDescriptor(
            state.rank, peer_rank, state.rank_size, 1,
            normalized_options.notify_num, 4096, 4096,
            normalized_options.require_thread_export, "sim",
            FlumeHcommProtocolName(normalized_options.protocol),
            "sim-desc", &descriptor, &error)) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    *out = MakeIo(FLUME_OK, descriptor.usable_hccl_buffer_bytes, 0,
                  std::string("sim HCOMM resource descriptor smoke passed; ") +
                      flume::hcomm_payload::DescribeResourceDescriptor(
                          descriptor));
    return FLUME_OK;
  }
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (state.rank_size == 0 || peer_rank >= state.rank_size ||
      peer_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string channel_detail;
  std::string error;
  HcommChannelResourceInfo resource_info;
  if (!ProbeHcommChannelResources(state, peer_rank, normalized_options,
                                  acl_stream, &usable_buffer_bytes,
                                  &probe_status, &channel_detail, &error,
                                  &resource_info)) {
    *out = MakeIo(probe_status, 0, 0, error);
    return FLUME_OK;
  }
  std::string descriptor_detail = MakeHcommResourceDescriptorDetail(
      state, peer_rank, resource_info, channel_detail);
  flume::hcomm_payload::SchedulerStatus scheduler_status =
      flume::hcomm_payload::CurrentSchedulerStatus();
  if (scheduler_status == flume::hcomm_payload::SchedulerStatus::kReady) {
    *out = MakeIo(FLUME_OK, usable_buffer_bytes, 0,
                  std::string("HCOMM resource descriptor smoke passed; "
                              "stage3b2_descriptor_handoff=passed ") +
                      descriptor_detail);
    return FLUME_OK;
  }
  *out = MakeIo(
      FLUME_ERR_UNSUPPORTED, usable_buffer_bytes, 0,
      std::string("HCOMM resource descriptor packaged on host, but "
                  "custom-op/AICPU descriptor handoff is missing: ") +
          flume::hcomm_payload::SchedulerStatusMessage(scheduler_status) +
          "; stage3b2_descriptor_handoff=missing " + descriptor_detail);
  return FLUME_OK;
#else
  (void)peer_rank;
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCOMM channel resources are unavailable in this build; "
                "resource descriptor smoke cannot run");
  return FLUME_OK;
#endif
}

int flume_hcomm_notify_only_smoke(flume_client_t* client,
                                  uint32_t peer_rank,
                                  void* acl_stream,
                                  flume_io_t** out) {
  flume_hcomm_channel_probe_options_t options = {};
  options.size = sizeof(options);
  options.notify_num = 2;
  options.engine = FLUME_HCOMM_ENGINE_AUTO;
  options.protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  options.require_thread_export = 0;
  return flume_hcomm_notify_only_smoke_ex(client, peer_rank, &options,
                                          acl_stream, out);
}

int flume_hcomm_aicpu_canary_smoke(flume_client_t* client,
                                   uint32_t peer_rank,
                                   void* acl_stream,
                                   flume_io_t** out) {
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  CommState state = SnapshotCommState(client);
  if (!state.hccl_attached || state.hccl_comm == nullptr ||
      state.rank_size != 2 || peer_rank >= state.rank_size ||
      peer_rank == state.rank || acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
#if FLUME_ENABLE_HCCL
  HcommLauncherDecision launcher = DecideHcommLauncherBackend();
  int status = FLUME_ERR_UNSUPPORTED;
  std::string detail = TryLaunchHcommDirectAclrtCanary(
      state, peer_rank, acl_stream, launcher, &status);
  *out = MakeIo(
      status, 0, 0,
      std::string("HCOMM standalone AICPU canary ") +
          (status == FLUME_OK ? "passed; " : "did not complete; ") + detail);
#else
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCOMM standalone AICPU canary requires an HCCL build");
#endif
  return FLUME_OK;
}

int flume_hcomm_notify_only_smoke_ex(
    flume_client_t* client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t* options,
    void* acl_stream,
    flume_io_t** out) {
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  HcommProbeOptions normalized_options;
  std::string option_error;
  if (!NormalizeHcommProbeOptions(options, &normalized_options,
                                  &option_error)) {
    (void)option_error;
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (normalized_options.protocol == FLUME_HCOMM_PROTOCOL_PCIE) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCOMM notify-only smoke does not support pcie protocol");
    return FLUME_OK;
  }

  CommState state = SnapshotCommState(client);
  flume::hcomm_payload::PayloadRole role =
      state.rank == 0 ? flume::hcomm_payload::PayloadRole::kSend :
                        flume::hcomm_payload::PayloadRole::kRecv;
  if (state.sim_comm_attached) {
    if (state.rank_size == 0 || peer_rank >= state.rank_size ||
        peer_rank == state.rank) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    (void)acl_stream;
    flume::hcomm_payload::NotifyOnlyPlan plan;
    std::string error;
    if (!flume::hcomm_payload::BuildNotifyOnlyPlan(
            role, state.rank, peer_rank, state.rank_size, 0, 1, &plan,
            &error)) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    *out = MakeIo(FLUME_OK, 0, 0,
                  std::string("sim HCOMM notify-only smoke passed; "
                              "stage3b2_kernel_consume=sim-passed ") +
                      flume::hcomm_payload::DescribeNotifyOnlyPlan(plan));
    return FLUME_OK;
  }
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (state.rank_size == 0 || peer_rank >= state.rank_size ||
      peer_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string channel_detail;
  std::string error;
  HcommChannelResourceInfo resource_info;
  if (!ProbeHcommChannelResources(state, peer_rank, normalized_options,
                                  acl_stream, &usable_buffer_bytes,
                                  &probe_status, &channel_detail, &error,
                                  &resource_info)) {
    *out = MakeIo(probe_status, 0, 0, error);
    return FLUME_OK;
  }
  std::string descriptor_detail = MakeHcommResourceDescriptorDetail(
      state, peer_rank, resource_info, channel_detail);
  std::string notify_detail =
      MakeHcommNotifyOnlyDetail(role, state, peer_rank, resource_info,
                                descriptor_detail);
  flume::hcomm_payload::SchedulerStatus scheduler_status =
      flume::hcomm_payload::CurrentSchedulerStatus();

#if FLUME_BUILD_HCOMM_CUSTOM_OP
  HcommProbeOptions launch_options = normalized_options;
  if (launch_options.engine == FLUME_HCOMM_ENGINE_AUTO ||
      launch_options.engine == FLUME_HCOMM_ENGINE_CPU ||
      launch_options.engine == FLUME_HCOMM_ENGINE_CPU_TS) {
    launch_options.engine = FLUME_HCOMM_ENGINE_AICPU_TS;
  }
  HcommChannelResourceInfo launch_resource_info;
  std::string launch_channel_detail;
  std::string launch_error;
  int launch_probe_status = FLUME_ERR_BACKEND;
  size_t launch_usable_buffer_bytes = 0;
  if (!ProbeHcommChannelResources(state, peer_rank, launch_options, acl_stream,
                                  &launch_usable_buffer_bytes,
                                  &launch_probe_status,
                                  &launch_channel_detail, &launch_error,
                                  &launch_resource_info)) {
    *out = MakeIo(
        FLUME_ERR_UNSUPPORTED, usable_buffer_bytes, 0,
        std::string("HCOMM notify-only AICPU_TS launch resources are "
                    "unavailable; stage3b2_kernel_consume=missing; "
                    "stage3b3a_kernel_launch=unsupported reason=\"") +
            launch_error + "\" " + notify_detail);
    return FLUME_OK;
  }
  std::string launch_descriptor_detail = MakeHcommResourceDescriptorDetail(
      state, peer_rank, launch_resource_info, launch_channel_detail);
  std::string launch_notify_detail =
      MakeHcommNotifyOnlyDetail(role, state, peer_rank, launch_resource_info,
                                launch_descriptor_detail);
  int kernel_status = FLUME_ERR_BACKEND;
  std::string kernel_detail;
  if (TryLaunchHcommNotifyOnlyKernel(role, state, peer_rank, acl_stream,
                                     launch_resource_info, &kernel_status,
                                     &kernel_detail)) {
    *out = MakeIo(FLUME_OK, launch_usable_buffer_bytes, 0,
                  std::string("HCOMM notify-only smoke passed; ") +
                      kernel_detail + " " + launch_notify_detail);
    return FLUME_OK;
  }
  *out = MakeIo(
      kernel_status, launch_usable_buffer_bytes, 0,
      std::string("HCOMM notify-only custom-op/AICPU kernel did not complete; "
                  "stage3b2_kernel_consume=missing; ") +
          kernel_detail + " " + launch_notify_detail);
  return FLUME_OK;
#endif

  if (scheduler_status == flume::hcomm_payload::SchedulerStatus::kReady) {
    *out = MakeIo(FLUME_OK, usable_buffer_bytes, 0,
                  std::string("HCOMM notify-only smoke passed; "
                              "stage3b2_kernel_consume=passed ") +
                      notify_detail);
    return FLUME_OK;
  }
  *out = MakeIo(
      FLUME_ERR_UNSUPPORTED, usable_buffer_bytes, 0,
      std::string("HCOMM notify-only custom-op/AICPU kernel is not "
                  "implemented; stage3b2_kernel_consume=missing; ") +
          flume::hcomm_payload::SchedulerStatusMessage(scheduler_status) +
          "; " + notify_detail);
  return FLUME_OK;
#else
  (void)peer_rank;
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCOMM channel resources are unavailable in this build; "
                "notify-only smoke cannot run");
  return FLUME_OK;
#endif
}

int flume_hcomm_payload_send_ex(
    flume_client_t* client,
    flume_buffer_t* src,
    size_t src_offset,
    uint64_t count,
    flume_data_type_t data_type,
    uint32_t dest_rank,
    const flume_hcomm_channel_probe_options_t* payload_options,
    void* acl_stream,
    flume_io_t** out) {
  if (client == nullptr || src == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

  size_t bytes = 0;
  if (!CheckedBytes(count, data_type, &bytes) ||
      !ValidateRange(src, src_offset, bytes) || src->client != client) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (src->type == FLUME_BUFFER_SIM_HBM) {
    (void)acl_stream;
    return SubmitSimHcommPayload(client, SimP2pRole::kSend, src, src_offset,
                                 count, data_type, dest_rank, out);
  }
  if (!IsRealHcclBufferType(src->type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  CommState state = SnapshotCommState(client);
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCOMM payload send requires flume_attach_hccl_comm");
    return FLUME_OK;
  }
  if (state.rank_size != 2) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCOMM payload copy is pair-only and requires exactly two ranks");
    return FLUME_OK;
  }
  if (dest_rank >= state.rank_size || dest_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
  HcommProbeOptions options;
  std::string options_error;
  if (!NormalizeHcommProbeOptions(payload_options, &options, &options_error)) {
    *out = MakeIo(FLUME_ERR_INVALID_ARGUMENT, 0, 0, options_error);
    return FLUME_OK;
  }
  if (options.engine == FLUME_HCOMM_ENGINE_AUTO) {
    options.engine = FLUME_HCOMM_ENGINE_AICPU_TS;
  }
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string detail;
  std::string error;
  HcommChannelResourceInfo resource_info;
  if (!ProbeHcommChannelResources(state, dest_rank, options, acl_stream,
                                  &usable_buffer_bytes, &probe_status,
                                  &detail, &error, &resource_info)) {
    std::string resource_detail = MakeHcommPayloadResourceAcquireFailedDetail(
        flume::hcomm_payload::PayloadRole::kSend, state, dest_rank, bytes,
        probe_status, error);
    *out = MakeIo(
        probe_status, 0, 0,
        std::string("HCOMM payload send custom-op/AICPU resource acquisition "
                    "failed; fallback=") +
            (FLUME_HAVE_HCCL_P2P ? "hccl-p2p; " : "none; ") +
            resource_detail);
    return FLUME_OK;
  }
  std::string plan_detail = MakeHcommPayloadPlanDetail(
      flume::hcomm_payload::PayloadRole::kSend, state, dest_rank, bytes,
      options, resource_info.resolved_protocol, detail);
  int launch_status = FLUME_ERR_BACKEND;
  HcommLauncherDecision launcher = DecideHcommLauncherBackend();
  std::string launch_detail = TryLaunchHcommPayloadCopyDirectAclrt(
      flume::hcomm_payload::PayloadRole::kSend, state, dest_rank, acl_stream,
      static_cast<uint8_t*>(src->ptr) + src_offset, bytes,
      options.disable_payload_batch_mode, options.payload_batch_tag,
      options.payload_recv_direct_output, options.payload_force_channel_fence,
      options.payload_write_path, options.payload_write_with_notify,
      options.payload_comm_binding,
      resource_info, launcher,
      &launch_status);
  *out = MakeIo(
      launch_status, launch_status == FLUME_OK ? bytes : usable_buffer_bytes,
      0,
      std::string(launch_status == FLUME_OK ?
                      "HCOMM payload send completed via custom-op/AICPU; "
                      "fallback=none; " :
                      "HCOMM payload send custom-op/AICPU path unavailable; "
                      "fallback=") +
          (launch_status == FLUME_OK ?
               "" :
               (FLUME_HAVE_HCCL_P2P ? "hccl-p2p; " : "none; ")) +
          launch_detail + "; " + plan_detail);
  return FLUME_OK;
#else
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                std::string("HCOMM channel resources are unavailable in this "
                            "build; fallback=") +
                    (FLUME_HAVE_HCCL_P2P ? "hccl-p2p" : "none"));
  return FLUME_OK;
#endif
}

int flume_hcomm_payload_send_async(flume_client_t* client,
                                  flume_buffer_t* src,
                                  size_t src_offset,
                                  uint64_t count,
                                  flume_data_type_t data_type,
                                  uint32_t dest_rank,
                                  void* acl_stream,
                                  flume_io_t** out) {
  return flume_hcomm_payload_send_ex(client, src, src_offset, count, data_type,
                                    dest_rank, nullptr, acl_stream, out);
}

int flume_hcomm_payload_recv_ex(
    flume_client_t* client,
    flume_buffer_t* dst,
    size_t dst_offset,
    uint64_t count,
    flume_data_type_t data_type,
    uint32_t src_rank,
    const flume_hcomm_channel_probe_options_t* payload_options,
    void* acl_stream,
    flume_io_t** out) {
  if (client == nullptr || dst == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

  size_t bytes = 0;
  if (!CheckedBytes(count, data_type, &bytes) ||
      !ValidateRange(dst, dst_offset, bytes) || dst->client != client) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (dst->type == FLUME_BUFFER_SIM_HBM) {
    (void)acl_stream;
    return SubmitSimHcommPayload(client, SimP2pRole::kRecv, dst, dst_offset,
                                 count, data_type, src_rank, out);
  }
  if (!IsRealHcclBufferType(dst->type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  CommState state = SnapshotCommState(client);
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCOMM payload recv requires flume_attach_hccl_comm");
    return FLUME_OK;
  }
  if (state.rank_size != 2) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCOMM payload copy is pair-only and requires exactly two ranks");
    return FLUME_OK;
  }
  if (src_rank >= state.rank_size || src_rank == state.rank) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
  HcommProbeOptions options;
  std::string options_error;
  if (!NormalizeHcommProbeOptions(payload_options, &options, &options_error)) {
    *out = MakeIo(FLUME_ERR_INVALID_ARGUMENT, 0, 0, options_error);
    return FLUME_OK;
  }
  if (options.engine == FLUME_HCOMM_ENGINE_AUTO) {
    options.engine = FLUME_HCOMM_ENGINE_AICPU_TS;
  }
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string detail;
  std::string error;
  HcommChannelResourceInfo resource_info;
  if (!ProbeHcommChannelResources(state, src_rank, options, acl_stream,
                                  &usable_buffer_bytes, &probe_status,
                                  &detail, &error, &resource_info)) {
    std::string resource_detail = MakeHcommPayloadResourceAcquireFailedDetail(
        flume::hcomm_payload::PayloadRole::kRecv, state, src_rank, bytes,
        probe_status, error);
    *out = MakeIo(
        probe_status, 0, 0,
        std::string("HCOMM payload recv custom-op/AICPU resource acquisition "
                    "failed; fallback=") +
            (FLUME_HAVE_HCCL_P2P ? "hccl-p2p; " : "none; ") +
            resource_detail);
    return FLUME_OK;
  }
  std::string plan_detail = MakeHcommPayloadPlanDetail(
      flume::hcomm_payload::PayloadRole::kRecv, state, src_rank, bytes,
      options, resource_info.resolved_protocol, detail);
  int launch_status = FLUME_ERR_BACKEND;
  HcommLauncherDecision launcher = DecideHcommLauncherBackend();
  std::string launch_detail = TryLaunchHcommPayloadCopyDirectAclrt(
      flume::hcomm_payload::PayloadRole::kRecv, state, src_rank, acl_stream,
      static_cast<uint8_t*>(dst->ptr) + dst_offset, bytes,
      options.disable_payload_batch_mode, options.payload_batch_tag,
      options.payload_recv_direct_output, options.payload_force_channel_fence,
      options.payload_write_path, options.payload_write_with_notify,
      options.payload_comm_binding,
      resource_info, launcher,
      &launch_status);
  *out = MakeIo(
      launch_status, launch_status == FLUME_OK ? bytes : usable_buffer_bytes,
      0,
      std::string(launch_status == FLUME_OK ?
                      "HCOMM payload recv completed via custom-op/AICPU; "
                      "fallback=none; " :
                      "HCOMM payload recv custom-op/AICPU path unavailable; "
                      "fallback=") +
          (launch_status == FLUME_OK ?
               "" :
               (FLUME_HAVE_HCCL_P2P ? "hccl-p2p; " : "none; ")) +
          launch_detail + "; " + plan_detail);
  return FLUME_OK;
#else
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                std::string("HCOMM channel resources are unavailable in this "
                            "build; fallback=") +
                    (FLUME_HAVE_HCCL_P2P ? "hccl-p2p" : "none"));
  return FLUME_OK;
#endif
}

int flume_hcomm_payload_recv_async(flume_client_t* client,
                                  flume_buffer_t* dst,
                                  size_t dst_offset,
                                  uint64_t count,
                                  flume_data_type_t data_type,
                                  uint32_t src_rank,
                                  void* acl_stream,
                                  flume_io_t** out) {
  return flume_hcomm_payload_recv_ex(client, dst, dst_offset, count, data_type,
                                    src_rank, nullptr, acl_stream, out);
}

int flume_prepare_storage_block_async(flume_file_t* file,
                                     uint64_t file_offset,
                                     size_t len,
                                     flume_storage_block_t** out_block,
                                     flume_io_t** out) {
  if (file == nullptr || out_block == nullptr || out == nullptr || len == 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out_block = nullptr;
  *out = nullptr;
  if (file->client == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  auto* block = new flume_storage_block;
  block->client = file->client;
  block->file_offset = file_offset;
  block->payload.assign(len, 0);

  flume_buffer temp;
  temp.client = file->client;
  temp.ptr = block->payload.data();
  temp.len = block->payload.size();
  temp.type = FLUME_BUFFER_SIM_HCCL_COMM;

  flume_io_t* read_io = nullptr;
  int submit_ret = flume_pread_async(file, &temp, len, file_offset, 0, nullptr,
                                     &read_io);
  if (submit_ret != FLUME_OK) {
    delete block;
    return submit_ret;
  }
  int wait_ret = flume_wait(read_io, -1);
  size_t bytes = flume_io_bytes(read_io);
  uint32_t checksum = flume_io_checksum(read_io);
  const char* detail = flume_io_error_message(read_io);
  std::string error = detail == nullptr ? "" : detail;
  (void)flume_io_release(read_io);
  if (wait_ret != FLUME_OK) {
    delete block;
    *out = MakeIo(wait_ret, bytes, checksum, error);
    return FLUME_OK;
  }
  block->payload.resize(bytes);
  *out_block = block;
  *out = MakeIo(FLUME_OK, bytes, checksum,
                "storage_hbm=sim-partial path=file->SIM_HCCL_COMM");
  return FLUME_OK;
}

size_t flume_storage_block_size(flume_storage_block_t* block) {
  return block == nullptr ? 0 : block->payload.size();
}

int flume_storage_block_release(flume_storage_block_t* block) {
  delete block;
  return FLUME_OK;
}

int flume_read_to_hbm_async(flume_client_t* client,
                           flume_storage_block_t* block,
                           flume_buffer_t* dst,
                           size_t dst_offset,
                           void* acl_stream,
                           flume_io_t** out) {
  if (client == nullptr || block == nullptr || dst == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  if (block->client != client || dst->client != client ||
      !ValidateRange(dst, dst_offset, block->payload.size())) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (dst->type == FLUME_BUFFER_SIM_HBM) {
    (void)acl_stream;
    auto* dst_ptr = static_cast<uint8_t*>(dst->ptr) + dst_offset;
    if (!block->payload.empty()) {
      memcpy(dst_ptr, block->payload.data(), block->payload.size());
    }
    *out = MakeIo(FLUME_OK, block->payload.size(),
                  flume::protocol::Checksum32(dst_ptr, block->payload.size()),
                  "storage_hbm=sim-partial path=SIM_HCCL_COMM->SIM_HBM");
    return FLUME_OK;
  }

  if (IsRealHcclBufferType(dst->type)) {
    *out = MakeIo(
        FLUME_ERR_UNSUPPORTED, 0, 0,
        "storage->HBM direct path not implemented; fallback=runtime-staging|none");
    return FLUME_OK;
  }
  return FLUME_ERR_INVALID_ARGUMENT;
}

int flume_register_storage_target_memory(
    flume_client_t* client,
    flume_buffer_t* buffer,
    size_t offset,
    size_t len,
    flume_storage_target_window_t** out) {
  if (client == nullptr || buffer == nullptr || out == nullptr || len == 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  if (buffer->client != client || buffer->type != FLUME_BUFFER_SIM_HBM ||
      !ValidateRange(buffer, offset, len)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  auto* window = new flume_storage_target_window;
  window->client = client;
  window->buffer = buffer;
  window->offset = offset;
  window->len = len;
  window->sim_lkey =
      (static_cast<uint64_t>(offset) << 32) ^ static_cast<uint64_t>(len);
  window->sim_rkey = window->sim_lkey ^ 0x5f1d000000000001ULL;
  RetainPendingBuffer(buffer);
  *out = window;
  return FLUME_OK;
}

int flume_storage_target_window_release(
    flume_storage_target_window_t* window) {
  if (window == nullptr) {
    return FLUME_OK;
  }
  if (window->plan_ref_count.load(std::memory_order_relaxed) != 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  ReleasePendingBuffer(window->buffer);
  delete window;
  return FLUME_OK;
}

int flume_get_storage_transfer_caps(flume_client_t* client,
                                    flume_storage_transfer_caps_t* out) {
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  flume_storage_transfer_caps_t caps = {};
  caps.size = sizeof(flume_storage_transfer_caps_t);
  caps.storage_host_staging = 1;
  caps.future_rdma_hbm = 0;
  caps.roce_storage_protocol = 1;
  caps.roce_storage_native = flume::roce::NativeTransportCompiled() ? 1U : 0U;
  {
    std::lock_guard<std::mutex> lock(client->mu);
    caps.storage_direct_sim = client->sim_comm_attached ? 1 : 0;
    caps.hcomm_payload_sim = client->sim_comm_attached ? 1 : 0;
  }
  caps.default_path = caps.storage_direct_sim ?
                          FLUME_STORAGE_TRANSFER_SIM_DIRECT :
                          FLUME_STORAGE_TRANSFER_HOST_STAGING;
  *out = caps;
  return FLUME_OK;
}

int flume_storage_direct_plan_create(
    flume_client_t* client,
    flume_storage_block_t* block,
    flume_buffer_t* dst,
    size_t dst_offset,
    const flume_storage_direct_options_t* options,
    flume_storage_direct_plan_t** out) {
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

  flume_storage_direct_options_t normalized = {};
  normalized.size = sizeof(flume_storage_direct_options_t);
  normalized.path = FLUME_STORAGE_TRANSFER_AUTO;
  normalized.role = FLUME_STORAGE_DIRECT_ROLE_AUTO;
  normalized.allow_host_staging = 1;
  if (options != nullptr) {
    normalized = *options;
  }

  flume_storage_transfer_path_t path = normalized.path;
  flume_storage_direct_role_t role = normalized.role;
  if (path == FLUME_STORAGE_TRANSFER_AUTO) {
    path = normalized.require_direct ? FLUME_STORAGE_TRANSFER_SIM_DIRECT :
                                      FLUME_STORAGE_TRANSFER_HOST_STAGING;
    std::lock_guard<std::mutex> lock(client->mu);
    if (client->sim_comm_attached) {
      path = FLUME_STORAGE_TRANSFER_SIM_DIRECT;
    }
  }
  if (role == FLUME_STORAGE_DIRECT_ROLE_AUTO) {
    role = path == FLUME_STORAGE_TRANSFER_HOST_STAGING ?
               FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING :
               (block != nullptr ? FLUME_STORAGE_DIRECT_ROLE_SOURCE_SEND :
                                   FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV);
  }
  if (role == FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING) {
    path = FLUME_STORAGE_TRANSFER_HOST_STAGING;
  }
  if (path == FLUME_STORAGE_TRANSFER_HOST_STAGING &&
      role != FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (normalized.require_direct &&
      path != FLUME_STORAGE_TRANSFER_SIM_DIRECT) {
    return FLUME_ERR_UNSUPPORTED;
  }
  if (!normalized.allow_host_staging &&
      path == FLUME_STORAGE_TRANSFER_HOST_STAGING) {
    return FLUME_ERR_UNSUPPORTED;
  }

  size_t bytes = normalized.len;
  if (role == FLUME_STORAGE_DIRECT_ROLE_SOURCE_SEND) {
    if (block == nullptr || block->client != client || dst != nullptr) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    if (bytes == 0) {
      bytes = block->payload.size();
    }
    if (bytes == 0 || bytes > block->payload.size()) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
  } else if (role == FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV) {
    if (dst == nullptr || dst->client != client || block != nullptr) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    if (bytes == 0) {
      if (dst_offset > dst->len) {
        return FLUME_ERR_INVALID_ARGUMENT;
      }
      bytes = dst->len - dst_offset;
    }
    if (bytes == 0 || !ValidateRange(dst, dst_offset, bytes)) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    if (path == FLUME_STORAGE_TRANSFER_SIM_DIRECT) {
      auto* window = normalized.target_window;
      if (window == nullptr || window->client != client ||
          window->buffer != dst) {
        return FLUME_ERR_INVALID_ARGUMENT;
      }
      if (dst_offset < window->offset ||
          bytes > window->len ||
          dst_offset - window->offset > window->len - bytes) {
        return FLUME_ERR_INVALID_ARGUMENT;
      }
    }
  } else if (role == FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING) {
    if (block == nullptr || dst == nullptr || block->client != client ||
        dst->client != client) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    if (bytes == 0) {
      bytes = block->payload.size();
    }
    if (bytes == 0 || bytes > block->payload.size() ||
        !ValidateRange(dst, dst_offset, bytes)) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    path = FLUME_STORAGE_TRANSFER_HOST_STAGING;
  } else {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (path == FLUME_STORAGE_TRANSFER_SIM_DIRECT) {
    std::lock_guard<std::mutex> lock(client->mu);
    if (!client->sim_comm_attached) {
      return FLUME_ERR_UNSUPPORTED;
    }
    if (client->rank_size != 2 || normalized.peer_rank >= client->rank_size ||
        normalized.peer_rank == client->rank) {
      return FLUME_ERR_INVALID_ARGUMENT;
    }
  }
  if (path == FLUME_STORAGE_TRANSFER_SIM_DIRECT &&
      role == FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV &&
      dst->type != FLUME_BUFFER_SIM_HBM) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  auto* plan = new flume_storage_direct_plan;
  plan->client = client;
  plan->block = block;
  plan->dst = dst;
  plan->target_window = normalized.target_window;
  plan->dst_offset = dst_offset;
  plan->path = path;
  plan->role = role;
  plan->peer_rank = normalized.peer_rank;
  plan->require_direct = normalized.require_direct != 0;
  plan->allow_host_staging = normalized.allow_host_staging != 0;
  plan->bytes = bytes;
  if (plan->target_window != nullptr) {
    plan->target_window->plan_ref_count.fetch_add(
        1, std::memory_order_relaxed);
  }
  *out = plan;
  return FLUME_OK;
}

int flume_storage_direct_plan_release(flume_storage_direct_plan_t* plan) {
  if (plan != nullptr && plan->target_window != nullptr) {
    plan->target_window->plan_ref_count.fetch_sub(
        1, std::memory_order_relaxed);
  }
  delete plan;
  return FLUME_OK;
}

void CompleteStorageDirectFromInner(flume_io_t* wrapper,
                                    flume_io_t* inner,
                                    const std::string& marker) {
  int status = flume_wait(inner, -1);
  size_t bytes = flume_io_bytes(inner);
  uint32_t checksum = flume_io_checksum(inner);
  std::string detail = marker;
  const char* inner_error = flume_io_error_message(inner);
  if (inner_error != nullptr && inner_error[0] != '\0') {
    detail += " inner_detail=\"";
    detail += inner_error;
    detail += "\"";
  }
  (void)flume_io_release(inner);
  CompletePendingIo(wrapper, status, bytes, checksum, detail);
}

int flume_read_storage_to_hbm_async(flume_client_t* client,
                                    flume_storage_direct_plan_t* plan,
                                    void* acl_stream,
                                    flume_io_t** out) {
  if (client == nullptr || plan == nullptr || out == nullptr ||
      plan->client != client) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  if (plan->path == FLUME_STORAGE_TRANSFER_HOST_STAGING) {
    if (plan->require_direct || !plan->allow_host_staging) {
      *out = MakeIo(
          FLUME_ERR_UNSUPPORTED, 0, 0,
          "storage_direct_sim=off storage_hbm_path=unsupported "
          "storage_host_payload_copy=not-used hcomm_payload_backend=none "
          "fallback=none reason=strict-direct-only-rejects-host-staging");
      return FLUME_OK;
    }
    flume_io_t* inner = nullptr;
    int ret = flume_read_to_hbm_async(client, plan->block, plan->dst,
                                      plan->dst_offset, acl_stream, &inner);
    if (ret != FLUME_OK) {
      return ret;
    }
    auto* wrapper = MakePendingIo();
    RetainPendingIo(wrapper);
    *out = wrapper;
    std::string marker =
        "storage_direct_sim=off storage_hbm_path=host-staging "
        "storage_host_payload_copy=used hcomm_payload_backend=none "
        "fallback=host-staging completion_notify=local-complete";
    std::thread(CompleteStorageDirectFromInner, wrapper, inner, marker).detach();
    return FLUME_OK;
  }

  if (plan->path != FLUME_STORAGE_TRANSFER_SIM_DIRECT) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "storage_direct_sim=off storage_hbm_path=unsupported "
                  "storage_host_payload_copy=not-used "
                  "hcomm_payload_backend=none fallback=none");
    return FLUME_OK;
  }

  flume_io_t* inner = nullptr;
  int submit_ret = FLUME_OK;
  if (plan->role == FLUME_STORAGE_DIRECT_ROLE_SOURCE_SEND) {
    flume_buffer temp;
    temp.client = client;
    temp.ptr = plan->block->payload.data();
    temp.len = plan->block->payload.size();
    temp.type = FLUME_BUFFER_SIM_HBM;
    submit_ret = flume_hcomm_payload_send_async(
        client, &temp, 0, static_cast<uint64_t>(plan->bytes),
        FLUME_DTYPE_UINT8, plan->peer_rank, acl_stream, &inner);
  } else if (plan->role == FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV) {
    submit_ret = flume_hcomm_payload_recv_async(
        client, plan->dst, plan->dst_offset, static_cast<uint64_t>(plan->bytes),
        FLUME_DTYPE_UINT8, plan->peer_rank, acl_stream, &inner);
  } else {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (submit_ret != FLUME_OK) {
    return submit_ret;
  }

  auto* wrapper = MakePendingIo();
  RetainPendingIo(wrapper);
  *out = wrapper;
  std::ostringstream marker;
  marker << "storage_direct_sim=on storage_hbm_path=sim-direct "
         << "storage_host_payload_copy=not-used hcomm_payload_backend=sim "
         << "storage_fabric=sim-rdma "
         << "storage_memory_registration=sim-hbm-window "
         << "storage_dma_direction=storage-to-hbm "
         << "storage_submit=doorbell storage_completion_queue=sim "
         << "storage_completion_status=success "
         << "storage_notify_order=submit->fabric-write->cq-complete->target-visible "
         << "fallback=none completion_notify=sim-channel "
         << "storage_direct_role=" << StorageDirectRoleName(plan->role)
         << " storage_direct_peer_rank=" << plan->peer_rank
         << " storage_direct_bytes=" << plan->bytes;
  if (plan->target_window != nullptr) {
    marker << " storage_target_window=registered"
           << " storage_target_lkey=" << plan->target_window->sim_lkey
           << " storage_target_rkey=" << plan->target_window->sim_rkey;
  }
  std::thread(CompleteStorageDirectFromInner, wrapper, inner,
              marker.str()).detach();
  return FLUME_OK;
}

int flume_roce_storage_session_open(
    flume_client_t* client,
    const flume_roce_storage_options_t* options,
    flume_roce_storage_session_t** out) {
  if (client == nullptr || options == nullptr || out == nullptr ||
      options->size < offsetof(flume_roce_storage_options_t,
                               npu_physical_device_valid) ||
      options->storage_server == nullptr || options->storage_server[0] == '\0' ||
      options->npu_rnic_ip == nullptr || options->npu_rnic_ip[0] == '\0' ||
      options->bootstrap_port == 0 || options->timeout_ms == 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (options->post_mode != FLUME_ROCE_POST_AUTO &&
      options->post_mode != FLUME_ROCE_POST_HOST_RA &&
      options->post_mode != FLUME_ROCE_POST_AICPU &&
      options->post_mode != FLUME_ROCE_POST_AIV) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (options->control_mode != FLUME_ROCE_CONTROL_TCP &&
      options->control_mode != FLUME_ROCE_CONTROL_NPU_RA) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (options->transfer_mode != FLUME_ROCE_TRANSFER_PUSH &&
      options->transfer_mode != FLUME_ROCE_TRANSFER_PULL) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (options->storage_backend != FLUME_ROCE_STORAGE_MEMORY &&
      options->storage_backend != FLUME_ROCE_STORAGE_POSIX &&
      options->storage_backend != FLUME_ROCE_STORAGE_SPDK) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  auto* session = new flume_roce_storage_session;
  session->client = client;
  session->storage_server = options->storage_server;
  session->npu_rnic_ip = options->npu_rnic_ip;
  session->storage_rnic_ip = options->storage_rnic_ip == nullptr ?
      "" : options->storage_rnic_ip;
  session->npu_device = options->npu_device;
  if (options->size >= sizeof(flume_roce_storage_options_t) &&
      options->npu_physical_device_valid != 0) {
    if (options->npu_physical_device >
        static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
      delete session;
      return FLUME_ERR_INVALID_ARGUMENT;
    }
    session->npu_physical_device =
        static_cast<int32_t>(options->npu_physical_device);
  }
  session->gid_index = options->gid_index;
  if (options->size >=
          offsetof(flume_roce_storage_options_t, path_mtu_bytes) +
              sizeof(options->path_mtu_bytes) &&
      options->path_mtu_bytes != 0) {
    if (!flume::roce::PathMtuFromBytes(options->path_mtu_bytes,
                                       &session->path_mtu)) {
      delete session;
      return FLUME_ERR_INVALID_ARGUMENT;
    }
  }
  session->bootstrap_port = options->bootstrap_port;
  session->timeout_ms = options->timeout_ms;
  session->post_mode = options->post_mode;
  session->control_mode = options->control_mode;
  session->transfer_mode = options->transfer_mode;
  session->storage_backend = options->storage_backend;
  session->require_compute_host_bypass = options->require_compute_host_bypass != 0;
  session->host_ra = std::make_shared<flume::roce::HostRaSession>();
  if (session->post_mode == FLUME_ROCE_POST_AICPU ||
      session->post_mode == FLUME_ROCE_POST_AIV) {
    session->native_open_error = "requested NPU-side post mode is not implemented; use host-ra";
  } else {
    flume::roce::HostRaConfig config;
    config.storage_server = session->storage_server;
    config.npu_rnic_ip = session->npu_rnic_ip;
    config.logical_device = session->npu_device;
    config.physical_device = session->npu_physical_device;
    config.gid_index = session->gid_index;
    config.path_mtu = session->path_mtu;
    config.bootstrap_port = session->bootstrap_port;
    config.timeout_ms = session->timeout_ms;
    config.control_mode = session->control_mode == FLUME_ROCE_CONTROL_TCP ?
        flume::roce::ControlMode::kTcp : flume::roce::ControlMode::kNpuRa;
    config.transfer_mode =
        session->transfer_mode == FLUME_ROCE_TRANSFER_PUSH ?
            flume::roce::TransferMode::kPush :
            flume::roce::TransferMode::kPull;
    session->host_ra->Open(config, &session->native_open_error);
  }
  *out = session;
  return FLUME_OK;
}

int flume_roce_storage_session_close(flume_roce_storage_session_t* session) {
  if (session == nullptr) return FLUME_OK;
  uint64_t expected = flume_roce_storage_session::kIdle;
  if (!session->request_state.compare_exchange_strong(
          expected, flume_roce_storage_session::kClosing,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  std::string error;
  if (session->host_ra != nullptr && session->host_ra->available() &&
      !session->host_ra->Close(&error)) {
    session->request_state.store(flume_roce_storage_session::kIdle,
                                 std::memory_order_release);
    return FLUME_ERR_BACKEND;
  }
  delete session;
  return FLUME_OK;
}

namespace {

const char* RocePostModeName(flume_roce_post_mode_t mode) {
  switch (mode) {
    case FLUME_ROCE_POST_AUTO:
      return "auto";
    case FLUME_ROCE_POST_HOST_RA:
      return "host-ra";
    case FLUME_ROCE_POST_AICPU:
      return "aicpu";
    case FLUME_ROCE_POST_AIV:
      return "aiv";
  }
  return "unknown";
}

int SubmitRoceStorage(flume_roce_storage_session_t* session,
                      flume::roce::Operation operation,
                      uint64_t object_id,
                      uint64_t storage_offset,
                      flume_buffer_t* buffer,
                      size_t buffer_offset,
                      size_t len,
                      void* acl_stream,
                      flume_io_t** out) {
  if (session == nullptr || buffer == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;
  uint64_t expected = flume_roce_storage_session::kIdle;
  if (!session->request_state.compare_exchange_strong(
          expected, flume_roce_storage_session::kActive,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  const auto release_request = [session]() {
    session->request_state.store(flume_roce_storage_session::kIdle,
                                 std::memory_order_release);
  };
  if (buffer->client != session->client || len == 0 ||
      !ValidateRange(buffer, buffer_offset, len)) {
    release_request();
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (buffer->type != FLUME_BUFFER_ASCEND_HBM &&
      buffer->type != FLUME_BUFFER_HCCL_COMM) {
    release_request();
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (session->host_ra == nullptr || !session->host_ra->available()) {
    const bool capability_available = session->host_ra != nullptr &&
                                      session->host_ra->capability_available();
    std::ostringstream marker;
    marker << "storage_hbm_path=roce-direct roce_storage="
           << (capability_available ? "backend-failed" : "unsupported") << " "
           << "roce_protocol=flume-roce-v2 roce_post_mode="
           << RocePostModeName(session->post_mode)
           << " compute_host_payload=not-used fallback=none reason=\""
           << (session->native_open_error.empty() ? flume::roce::NativeTransportReason() :
                                                    session->native_open_error)
           << "\"";
    *out = MakeIo(capability_available ? FLUME_ERR_BACKEND : FLUME_ERR_UNSUPPORTED,
                  0, 0, marker.str());
    release_request();
    return FLUME_OK;
  }
  const uint64_t request_id = session->next_request_id.fetch_add(1, std::memory_order_relaxed);
  auto* io = MakePendingIo();
  RetainPendingIo(io);
  RetainPendingBuffer(buffer);
  *out = io;
  std::shared_ptr<flume::roce::HostRaSession> host_ra = session->host_ra;
  void* npu_address = static_cast<uint8_t*>(buffer->ptr) + buffer_offset;
  try {
    std::thread([session, host_ra, operation, request_id, object_id,
                 storage_offset, npu_address, len, acl_stream, buffer, io]() {
      flume::roce::HostRaResult result;
      std::string error;
      const bool ok = host_ra->SubmitAndWait(operation, request_id, object_id,
                                             storage_offset, npu_address, len,
                                             acl_stream, &result, &error);
      ReleasePendingBuffer(buffer);
      session->request_state.store(flume_roce_storage_session::kIdle,
                                   std::memory_order_release);
      CompletePendingIo(io, ok ? FLUME_OK : FLUME_ERR_BACKEND,
                        ok ? result.bytes : 0, ok ? result.checksum : 0,
                        ok ? result.marker : "Host-RA request failed: " + error);
    }).detach();
  } catch (...) {
    ReleasePendingBuffer(buffer);
    release_request();
    CompletePendingIo(io, FLUME_ERR_BACKEND, 0, 0,
                      "failed to start Host-RA request worker");
  }
  return FLUME_OK;
}

}  // namespace

int flume_roce_storage_read_async(
    flume_roce_storage_session_t* session,
    uint64_t object_id,
    uint64_t storage_offset,
    flume_buffer_t* dst,
    size_t dst_offset,
    size_t len,
    void* acl_stream,
    flume_io_t** out) {
  return SubmitRoceStorage(session, flume::roce::Operation::kRead, object_id,
                           storage_offset, dst, dst_offset, len, acl_stream, out);
}

int flume_roce_storage_write_async(
    flume_roce_storage_session_t* session,
    uint64_t object_id,
    uint64_t storage_offset,
    flume_buffer_t* src,
    size_t src_offset,
    size_t len,
    void* acl_stream,
    flume_io_t** out) {
  return SubmitRoceStorage(session, flume::roce::Operation::kWrite, object_id,
                           storage_offset, src, src_offset, len, acl_stream, out);
}

int flume_swap_in_async(
    flume_roce_storage_session_t* session,
    uint64_t storage_offset,
    flume_buffer_t* dst_hbm,
    size_t dst_offset,
    size_t len,
    void* acl_stream,
    flume_io_t** out) {
  return flume_roce_storage_read_async(session, 0, storage_offset, dst_hbm,
                                       dst_offset, len, acl_stream, out);
}

int flume_swap_out_async(
    flume_roce_storage_session_t* session,
    flume_buffer_t* src_hbm,
    size_t src_offset,
    uint64_t storage_offset,
    size_t len,
    void* acl_stream,
    flume_io_t** out) {
  return flume_roce_storage_write_async(session, 0, storage_offset, src_hbm,
                                        src_offset, len, acl_stream, out);
}

int flume_allreduce_async(flume_client_t* client, flume_buffer_t* dst,
                         size_t dst_offset, flume_buffer_t* src,
                         size_t src_offset, uint64_t count,
                         flume_data_type_t data_type,
                         flume_reduce_op_t op, void* acl_stream,
                         flume_io_t** out) {
  if (client == nullptr || dst == nullptr || src == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

  size_t bytes = 0;
  if (!CheckedBytes(count, data_type, &bytes) ||
      !ValidateRange(src, src_offset, bytes) ||
      !ValidateRange(dst, dst_offset, bytes)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (src->client != client || dst->client != client ||
      !IsCollectiveBufferType(src->type) || !IsCollectiveBufferType(dst->type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (IsSimBufferType(src->type) && IsSimBufferType(dst->type)) {
    return SubmitSimCollective(client, SimCollectiveKind::kAllReduce, dst,
                               dst_offset, src, src_offset, count, data_type,
                               op, out);
  }

  if (!(IsRealHcclBufferType(src->type) && IsRealHcclBufferType(dst->type))) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "allreduce requires both buffers to be simulated or Ascend HBM");
    return FLUME_OK;
  }

#if FLUME_ENABLE_HCCL
  CommState state = SnapshotCommState(client);
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCCL allreduce requires flume_attach_hccl_comm");
    return FLUME_OK;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  HcclDataType hccl_data_type;
  HcclReduceOp hccl_op;
  if (!ToHcclDataType(data_type, &hccl_data_type) ||
      !ToHcclReduceOp(op, &hccl_op)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  auto* send_ptr = static_cast<uint8_t*>(src->ptr) + src_offset;
  auto* recv_ptr = static_cast<uint8_t*>(dst->ptr) + dst_offset;
  HcclResult result =
      HcclAllReduce(send_ptr, recv_ptr, count, hccl_data_type, hccl_op,
                    static_cast<HcclComm>(state.hccl_comm),
                    static_cast<aclrtStream>(acl_stream));
  if (result != HCCL_SUCCESS) {
    *out = MakeIo(FLUME_ERR_BACKEND, 0, 0, HcclErrorMessage(result));
    return FLUME_OK;
  }

  *out = MakeAclStreamIo(acl_stream, bytes);
  return FLUME_OK;
#else
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCCL allreduce is unavailable in this build");
  return FLUME_OK;
#endif
}

int flume_allgather_async(flume_client_t* client, flume_buffer_t* dst,
                         size_t dst_offset, flume_buffer_t* src,
                         size_t src_offset, uint64_t send_count,
                         flume_data_type_t data_type, void* acl_stream,
                         flume_io_t** out) {
  if (client == nullptr || dst == nullptr || src == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

  size_t send_bytes = 0;
  if (!CheckedBytes(send_count, data_type, &send_bytes)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  CommState state = SnapshotCommState(client);
  uint32_t rank_size = state.rank_size;
  if (rank_size == 0) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "allgather requires an attached HCCL or sim communicator");
    return FLUME_OK;
  }
  size_t recv_bytes = 0;
  if (!CheckedMul(send_bytes, rank_size, &recv_bytes) ||
      !ValidateRange(src, src_offset, send_bytes) ||
      !ValidateRange(dst, dst_offset, recv_bytes)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  if (src->client != client || dst->client != client ||
      !IsCollectiveBufferType(src->type) || !IsCollectiveBufferType(dst->type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  if (IsSimBufferType(src->type) && IsSimBufferType(dst->type)) {
    return SubmitSimCollective(client, SimCollectiveKind::kAllGather, dst,
                               dst_offset, src, src_offset, send_count,
                               data_type, FLUME_REDUCE_SUM, out);
  }

  if (!(IsRealHcclBufferType(src->type) && IsRealHcclBufferType(dst->type))) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "allgather requires both buffers to be simulated or Ascend HBM");
    return FLUME_OK;
  }

#if FLUME_ENABLE_HCCL
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                  "HCCL allgather requires flume_attach_hccl_comm");
    return FLUME_OK;
  }
  if (acl_stream == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  HcclDataType hccl_data_type;
  if (!ToHcclDataType(data_type, &hccl_data_type)) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }

  auto* send_ptr = static_cast<uint8_t*>(src->ptr) + src_offset;
  auto* recv_ptr = static_cast<uint8_t*>(dst->ptr) + dst_offset;
  HcclResult result =
      HcclAllGather(send_ptr, recv_ptr, send_count, hccl_data_type,
                    static_cast<HcclComm>(state.hccl_comm),
                    static_cast<aclrtStream>(acl_stream));
  if (result != HCCL_SUCCESS) {
    *out = MakeIo(FLUME_ERR_BACKEND, 0, 0, HcclErrorMessage(result));
    return FLUME_OK;
  }

  *out = MakeAclStreamIo(acl_stream, recv_bytes);
  return FLUME_OK;
#else
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCCL allgather is unavailable in this build");
  return FLUME_OK;
#endif
}

int flume_wait(flume_io_t* io, int timeout_ms) {
  if (io == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
#if FLUME_ENABLE_HCCL
  bool sync_acl_stream = false;
  void* acl_stream = nullptr;
  size_t bytes = 0;
  {
    std::unique_lock<std::mutex> lock(io->mu);
    if (io->sync_acl_stream && !io->done) {
      if (timeout_ms == 0) {
        return FLUME_ERR_TIMEOUT;
      }
      if (!io->sync_started) {
        io->sync_started = true;
        sync_acl_stream = true;
        acl_stream = io->acl_stream;
        bytes = io->bytes;
      }
    }
  }
  if (sync_acl_stream) {
#if FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT
    int32_t acl_timeout = timeout_ms < 0 ? -1 : timeout_ms;
    aclError ret = aclrtSynchronizeStreamWithTimeout(
        static_cast<aclrtStream>(acl_stream), acl_timeout);
    if (IsAclStreamSyncTimeout(ret)) {
      std::lock_guard<std::mutex> lock(io->mu);
      io->sync_started = false;
      return FLUME_ERR_TIMEOUT;
    }
#else
    aclError ret = aclrtSynchronizeStream(static_cast<aclrtStream>(acl_stream));
#endif
    CompleteIo(io, ret == ACL_SUCCESS ? FLUME_OK : FLUME_ERR_BACKEND, bytes, 0,
               ret == ACL_SUCCESS ? "" : "aclrtSynchronizeStream failed");
  }
#endif
  std::unique_lock<std::mutex> lock(io->mu);
  if (!io->done) {
    if (timeout_ms < 0) {
      io->cv.wait(lock, [&] { return io->done; });
    } else if (timeout_ms == 0) {
      return FLUME_ERR_TIMEOUT;
    } else if (!io->cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                [&] { return io->done; })) {
      return FLUME_ERR_TIMEOUT;
    }
  }
  return io->status;
}

int flume_io_status(flume_io_t* io) {
  if (io == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  std::lock_guard<std::mutex> lock(io->mu);
  return io->status;
}

size_t flume_io_bytes(flume_io_t* io) {
  if (io == nullptr) {
    return 0;
  }
  std::lock_guard<std::mutex> lock(io->mu);
  return io->bytes;
}

uint32_t flume_io_checksum(flume_io_t* io) {
  if (io == nullptr) {
    return 0;
  }
  std::lock_guard<std::mutex> lock(io->mu);
  return io->checksum;
}

const char* flume_io_error_message(flume_io_t* io) {
  thread_local std::string message;
  if (io == nullptr) {
    message = "io handle is null";
    return message.c_str();
  }
  std::lock_guard<std::mutex> lock(io->mu);
  message = io->error;
  return message.c_str();
}

int flume_io_release(flume_io_t* io) {
  if (io == nullptr) {
    return FLUME_OK;
  }
  if (io->pending_ref_count.load(std::memory_order_relaxed) != 0) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  delete io;
  return FLUME_OK;
}
