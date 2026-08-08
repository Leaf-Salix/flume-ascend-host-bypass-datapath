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
#ifndef FLUME_HAVE_HCOMM_RANK_GRAPH
#define FLUME_HAVE_HCOMM_RANK_GRAPH 0
#endif
#ifndef FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH
#define FLUME_HAVE_HCCL_AICPU_KERNEL_LAUNCH 0
#endif
#ifndef FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH
#define FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH 0
#endif
#ifndef FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT
#define FLUME_HAVE_ACL_SYNC_STREAM_TIMEOUT 0
#endif

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
#if __has_include(<hccl/hccl_res.h>)
#include <hccl/hccl_res.h>
#else
#error "FLUME_HAVE_HCOMM_CHANNEL_RES=1 requires hccl/hccl_res.h"
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
#else
#error "FLUME_HAVE_HCOMM_THREAD_EXPORT=1 requires hccl/hccl_res_expt.h"
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
  std::atomic<size_t> sim_pending_ref_count{0};
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
    buffer->sim_pending_ref_count.fetch_add(1, std::memory_order_relaxed);
  }
}

void ReleasePendingBuffer(flume_buffer_t* buffer) {
  if (buffer != nullptr) {
    buffer->sim_pending_ref_count.fetch_sub(1, std::memory_order_relaxed);
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
    const std::string& channel_detail) {
  flume::hcomm_payload::PayloadPlan plan;
  std::string error;
  if (!flume::hcomm_payload::BuildPairCopyPlan(
          role, state.rank, peer_rank, state.rank_size, bytes, &plan, &error)) {
    return std::string("stage3b_plan=invalid error=\"") + error +
           "\" channel_detail=\"" + channel_detail + "\"";
  }
  return flume::hcomm_payload::DescribePlan(plan) + " channel_detail=\"" +
         channel_detail + "\"";
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
const char kFlumeHcommPayloadBatchTag[] = "";

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
      resource_info.resolved_protocol == FLUME_HCOMM_PROTOCOL_ROCE ?
          FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN :
          FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY;
  desc->aicpu_thread = resource_info.aicpu_ts_thread;
  desc->channel_handle = resource_info.channel_handle;
  desc->user_buffer = reinterpret_cast<uint64_t>(user_buffer);
  desc->local_hccl_buffer =
      reinterpret_cast<uint64_t>(resource_info.local_buffer);
  desc->remote_hccl_buffer =
      reinterpret_cast<uint64_t>(resource_info.remote_buffer);
  desc->local_hccl_buffer_bytes = resource_info.local_buffer_bytes;
  desc->remote_hccl_buffer_bytes = resource_info.remote_buffer_bytes;
  // Empty tag still enables HCOMM temporary batch mode. HCOMM docs note that
  // non-empty tag caching is not fully supported on AICPU+TS.
  static_assert(sizeof(kFlumeHcommPayloadBatchTag) <=
                    FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES,
                "Flume HCOMM payload batch tag exceeds descriptor field");
  memcpy(desc->batch_tag, kFlumeHcommPayloadBatchTag,
         sizeof(kFlumeHcommPayloadBatchTag));
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
  const char* required[] = {
      FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V4_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_FUNC,
      FLUME_HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE_FUNC,
      FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION_FUNC,
      FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT_FUNC,
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
  if (explicit_json != nullptr && explicit_json[0] != '\0') {
    probe.installed = FileExists(explicit_json);
    probe.vendor = "explicit";
    probe.json_path = explicit_json;
    probe.aicpu_tar_path = FindAicpuTarForJson(probe.json_path);
    probe.aicpu_tar_present = FileExists(probe.aicpu_tar_path);
    probe.source = probe.installed ? "explicit-json" : "explicit-json-missing";
    if (probe.installed) {
      probe.payload_ready =
          JsonLooksPayloadReady(ReadTextFile(probe.json_path),
                                &probe.payload_reason);
      if (probe.payload_ready && !probe.aicpu_tar_present) {
        probe.payload_ready = false;
        probe.payload_reason = "payload AICPU tar missing beside JSON";
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
        probe.source = "root-scan";
        probe.payload_ready =
            JsonLooksPayloadReady(ReadTextFile(probe.json_path),
                                  &probe.payload_reason);
        if (probe.payload_ready && !probe.aicpu_tar_present) {
          probe.payload_ready = false;
          probe.payload_reason = "payload AICPU tar missing in OPP layout";
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
    default:
      return std::string("unknown-") + std::to_string(status);
  }
}

const char* PayloadRoleName(flume::hcomm_payload::PayloadRole role) {
  return role == flume::hcomm_payload::PayloadRole::kSend ? "send" : "recv";
}

uint64_t PayloadEchoBytes(const uint32_t* status_words) {
  return static_cast<uint64_t>(status_words[4]) |
         (static_cast<uint64_t>(status_words[5]) << 32U);
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

std::string PayloadDescriptorDetail(
    const flume_hcomm_payload_copy_desc_v1& desc) {
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
         " payload_desc_timeout_sec=" + std::to_string(desc.timeout_sec) +
         " payload_desc_status_schema=v" +
         std::to_string(desc.status_schema_version) +
         " payload_desc_status_word_count=" +
         std::to_string(desc.status_word_count) +
         " payload_desc_local_hccl_buffer_bytes=" +
         std::to_string(desc.local_hccl_buffer_bytes) +
         " payload_desc_remote_hccl_buffer_bytes=" +
         std::to_string(desc.remote_hccl_buffer_bytes);
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
         std::to_string(status_words[7]);
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
    const HcommChannelResourceInfo& resource_info) {
  std::string detail = resource_info.host_thread_notify_ready ?
      " payload_thread_notify=host-aicpu" :
      " payload_thread_notify=unavailable";
  detail += std::string(" payload_sync_api=") + AclStreamSyncApiName();
  detail += " payload_sync_timeout_sec=" +
      std::to_string(resource_info.timeout_sec);
  detail += resource_info.resolved_protocol == FLUME_HCOMM_PROTOCOL_ROCE ?
      " payload_completion_mode=channel-fence" :
      " payload_completion_mode=ordered-notify";
  detail += resource_info.host_thread_notify_ready ?
      " payload_completion=thread-notify+stream-sync+status-word" :
      " payload_completion=stream-sync+status-word";
  detail += resource_info.host_thread_notify_ready ?
      " payload_thread_notify_order=batch-end-before-host-notify" :
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

#if FLUME_BUILD_HCOMM_CUSTOM_OP && FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH
std::string TryLaunchHcommPayloadCopyDirectAclrt(
    flume::hcomm_payload::PayloadRole role,
    const CommState& state,
    uint32_t peer_rank,
    void* acl_stream,
    void* user_buffer,
    uint64_t bytes,
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

  char comm_name[FLUME_HCOMM_PAYLOAD_COMM_NAME_BYTES] = {};
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
                           bytes, comm_name, &desc);
  desc.status_word = reinterpret_cast<uint64_t>(kernel_status_dev);
  desc.status_word_count = FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT;
  desc.status_schema_version = FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION;

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
           "\"" + PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle abi_func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V4_FUNC,
      &abi_func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3e_payload_copy=unsupported "
                       "stage3b3e_direct_aclrt_payload_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3e_payload_descriptor_handoff=blocked "
           "stage3b3e_direct_aclrt_payload_launch=not-attempted "
           "payload_abi=v4-missing kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_ABI_VERSION_V4_FUNC +
           PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle semantic_func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_FUNC,
      &semantic_func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3e_payload_copy=unsupported "
                       "stage3b3e_direct_aclrt_payload_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3e_payload_descriptor_handoff=blocked "
           "stage3b3e_direct_aclrt_payload_launch=not-attempted "
           "payload_semantic=missing kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_FUNC +
           PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle semantic_v5_func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5_FUNC,
      &semantic_v5_func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3e_payload_copy=unsupported "
                       "stage3b3e_direct_aclrt_payload_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3e_payload_descriptor_handoff=blocked "
           "stage3b3e_direct_aclrt_payload_launch=not-attempted "
           "payload_semantic=present payload_semantic_v5=missing kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5_FUNC +
           PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle comm_acquire_func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE_FUNC,
      &comm_acquire_func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3e_payload_copy=unsupported "
                       "stage3b3e_direct_aclrt_payload_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3e_payload_descriptor_handoff=blocked "
           "stage3b3e_direct_aclrt_payload_launch=not-attempted "
           "payload_requires_comm_acquire=missing kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE_FUNC +
           PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle status_schema_func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION_FUNC,
      &status_schema_func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3e_payload_copy=unsupported "
                       "stage3b3e_direct_aclrt_payload_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3e_payload_descriptor_handoff=blocked "
           "stage3b3e_direct_aclrt_payload_launch=not-attempted "
           "payload_status_schema_marker=missing kernel_func=" +
           FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION_FUNC +
           PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle status_word_count_func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT_FUNC,
      &status_word_count_func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3e_payload_copy=unsupported "
                       "stage3b3e_direct_aclrt_payload_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3e_payload_descriptor_handoff=blocked "
           "stage3b3e_direct_aclrt_payload_launch=not-attempted "
           "payload_status_word_count_marker=missing kernel_func=" +
           FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT_FUNC +
           PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle internal_payload_func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_PAYLOAD_BUILD_MODE_INTERNAL_PAYLOAD_FUNC,
      &internal_payload_func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3e_payload_copy=unsupported "
                       "stage3b3e_direct_aclrt_payload_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3e_payload_descriptor_handoff=blocked "
           "stage3b3e_direct_aclrt_payload_launch=not-attempted "
           "payload_build_mode=not-internal kernel_func=" +
           FLUME_HCOMM_PAYLOAD_BUILD_MODE_INTERNAL_PAYLOAD_FUNC +
           PayloadDescriptorDetail(desc) +
           " custom_op_package=present" + HcommPackageDetail(decision);
  }

  aclrtFuncHandle func_handle = nullptr;
  acl_ret = aclrtBinaryGetFunction(
      bin_handle, FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC,
      &func_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_UNSUPPORTED;
    return std::string("stage3b3e_payload_copy=unsupported "
                       "stage3b3e_direct_aclrt_payload_loader=unsupported "
                       "api=aclrtBinaryGetFunction error=\"") +
           AclErrorMessage(acl_ret) +
           "\" stage3b3e_payload_descriptor_handoff=blocked "
           "stage3b3e_direct_aclrt_payload_launch=not-attempted kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           " payload_kernel=missing" + PayloadDescriptorDetail(desc) +
           " custom_op_package=present" +
           HcommPackageDetail(decision);
  }

  aclrtArgsHandle args_handle = nullptr;
  acl_ret = aclrtKernelArgsInit(func_handle, &args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtKernelArgsInit error=\"") +
           AclErrorMessage(acl_ret) + "\"" + PayloadDescriptorDetail(desc);
  }

  aclrtParamHandle param_handle = nullptr;
  acl_ret =
      aclrtKernelArgsAppend(args_handle, &desc, sizeof(desc), &param_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtKernelArgsAppend error=\"") +
           AclErrorMessage(acl_ret) + "\"" + PayloadDescriptorDetail(desc);
  }

  acl_ret = aclrtKernelArgsFinalize(args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=failed "
                       "api=aclrtKernelArgsFinalize error=\"") +
           AclErrorMessage(acl_ret) + "\"" + PayloadDescriptorDetail(desc);
  }

#if FLUME_HAVE_HCOMM_PRIMITIVES
  if (resource_info.host_thread_notify_ready) {
    int32_t notify_ret = HcommThreadNotifyRecordOnThread(
        static_cast<ThreadHandle>(resource_info.cpu_ts_thread),
        static_cast<ThreadHandle>(resource_info.aicpu_thread_on_cpu), 0);
    if (notify_ret != 0) {
      (void)aclrtBinaryUnLoad(bin_handle);
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
             PayloadDescriptorDetail(desc);
    }
  }
#endif

  aclrtLaunchKernelAttr attr = {};
  attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
  attr.value.timeout = desc.timeout_sec;
  aclrtLaunchKernelCfg cfg = {};
  cfg.attrs = &attr;
  cfg.numAttrs = 1;
  acl_ret = aclrtLaunchKernelWithConfig(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, args_handle,
      nullptr);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=failed "
                       "api=aclrtLaunchKernelWithConfig error=\"") +
           AclErrorMessage(acl_ret) + "\" kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           PayloadDescriptorDetail(desc);
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
      (void)aclrtBinaryUnLoad(bin_handle);
      (void)aclrtFree(kernel_status_dev);
      *status = FLUME_ERR_BACKEND;
      return std::string("stage3b3e_payload_copy=failed "
                         "stage3b3e_direct_aclrt_payload_loader=passed "
                         "stage3b3e_payload_descriptor_handoff=passed "
                         "stage3b3e_direct_aclrt_payload_launch=passed "
                         "stage3b3e_payload_sync=failed "
                         "payload_thread_notify=host-aicpu "
                         "api=HcommThreadNotifyWaitOnThread hcomm_ret=") +
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
             PayloadEchoWordsDetail(observed_status_words) +
             " kernel_func=" +
             FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
             PayloadDescriptorDetail(desc);
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
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=failed "
                       "api=") + AclStreamSyncApiName() + " error=\"" +
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
           PayloadEchoWordsDetail(observed_status_words) +
           " kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           PayloadDescriptorDetail(desc);
  }

  acl_ret = aclrtMemcpy(kernel_status_words, sizeof(kernel_status_words),
                        kernel_status_dev, sizeof(kernel_status_words),
                        ACL_MEMCPY_DEVICE_TO_HOST);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtBinaryUnLoad(bin_handle);
    (void)aclrtFree(kernel_status_dev);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=failed "
                       "api=aclrtMemcpy(payload_status_d2h) error=\"") +
           AclErrorMessage(acl_ret) + "\" kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           PayloadDescriptorDetail(desc);
  }

  (void)aclrtBinaryUnLoad(bin_handle);
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
           "payload_batch_mode=on payload_kernel_status=") +
           PayloadKernelStatusName(kernel_status) +
           " payload_failure_step=" +
           PayloadFailureStepName(kernel_status) +
           " payload_status_word=" +
           std::to_string(kernel_status) +
           " payload_kernel_hcomm_ret=" +
           std::to_string(kernel_hcomm_ret) +
           " payload_echo=observed" + PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(kernel_status_words) +
           PayloadEchoWordsDetail(kernel_status_words) + " kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           PayloadDescriptorDetail(desc) +
           HcommPayloadCompletionDetail(resource_info);
  }
  if (kernel_hcomm_ret != 0) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=passed "
                     "payload_batch_mode=on payload_kernel_status=success "
                     "payload_failure_step=none "
                     "payload_status_word=0 payload_kernel_hcomm_ret=") +
           std::to_string(kernel_hcomm_ret) +
           " payload_echo=observed" + PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(kernel_status_words) +
           PayloadEchoWordsDetail(kernel_status_words) + " kernel_func=" +
           FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           PayloadDescriptorDetail(desc) +
           HcommPayloadCompletionDetail(resource_info);
  }
  const uint32_t expected_role =
      role == flume::hcomm_payload::PayloadRole::kSend ?
          FLUME_HCOMM_NOTIFY_ROLE_SEND :
          FLUME_HCOMM_NOTIFY_ROLE_RECV;
  const uint32_t expected_completion_mode =
      resource_info.resolved_protocol == FLUME_HCOMM_PROTOCOL_ROCE ?
          FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN :
          FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY;
  if (kernel_status_words[2] != expected_role ||
      kernel_status_words[3] != peer_rank || echo_bytes != bytes ||
      kernel_status_words[6] != state.rank ||
      kernel_status_words[7] != expected_completion_mode) {
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3e_payload_copy=failed "
                       "stage3b3e_direct_aclrt_payload_loader=passed "
                       "stage3b3e_payload_descriptor_handoff=passed "
                       "stage3b3e_direct_aclrt_payload_launch=passed "
                       "stage3b3e_payload_sync=passed "
                     "payload_batch_mode=on payload_kernel_status=success "
                     "payload_failure_step=none "
                     "payload_status_word=0 payload_kernel_hcomm_ret=0 "
                       "payload_echo=failed") +
           PayloadStatusSchemaDetail() +
           PayloadPrimitiveStateDetail(kernel_status_words) +
           PayloadEchoWordsDetail(kernel_status_words) +
           " expected_role=" + std::to_string(expected_role) +
           " expected_peer_rank=" + std::to_string(peer_rank) +
           " expected_bytes=" + std::to_string(bytes) +
           " expected_local_rank=" + std::to_string(state.rank) +
           " expected_completion_mode=" +
           std::to_string(expected_completion_mode) +
           " kernel_func=" + FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
           PayloadDescriptorDetail(desc) +
           HcommPayloadCompletionDetail(resource_info);
  }
  *status = FLUME_OK;
  return std::string("stage3b3e_payload_copy=passed "
                     "stage3b3e_direct_aclrt_payload_loader=passed "
                     "stage3b3e_payload_descriptor_handoff=passed "
                     "stage3b3e_direct_aclrt_payload_launch=passed "
                     "stage3b3e_payload_sync=passed "
                     "payload_batch_mode=on payload_kernel_status=success "
                     "payload_failure_step=none "
                     "payload_status_word=0 "
                     "payload_kernel_hcomm_ret=") +
         std::to_string(kernel_hcomm_ret) + " " +
         "payload_echo=passed payload_role=" + PayloadRoleName(role) +
         PayloadStatusSchemaDetail() +
         PayloadPrimitiveStateDetail(kernel_status_words) +
         PayloadEchoWordsDetail(kernel_status_words) + " " +
         "kernel_func=" +
         FLUME_HCOMM_PAYLOAD_COPY_DIRECT_ACLRT_KERNEL_FUNC +
         PayloadDescriptorDetail(desc) +
         HcommPayloadCompletionDetail(resource_info);
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

  aclrtArgsHandle args_handle = nullptr;
  acl_ret = aclrtKernelArgsInit(func_handle, &args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(canary_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3d_no_internal_headers=on "
                       "stage3b3d_direct_aclrt_canary_loader=passed "
                       "stage3b3d_direct_aclrt_canary_handoff=failed "
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
                       "api=aclrtKernelArgsFinalize error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }

  aclrtLaunchKernelAttr attr = {};
  attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
  attr.value.timeout = kDefaultHcommTimeoutSeconds;
  aclrtLaunchKernelCfg cfg = {};
  cfg.attrs = &attr;
  cfg.numAttrs = 1;
  acl_ret = aclrtLaunchKernelWithConfig(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, args_handle,
      nullptr);
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
                         "api=") + AclStreamSyncApiName() + " error=\"" +
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
                         "api=aclrtMemcpy(canary_status_d2h) error=\"") +
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
                         "canary_status_word=") +
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
                       "canary_status_word=0 canary_observed_token=") +
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
                     "api=aclrtLaunchKernelWithConfig error=\"") +
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

  aclrtArgsHandle args_handle = nullptr;
  acl_ret = aclrtKernelArgsInit(func_handle, &args_handle);
  if (acl_ret != ACL_SUCCESS) {
    (void)aclrtFree(notify_status_dev);
    (void)aclrtBinaryUnLoad(bin_handle);
    *status = FLUME_ERR_BACKEND;
    return std::string("stage3b3c_direct_aclrt_loader=passed "
                       "stage3b3c_descriptor_handoff=failed "
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
                       "api=aclrtKernelArgsFinalize error=\"") +
           AclErrorMessage(acl_ret) + "\"";
  }

  aclrtLaunchKernelAttr attr = {};
  attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
  attr.value.timeout = desc.timeout_sec;
  aclrtLaunchKernelCfg cfg = {};
  cfg.attrs = &attr;
  cfg.numAttrs = 1;
  acl_ret = aclrtLaunchKernelWithConfig(
      func_handle, 1, static_cast<aclrtStream>(acl_stream), &cfg, args_handle,
      nullptr);
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
                         "api=") + AclStreamSyncApiName() + " error=\"" +
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
                         "api=aclrtMemcpy(notify_status_d2h) error=\"") +
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
                         "notify_kernel_status=") +
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
                       "notify_kernel_status=success "
                       "notify_status_word=0 kernel_func=") +
           FLUME_HCOMM_NOTIFY_ONLY_DIRECT_ACLRT_KERNEL_FUNC;
  }

  (void)aclrtFree(notify_status_dev);
  (void)aclrtBinaryUnLoad(bin_handle);
  *status = FLUME_ERR_BACKEND;
  return std::string("stage3b3c_direct_aclrt_loader=passed "
                     "stage3b3c_descriptor_handoff=passed "
                     "stage3b3c_direct_aclrt_launch=failed "
                     "api=aclrtLaunchKernelWithConfig error=\"") +
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
    const HcommChannelResourceInfo& resource_info,
    const HcommLauncherDecision& decision,
    int* status) {
  (void)role;
  (void)state;
  (void)peer_rank;
  (void)acl_stream;
  (void)user_buffer;
  (void)bytes;
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
    int direct_status = FLUME_ERR_UNSUPPORTED;
    std::string direct_detail = TryLaunchHcommNotifyOnlyDirectAclrt(
        role, state, peer_rank, acl_stream, resource_info, launcher,
        &direct_status);
    int canary_status = FLUME_ERR_UNSUPPORTED;
    std::string canary_detail = TryLaunchHcommDirectAclrtCanary(
        state, peer_rank, acl_stream, launcher, &canary_status);
    if (direct_status == FLUME_OK) {
      *status = FLUME_OK;
      *detail = std::string("stage3b3a_kernel_launch=passed ") +
                router_detail + " " + direct_detail + " " + canary_detail;
      return true;
    }
    if (direct_status != FLUME_ERR_UNSUPPORTED) {
      *status = direct_status;
    } else if (canary_status != FLUME_OK &&
               canary_status != FLUME_ERR_UNSUPPORTED) {
      *status = canary_status;
    } else {
      *status = FLUME_ERR_UNSUPPORTED;
    }
    *detail = std::string("stage3b3a_kernel_launch=") +
              (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
              " " + router_detail + " " + direct_detail + " " +
              canary_detail;
    return false;
  }
  if (launcher.backend == HcommLauncherBackend::kDirectAclrtPending) {
    std::string direct_detail = TryLaunchHcommNotifyOnlyDirectAclrt(
        role, state, peer_rank, acl_stream, resource_info, launcher, status);
    int canary_status = FLUME_ERR_UNSUPPORTED;
    std::string canary_detail = TryLaunchHcommDirectAclrtCanary(
        state, peer_rank, acl_stream, launcher, &canary_status);
    if (*status == FLUME_OK) {
      *detail = std::string("stage3b3a_kernel_launch=passed ") +
                router_detail + " " + direct_detail + " " + canary_detail;
      return true;
    }
    if (*status == FLUME_ERR_UNSUPPORTED && canary_status != FLUME_OK &&
        canary_status != FLUME_ERR_UNSUPPORTED) {
      *status = canary_status;
    }
    *detail = std::string("stage3b3a_kernel_launch=") +
              (*status == FLUME_ERR_UNSUPPORTED ? "unsupported" : "failed") +
              " " + router_detail + " " + direct_detail + " " +
              canary_detail;
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
      ((FLUME_HAVE_HCOMM_CHANNEL_RES && FLUME_HAVE_HCOMM_PRIMITIVES) ||
       sim_attached) ? 1U : 0U;
  caps.hcomm_payload_scheduler = sim_attached ? 1U : 0U;
  caps.storage_hbm = sim_attached ? 1U : 0U;
  caps.fallback_hccl_p2p = (FLUME_HAVE_HCCL_P2P || sim_attached) ? 1U : 0U;
  caps.fallback_runtime_staging = (!sim_attached && hccl_attached) ? 1U : 0U;
  caps.hcomm_payload_direct_aclrt =
      (FLUME_BUILD_HCOMM_CUSTOM_OP && FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH) ? 1U : 0U;
  caps.hcomm_payload_thread_notify =
      (FLUME_HAVE_HCOMM_THREAD_EXPORT && FLUME_HAVE_HCOMM_PRIMITIVES) ? 1U : 0U;
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
      buffer->sim_pending_ref_count.load(std::memory_order_relaxed) != 0) {
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
      detail);
#if FLUME_HAVE_HCOMM_PRIMITIVES
  *out = MakeIo(
      FLUME_ERR_UNSUPPORTED, usable_buffer_bytes, 0,
      std::string("host HCOMM payload primitive symbols are available; "
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
  options.engine = FLUME_HCOMM_ENGINE_AICPU_TS;
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string detail;
  std::string error;
  HcommChannelResourceInfo resource_info;
  if (!ProbeHcommChannelResources(state, dest_rank, options, acl_stream,
                                  &usable_buffer_bytes, &probe_status,
                                  &detail, &error, &resource_info)) {
    *out = MakeIo(probe_status, 0, 0, error);
    return FLUME_OK;
  }
  std::string plan_detail = MakeHcommPayloadPlanDetail(
      flume::hcomm_payload::PayloadRole::kSend, state, dest_rank, bytes,
      detail);
  int launch_status = FLUME_ERR_BACKEND;
  HcommLauncherDecision launcher = DecideHcommLauncherBackend();
  std::string launch_detail = TryLaunchHcommPayloadCopyDirectAclrt(
      flume::hcomm_payload::PayloadRole::kSend, state, dest_rank, acl_stream,
      static_cast<uint8_t*>(src->ptr) + src_offset, bytes, resource_info,
      launcher, &launch_status);
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
  options.engine = FLUME_HCOMM_ENGINE_AICPU_TS;
  size_t usable_buffer_bytes = 0;
  int probe_status = FLUME_ERR_BACKEND;
  std::string detail;
  std::string error;
  HcommChannelResourceInfo resource_info;
  if (!ProbeHcommChannelResources(state, src_rank, options, acl_stream,
                                  &usable_buffer_bytes, &probe_status,
                                  &detail, &error, &resource_info)) {
    *out = MakeIo(probe_status, 0, 0, error);
    return FLUME_OK;
  }
  std::string plan_detail = MakeHcommPayloadPlanDetail(
      flume::hcomm_payload::PayloadRole::kRecv, state, src_rank, bytes,
      detail);
  int launch_status = FLUME_ERR_BACKEND;
  HcommLauncherDecision launcher = DecideHcommLauncherBackend();
  std::string launch_detail = TryLaunchHcommPayloadCopyDirectAclrt(
      flume::hcomm_payload::PayloadRole::kRecv, state, src_rank, acl_stream,
      static_cast<uint8_t*>(dst->ptr) + dst_offset, bytes, resource_info,
      launcher, &launch_status);
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
