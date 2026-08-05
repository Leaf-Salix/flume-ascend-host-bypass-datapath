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
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#if FLUME_ENABLE_HCCL
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
#ifndef FLUME_HAVE_HCOMM_CHANNEL_RES
#define FLUME_HAVE_HCOMM_CHANNEL_RES 0
#endif
#ifndef FLUME_HAVE_HCOMM_THREAD_EXPORT
#define FLUME_HAVE_HCOMM_THREAD_EXPORT 0
#endif
#ifndef FLUME_HAVE_HCOMM_PRIMITIVES
#define FLUME_HAVE_HCOMM_PRIMITIVES 0
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

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_THREAD_EXPORT
#if __has_include(<hccl/hccl_res_expt.h>)
#include <hccl/hccl_res_expt.h>
#else
#error "FLUME_HAVE_HCOMM_THREAD_EXPORT=1 requires hccl/hccl_res_expt.h"
#endif
#endif

#include "protocol/framing.h"

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

struct CommState {
  bool hccl_attached = false;
  void* hccl_comm = nullptr;
  bool sim_comm_attached = false;
  std::string sim_comm_name;
  uint32_t rank = 0;
  uint32_t rank_size = 0;
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
bool ProbeHcommChannelResources(const CommState& state,
                                uint32_t peer_rank,
                                void* acl_stream,
                                size_t* usable_buffer_bytes,
                                std::string* error) {
  if (usable_buffer_bytes == nullptr || error == nullptr) {
    return false;
  }
  *usable_buffer_bytes = 0;
  if (!state.hccl_attached || state.hccl_comm == nullptr) {
    *error = "HCOMM channel probe requires flume_attach_hccl_comm";
    return false;
  }
  if (state.rank_size == 0 || peer_rank >= state.rank_size ||
      peer_rank == state.rank) {
    *error = "HCOMM channel probe peer rank is outside the attached HCCL comm";
    return false;
  }
  if (acl_stream == nullptr) {
    *error = "HCOMM channel probe requires an ACL stream";
    return false;
  }

  auto comm = static_cast<HcclComm>(state.hccl_comm);
  void* local_buffer = nullptr;
  uint64_t local_size = 0;
  if (!CheckHcclResource(HcclGetHcclBuffer(comm, &local_buffer, &local_size),
                         "HcclGetHcclBuffer", error)) {
    return false;
  }
  if (local_buffer == nullptr || local_size == 0) {
    *error = "HcclGetHcclBuffer returned an empty HCCL buffer";
    return false;
  }

  ThreadHandle cpu_ts_thread = 0;
  if (!CheckHcclResource(
          HcclThreadAcquireWithStream(
              comm, COMM_ENGINE_CPU_TS, static_cast<aclrtStream>(acl_stream),
              1, &cpu_ts_thread),
          "HcclThreadAcquireWithStream(CPU_TS)", error)) {
    return false;
  }

  ThreadHandle aicpu_ts_thread = 0;
  if (!CheckHcclResource(
          HcclThreadAcquire(comm, COMM_ENGINE_AICPU_TS, 1, 1,
                            &aicpu_ts_thread),
          "HcclThreadAcquire(AICPU_TS)", error)) {
    return false;
  }

#if FLUME_HAVE_HCOMM_THREAD_EXPORT
  ThreadHandle cpu_thread_on_aicpu = 0;
  if (!CheckHcclResource(
          HcclThreadExportToCommEngine(comm, 1, &cpu_ts_thread,
                                       COMM_ENGINE_AICPU_TS,
                                       &cpu_thread_on_aicpu),
          "HcclThreadExportToCommEngine(CPU_TS->AICPU_TS)", error)) {
    return false;
  }
  ThreadHandle aicpu_thread_on_cpu = 0;
  if (!CheckHcclResource(
          HcclThreadExportToCommEngine(comm, 1, &aicpu_ts_thread,
                                       COMM_ENGINE_CPU_TS,
                                       &aicpu_thread_on_cpu),
          "HcclThreadExportToCommEngine(AICPU_TS->CPU_TS)", error)) {
    return false;
  }
#else
  (void)cpu_ts_thread;
  (void)aicpu_ts_thread;
#endif

  HcclChannelDesc desc;
  if (!CheckHcclResource(HcclChannelDescInit(&desc, 1),
                         "HcclChannelDescInit", error)) {
    return false;
  }
  desc.remoteRank = peer_rank;
  desc.channelProtocol = COMM_PROTOCOL_HCCS;
  desc.notifyNum = 2;

  ChannelHandle channel = 0;
  if (!CheckHcclResource(
          HcclChannelAcquire(comm, COMM_ENGINE_AICPU, &desc, 1, &channel),
          "HcclChannelAcquire(AICPU/HCCS)", error)) {
    return false;
  }

  void* remote_buffer = nullptr;
  uint64_t remote_size = 0;
  if (!CheckHcclResource(
          HcclChannelGetHcclBuffer(comm, channel, &remote_buffer, &remote_size),
          "HcclChannelGetHcclBuffer", error)) {
    return false;
  }
  if (remote_buffer == nullptr || remote_size == 0) {
    *error = "HcclChannelGetHcclBuffer returned an empty remote HCCL buffer";
    return false;
  }

  uint64_t usable = std::min(local_size, remote_size);
  if (usable > std::numeric_limits<size_t>::max()) {
    usable = std::numeric_limits<size_t>::max();
  }
  *usable_buffer_bytes = static_cast<size_t>(usable);
  return true;
}
#endif
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
  client->sim_a3_register_seq = 0;
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
  if (client == nullptr || out == nullptr) {
    return FLUME_ERR_INVALID_ARGUMENT;
  }
  *out = nullptr;

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

#if FLUME_ENABLE_HCCL && FLUME_HAVE_HCOMM_CHANNEL_RES
  size_t usable_buffer_bytes = 0;
  std::string error;
  if (!ProbeHcommChannelResources(state, peer_rank, acl_stream,
                                  &usable_buffer_bytes, &error)) {
    *out = MakeIo(FLUME_ERR_BACKEND, 0, 0, error);
    return FLUME_OK;
  }
  *out = MakeIo(FLUME_OK, usable_buffer_bytes, 0);
  return FLUME_OK;
#else
  (void)peer_rank;
  (void)acl_stream;
  *out = MakeIo(FLUME_ERR_UNSUPPORTED, 0, 0,
                "HCOMM channel resources are unavailable in this build");
  return FLUME_OK;
#endif
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
