#include <acl/acl.h>
#if __has_include(<hccl/hccl.h>)
#include <hccl/hccl.h>
#elif __has_include(<hccl.h>)
#include <hccl.h>
#else
#error "flume-hccl-collective-smoke requires hccl/hccl.h or hccl.h"
#endif

#include <filesystem>
#include <cstdlib>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

#ifndef FLUME_HAVE_HCCL_ROOT_INFO_CONFIG
#define FLUME_HAVE_HCCL_ROOT_INFO_CONFIG 0
#endif
#ifndef FLUME_HAVE_HCCL_ROOT_INFO
#define FLUME_HAVE_HCCL_ROOT_INFO 0
#endif
#ifndef FLUME_HAVE_HCCL_COMM_INIT_ALL
#define FLUME_HAVE_HCCL_COMM_INIT_ALL 0
#endif
#ifndef FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE
#define FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE 0
#endif
#ifndef FLUME_HAVE_HCCL_CLUSTER_INFO
#define FLUME_HAVE_HCCL_CLUSTER_INFO 0
#endif
#ifndef FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG
#define FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG 0
#endif
#ifndef FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW
#define FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW 0
#endif
#ifndef FLUME_HAVE_HCCL_SYM_WINDOW
#define FLUME_HAVE_HCCL_SYM_WINDOW 0
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
#ifndef FLUME_HAVE_HCOMM_RANK_GRAPH
#define FLUME_HAVE_HCOMM_RANK_GRAPH 0
#endif
#ifndef FLUME_HAVE_ACL_VMM
#define FLUME_HAVE_ACL_VMM 0
#endif
#ifndef FLUME_HAVE_ACL_PHY_DEVICE_ID
#define FLUME_HAVE_ACL_PHY_DEVICE_ID 0
#endif

namespace {

enum class HcclInitMode {
  kAll,
  kRootInfo,
  kRankTable,
};

struct RankContext {
  std::string endpoint;
  HcclRootInfo* root_info = nullptr;
  std::string rank_table_path;
  HcclInitMode init_mode = HcclInitMode::kAll;
  HcclComm precreated_comm = nullptr;
  uint32_t rank = 0;
  uint32_t rank_size = 0;
  int32_t device = 0;
  uint64_t count = 0;
  bool a3_symmetric = false;
  bool p2p_copy = false;
  bool hcomm_channel_probe = false;
  flume_hcomm_engine_t hcomm_engine = FLUME_HCOMM_ENGINE_AUTO;
  flume_hcomm_protocol_t hcomm_protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  uint32_t hcomm_notify_num = 2;
  bool hcomm_require_thread_export = false;
  uint64_t sym_win_gb = 1;
  int status = 0;
  std::string error;
};

struct A3MappedMemory {
  void* base = nullptr;
#if FLUME_HAVE_ACL_VMM
  aclrtDrvMemHandle handle = nullptr;
#endif
  size_t size = 0;
  bool mapped = false;
};

std::mutex& LogMutex() {
  static std::mutex mutex;
  return mutex;
}

void LogLine(const std::string& line) {
  std::lock_guard<std::mutex> lock(LogMutex());
  std::cout << line << "\n";
}

struct BufferLayout {
  size_t one_rank_bytes = 0;
  size_t gather_bytes = 0;
  size_t reduce_send_offset = 0;
  size_t reduce_recv_offset = 0;
  size_t gather_send_offset = 0;
  size_t gather_recv_offset = 0;
  size_t a3_total_bytes = 0;
};

bool ParseU64(const std::string& text, uint64_t* out) {
  if (out == nullptr || text.empty() || text[0] == '-') {
    return false;
  }
  size_t pos = 0;
  try {
    unsigned long long value = std::stoull(text, &pos, 10);
    if (pos != text.size()) {
      return false;
    }
    *out = static_cast<uint64_t>(value);
    return true;
  } catch (...) {
    return false;
  }
}

bool ParseI32(const std::string& text, int32_t* out) {
  if (out == nullptr || text.empty()) {
    return false;
  }
  size_t pos = 0;
  try {
    long long value = std::stoll(text, &pos, 10);
    if (pos != text.size() ||
        value < std::numeric_limits<int32_t>::min() ||
        value > std::numeric_limits<int32_t>::max()) {
      return false;
    }
    *out = static_cast<int32_t>(value);
    return true;
  } catch (...) {
    return false;
  }
}

bool ParseDevices(const std::string& spec,
                  std::vector<int32_t>* devices,
                  std::string* error) {
  if (devices == nullptr || error == nullptr) {
    return false;
  }
  devices->clear();
  std::stringstream ss(spec);
  std::string item;
  while (std::getline(ss, item, ',')) {
    if (!item.empty()) {
      int32_t device = 0;
      if (!ParseI32(item, &device)) {
        *error = "invalid device id: " + item;
        return false;
      }
      devices->push_back(device);
    }
  }
  return true;
}

bool CheckAcl(aclError ret, const char* label, std::string* error) {
  if (ret == ACL_SUCCESS) {
    return true;
  }
  *error = std::string(label) + " failed, acl ret=" +
           std::to_string(static_cast<int>(ret));
  return false;
}

void LogAclDeviceMapping(const RankContext& ctx) {
  int32_t logic_device = -1;
  aclError get_ret = aclrtGetDevice(&logic_device);
  std::ostringstream line;
  line << "rank " << ctx.rank << " requested_device " << ctx.device;
  if (get_ret == ACL_SUCCESS) {
    line << " acl_logic_device " << logic_device;
  } else {
    line << " aclrtGetDevice_failed " << static_cast<int>(get_ret);
  }
#if FLUME_HAVE_ACL_PHY_DEVICE_ID
  if (get_ret == ACL_SUCCESS) {
    int32_t phy_device = -1;
    aclError phy_ret = aclrtGetPhyDevIdByLogicDevId(logic_device, &phy_device);
    if (phy_ret == ACL_SUCCESS) {
      line << " acl_physical_device " << phy_device;
    } else {
      line << " aclrtGetPhyDevIdByLogicDevId_failed "
           << static_cast<int>(phy_ret);
    }
  }
#else
  line << " acl_physical_device unavailable";
#endif
  LogLine(line.str());
}

const char* HcclResultName(HcclResult ret) {
  switch (static_cast<int>(ret)) {
    case 0:
      return "HCCL_SUCCESS";
    case 1:
      return "HCCL_E_PARA";
    case 2:
      return "HCCL_E_PTR";
    case 3:
      return "HCCL_E_MEMORY";
    case 4:
      return "HCCL_E_INTERNAL";
    case 5:
      return "HCCL_E_NOT_SUPPORT";
    case 6:
      return "HCCL_E_NOT_FOUND";
    case 7:
      return "HCCL_E_UNAVAIL";
    case 8:
      return "HCCL_E_SYSCALL";
    case 9:
      return "HCCL_E_TIMEOUT";
    case 10:
      return "HCCL_E_OPEN_FILE_FAILURE";
    case 11:
      return "HCCL_E_TCP_CONNECT";
    case 12:
      return "HCCL_E_ROCE_CONNECT";
    case 13:
      return "HCCL_E_TCP_TRANSFER";
    case 14:
      return "HCCL_E_ROCE_TRANSFER";
    case 15:
      return "HCCL_E_RUNTIME";
    case 16:
      return "HCCL_E_DRV";
    case 17:
      return "HCCL_E_PROFILING";
    case 18:
      return "HCCL_E_CCE";
    case 19:
      return "HCCL_E_NETWORK";
    case 20:
      return "HCCL_E_AGAIN";
    case 21:
      return "HCCL_E_REMOTE";
    case 22:
      return "HCCL_E_SUSPENDING";
    case 23:
      return "HCCL_E_OPRETRY_FAIL";
    case 24:
      return "HCCL_E_OOM";
    case 1041:
      return "HCCL_E_IN_STATUS";
    default:
      return "HCCL_UNKNOWN";
  }
}

bool CheckHccl(HcclResult ret, const char* label, std::string* error) {
  if (ret == HCCL_SUCCESS) {
    return true;
  }
  *error = std::string(label) + " failed, hccl ret=" +
           HcclResultName(ret) + "(" + std::to_string(static_cast<int>(ret)) +
           ")";
  return false;
}

bool CheckFlume(int ret, const char* label, std::string* error) {
  if (ret == FLUME_OK) {
    return true;
  }
  *error = std::string(label) + " failed, flume ret=" + flume_status_string(ret);
  return false;
}

bool WaitFlumeIo(flume_io_t* io, const char* label, std::string* error) {
  int ret = flume_wait(io, -1);
  if (ret == FLUME_OK) {
    return true;
  }
  *error = std::string(label) + " failed, flume ret=" +
           flume_status_string(ret);
  const char* detail = flume_io_error_message(io);
  if (detail != nullptr && detail[0] != '\0') {
    *error += ": ";
    *error += detail;
  }
  return false;
}

bool WriteRootInfoFile(const std::string& path,
                       const HcclRootInfo& root_info,
                       std::string* error) {
  std::filesystem::path output(path);
  if (output.has_parent_path()) {
    std::error_code ec;
    std::filesystem::create_directories(output.parent_path(), ec);
    if (ec) {
      *error = "failed to create root-info directory " +
               output.parent_path().string() + ": " + ec.message();
      return false;
    }
  }
  std::filesystem::path temp =
      output.string() + ".tmp." +
      std::to_string(static_cast<long long>(
          std::chrono::steady_clock::now().time_since_epoch().count()));
  std::ofstream stream(temp, std::ios::binary | std::ios::trunc);
  if (!stream) {
    *error = "failed to open root-info temp file: " + temp.string();
    return false;
  }
  stream.write(reinterpret_cast<const char*>(&root_info), sizeof(HcclRootInfo));
  stream.close();
  if (!stream) {
    std::error_code ignored;
    std::filesystem::remove(temp, ignored);
    *error = "failed to write root-info temp file: " + temp.string();
    return false;
  }
  std::error_code ec;
  std::filesystem::rename(temp, output, ec);
  if (ec) {
    std::error_code ignored;
    std::filesystem::remove(temp, ignored);
    *error = "failed to publish root-info file " + output.string() +
             ": " + ec.message();
    return false;
  }
  return true;
}

bool ReadRootInfoFile(const std::string& path,
                      HcclRootInfo* root_info,
                      std::string* error) {
  if (root_info == nullptr) {
    *error = "root-info destination is null";
    return false;
  }
  std::ifstream stream(path, std::ios::binary);
  if (!stream) {
    *error = "failed to open root-info file: " + path;
    return false;
  }
  stream.read(reinterpret_cast<char*>(root_info), sizeof(HcclRootInfo));
  if (stream.gcount() != static_cast<std::streamsize>(sizeof(HcclRootInfo))) {
    *error = "short root-info file: " + path;
    return false;
  }
  return true;
}

bool ParseHcclInitMode(const std::string& text,
                       HcclInitMode* out,
                       std::string* error) {
  if (out == nullptr || error == nullptr) {
    return false;
  }
  if (text == "all") {
    *out = HcclInitMode::kAll;
    return true;
  }
  if (text == "init-all") {
    *out = HcclInitMode::kAll;
    return true;
  }
  if (text == "root-info") {
    *out = HcclInitMode::kRootInfo;
    return true;
  }
  if (text == "rank-table") {
    *out = HcclInitMode::kRankTable;
    return true;
  }
  *error = "invalid --init, expected all, init-all, root-info, or rank-table";
  return false;
}

const char* HcclInitModeName(HcclInitMode mode) {
  switch (mode) {
    case HcclInitMode::kAll:
      return "all";
    case HcclInitMode::kRootInfo:
      return "root-info";
    case HcclInitMode::kRankTable:
      return "rank-table";
  }
  return "unknown";
}

bool ParseHcommEngine(const std::string& text,
                      flume_hcomm_engine_t* out,
                      std::string* error) {
  if (out == nullptr || error == nullptr) {
    return false;
  }
  if (text == "auto") {
    *out = FLUME_HCOMM_ENGINE_AUTO;
    return true;
  }
  if (text == "aicpu") {
    *out = FLUME_HCOMM_ENGINE_AICPU;
    return true;
  }
  if (text == "aicpu-ts") {
    *out = FLUME_HCOMM_ENGINE_AICPU_TS;
    return true;
  }
  if (text == "cpu") {
    *out = FLUME_HCOMM_ENGINE_CPU;
    return true;
  }
  if (text == "cpu-ts") {
    *out = FLUME_HCOMM_ENGINE_CPU_TS;
    return true;
  }
  *error = "invalid --hcomm-channel-engine, expected auto, aicpu, aicpu-ts, cpu, or cpu-ts";
  return false;
}

const char* HcommEngineName(flume_hcomm_engine_t engine) {
  switch (engine) {
    case FLUME_HCOMM_ENGINE_AICPU:
      return "aicpu";
    case FLUME_HCOMM_ENGINE_AICPU_TS:
      return "aicpu-ts";
    case FLUME_HCOMM_ENGINE_CPU:
      return "cpu";
    case FLUME_HCOMM_ENGINE_CPU_TS:
      return "cpu-ts";
    case FLUME_HCOMM_ENGINE_AUTO:
      return "auto";
  }
  return "unknown";
}

bool ParseHcommProtocol(const std::string& text,
                        flume_hcomm_protocol_t* out,
                        std::string* error) {
  if (out == nullptr || error == nullptr) {
    return false;
  }
  if (text == "auto" || text == "hccs") {
    *out = FLUME_HCOMM_PROTOCOL_HCCS;
    return true;
  }
  if (text == "roce") {
    *out = FLUME_HCOMM_PROTOCOL_ROCE;
    return true;
  }
  if (text == "pcie") {
    *out = FLUME_HCOMM_PROTOCOL_PCIE;
    return true;
  }
  if (text == "sio") {
    *out = FLUME_HCOMM_PROTOCOL_SIO;
    return true;
  }
  if (text == "hccs-only") {
    *out = FLUME_HCOMM_PROTOCOL_HCCS_ONLY;
    return true;
  }
  *error = "invalid --hcomm-channel-protocol, expected auto, hccs, roce, pcie, sio, or hccs-only";
  return false;
}

const char* HcommProtocolName(flume_hcomm_protocol_t protocol) {
  switch (protocol) {
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
    case FLUME_HCOMM_PROTOCOL_AUTO:
      return "auto";
  }
  return "unknown";
}

flume_hcomm_engine_t ResolveHcommSmokeEngine(flume_hcomm_engine_t engine) {
  if (engine != FLUME_HCOMM_ENGINE_AUTO) {
    return engine;
  }
#if FLUME_HAVE_HCOMM_THREAD_EXPORT
  return FLUME_HCOMM_ENGINE_AICPU_TS;
#else
  return FLUME_HCOMM_ENGINE_CPU_TS;
#endif
}

const char* A3UnavailableReason(HcclInitMode init_mode) {
#if !FLUME_HAVE_ACL_VMM
  (void)init_mode;
  return "ACL VMM/mapped HBM APIs are not available in this CANN runtime";
#elif !FLUME_HAVE_HCCL_SYM_WINDOW
  (void)init_mode;
  return "HcclCommSymWinRegister/HcclCommSymWinDeregister are not available in this HCCL header";
#elif !FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW
  (void)init_mode;
  return "HcclCommConfig.hcclSymWinMaxMemSizePerRank is not available in this HCCL header";
#else
  if (init_mode == HcclInitMode::kAll) {
    return "HcclCommInitAll does not expose HcclCommConfig for symmetric-window reservation";
  }
  if (init_mode == HcclInitMode::kRootInfo && !FLUME_HAVE_HCCL_ROOT_INFO_CONFIG) {
    return "HcclCommInitRootInfoConfig/HcclCommConfig are not available in this HCCL header";
  }
  if (init_mode == HcclInitMode::kRankTable && !FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG) {
    return "HcclCommInitClusterInfoConfig/HcclCommConfig are not available in this HCCL header";
  }
  return nullptr;
#endif
}

bool CheckedMulSize(size_t lhs, size_t rhs, size_t* out) {
  if (out == nullptr ||
      (rhs != 0 && lhs > std::numeric_limits<size_t>::max() / rhs)) {
    return false;
  }
  *out = lhs * rhs;
  return true;
}

bool CheckedAddSize(size_t lhs, size_t rhs, size_t* out) {
  if (out == nullptr || lhs > std::numeric_limits<size_t>::max() - rhs) {
    return false;
  }
  *out = lhs + rhs;
  return true;
}

bool ComputeBufferLayout(uint64_t count,
                         uint32_t rank_size,
                         BufferLayout* layout,
                         std::string* error) {
  if (layout == nullptr || error == nullptr || rank_size == 0 ||
      count > std::numeric_limits<size_t>::max()) {
    if (error != nullptr) {
      *error = "invalid collective layout parameters";
    }
    return false;
  }

  *layout = BufferLayout{};
  size_t one_rank_count = static_cast<size_t>(count);
  if (!CheckedMulSize(one_rank_count, sizeof(float), &layout->one_rank_bytes) ||
      !CheckedMulSize(layout->one_rank_bytes, rank_size, &layout->gather_bytes) ||
      !CheckedAddSize(layout->reduce_send_offset, layout->one_rank_bytes,
                      &layout->reduce_recv_offset) ||
      !CheckedAddSize(layout->reduce_recv_offset, layout->one_rank_bytes,
                      &layout->gather_send_offset) ||
      !CheckedAddSize(layout->gather_send_offset, layout->one_rank_bytes,
                      &layout->gather_recv_offset) ||
      !CheckedAddSize(layout->gather_recv_offset, layout->gather_bytes,
                      &layout->a3_total_bytes)) {
    *error = "collective buffer size overflow";
    return false;
  }
  return true;
}

bool SymmetricWindowCapacityBytes(uint64_t sym_win_gb, size_t* out) {
  constexpr size_t kGiB = 1024ULL * 1024ULL * 1024ULL;
  if (out == nullptr ||
      sym_win_gb > std::numeric_limits<size_t>::max() / kGiB) {
    return false;
  }
  *out = static_cast<size_t>(sym_win_gb) * kGiB;
  return true;
}

size_t AlignUp(size_t value, size_t alignment) {
  if (alignment == 0) {
    return value;
  }
  if (value > std::numeric_limits<size_t>::max() - (alignment - 1)) {
    return 0;
  }
  return ((value + alignment - 1) / alignment) * alignment;
}

void FreeA3MappedMemory(A3MappedMemory* mem) {
  if (mem == nullptr) {
    return;
  }
#if FLUME_HAVE_ACL_VMM
  if (mem->mapped && mem->base != nullptr) {
    (void)aclrtUnmapMem(mem->base);
    mem->mapped = false;
  }
  if (mem->handle != nullptr) {
    (void)aclrtFreePhysical(mem->handle);
    mem->handle = nullptr;
  }
  if (mem->base != nullptr) {
    (void)aclrtReleaseMemAddress(mem->base);
    mem->base = nullptr;
  }
#else
  mem->base = nullptr;
  mem->mapped = false;
#endif
  mem->size = 0;
}

bool AllocateA3MappedMemory(size_t requested,
                            int32_t device,
                            A3MappedMemory* mem,
                            std::string* error) {
#if FLUME_HAVE_ACL_VMM
  aclrtPhysicalMemProp prop = {};
  prop.handleType = ACL_MEM_HANDLE_TYPE_NONE;
  prop.allocationType = ACL_MEM_ALLOCATION_TYPE_PINNED;
  prop.memAttr = ACL_HBM_MEM_HUGE;
  prop.location.id = static_cast<uint32_t>(device);
  prop.location.type = ACL_MEM_LOCATION_TYPE_DEVICE;
  prop.reserve = 0;

  size_t granularity = 0;
  if (!CheckAcl(aclrtMemGetAllocationGranularity(
                    &prop, ACL_RT_MEM_ALLOC_GRANULARITY_RECOMMENDED,
                    &granularity),
                "aclrtMemGetAllocationGranularity", error)) {
    return false;
  }
  size_t alloc_size = AlignUp(requested, granularity);
  if (alloc_size == 0) {
    *error = "A3 mapped memory size overflow";
    return false;
  }

  if (!CheckAcl(aclrtReserveMemAddress(&mem->base, alloc_size, 0, nullptr, 1),
                "aclrtReserveMemAddress", error)) {
    return false;
  }
  mem->size = alloc_size;
  if (!CheckAcl(aclrtMallocPhysical(&mem->handle, alloc_size, &prop, 0),
                "aclrtMallocPhysical", error) ||
      !CheckAcl(aclrtMapMem(mem->base, alloc_size, 0, mem->handle, 0),
                "aclrtMapMem", error)) {
    FreeA3MappedMemory(mem);
    return false;
  }
  mem->mapped = true;
  return true;
#else
  (void)requested;
  (void)device;
  (void)mem;
  *error = A3UnavailableReason(HcclInitMode::kRootInfo);
  return false;
#endif
}

bool InitHcclComm(RankContext* ctx, HcclComm* out, std::string* error) {
  if (ctx == nullptr || out == nullptr || error == nullptr) {
    return false;
  }
  *out = nullptr;
  if (ctx->init_mode == HcclInitMode::kAll) {
    if (ctx->precreated_comm == nullptr) {
      *error = "HcclCommInitAll did not provide a communicator for this rank";
      return false;
    }
    *out = ctx->precreated_comm;
    ctx->precreated_comm = nullptr;
    return true;
  }
  if (ctx->init_mode == HcclInitMode::kRankTable) {
    if (ctx->rank_table_path.empty()) {
      *error = "rank-table init requires --rank-table";
      return false;
    }
    if (!ctx->a3_symmetric) {
#if FLUME_HAVE_HCCL_CLUSTER_INFO
      // Kept as an experimental diagnostic path: tested single-node HCCS_SW
      // pairs can require driver P2P memory-share enable and have not passed
      // hardware validation yet. Prefer root-info/all for first fabric bring-up.
      HcclResult init_ret =
          HcclCommInitClusterInfo(ctx->rank_table_path.c_str(), ctx->rank, out);
      return CheckHccl(init_ret, "HcclCommInitClusterInfo", error);
#else
      *error = "rank-table base smoke requires HcclCommInitClusterInfo; "
               "HcclCommInitClusterInfoConfig is reserved for A3 symmetric mode";
      return false;
#endif
    }
#if FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG
    HcclCommConfig config;
    HcclCommConfigInit(&config);
#if FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE
    config.hcclBufferSize = 50;
#endif
#if FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW
    if (ctx->a3_symmetric) {
      config.hcclSymWinMaxMemSizePerRank = ctx->sym_win_gb;
    }
#endif
    HcclResult init_ret =
        HcclCommInitClusterInfoConfig(ctx->rank_table_path.c_str(), ctx->rank,
                                      &config, out);
    if (!CheckHccl(init_ret, "HcclCommInitClusterInfoConfig", error)) {
      return false;
    }
    return true;
#else
    *error = "rank-table A3 symmetric init requires HcclCommInitClusterInfoConfig";
    return false;
#endif
  }

  if (!ctx->a3_symmetric) {
#if FLUME_HAVE_HCCL_ROOT_INFO
    HcclResult init_ret =
        HcclCommInitRootInfo(ctx->rank_size, ctx->root_info, ctx->rank, out);
    return CheckHccl(init_ret, "HcclCommInitRootInfo", error);
#elif FLUME_HAVE_HCCL_ROOT_INFO_CONFIG
    HcclCommConfig config;
    HcclCommConfigInit(&config);
    HcclResult init_ret =
        HcclCommInitRootInfoConfig(ctx->rank_size, ctx->root_info, ctx->rank,
                                   &config, out);
    return CheckHccl(init_ret, "HcclCommInitRootInfoConfig", error);
#else
    *error = "root-info init is not available in this HCCL header";
    return false;
#endif
  }
#if FLUME_HAVE_HCCL_ROOT_INFO_CONFIG
  HcclCommConfig config;
  HcclCommConfigInit(&config);
#if FLUME_HAVE_HCCL_CONFIG_BUFFER_SIZE
  config.hcclBufferSize = 50;
#endif
#if FLUME_HAVE_HCCL_CONFIG_SYM_WINDOW
  if (ctx->a3_symmetric) {
    config.hcclSymWinMaxMemSizePerRank = ctx->sym_win_gb;
  }
#endif
  HcclResult init_ret =
      HcclCommInitRootInfoConfig(ctx->rank_size, ctx->root_info, ctx->rank,
                                 &config, out);
  if (!CheckHccl(init_ret, "HcclCommInitRootInfoConfig", error)) {
    return false;
  }
  return true;
#else
  *error = "root-info A3 symmetric init requires HcclCommInitRootInfoConfig";
  return false;
#endif
}

void RankMain(RankContext* ctx) {
  void* reduce_send = nullptr;
  void* reduce_recv = nullptr;
  void* gather_send = nullptr;
  void* gather_recv = nullptr;
  void* host_buf = nullptr;
  aclrtStream stream = nullptr;
  HcclComm hccl_comm = nullptr;
  A3MappedMemory a3_mem;
  flume_client_t* client = nullptr;
  flume_buffer_t* a3_block_buf = nullptr;
  flume_buffer_t* reduce_send_buf = nullptr;
  flume_buffer_t* reduce_recv_buf = nullptr;
  flume_buffer_t* gather_send_buf = nullptr;
  flume_buffer_t* gather_recv_buf = nullptr;
  flume_a3_symmetric_window_t* a3_window = nullptr;
  bool a3_window_deregistered = true;
  flume_io_t* reduce_io = nullptr;
  flume_io_t* gather_io = nullptr;
  flume_io_t* p2p_io = nullptr;
  flume_io_t* hcomm_channel_io = nullptr;

  BufferLayout layout;
  std::string error;
  size_t one_rank_bytes = 0;
  size_t gather_bytes = 0;
  if (!ComputeBufferLayout(ctx->count, ctx->rank_size, &layout, &error)) {
    goto cleanup;
  }
  one_rank_bytes = layout.one_rank_bytes;
  gather_bytes = layout.gather_bytes;

  if (!CheckAcl(aclrtSetDevice(ctx->device), "aclrtSetDevice", &error)) {
    goto cleanup;
  }
  LogAclDeviceMapping(*ctx);

  if (ctx->a3_symmetric) {
    const char* reason = A3UnavailableReason(ctx->init_mode);
    if (reason != nullptr) {
      error = std::string("A3 symmetric smoke unavailable: ") + reason;
      goto cleanup;
    }
  }

  if (!InitHcclComm(ctx, &hccl_comm, &error)) {
    goto cleanup;
  }

  if (!CheckAcl(aclrtCreateStream(&stream), "aclrtCreateStream", &error) ||
      !CheckAcl(aclrtMallocHost(&host_buf, gather_bytes), "aclrtMallocHost",
                &error)) {
    goto cleanup;
  }

  if (ctx->a3_symmetric) {
    if (!AllocateA3MappedMemory(layout.a3_total_bytes, ctx->device, &a3_mem,
                                &error)) {
      goto cleanup;
    }
    auto* base = static_cast<uint8_t*>(a3_mem.base);
    reduce_send = base + layout.reduce_send_offset;
    reduce_recv = base + layout.reduce_recv_offset;
    gather_send = base + layout.gather_send_offset;
    gather_recv = base + layout.gather_recv_offset;
  } else if (!CheckAcl(aclrtMalloc(&reduce_send, one_rank_bytes,
                                   ACL_MEM_MALLOC_HUGE_FIRST),
                       "aclrtMalloc reduce_send", &error) ||
             !CheckAcl(aclrtMalloc(&reduce_recv, one_rank_bytes,
                                   ACL_MEM_MALLOC_HUGE_FIRST),
                       "aclrtMalloc reduce_recv", &error) ||
             !CheckAcl(aclrtMalloc(&gather_send, one_rank_bytes,
                                   ACL_MEM_MALLOC_HUGE_FIRST),
                       "aclrtMalloc gather_send", &error) ||
             !CheckAcl(aclrtMalloc(&gather_recv, gather_bytes,
                                   ACL_MEM_MALLOC_HUGE_FIRST),
                       "aclrtMalloc gather_recv", &error)) {
    goto cleanup;
  }

  if (!CheckFlume(flume_client_open(ctx->endpoint.c_str(), &client),
                 "flume_client_open", &error) ||
      !CheckFlume(flume_attach_hccl_comm(client, hccl_comm, ctx->rank,
                                       ctx->rank_size),
                 "flume_attach_hccl_comm", &error)) {
    goto cleanup;
  }

  {
    auto* host = static_cast<float*>(host_buf);
    for (uint64_t i = 0; i < ctx->count; ++i) {
      host[i] = static_cast<float>(ctx->rank + 1 + i);
    }
    if (!CheckAcl(aclrtMemcpy(reduce_send, one_rank_bytes, host, one_rank_bytes,
                              ACL_MEMCPY_HOST_TO_DEVICE),
                  "aclrtMemcpy reduce H2D", &error)) {
      goto cleanup;
    }
    for (uint64_t i = 0; i < ctx->count; ++i) {
      host[i] = static_cast<float>(ctx->rank * 10 + i);
    }
    if (!CheckAcl(aclrtMemcpy(gather_send, one_rank_bytes, host, one_rank_bytes,
                              ACL_MEMCPY_HOST_TO_DEVICE),
                  "aclrtMemcpy gather H2D", &error)) {
      goto cleanup;
    }
  }

  if (ctx->a3_symmetric) {
    if (!CheckFlume(flume_register_buffer(client, a3_mem.base,
                                        layout.a3_total_bytes,
                                        FLUME_BUFFER_ASCEND_HBM, &a3_block_buf),
                   "flume_register_buffer a3_block", &error) ||
        !CheckFlume(flume_a3_register_symmetric_memory(
                       client, a3_block_buf, 0, layout.a3_total_bytes,
                       &a3_window),
                   "flume_a3_register_symmetric_memory", &error)) {
      goto cleanup;
    }
    reduce_send_buf = a3_block_buf;
    reduce_recv_buf = a3_block_buf;
    gather_send_buf = a3_block_buf;
    gather_recv_buf = a3_block_buf;
  } else if (!CheckFlume(flume_register_buffer(client, reduce_send, one_rank_bytes,
                                            FLUME_BUFFER_ASCEND_HBM,
                                            &reduce_send_buf),
                       "flume_register_buffer reduce_send", &error) ||
             !CheckFlume(flume_register_buffer(client, reduce_recv, one_rank_bytes,
                                            FLUME_BUFFER_ASCEND_HBM,
                                            &reduce_recv_buf),
                       "flume_register_buffer reduce_recv", &error) ||
             !CheckFlume(flume_register_buffer(client, gather_send, one_rank_bytes,
                                            FLUME_BUFFER_ASCEND_HBM,
                                            &gather_send_buf),
                       "flume_register_buffer gather_send", &error) ||
             !CheckFlume(flume_register_buffer(client, gather_recv, gather_bytes,
                                            FLUME_BUFFER_ASCEND_HBM,
                                            &gather_recv_buf),
                       "flume_register_buffer gather_recv", &error)) {
    goto cleanup;
  }

  if (!CheckFlume(flume_allreduce_async(client, reduce_recv_buf,
                                      ctx->a3_symmetric ?
                                          layout.reduce_recv_offset : 0,
                                      reduce_send_buf,
                                      ctx->a3_symmetric ?
                                          layout.reduce_send_offset : 0,
                                      ctx->count,
                                      FLUME_DTYPE_FP32, FLUME_REDUCE_SUM, stream,
                                      &reduce_io),
                 "flume_allreduce_async", &error) ||
      !CheckFlume(flume_wait(reduce_io, -1), "flume_wait allreduce", &error) ||
      !CheckFlume(flume_allgather_async(client, gather_recv_buf,
                                      ctx->a3_symmetric ?
                                          layout.gather_recv_offset : 0,
                                      gather_send_buf,
                                      ctx->a3_symmetric ?
                                          layout.gather_send_offset : 0,
                                      ctx->count,
                                      FLUME_DTYPE_FP32, stream, &gather_io),
                 "flume_allgather_async", &error) ||
      !CheckFlume(flume_wait(gather_io, -1), "flume_wait allgather", &error)) {
    goto cleanup;
  }

  {
    auto* host = static_cast<float*>(host_buf);
    if (!CheckAcl(aclrtMemcpy(host, one_rank_bytes, reduce_recv, one_rank_bytes,
                              ACL_MEMCPY_DEVICE_TO_HOST),
                  "aclrtMemcpy reduce D2H", &error)) {
      goto cleanup;
    }
    for (uint64_t i = 0; i < ctx->count; ++i) {
      float expected =
          static_cast<float>(ctx->rank_size) * static_cast<float>(i) +
          static_cast<float>(ctx->rank_size * (ctx->rank_size + 1) / 2);
      if (host[i] != expected) {
        error = "allreduce verification failed";
        goto cleanup;
      }
    }
    if (!CheckAcl(aclrtMemcpy(host, gather_bytes, gather_recv, gather_bytes,
                              ACL_MEMCPY_DEVICE_TO_HOST),
                  "aclrtMemcpy gather D2H", &error)) {
      goto cleanup;
    }
    for (uint32_t rank = 0; rank < ctx->rank_size; ++rank) {
      for (uint64_t i = 0; i < ctx->count; ++i) {
        float expected = static_cast<float>(rank * 10 + i);
        if (host[static_cast<size_t>(rank) * ctx->count + i] != expected) {
          error = "allgather verification failed";
          goto cleanup;
        }
      }
    }
  }

  if (ctx->p2p_copy) {
    if (ctx->rank_size < 2) {
      error = "P2P copy smoke requires at least two ranks";
      goto cleanup;
    }
    if (ctx->rank == 1) {
      auto* host = static_cast<float*>(host_buf);
      for (uint64_t i = 0; i < ctx->count; ++i) {
        host[i] = -1.0F;
      }
      if (!CheckAcl(aclrtMemcpy(reduce_recv, one_rank_bytes, host,
                                one_rank_bytes, ACL_MEMCPY_HOST_TO_DEVICE),
                    "aclrtMemcpy p2p recv clear H2D", &error)) {
        goto cleanup;
      }
    }

    if (ctx->rank == 0) {
      if (!CheckFlume(flume_p2p_send_async(
                            client, reduce_send_buf,
                            ctx->a3_symmetric ? layout.reduce_send_offset : 0,
                            ctx->count, FLUME_DTYPE_FP32, 1, stream, &p2p_io),
                      "flume_p2p_send_async", &error) ||
          !CheckFlume(flume_wait(p2p_io, -1), "flume_wait p2p send",
                      &error)) {
        goto cleanup;
      }
    } else if (ctx->rank == 1) {
      if (!CheckFlume(flume_p2p_recv_async(
                            client, reduce_recv_buf,
                            ctx->a3_symmetric ? layout.reduce_recv_offset : 0,
                            ctx->count, FLUME_DTYPE_FP32, 0, stream, &p2p_io),
                      "flume_p2p_recv_async", &error) ||
          !CheckFlume(flume_wait(p2p_io, -1), "flume_wait p2p recv",
                      &error)) {
        goto cleanup;
      }
      auto* host = static_cast<float*>(host_buf);
      if (!CheckAcl(aclrtMemcpy(host, one_rank_bytes, reduce_recv,
                                one_rank_bytes, ACL_MEMCPY_DEVICE_TO_HOST),
                    "aclrtMemcpy p2p recv D2H", &error)) {
        goto cleanup;
      }
      for (uint64_t i = 0; i < ctx->count; ++i) {
        float expected = static_cast<float>(1 + i);
        if (host[i] != expected) {
          error = "p2p copy verification failed";
          goto cleanup;
        }
      }
    }
  }

  if (ctx->hcomm_channel_probe) {
    if (ctx->rank_size < 2) {
      error = "HCOMM channel probe requires at least two ranks";
      goto cleanup;
    }
    if (ctx->rank == 0 || ctx->rank == 1) {
      uint32_t peer_rank = (ctx->rank == 0) ? 1 : 0;
      flume_hcomm_channel_probe_options_t options = {};
      options.size = sizeof(options);
      options.notify_num = ctx->hcomm_notify_num;
      options.engine = ctx->hcomm_engine;
      options.protocol = ctx->hcomm_protocol;
      options.require_thread_export = ctx->hcomm_require_thread_export ? 1U : 0U;
      if (!CheckFlume(flume_hcomm_channel_probe_ex(client, peer_rank, &options,
                                                   stream, &hcomm_channel_io),
                      "flume_hcomm_channel_probe", &error) ||
          !WaitFlumeIo(hcomm_channel_io, "flume_wait hcomm channel probe",
                       &error)) {
        goto cleanup;
      }
      std::ostringstream line;
      line << "rank " << ctx->rank
           << " hcomm channel probe passed: peer_rank=" << peer_rank
           << " usable_hccl_buffer_bytes="
           << flume_io_bytes(hcomm_channel_io)
           << " requested_engine=" << HcommEngineName(ctx->hcomm_engine)
           << " resolved_engine="
           << HcommEngineName(ResolveHcommSmokeEngine(ctx->hcomm_engine))
           << " protocol=" << HcommProtocolName(ctx->hcomm_protocol)
           << " notify_num=" << ctx->hcomm_notify_num
           << " require_thread_export="
           << (ctx->hcomm_require_thread_export ? "on" : "off")
           << " thread_export="
           << (FLUME_HAVE_HCOMM_THREAD_EXPORT ? "available" : "not-built")
           << " primitives="
           << (FLUME_HAVE_HCOMM_PRIMITIVES ? "available" : "not-built");
      const char* detail = flume_io_error_message(hcomm_channel_io);
      if (detail != nullptr && detail[0] != '\0') {
        line << " detail=\"" << detail << "\"";
      }
      LogLine(line.str());
    }
  }

cleanup:
  flume_io_release(hcomm_channel_io);
  if (a3_window != nullptr) {
    int ret = flume_a3_deregister_symmetric_memory(a3_window);
    if (ret != FLUME_OK && error.empty()) {
      error = std::string("flume_a3_deregister_symmetric_memory failed: ") +
              flume_status_string(ret);
    }
    if (ret != FLUME_OK) {
      a3_window_deregistered = false;
    }
    a3_window = nullptr;
  }
  flume_io_release(p2p_io);
  flume_io_release(gather_io);
  flume_io_release(reduce_io);
  if (a3_block_buf != nullptr) {
    flume_buffer_release(a3_block_buf);
  } else {
    flume_buffer_release(gather_recv_buf);
    flume_buffer_release(gather_send_buf);
    flume_buffer_release(reduce_recv_buf);
    flume_buffer_release(reduce_send_buf);
  }
  flume_client_close(client);
  if (host_buf != nullptr) {
    (void)aclrtFreeHost(host_buf);
  }
  if (ctx->a3_symmetric) {
    if (a3_window_deregistered) {
      FreeA3MappedMemory(&a3_mem);
    }
  } else if (gather_recv != nullptr) {
    (void)aclrtFree(gather_recv);
  }
  if (!ctx->a3_symmetric && gather_send != nullptr) {
    (void)aclrtFree(gather_send);
  }
  if (!ctx->a3_symmetric && reduce_recv != nullptr) {
    (void)aclrtFree(reduce_recv);
  }
  if (!ctx->a3_symmetric && reduce_send != nullptr) {
    (void)aclrtFree(reduce_send);
  }
  if (stream != nullptr) {
    aclError ret = aclrtDestroyStream(stream);
    if (ret != ACL_SUCCESS && error.empty()) {
      error = "aclrtDestroyStream failed, acl ret=" +
              std::to_string(static_cast<int>(ret));
    }
  }
  if (hccl_comm != nullptr) {
    HcclResult ret = HcclCommDestroy(hccl_comm);
    if (ret != HCCL_SUCCESS && error.empty()) {
      error = "HcclCommDestroy failed, hccl ret=" +
              std::to_string(static_cast<int>(ret));
    }
  }
  if (ctx != nullptr && ctx->precreated_comm != nullptr) {
    HcclResult ret = HcclCommDestroy(ctx->precreated_comm);
    ctx->precreated_comm = nullptr;
    if (ret != HCCL_SUCCESS && error.empty()) {
      error = "HcclCommDestroy(precreated) failed, hccl ret=" +
              std::to_string(static_cast<int>(ret));
    }
  }
  if (!error.empty()) {
    ctx->status = 1;
    ctx->error = error;
  }
}

}  // namespace

int main(int argc, char** argv) {
  uint64_t count = 1024;
  bool a3_symmetric = false;
  bool p2p_copy = false;
  bool hcomm_channel_probe = false;
  flume_hcomm_engine_t hcomm_engine = FLUME_HCOMM_ENGINE_AUTO;
  flume_hcomm_protocol_t hcomm_protocol = FLUME_HCOMM_PROTOCOL_HCCS;
  uint32_t hcomm_notify_num = 2;
  bool hcomm_require_thread_export = false;
  uint64_t sym_win_gb = 1;
  HcclInitMode init_mode = HcclInitMode::kAll;
  std::string rank_table_path;
  std::string root_info_path;
  std::string root_info_out_path;
  std::vector<int32_t> devices;
  bool single_rank_mode = false;
  uint32_t single_rank = 0;
  uint32_t single_rank_size = 0;
  std::string parse_error;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--devices=", 0) == 0) {
      if (!ParseDevices(arg.substr(std::string("--devices=").size()),
                        &devices, &parse_error)) {
        std::cerr << parse_error << "\n";
        return 2;
      }
    } else if (arg.rfind("--count=", 0) == 0) {
      if (!ParseU64(arg.substr(std::string("--count=").size()), &count)) {
        std::cerr << "invalid --count\n";
        return 2;
      }
    } else if (arg == "--a3-symmetric") {
      a3_symmetric = true;
    } else if (arg == "--p2p-copy") {
      p2p_copy = true;
    } else if (arg == "--hcomm-channel-probe") {
      hcomm_channel_probe = true;
    } else if (arg == "--hcomm-require-thread-export") {
      hcomm_require_thread_export = true;
    } else if (arg.rfind("--hcomm-channel-engine=", 0) == 0) {
      if (!ParseHcommEngine(
              arg.substr(std::string("--hcomm-channel-engine=").size()),
              &hcomm_engine, &parse_error)) {
        std::cerr << parse_error << "\n";
        return 2;
      }
    } else if (arg.rfind("--hcomm-channel-protocol=", 0) == 0) {
      if (!ParseHcommProtocol(
              arg.substr(std::string("--hcomm-channel-protocol=").size()),
              &hcomm_protocol, &parse_error)) {
        std::cerr << parse_error << "\n";
        return 2;
      }
    } else if (arg.rfind("--hcomm-notify-num=", 0) == 0) {
      uint64_t value = 0;
      if (!ParseU64(arg.substr(std::string("--hcomm-notify-num=").size()),
                    &value) || value == 0 ||
          value > std::numeric_limits<uint32_t>::max()) {
        std::cerr << "invalid --hcomm-notify-num\n";
        return 2;
      }
      hcomm_notify_num = static_cast<uint32_t>(value);
    } else if (arg.rfind("--init=", 0) == 0) {
      if (!ParseHcclInitMode(arg.substr(std::string("--init=").size()),
                             &init_mode, &parse_error)) {
        std::cerr << parse_error << "\n";
        return 2;
      }
    } else if (arg.rfind("--rank-table=", 0) == 0) {
      rank_table_path = arg.substr(std::string("--rank-table=").size());
    } else if (arg.rfind("--root-info=", 0) == 0) {
      root_info_path = arg.substr(std::string("--root-info=").size());
    } else if (arg.rfind("--root-info-out=", 0) == 0) {
      root_info_out_path = arg.substr(std::string("--root-info-out=").size());
    } else if (arg.rfind("--rank=", 0) == 0) {
      uint64_t value = 0;
      if (!ParseU64(arg.substr(std::string("--rank=").size()), &value) ||
          value > std::numeric_limits<uint32_t>::max()) {
        std::cerr << "invalid --rank\n";
        return 2;
      }
      single_rank = static_cast<uint32_t>(value);
      single_rank_mode = true;
    } else if (arg.rfind("--rank-size=", 0) == 0) {
      uint64_t value = 0;
      if (!ParseU64(arg.substr(std::string("--rank-size=").size()), &value) ||
          value > std::numeric_limits<uint32_t>::max()) {
        std::cerr << "invalid --rank-size\n";
        return 2;
      }
      single_rank_size = static_cast<uint32_t>(value);
      single_rank_mode = true;
    } else if (arg.rfind("--sym-win-gb=", 0) == 0) {
      if (!ParseU64(arg.substr(std::string("--sym-win-gb=").size()),
                    &sym_win_gb)) {
        std::cerr << "invalid --sym-win-gb\n";
        return 2;
      }
    } else {
      std::cerr << "usage: " << argv[0]
                << " [--devices=0,1] [--count=1024]"
                << " [--init=all|root-info|rank-table] [--rank-table=path]"
                << " [--root-info=path|--root-info-out=path]"
                << " [--rank=0 --rank-size=2]"
                << " [--a3-symmetric]"
                << " [--p2p-copy]"
                << " [--hcomm-channel-probe]"
                << " [--hcomm-channel-engine=auto]"
                << " [--hcomm-channel-protocol=hccs]"
                << " [--hcomm-notify-num=2]"
                << " [--hcomm-require-thread-export]"
                << " [--sym-win-gb=1]\n";
      return 2;
    }
  }
  if (count == 0) {
    std::cerr << "--count must be greater than 0\n";
    return 2;
  }
  if (sym_win_gb == 0) {
    std::cerr << "--sym-win-gb must be greater than 0\n";
    return 2;
  }
  if (hcomm_notify_num == 0 || hcomm_notify_num > 64) {
    std::cerr << "--hcomm-notify-num must be in [1, 64]\n";
    return 2;
  }
  if (p2p_copy && a3_symmetric) {
    std::cerr << "--p2p-copy currently uses ordinary HBM buffers; do not "
                 "combine it with --a3-symmetric\n";
    return 2;
  }
#if !FLUME_HAVE_HCCL_P2P
  if (p2p_copy) {
    std::cerr << "--p2p-copy requires HcclSend/HcclRecv in this HCCL header\n";
    return 2;
  }
#endif
#if !FLUME_HAVE_HCOMM_CHANNEL_RES
  if (hcomm_channel_probe) {
    std::cerr << "--hcomm-channel-probe requires HCOMM channel resource APIs "
                 "such as HcclChannelAcquire in this build\n";
    return 2;
  }
#endif
  if (init_mode == HcclInitMode::kRankTable && rank_table_path.empty()) {
    std::cerr << "--init=rank-table requires --rank-table\n";
    return 2;
  }
  if (init_mode != HcclInitMode::kRootInfo &&
      (!root_info_path.empty() || !root_info_out_path.empty())) {
    std::cerr << "--root-info/--root-info-out require --init=root-info\n";
    return 2;
  }
  if (!root_info_path.empty() && !root_info_out_path.empty()) {
    std::cerr << "pass only one of --root-info or --root-info-out\n";
    return 2;
  }
  if (single_rank_mode) {
    if (single_rank_size == 0 || single_rank >= single_rank_size) {
      std::cerr << "--rank-size must be greater than --rank in single-rank mode\n";
      return 2;
    }
    if (init_mode == HcclInitMode::kAll) {
      std::cerr << "--rank/--rank-size single-rank mode is for root-info or "
                   "rank-table init, not HcclCommInitAll\n";
      return 2;
    }
  }

  if (aclInit(nullptr) != ACL_SUCCESS) {
    std::cerr << "aclInit failed\n";
    return 1;
  }

  uint32_t device_count = 0;
  if (aclrtGetDeviceCount(&device_count) != ACL_SUCCESS || device_count == 0) {
    std::cerr << "aclrtGetDeviceCount failed or no device is available\n";
    (void)aclFinalize();
    return 1;
  }
  if (devices.empty()) {
    for (uint32_t i = 0; i < device_count; ++i) {
      devices.push_back(static_cast<int32_t>(i));
    }
  }
  if (single_rank_mode && devices.size() != 1) {
    std::cerr << "--rank/--rank-size single-rank mode requires exactly one "
                 "--devices entry\n";
    (void)aclFinalize();
    return 2;
  }
  if (!single_rank_mode && init_mode != HcclInitMode::kAll &&
      devices.size() > 1) {
    std::cerr << HcclInitModeName(init_mode)
              << " init requires one process per rank. Pass --rank and "
                 "--rank-size, or use tools/flume_tool.py as the launcher.\n";
    (void)aclFinalize();
    return 2;
  }
  const uint32_t global_rank_size =
      single_rank_mode ? single_rank_size : static_cast<uint32_t>(devices.size());
  if ((p2p_copy || hcomm_channel_probe) && global_rank_size != 2) {
    std::cerr << "--p2p-copy and --hcomm-channel-probe are pair-only smokes "
                 "and require exactly two ranks for now\n";
    (void)aclFinalize();
    return 2;
  }
  const char* visible_devices = std::getenv("ASCEND_RT_VISIBLE_DEVICES");
  std::cout << "hccl smoke config: init=" << HcclInitModeName(init_mode)
            << " acl_device_count=" << device_count
            << " devices=[";
  for (size_t i = 0; i < devices.size(); ++i) {
    if (i != 0) {
      std::cout << ",";
    }
    std::cout << devices[i];
  }
  std::cout << "] ASCEND_RT_VISIBLE_DEVICES="
            << (visible_devices == nullptr ? "not set" : visible_devices)
            << " process_mode=" << (single_rank_mode ? "single-rank" : "local")
            << " rank_size=" << global_rank_size
            << " hcomm_engine=" << HcommEngineName(hcomm_engine)
            << " resolved_hcomm_engine="
            << HcommEngineName(ResolveHcommSmokeEngine(hcomm_engine))
            << " hcomm_protocol=" << HcommProtocolName(hcomm_protocol)
            << " hcomm_notify_num=" << hcomm_notify_num
            << " hcomm_require_thread_export="
            << (hcomm_require_thread_export ? "on" : "off")
            << "\n";
  std::cout << "hccl feature probe: root_info=" << FLUME_HAVE_HCCL_ROOT_INFO
            << " root_info_config=" << FLUME_HAVE_HCCL_ROOT_INFO_CONFIG
            << " cluster_info=" << FLUME_HAVE_HCCL_CLUSTER_INFO
            << " cluster_info_config=" << FLUME_HAVE_HCCL_CLUSTER_INFO_CONFIG
            << " sym_window=" << FLUME_HAVE_HCCL_SYM_WINDOW
            << " p2p=" << FLUME_HAVE_HCCL_P2P
            << " hcomm_channel_res=" << FLUME_HAVE_HCOMM_CHANNEL_RES
            << " hcomm_thread_export=" << FLUME_HAVE_HCOMM_THREAD_EXPORT
            << " hcomm_primitives=" << FLUME_HAVE_HCOMM_PRIMITIVES
            << " hcomm_rank_graph=" << FLUME_HAVE_HCOMM_RANK_GRAPH
            << " acl_phy_device_id=" << FLUME_HAVE_ACL_PHY_DEVICE_ID
            << " acl_vmm=" << FLUME_HAVE_ACL_VMM << "\n";
  std::cout << "FLUME_BACKEND_CAPS"
            << " hccl_root_info="
            << (FLUME_HAVE_HCCL_ROOT_INFO ? "on" : "off")
            << " hccl_init_all="
            << (FLUME_HAVE_HCCL_COMM_INIT_ALL ? "on" : "off")
            << " hccl_p2p=" << (FLUME_HAVE_HCCL_P2P ? "on" : "off")
            << " hcomm_channel="
            << (FLUME_HAVE_HCOMM_CHANNEL_RES ? "on" : "off")
            << " hcomm_default_engine="
            << HcommEngineName(ResolveHcommSmokeEngine(FLUME_HCOMM_ENGINE_AUTO))
            << " hcomm_rank_graph="
            << (FLUME_HAVE_HCOMM_RANK_GRAPH ? "on" : "off")
            << " hcomm_aicpu_thread_export="
            << (FLUME_HAVE_HCOMM_THREAD_EXPORT ? "on" : "off")
            << " hcomm_primitives="
            << (FLUME_HAVE_HCOMM_PRIMITIVES ? "on" : "off")
            << " hcomm_payload=not-implemented"
            << " storage_hbm=not-implemented"
            << " cann85_baseline=feature-probed"
            << "\n";
  std::set<int32_t> unique_devices;
  for (int32_t device : devices) {
    if (device < 0 || static_cast<uint32_t>(device) >= device_count) {
      std::cerr << "invalid device id: " << device << "\n";
      (void)aclFinalize();
      return 2;
    }
    if (!unique_devices.insert(device).second) {
      std::cerr << "duplicate device id: " << device << "\n";
      (void)aclFinalize();
      return 2;
    }
  }
  if (p2p_copy && global_rank_size < 2) {
    std::cerr << "--p2p-copy requires at least two ranks\n";
    (void)aclFinalize();
    return 2;
  }
  if (hcomm_channel_probe && global_rank_size < 2) {
    std::cerr << "--hcomm-channel-probe requires at least two ranks\n";
    (void)aclFinalize();
    return 2;
  }

  BufferLayout preflight_layout;
  std::string layout_error;
  if (!ComputeBufferLayout(count, global_rank_size, &preflight_layout,
                           &layout_error)) {
    std::cerr << layout_error << "\n";
    (void)aclFinalize();
    return 2;
  }
  if (a3_symmetric) {
    const char* reason = A3UnavailableReason(init_mode);
    if (reason != nullptr) {
      std::cerr << "A3 symmetric smoke unavailable: " << reason << "\n";
      (void)aclFinalize();
      return 2;
    }
    size_t sym_win_bytes = 0;
    if (!SymmetricWindowCapacityBytes(sym_win_gb, &sym_win_bytes)) {
      std::cerr << "--sym-win-gb is too large\n";
      (void)aclFinalize();
      return 2;
    }
    if (preflight_layout.a3_total_bytes > sym_win_bytes) {
      std::cerr << "A3 symmetric buffer bytes " << preflight_layout.a3_total_bytes
                << " exceed --sym-win-gb capacity " << sym_win_bytes << "\n";
      (void)aclFinalize();
      return 2;
    }
  }

  std::filesystem::path root =
      std::filesystem::temp_directory_path() /
      ("flume-hccl-collective-smoke-" +
       std::to_string(static_cast<long long>(
           std::chrono::steady_clock::now().time_since_epoch().count())));
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);
  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "agent start failed: " << error << "\n";
    (void)aclFinalize();
    return 1;
  }
  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  void* root_info_buf = nullptr;
  HcclRootInfo* root_info = nullptr;
  std::vector<HcclComm> precreated_comms(devices.size(), nullptr);

  if (init_mode == HcclInitMode::kRootInfo) {
    if (aclrtSetDevice(devices[0]) != ACL_SUCCESS) {
      std::cerr << "aclrtSetDevice(root) failed\n";
      agent.Stop();
      (void)aclFinalize();
      return 1;
    }

    if (aclrtMallocHost(&root_info_buf, sizeof(HcclRootInfo)) != ACL_SUCCESS) {
      std::cerr << "aclrtMallocHost(root_info) failed\n";
      agent.Stop();
      (void)aclFinalize();
      return 1;
    }
    root_info = static_cast<HcclRootInfo*>(root_info_buf);
    if (!root_info_path.empty()) {
      std::string file_error;
      if (!ReadRootInfoFile(root_info_path, root_info, &file_error)) {
        std::cerr << file_error << "\n";
        (void)aclrtFreeHost(root_info_buf);
        agent.Stop();
        (void)aclFinalize();
        return 1;
      }
    } else {
      if (HcclGetRootInfo(root_info) != HCCL_SUCCESS) {
        std::cerr << "HcclGetRootInfo failed\n";
        (void)aclrtFreeHost(root_info_buf);
        agent.Stop();
        (void)aclFinalize();
        return 1;
      }
      if (!root_info_out_path.empty()) {
        std::string file_error;
        if (!WriteRootInfoFile(root_info_out_path, *root_info, &file_error)) {
          std::cerr << file_error << "\n";
          (void)aclrtFreeHost(root_info_buf);
          agent.Stop();
          (void)aclFinalize();
          return 1;
        }
        std::cout << "root-info file written: " << root_info_out_path << "\n";
      }
    }
  } else if (init_mode == HcclInitMode::kAll) {
#if FLUME_HAVE_HCCL_COMM_INIT_ALL
    if (aclrtSetDevice(devices[0]) != ACL_SUCCESS) {
      std::cerr << "aclrtSetDevice(init-all root) failed\n";
      agent.Stop();
      (void)aclFinalize();
      return 1;
    }
    std::vector<int32_t> hccl_devices = devices;
    HcclResult init_ret = HcclCommInitAll(
        static_cast<uint32_t>(hccl_devices.size()), hccl_devices.data(),
        precreated_comms.data());
    if (init_ret != HCCL_SUCCESS) {
      std::cerr << "HcclCommInitAll failed, hccl ret="
                << HcclResultName(init_ret) << "("
                << static_cast<int>(init_ret) << ")\n";
      agent.Stop();
      (void)aclFinalize();
      return 1;
    }
#else
    std::cerr << "HcclCommInitAll is not available in this HCCL header\n";
    agent.Stop();
    (void)aclFinalize();
    return 1;
#endif
  }

  std::vector<RankContext> contexts(devices.size());
  std::vector<std::thread> threads;
  threads.reserve(contexts.size());
  for (uint32_t local_index = 0; local_index < contexts.size(); ++local_index) {
    uint32_t rank = single_rank_mode ? single_rank : local_index;
    contexts[local_index].endpoint = endpoint;
    contexts[local_index].root_info = root_info;
    contexts[local_index].rank_table_path = rank_table_path;
    contexts[local_index].init_mode = init_mode;
    contexts[local_index].precreated_comm = precreated_comms[local_index];
    precreated_comms[local_index] = nullptr;
    contexts[local_index].rank = rank;
    contexts[local_index].rank_size = global_rank_size;
    contexts[local_index].device = devices[local_index];
    contexts[local_index].count = count;
    contexts[local_index].a3_symmetric = a3_symmetric;
    contexts[local_index].p2p_copy = p2p_copy;
    contexts[local_index].hcomm_channel_probe = hcomm_channel_probe;
    contexts[local_index].hcomm_engine = hcomm_engine;
    contexts[local_index].hcomm_protocol = hcomm_protocol;
    contexts[local_index].hcomm_notify_num = hcomm_notify_num;
    contexts[local_index].hcomm_require_thread_export =
        hcomm_require_thread_export;
    contexts[local_index].sym_win_gb = sym_win_gb;
    threads.emplace_back(RankMain, &contexts[local_index]);
  }
  for (auto& thread : threads) {
    thread.join();
  }

  bool ok = true;
  for (const auto& ctx : contexts) {
    if (ctx.status != 0) {
      ok = false;
      std::cerr << "rank " << ctx.rank << " device " << ctx.device
                << " failed: " << ctx.error << "\n";
    }
  }

  agent.Stop();
  std::filesystem::remove_all(root);
  if (root_info_buf != nullptr) {
    (void)aclrtFreeHost(root_info_buf);
  }
  (void)aclFinalize();
  if (!ok) {
    return 1;
  }
  std::cout << "hccl collective smoke passed: global_rank_size="
            << global_rank_size
            << " local_ranks=" << devices.size()
            << " count=" << count
            << " init=" << HcclInitModeName(init_mode)
            << " a3_symmetric=" << (a3_symmetric ? "on" : "off")
            << " p2p_copy=" << (p2p_copy ? "on" : "off")
            << " hcomm_channel_probe="
            << (hcomm_channel_probe ? "on" : "off")
            << " hcomm_engine=" << HcommEngineName(hcomm_engine)
            << " resolved_hcomm_engine="
            << HcommEngineName(ResolveHcommSmokeEngine(hcomm_engine))
            << " hcomm_protocol=" << HcommProtocolName(hcomm_protocol)
            << " hcomm_notify_num=" << hcomm_notify_num
            << " hcomm_require_thread_export="
            << (hcomm_require_thread_export ? "on" : "off") << "\n";
  return 0;
}
