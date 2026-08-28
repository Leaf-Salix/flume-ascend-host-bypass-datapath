#ifndef FLUME_ROCE_STORAGE_CANN_RA_ABI_H_
#define FLUME_ROCE_STORAGE_CANN_RA_ABI_H_

#include <cstddef>
#include <cstdint>
#include <netinet/in.h>

// Minimal ABI surface for symbols exported by the CANN runtime and HCCP RA
// libraries. These declarations are independently reduced from the public
// CANN hccp.h/hccp_common.h and runtime API declarations; no implementation
// code is included here.
namespace flume::roce::cann {

constexpr int kSuccess = 0;
constexpr int kNetworkOffline = 1;
constexpr unsigned kNicDeploymentDevice = 1;
constexpr unsigned kNotify = 1;
// RDMA_V2 is process-scoped and is the mode used by current HCCL network
// manager paths. Legacy RDMA (6) is intentionally not selected implicitly.
constexpr int kDefaultHdcType = 18;
constexpr int kLegacyHdcType = 6;
constexpr int kOpbaseQpMode = 2;
constexpr int kRaAccessLocalWrite = 1;
constexpr int kRaAccessRemoteWrite = 1 << 1;
constexpr int kRaAccessRemoteRead = 1 << 2;
constexpr int kRaSendSignaled = 1 << 1;
constexpr uint32_t kRaWrRdmaWrite = 0;
constexpr uint32_t kRaWrSend = 2;
constexpr int kAclMallocNormalOnly = 2;
constexpr int kAclMemcpyHostToDevice = 1;
constexpr int kAclMemcpyDeviceToHost = 2;

union HccpIpAddr {
  in_addr addr;
  in6_addr addr6;
};

struct RaInitConfig {
  unsigned phy_id;
  unsigned nic_position;
  int hdc_type;
  bool enable_hdc_async;
};

struct Rdev {
  unsigned phy_id;
  int family;
  HccpIpAddr local_ip;
};

struct RdevInitInfo {
  int mode;
  unsigned notify_type;
  bool enabled_910a_lite;
  bool disabled_lite_thread;
  bool enabled_2mb_lite;
};

struct TypicalQp {
  uint32_t qpn;
  uint32_t psn;
  uint32_t gid_index;
  uint8_t reserved1[4];
  uint8_t gid[16];
  uint32_t traffic_class;
  uint32_t service_level;
  uint32_t retry_count;
  uint32_t retry_time;
  int version;
  uint32_t reserved[32];
  uint8_t reserved2[4];
};

struct MrInfo {
  void* address;
  unsigned long long size;
  int access;
  unsigned lkey;
  unsigned rkey;
};

struct SgList {
  uint64_t address;
  uint32_t length;
  uint32_t lkey;
};

struct DbInfo {
  unsigned index;
  unsigned long info;
};

union SendWrResponse {
  uint8_t opaque[16];
  DbInfo db;
};

struct SendWr {
  SgList* buffers;
  uint16_t buffer_count;
  uint64_t destination_address;
  uint32_t rkey;
  uint32_t opcode;
  int send_flags;
};

// Public HCCP RaPollCq writes the standard ibv_wc layout. This reduced ABI
// avoids making the NPU-RA backend depend on a host rdma-core installation.
struct WorkCompletion {
  uint64_t wr_id;
  int status;
  int opcode;
  uint32_t vendor_error;
  uint32_t byte_length;
  uint32_t immediate_data;
  uint32_t qp_number;
  uint32_t source_qp;
  uint32_t flags;
  uint16_t pkey_index;
  uint16_t source_lid;
  uint8_t service_level;
  uint8_t destination_path_bits;
};

struct RtProcExtParam {
  const char* param_info;
  uint64_t param_length;
};

struct RtNetServiceOpenArgs {
  RtProcExtParam* params;
  uint64_t param_count;
};

static_assert(sizeof(TypicalQp) == 184, "CANN TypicalQp ABI size changed");
static_assert(sizeof(MrInfo) == 32, "CANN MrInfoT ABI size changed");
static_assert(sizeof(SgList) == 16, "CANN SgList ABI size changed");
static_assert(sizeof(RaInitConfig) == 16, "CANN RaInitConfig ABI size changed");
static_assert(sizeof(Rdev) == 24, "CANN rdev ABI size changed");
static_assert(sizeof(RdevInitInfo) == 12, "CANN RdevInitInfo ABI size changed");
static_assert(sizeof(SendWrResponse) == 16, "CANN SendWrRsp ABI size changed");
static_assert(sizeof(SendWr) == 40, "CANN SendWr ABI size changed");
static_assert(sizeof(WorkCompletion) == 48,
              "CANN/HCCP work completion ABI size changed");

}  // namespace flume::roce::cann

#endif  // FLUME_ROCE_STORAGE_CANN_RA_ABI_H_
