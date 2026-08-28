#ifndef FLUME_ROCE_STORAGE_CANN_RA_LOADER_H_
#define FLUME_ROCE_STORAGE_CANN_RA_LOADER_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "roce_storage/cann_ra_abi.h"

namespace flume::roce {

enum class CannRaSymbolProfile {
  kUnavailable,
  kModern,
  kLegacyLowercase,
  kMixed,
};

enum class CannRaRdevInitProfile {
  kUnavailable,
  kV2,
  kLegacy,
};

enum class CannRaNetServiceProfile {
  kUnavailable,
  kExplicitRuntime,
  kRaManaged,
};

// Isolates CANN/HCCP symbol and entry-point differences from the storage
// session. The public methods below are the only RA ABI used by Flume's data
// path; raw function pointers remain private to this adapter.
class CannRaApi {
 public:
  CannRaApi() = default;
  ~CannRaApi();
  CannRaApi(const CannRaApi&) = delete;
  CannRaApi& operator=(const CannRaApi&) = delete;

  bool Open(std::string* error);
  bool Open(bool require_command_posting, std::string* error);
  void Close();
  bool available() const { return available_; }
  bool command_posting_available() const { return command_posting_available_; }
  bool physical_device_lookup_available() const {
    return acl_get_physical_device_ != nullptr;
  }
  CannRaSymbolProfile symbol_profile() const { return symbol_profile_; }
  CannRaRdevInitProfile rdev_init_profile() const {
    return rdev_init_profile_;
  }
  CannRaNetServiceProfile net_service_profile() const {
    return net_service_profile_;
  }
  bool explicit_net_service_available() const {
    return net_service_profile_ == CannRaNetServiceProfile::kExplicitRuntime;
  }
  const char* symbol_profile_name() const;
  const char* rdev_init_profile_name() const;
  const char* net_service_profile_name() const;

  int SetDevice(int32_t logical_device) const;
  bool ResolvePhysicalDevice(int32_t logical_device,
                             int32_t explicit_physical_device,
                             uint32_t* physical_device,
                             std::string* error) const;
  int OpenNetService(const cann::RtNetServiceOpenArgs* args) const;
  int CloseNetService() const;
  int Init(cann::RaInitConfig* config) const;
  int Deinit(cann::RaInitConfig* config) const;
  int RdevInit(cann::RdevInitInfo init_info, cann::Rdev rdev,
               void** handle) const;
  int RdevDeinit(void* handle, unsigned notify_type) const;
  int TypicalQpCreate(void* rdma_handle, int qp_type, int qp_mode,
                      cann::TypicalQp* qp, void** handle) const;
  int TypicalQpModify(void* qp_handle, cann::TypicalQp* local,
                      cann::TypicalQp* remote) const;
  int QpDestroy(void* qp_handle) const;
  int RegisterMr(const void* rdma_handle, cann::MrInfo* info,
                 void** mr_handle) const;
  int DeregisterMr(const void* rdma_handle, void* mr_handle) const;
  int TypicalSendWr(void* qp_handle, cann::SendWr* wr,
                    cann::SendWrResponse* response) const;
  int PollCq(void* qp_handle, bool send, unsigned count, void* output) const;
  int RdmaDbSend(uint32_t index, uint64_t info, void* stream) const;
  int DeviceMalloc(void** address, size_t bytes, int policy) const;
  int DeviceFree(void* address) const;
  int DeviceMemcpy(void* destination, size_t destination_bytes,
                   const void* source, size_t bytes, int kind) const;

 private:
  using RaInitFn = int (*)(cann::RaInitConfig*);
  using RaRdevInitV2Fn = int (*)(cann::RdevInitInfo, cann::Rdev, void**);
  using RaRdevInitLegacyFn = int (*)(int, unsigned, cann::Rdev, void**);
  using RaRdevDeinitFn = int (*)(void*, unsigned);
  using RaTypicalQpCreateFn =
      int (*)(void*, int, int, cann::TypicalQp*, void**);
  using RaTypicalQpModifyFn =
      int (*)(void*, cann::TypicalQp*, cann::TypicalQp*);
  using RaQpDestroyFn = int (*)(void*);
  using RaRegisterMrFn = int (*)(const void*, cann::MrInfo*, void**);
  using RaDeregisterMrFn = int (*)(const void*, void*);
  using RaTypicalSendWrFn =
      int (*)(void*, cann::SendWr*, cann::SendWrResponse*);
  using RaPollCqFn = int (*)(void*, bool, unsigned, void*);

  RaInitFn ra_init_ = nullptr;
  RaInitFn ra_deinit_ = nullptr;
  RaRdevInitV2Fn ra_rdev_init_v2_ = nullptr;
  RaRdevInitLegacyFn ra_rdev_init_legacy_ = nullptr;
  RaRdevDeinitFn ra_rdev_deinit_ = nullptr;
  RaTypicalQpCreateFn ra_typical_qp_create_ = nullptr;
  RaTypicalQpModifyFn ra_typical_qp_modify_ = nullptr;
  RaQpDestroyFn ra_qp_destroy_ = nullptr;
  RaRegisterMrFn ra_register_mr_ = nullptr;
  RaDeregisterMrFn ra_deregister_mr_ = nullptr;
  RaTypicalSendWrFn ra_typical_send_wr_ = nullptr;
  RaPollCqFn ra_poll_cq_ = nullptr;

  int (*rt_set_device_)(int32_t) = nullptr;
  int (*rt_open_net_service_)(const cann::RtNetServiceOpenArgs*) = nullptr;
  int (*rt_close_net_service_)() = nullptr;
  int (*rt_rdma_db_send_)(uint32_t, uint64_t, void*) = nullptr;

  int (*acl_get_physical_device_)(int32_t, int32_t*) = nullptr;
  int (*acl_malloc_)(void**, size_t, int) = nullptr;
  int (*acl_free_)(void*) = nullptr;
  int (*acl_memcpy_)(void*, size_t, const void*, size_t, int) = nullptr;

  void* ra_library_ = nullptr;
  void* runtime_library_ = nullptr;
  void* acl_library_ = nullptr;
  CannRaSymbolProfile symbol_profile_ = CannRaSymbolProfile::kUnavailable;
  CannRaRdevInitProfile rdev_init_profile_ =
      CannRaRdevInitProfile::kUnavailable;
  CannRaNetServiceProfile net_service_profile_ =
      CannRaNetServiceProfile::kUnavailable;
  bool available_ = false;
  bool command_posting_available_ = false;
};

bool CannRaLoaderCompiled();

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_CANN_RA_LOADER_H_
