#include "roce_storage/cann_ra_loader.h"

#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <sstream>

#if defined(__unix__) || defined(__APPLE__)
#include <dlfcn.h>
#define FLUME_HAVE_DLOPEN 1
#else
#define FLUME_HAVE_DLOPEN 0
#endif

namespace flume::roce {
namespace {

constexpr int kUnavailable = -1;

#if FLUME_HAVE_DLOPEN
const char* LibraryName(const char* env_name, const char* fallback) {
  const char* value = std::getenv(env_name);
  return value != nullptr && value[0] != '\0' ? value : fallback;
}

enum class SymbolStyle {
  kModern,
  kLegacyLowercase,
};

template <typename Function>
bool LoadSymbolAny(void* library,
                   std::initializer_list<const char*> names,
                   Function* out,
                   SymbolStyle* style,
                   std::string* error,
                   bool required = true) {
  size_t index = 0;
  for (const char* name : names) {
    dlerror();
    void* symbol = dlsym(library, name);
    if (symbol != nullptr) {
      static_assert(sizeof(*out) == sizeof(symbol),
                    "function pointer size is unsupported");
      std::memcpy(out, &symbol, sizeof(symbol));
      if (style != nullptr) {
        *style = index == 0 ? SymbolStyle::kModern
                            : SymbolStyle::kLegacyLowercase;
      }
      return true;
    }
    ++index;
  }
  if (!required) return false;
  if (error != nullptr) {
    std::ostringstream message;
    message << "missing CANN symbol ";
    size_t name_index = 0;
    for (const char* name : names) {
      if (name_index++ != 0) message << '/';
      message << name;
    }
    *error = message.str();
  }
  return false;
}

void UpdateProfile(SymbolStyle style, bool* modern, bool* legacy) {
  if (style == SymbolStyle::kModern) {
    *modern = true;
  } else {
    *legacy = true;
  }
}

void* OpenLibrary(const char* env_name, const char* fallback,
                  std::string* error) {
  const char* path = LibraryName(env_name, fallback);
  void* library = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (library == nullptr && error != nullptr) {
    const char* detail = dlerror();
    *error = std::string("failed to load ") + path +
             (detail == nullptr ? "" : ": ") +
             (detail == nullptr ? "" : detail);
  }
  return library;
}

void* OpenOptionalLibrary(const char* env_name, const char* fallback,
                          std::string* error) {
  const char* configured = std::getenv(env_name);
  const char* path = configured != nullptr && configured[0] != '\0'
                         ? configured
                         : fallback;
  void* library = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (library == nullptr && configured != nullptr && configured[0] != '\0' &&
      error != nullptr) {
    const char* detail = dlerror();
    *error = std::string("failed to load ") + path +
             (detail == nullptr ? "" : ": ") +
             (detail == nullptr ? "" : detail);
  }
  return library;
}
#endif

}  // namespace

CannRaApi::~CannRaApi() { Close(); }

bool CannRaApi::Open(std::string* error) { return Open(true, error); }

bool CannRaApi::Open(bool require_command_posting, std::string* error) {
  Close();
#if !FLUME_HAVE_DLOPEN
  if (error != nullptr) {
    *error = "dynamic library loading is unavailable on this host";
  }
  return false;
#else
  ra_library_ = OpenLibrary("FLUME_CANN_RA_LIBRARY", "libra.so", error);
  if (ra_library_ == nullptr) return false;
  runtime_library_ =
      OpenLibrary("FLUME_CANN_RUNTIME_LIBRARY", "libruntime.so", error);
  if (runtime_library_ == nullptr) {
    Close();
    return false;
  }
  acl_library_ =
      OpenLibrary("FLUME_CANN_ACL_LIBRARY", "libascendcl.so", error);
  if (acl_library_ == nullptr) {
    Close();
    return false;
  }

  bool modern = false;
  bool legacy = false;
  SymbolStyle style = SymbolStyle::kModern;
#define FLUME_LOAD_RA(field, modern_name, legacy_name)                         \
  do {                                                                         \
    if (!LoadSymbolAny(ra_library_, {modern_name, legacy_name}, &field,       \
                       &style, error)) {                                       \
      Close();                                                                 \
      return false;                                                            \
    }                                                                          \
    UpdateProfile(style, &modern, &legacy);                                   \
  } while (0)
#define FLUME_LOAD(lib, field, symbol)                                         \
  do {                                                                         \
    if (!LoadSymbolAny(lib, {symbol}, &field, nullptr, error)) {              \
      Close();                                                                 \
      return false;                                                            \
    }                                                                          \
  } while (0)

  FLUME_LOAD_RA(ra_init_, "RaInit", "ra_init");
  FLUME_LOAD_RA(ra_deinit_, "RaDeinit", "ra_deinit");
  if (LoadSymbolAny(ra_library_, {"RaRdevInitV2", "ra_rdev_init_v2"},
                    &ra_rdev_init_v2_, &style, nullptr, false)) {
    UpdateProfile(style, &modern, &legacy);
    rdev_init_profile_ = CannRaRdevInitProfile::kV2;
  } else if (LoadSymbolAny(ra_library_, {"RaRdevInit", "ra_rdev_init"},
                           &ra_rdev_init_legacy_, &style, error)) {
    UpdateProfile(style, &modern, &legacy);
    rdev_init_profile_ = CannRaRdevInitProfile::kLegacy;
  } else {
    Close();
    return false;
  }
  FLUME_LOAD_RA(ra_rdev_deinit_, "RaRdevDeinit", "ra_rdev_deinit");
  FLUME_LOAD_RA(ra_typical_qp_create_, "RaTypicalQpCreate",
                "ra_typical_qp_create");
  FLUME_LOAD_RA(ra_typical_qp_modify_, "RaTypicalQpModify",
                "ra_typical_qp_modify");
  FLUME_LOAD_RA(ra_qp_destroy_, "RaQpDestroy", "ra_qp_destroy");
  FLUME_LOAD_RA(ra_register_mr_, "RaRegisterMr", "ra_register_mr");
  FLUME_LOAD_RA(ra_deregister_mr_, "RaDeregisterMr", "ra_deregister_mr");
  FLUME_LOAD(runtime_library_, rt_set_device_, "rtSetDevice");
  const bool have_open_net_service =
      LoadSymbolAny(runtime_library_, {"rtOpenNetService"},
                    &rt_open_net_service_, nullptr, nullptr, false);
  const bool have_close_net_service =
      LoadSymbolAny(runtime_library_, {"rtCloseNetService"},
                    &rt_close_net_service_, nullptr, nullptr, false);
  if (have_open_net_service != have_close_net_service) {
    if (error != nullptr) {
      *error = "incomplete CANN NetService API: rtOpenNetService and "
               "rtCloseNetService must be exported together";
    }
    Close();
    return false;
  }
  if (have_open_net_service) {
    network_bootstrap_profile_ =
        CannRaNetworkBootstrapProfile::kExplicitRuntime;
  } else {
    tsd_library_ =
        OpenOptionalLibrary("FLUME_CANN_TSD_LIBRARY", "libtsdclient.so",
                            error);
    if (tsd_library_ == nullptr ||
        !LoadSymbolAny(tsd_library_, {"TsdOpen"}, &tsd_open_, nullptr,
                       error) ||
        !LoadSymbolAny(tsd_library_, {"TsdClose"}, &tsd_close_, nullptr,
                       error)) {
      if (error != nullptr && error->empty()) {
        *error = "CANN runtime has no NetService API and legacy "
                 "libtsdclient.so is unavailable";
      }
      Close();
      return false;
    }
    network_bootstrap_profile_ = CannRaNetworkBootstrapProfile::kLegacyTsd;
  }
  LoadSymbolAny(acl_library_, {"aclrtGetPhyDevIdByLogicDevId"},
                &acl_get_physical_device_, nullptr, nullptr, false);
  if (require_command_posting) {
    if (!LoadSymbolAny(ra_library_,
                       {"RaTypicalSendWr", "ra_typical_send_wr", "ra_send_wr"},
                       &ra_typical_send_wr_, &style, error)) {
      Close();
      return false;
    }
    UpdateProfile(style, &modern, &legacy);
    FLUME_LOAD_RA(ra_poll_cq_, "RaPollCq", "ra_poll_cq");
    FLUME_LOAD(runtime_library_, rt_rdma_db_send_, "rtRDMADBSend");
    FLUME_LOAD(acl_library_, acl_malloc_, "aclrtMalloc");
    FLUME_LOAD(acl_library_, acl_free_, "aclrtFree");
    FLUME_LOAD(acl_library_, acl_memcpy_, "aclrtMemcpy");
    command_posting_available_ = true;
  }
#undef FLUME_LOAD
#undef FLUME_LOAD_RA

  symbol_profile_ = modern && legacy ? CannRaSymbolProfile::kMixed
                    : legacy         ? CannRaSymbolProfile::kLegacyLowercase
                                     : CannRaSymbolProfile::kModern;
  available_ = true;
  return true;
#endif
}

void CannRaApi::Close() {
  available_ = false;
  command_posting_available_ = false;
  symbol_profile_ = CannRaSymbolProfile::kUnavailable;
  rdev_init_profile_ = CannRaRdevInitProfile::kUnavailable;
  network_bootstrap_profile_ =
      CannRaNetworkBootstrapProfile::kUnavailable;
#if FLUME_HAVE_DLOPEN
  if (tsd_library_ != nullptr) dlclose(tsd_library_);
  if (acl_library_ != nullptr) dlclose(acl_library_);
  if (runtime_library_ != nullptr) dlclose(runtime_library_);
  if (ra_library_ != nullptr) dlclose(ra_library_);
#endif
  acl_library_ = nullptr;
  tsd_library_ = nullptr;
  runtime_library_ = nullptr;
  ra_library_ = nullptr;
#define FLUME_CLEAR(field) field = nullptr
  FLUME_CLEAR(ra_init_);
  FLUME_CLEAR(ra_deinit_);
  FLUME_CLEAR(ra_rdev_init_v2_);
  FLUME_CLEAR(ra_rdev_init_legacy_);
  FLUME_CLEAR(ra_rdev_deinit_);
  FLUME_CLEAR(ra_typical_qp_create_);
  FLUME_CLEAR(ra_typical_qp_modify_);
  FLUME_CLEAR(ra_qp_destroy_);
  FLUME_CLEAR(ra_register_mr_);
  FLUME_CLEAR(ra_deregister_mr_);
  FLUME_CLEAR(ra_typical_send_wr_);
  FLUME_CLEAR(ra_poll_cq_);
  FLUME_CLEAR(rt_set_device_);
  FLUME_CLEAR(rt_open_net_service_);
  FLUME_CLEAR(rt_close_net_service_);
  FLUME_CLEAR(rt_rdma_db_send_);
  FLUME_CLEAR(tsd_open_);
  FLUME_CLEAR(tsd_close_);
  FLUME_CLEAR(acl_get_physical_device_);
  FLUME_CLEAR(acl_malloc_);
  FLUME_CLEAR(acl_free_);
  FLUME_CLEAR(acl_memcpy_);
#undef FLUME_CLEAR
}

const char* CannRaApi::symbol_profile_name() const {
  switch (symbol_profile_) {
    case CannRaSymbolProfile::kModern:
      return "modern-camelcase";
    case CannRaSymbolProfile::kLegacyLowercase:
      return "legacy-lowercase";
    case CannRaSymbolProfile::kMixed:
      return "mixed";
    case CannRaSymbolProfile::kUnavailable:
      return "unavailable";
  }
  return "unavailable";
}

const char* CannRaApi::rdev_init_profile_name() const {
  switch (rdev_init_profile_) {
    case CannRaRdevInitProfile::kV2:
      return "rdev-init-v2";
    case CannRaRdevInitProfile::kLegacy:
      return "rdev-init-legacy";
    case CannRaRdevInitProfile::kUnavailable:
      return "unavailable";
  }
  return "unavailable";
}

const char* CannRaApi::network_bootstrap_profile_name() const {
  switch (network_bootstrap_profile_) {
    case CannRaNetworkBootstrapProfile::kExplicitRuntime:
      return "explicit-runtime";
    case CannRaNetworkBootstrapProfile::kLegacyTsd:
      return "legacy-tsd";
    case CannRaNetworkBootstrapProfile::kUnavailable:
      return "unavailable";
  }
  return "unavailable";
}

int CannRaApi::EffectiveHdcType(int requested_hdc_type) const {
  return network_bootstrap_profile_ ==
                 CannRaNetworkBootstrapProfile::kLegacyTsd
             ? cann::kLegacyHdcType
             : requested_hdc_type;
}

int CannRaApi::SetDevice(int32_t logical_device) const {
  return rt_set_device_ == nullptr ? kUnavailable
                                   : rt_set_device_(logical_device);
}

bool CannRaApi::ResolvePhysicalDevice(int32_t logical_device,
                                      int32_t explicit_physical_device,
                                      uint32_t* physical_device,
                                      std::string* error) const {
  if (physical_device == nullptr) {
    if (error != nullptr) *error = "physical device output is null";
    return false;
  }
  if (explicit_physical_device >= 0) {
    *physical_device = static_cast<uint32_t>(explicit_physical_device);
    return true;
  }
  if (acl_get_physical_device_ == nullptr) {
    if (error != nullptr) {
      *error = "aclrtGetPhyDevIdByLogicDevId is unavailable in this CANN "
               "build; provide an explicit physical device id";
    }
    return false;
  }
  int32_t resolved = -1;
  if (acl_get_physical_device_(logical_device, &resolved) != cann::kSuccess ||
      resolved < 0) {
    if (error != nullptr) *error = "failed to resolve physical device id";
    return false;
  }
  *physical_device = static_cast<uint32_t>(resolved);
  return true;
}

int CannRaApi::BootstrapNetwork(int32_t logical_device, int hdc_type) const {
  if (network_bootstrap_profile_ ==
      CannRaNetworkBootstrapProfile::kExplicitRuntime) {
    const std::string hdc_arg = "--hdcType=" + std::to_string(hdc_type);
    cann::RtProcExtParam parameter{hdc_arg.c_str(), hdc_arg.size()};
    cann::RtNetServiceOpenArgs open_args{&parameter, 1};
    return rt_open_net_service_ == nullptr ? kUnavailable
                                           : rt_open_net_service_(&open_args);
  }
  if (network_bootstrap_profile_ ==
      CannRaNetworkBootstrapProfile::kLegacyTsd) {
    // The public legacy TSD contract starts HCCP only when rankSize > 1.
    constexpr uint32_t kBootstrapRankSize = 2;
    return tsd_open_ == nullptr
               ? kUnavailable
               : tsd_open_(static_cast<uint32_t>(logical_device),
                           kBootstrapRankSize);
  }
  return kUnavailable;
}

int CannRaApi::ShutdownNetwork(int32_t logical_device) const {
  if (network_bootstrap_profile_ ==
      CannRaNetworkBootstrapProfile::kExplicitRuntime) {
    return rt_close_net_service_ == nullptr ? kUnavailable
                                            : rt_close_net_service_();
  }
  if (network_bootstrap_profile_ ==
      CannRaNetworkBootstrapProfile::kLegacyTsd) {
    return tsd_close_ == nullptr
               ? kUnavailable
               : tsd_close_(static_cast<uint32_t>(logical_device));
  }
  return kUnavailable;
}

int CannRaApi::Init(cann::RaInitConfig* config) const {
  return ra_init_ == nullptr ? kUnavailable : ra_init_(config);
}

int CannRaApi::Deinit(cann::RaInitConfig* config) const {
  return ra_deinit_ == nullptr ? kUnavailable : ra_deinit_(config);
}

int CannRaApi::RdevInit(cann::RdevInitInfo init_info, cann::Rdev rdev,
                        void** handle) const {
  if (ra_rdev_init_v2_ != nullptr) {
    return ra_rdev_init_v2_(init_info, rdev, handle);
  }
  return ra_rdev_init_legacy_ == nullptr
             ? kUnavailable
             : ra_rdev_init_legacy_(init_info.mode, init_info.notify_type,
                                    rdev, handle);
}

int CannRaApi::RdevDeinit(void* handle, unsigned notify_type) const {
  return ra_rdev_deinit_ == nullptr ? kUnavailable
                                    : ra_rdev_deinit_(handle, notify_type);
}

int CannRaApi::TypicalQpCreate(void* rdma_handle, int qp_type, int qp_mode,
                               cann::TypicalQp* qp, void** handle) const {
  return ra_typical_qp_create_ == nullptr
             ? kUnavailable
             : ra_typical_qp_create_(rdma_handle, qp_type, qp_mode, qp,
                                     handle);
}

int CannRaApi::TypicalQpModify(void* qp_handle, cann::TypicalQp* local,
                               cann::TypicalQp* remote) const {
  return ra_typical_qp_modify_ == nullptr
             ? kUnavailable
             : ra_typical_qp_modify_(qp_handle, local, remote);
}

int CannRaApi::QpDestroy(void* qp_handle) const {
  return ra_qp_destroy_ == nullptr ? kUnavailable : ra_qp_destroy_(qp_handle);
}

int CannRaApi::RegisterMr(const void* rdma_handle, cann::MrInfo* info,
                          void** mr_handle) const {
  return ra_register_mr_ == nullptr
             ? kUnavailable
             : ra_register_mr_(rdma_handle, info, mr_handle);
}

int CannRaApi::DeregisterMr(const void* rdma_handle, void* mr_handle) const {
  return ra_deregister_mr_ == nullptr
             ? kUnavailable
             : ra_deregister_mr_(rdma_handle, mr_handle);
}

int CannRaApi::TypicalSendWr(void* qp_handle, cann::SendWr* wr,
                             cann::SendWrResponse* response) const {
  return ra_typical_send_wr_ == nullptr
             ? kUnavailable
             : ra_typical_send_wr_(qp_handle, wr, response);
}

int CannRaApi::PollCq(void* qp_handle, bool send, unsigned count,
                      void* output) const {
  return ra_poll_cq_ == nullptr ? kUnavailable
                                : ra_poll_cq_(qp_handle, send, count, output);
}

int CannRaApi::RdmaDbSend(uint32_t index, uint64_t info, void* stream) const {
  return rt_rdma_db_send_ == nullptr ? kUnavailable
                                     : rt_rdma_db_send_(index, info, stream);
}

int CannRaApi::DeviceMalloc(void** address, size_t bytes, int policy) const {
  return acl_malloc_ == nullptr ? kUnavailable
                                : acl_malloc_(address, bytes, policy);
}

int CannRaApi::DeviceFree(void* address) const {
  return acl_free_ == nullptr ? kUnavailable : acl_free_(address);
}

int CannRaApi::DeviceMemcpy(void* destination, size_t destination_bytes,
                            const void* source, size_t bytes, int kind) const {
  return acl_memcpy_ == nullptr
             ? kUnavailable
             : acl_memcpy_(destination, destination_bytes, source, bytes,
                           kind);
}

bool CannRaLoaderCompiled() { return FLUME_HAVE_DLOPEN != 0; }

}  // namespace flume::roce
