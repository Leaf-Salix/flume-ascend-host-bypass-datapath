#include "roce_storage/cann_ra_loader.h"

#include <cstdlib>
#include <cstring>
#include <sstream>

#if defined(__unix__) || defined(__APPLE__)
#include <dlfcn.h>
#define FLUME_HAVE_DLOPEN 1
#else
#define FLUME_HAVE_DLOPEN 0
#endif

namespace flume::roce {
namespace {

#if FLUME_HAVE_DLOPEN
const char* LibraryName(const char* env_name, const char* fallback) {
  const char* value = std::getenv(env_name);
  return value != nullptr && value[0] != '\0' ? value : fallback;
}

template <typename Function>
bool LoadSymbol(void* library, const char* name, Function* out, std::string* error) {
  void* symbol = dlsym(library, name);
  if (symbol == nullptr) {
    if (error != nullptr) *error = std::string("missing CANN symbol ") + name;
    return false;
  }
  static_assert(sizeof(*out) == sizeof(symbol), "function pointer size is unsupported");
  std::memcpy(out, &symbol, sizeof(symbol));
  return true;
}

void* OpenLibrary(const char* env_name, const char* fallback, std::string* error) {
  const char* path = LibraryName(env_name, fallback);
  void* library = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (library == nullptr && error != nullptr) {
    const char* detail = dlerror();
    *error = std::string("failed to load ") + path + (detail == nullptr ? "" : ": ") +
             (detail == nullptr ? "" : detail);
  }
  return library;
}
#endif

}  // namespace

CannRaApi::~CannRaApi() { Close(); }

bool CannRaApi::Open(std::string* error) {
  Close();
#if !FLUME_HAVE_DLOPEN
  if (error != nullptr) *error = "dynamic library loading is unavailable on this host";
  return false;
#else
  ra_library_ = OpenLibrary("FLUME_CANN_RA_LIBRARY", "libra.so", error);
  if (ra_library_ == nullptr) return false;
  runtime_library_ = OpenLibrary("FLUME_CANN_RUNTIME_LIBRARY", "libruntime.so", error);
  if (runtime_library_ == nullptr) { Close(); return false; }
  acl_library_ = OpenLibrary("FLUME_CANN_ACL_LIBRARY", "libascendcl.so", error);
  if (acl_library_ == nullptr) { Close(); return false; }

#define FLUME_LOAD(lib, field, symbol) \
  if (!LoadSymbol(lib, symbol, &field, error)) { Close(); return false; }
  FLUME_LOAD(ra_library_, ra_init, "RaInit")
  FLUME_LOAD(ra_library_, ra_deinit, "RaDeinit")
  FLUME_LOAD(ra_library_, ra_rdev_init_v2, "RaRdevInitV2")
  FLUME_LOAD(ra_library_, ra_rdev_deinit, "RaRdevDeinit")
  FLUME_LOAD(ra_library_, ra_typical_qp_create, "RaTypicalQpCreate")
  FLUME_LOAD(ra_library_, ra_typical_qp_modify, "RaTypicalQpModify")
  FLUME_LOAD(ra_library_, ra_qp_destroy, "RaQpDestroy")
  FLUME_LOAD(ra_library_, ra_register_mr, "RaRegisterMr")
  FLUME_LOAD(ra_library_, ra_deregister_mr, "RaDeregisterMr")
  FLUME_LOAD(ra_library_, ra_typical_send_wr, "RaTypicalSendWr")
  FLUME_LOAD(runtime_library_, rt_set_device, "rtSetDevice")
  FLUME_LOAD(runtime_library_, rt_open_net_service, "rtOpenNetService")
  FLUME_LOAD(runtime_library_, rt_close_net_service, "rtCloseNetService")
  FLUME_LOAD(runtime_library_, rt_rdma_db_send, "rtRDMADBSend")
  FLUME_LOAD(acl_library_, acl_get_physical_device, "aclrtGetPhyDevIdByLogicDevId")
  FLUME_LOAD(acl_library_, acl_malloc, "aclrtMalloc")
  FLUME_LOAD(acl_library_, acl_free, "aclrtFree")
  FLUME_LOAD(acl_library_, acl_memcpy, "aclrtMemcpy")
#undef FLUME_LOAD
  available_ = true;
  return true;
#endif
}

void CannRaApi::Close() {
  available_ = false;
#if FLUME_HAVE_DLOPEN
  if (acl_library_ != nullptr) dlclose(acl_library_);
  if (runtime_library_ != nullptr) dlclose(runtime_library_);
  if (ra_library_ != nullptr) dlclose(ra_library_);
#endif
  acl_library_ = nullptr;
  runtime_library_ = nullptr;
  ra_library_ = nullptr;
#define FLUME_CLEAR(field) field = nullptr
  FLUME_CLEAR(ra_init); FLUME_CLEAR(ra_deinit); FLUME_CLEAR(ra_rdev_init_v2);
  FLUME_CLEAR(ra_rdev_deinit); FLUME_CLEAR(ra_typical_qp_create);
  FLUME_CLEAR(ra_typical_qp_modify); FLUME_CLEAR(ra_qp_destroy);
  FLUME_CLEAR(ra_register_mr); FLUME_CLEAR(ra_deregister_mr);
  FLUME_CLEAR(ra_typical_send_wr); FLUME_CLEAR(rt_set_device);
  FLUME_CLEAR(rt_open_net_service); FLUME_CLEAR(rt_close_net_service);
  FLUME_CLEAR(rt_rdma_db_send); FLUME_CLEAR(acl_get_physical_device);
  FLUME_CLEAR(acl_malloc); FLUME_CLEAR(acl_free); FLUME_CLEAR(acl_memcpy);
#undef FLUME_CLEAR
}

bool CannRaLoaderCompiled() { return FLUME_HAVE_DLOPEN != 0; }

}  // namespace flume::roce
