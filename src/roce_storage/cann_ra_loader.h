#ifndef FLUME_ROCE_STORAGE_CANN_RA_LOADER_H_
#define FLUME_ROCE_STORAGE_CANN_RA_LOADER_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "roce_storage/cann_ra_abi.h"

namespace flume::roce {

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

  int (*ra_init)(cann::RaInitConfig*) = nullptr;
  int (*ra_deinit)(cann::RaInitConfig*) = nullptr;
  int (*ra_rdev_init_v2)(cann::RdevInitInfo, cann::Rdev, void**) = nullptr;
  int (*ra_rdev_deinit)(void*, unsigned) = nullptr;
  int (*ra_typical_qp_create)(void*, int, int, cann::TypicalQp*, void**) = nullptr;
  int (*ra_typical_qp_modify)(void*, cann::TypicalQp*, cann::TypicalQp*) = nullptr;
  int (*ra_qp_destroy)(void*) = nullptr;
  int (*ra_register_mr)(const void*, cann::MrInfo*, void**) = nullptr;
  int (*ra_deregister_mr)(const void*, void*) = nullptr;
  int (*ra_typical_send_wr)(void*, cann::SendWr*, cann::SendWrResponse*) = nullptr;
  int (*ra_poll_cq)(void*, bool, unsigned, void*) = nullptr;

  int (*rt_set_device)(int32_t) = nullptr;
  int (*rt_open_net_service)(const cann::RtNetServiceOpenArgs*) = nullptr;
  int (*rt_close_net_service)() = nullptr;
  int (*rt_rdma_db_send)(uint32_t, uint64_t, void*) = nullptr;

  int (*acl_get_physical_device)(int32_t, int32_t*) = nullptr;
  int (*acl_malloc)(void**, size_t, int) = nullptr;
  int (*acl_free)(void*) = nullptr;
  int (*acl_memcpy)(void*, size_t, const void*, size_t, int) = nullptr;

 private:
  void* ra_library_ = nullptr;
  void* runtime_library_ = nullptr;
  void* acl_library_ = nullptr;
  bool available_ = false;
  bool command_posting_available_ = false;
};

bool CannRaLoaderCompiled();

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_CANN_RA_LOADER_H_
