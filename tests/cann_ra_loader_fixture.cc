#include "roce_storage/cann_ra_abi.h"

#include <cstdlib>
#include <cstring>

using namespace flume::roce::cann;

namespace {

int DeregisterResult() {
  const char* mode = std::getenv("FLUME_RA_FIXTURE_DEREG_MODE");
  return mode != nullptr && std::strcmp(mode, "error") == 0 ? -1 : 0;
}

}  // namespace

#if defined(FLUME_RA_LEGACY_FIXTURE)
extern "C" int ra_init(RaInitConfig*) { return 0; }
extern "C" int ra_deinit(RaInitConfig*) { return 0; }
extern "C" int ra_rdev_init(int, unsigned, Rdev, void** handle) {
  *handle = reinterpret_cast<void*>(1);
  return 0;
}
extern "C" int ra_rdev_deinit(void*, unsigned) { return 0; }
extern "C" int ra_typical_qp_create(void*, int, int, TypicalQp* qp,
                                      void** handle) {
#if defined(FLUME_RA_LEGACY_PUSH_FIXTURE)
  qp->qpn = 101;
  qp->psn = 202;
  qp->gid_index = 0;
  qp->gid[15] = 1;
#else
  (void)qp;
#endif
  *handle = reinterpret_cast<void*>(2);
  return 0;
}
extern "C" int ra_typical_qp_modify(void*, TypicalQp*, TypicalQp*) {
  return 0;
}
extern "C" int ra_qp_destroy(void*) { return 0; }
extern "C" int ra_register_mr(const void*, MrInfo* mr, void** handle) {
#if defined(FLUME_RA_LEGACY_PUSH_FIXTURE)
  mr->lkey = 0x55U;
  mr->rkey = 0x66U;
#else
  (void)mr;
#endif
  *handle = reinterpret_cast<void*>(3);
  return 0;
}
extern "C" int ra_deregister_mr(const void*, void*) {
  return DeregisterResult();
}
#if defined(FLUME_RA_LEGACY_PUSH_FIXTURE)
extern "C" int ra_send_wr(void*, SendWr* wr, SendWrResponse* response) {
  if (wr == nullptr || wr->buffers == nullptr || wr->buffer_count != 1 ||
      wr->buffers[0].length != 64 || wr->buffers[0].lkey != 0x55U ||
      wr->destination_address != 0x1000U || wr->rkey != 0x1234U ||
      wr->opcode != kRaWrRdmaWrite) {
    return -1;
  }
  response->db.index = 7;
  response->db.info = 9;
  return 0;
}
extern "C" int ra_poll_cq(void*, bool, unsigned count, void* output) {
  const char* mode = std::getenv("FLUME_RA_FIXTURE_POLL_MODE");
  if (mode != nullptr && std::strcmp(mode, "error") == 0) return -1;
  if (mode != nullptr && std::strcmp(mode, "timeout") == 0) return 0;
  if (count == 0 || output == nullptr) return -1;
  auto* completion = static_cast<WorkCompletion*>(output);
  completion->status = 0;
  completion->byte_length = 64;
  return 1;
}
#endif
#elif defined(FLUME_RA_PUSH_FIXTURE)
extern "C" int RaInit(RaInitConfig*) { return 0; }
extern "C" int RaDeinit(RaInitConfig*) { return 0; }
extern "C" int RaRdevInitV2(RdevInitInfo, Rdev, void** handle) {
  *handle = reinterpret_cast<void*>(1);
  return 0;
}
extern "C" int RaRdevDeinit(void*, unsigned) { return 0; }
extern "C" int RaTypicalQpCreate(void*, int, int, TypicalQp* qp,
                                  void** handle) {
  qp->qpn = 101;
  qp->psn = 202;
  qp->gid_index = 0;
  qp->gid[15] = 1;
  *handle = reinterpret_cast<void*>(2);
  return 0;
}
extern "C" int RaTypicalQpModify(void*, TypicalQp*, TypicalQp*) { return 0; }
extern "C" int RaPollCq(void*, bool send, unsigned count, void* output) {
  const char* mode = std::getenv("FLUME_RA_FIXTURE_POLL_MODE");
  if (mode != nullptr && std::strcmp(mode, "error") == 0) return -1;
  if (mode != nullptr && std::strcmp(mode, "timeout") == 0) return 0;
  if (!send || count == 0 || output == nullptr) return -1;
  auto* completion = static_cast<WorkCompletion*>(output);
  completion->status = 0;
  completion->byte_length = 64;
  return 1;
}
extern "C" int RaQpDestroy(void*) { return 0; }
extern "C" int RaRegisterMr(const void*, MrInfo* mr, void** handle) {
  mr->lkey = 0x55U;
  mr->rkey = 0x66U;
  *handle = reinterpret_cast<void*>(3);
  return 0;
}
extern "C" int RaDeregisterMr(const void*, void*) {
  return DeregisterResult();
}
extern "C" int RaTypicalSendWr(void*, SendWr* wr, SendWrResponse* response) {
  if (wr == nullptr || wr->buffers == nullptr || wr->buffer_count != 1 ||
      wr->buffers[0].length != 64 || wr->buffers[0].lkey != 0x55U ||
      wr->destination_address != 0x1000U || wr->rkey != 0x1234U ||
      wr->opcode != kRaWrRdmaWrite) {
    return -1;
  }
  response->db.index = 7;
  response->db.info = 9;
  return 0;
}
#elif defined(FLUME_RA_FIXTURE)
extern "C" int RaInit(RaInitConfig*) { return 0; }
extern "C" int RaDeinit(RaInitConfig*) { return 0; }
extern "C" int RaRdevInitV2(RdevInitInfo, Rdev, void** handle) {
  *handle = reinterpret_cast<void*>(1);
  return 0;
}
extern "C" int RaRdevDeinit(void*, unsigned) { return 0; }
extern "C" int RaTypicalQpCreate(void*, int, int, TypicalQp*, void** handle) {
  *handle = reinterpret_cast<void*>(1);
  return 0;
}
extern "C" int RaTypicalQpModify(void*, TypicalQp*, TypicalQp*) { return 0; }
extern "C" int RaPollCq(void*, bool, unsigned, void*) { return 0; }
extern "C" int RaQpDestroy(void*) { return 0; }
extern "C" int RaRegisterMr(const void*, MrInfo*, void** handle) {
  *handle = reinterpret_cast<void*>(1);
  return 0;
}
extern "C" int RaDeregisterMr(const void*, void*) {
  return DeregisterResult();
}
#elif defined(FLUME_RUNTIME_PUSH_FIXTURE)
extern "C" int rtSetDevice(int32_t) { return 0; }
#if !defined(FLUME_RUNTIME_NO_NET_SERVICE)
extern "C" int rtOpenNetService(const RtNetServiceOpenArgs*) { return 0; }
extern "C" int rtCloseNetService() { return 0; }
#endif
extern "C" int rtRDMADBSend(uint32_t index, uint64_t info, void* stream) {
  return index == 7 && info == 9 && stream != nullptr ? 0 : -1;
}
#elif defined(FLUME_RUNTIME_FIXTURE)
extern "C" int rtSetDevice(int32_t) { return 0; }
extern "C" int rtOpenNetService(const RtNetServiceOpenArgs*) { return 0; }
extern "C" int rtCloseNetService() { return 0; }
#elif defined(FLUME_RUNTIME_LEGACY_FIXTURE)
extern "C" int rtSetDevice(int32_t) { return 0; }
#elif defined(FLUME_ACL_PUSH_FIXTURE)
#include <cstdlib>
#include <cstring>

extern "C" int aclrtGetPhyDevIdByLogicDevId(int32_t logical,
                                               int32_t* physical) {
  *physical = logical;
  return 0;
}
extern "C" int aclrtMalloc(void** address, size_t bytes, int) {
  *address = std::malloc(bytes);
  return *address == nullptr ? -1 : 0;
}
extern "C" int aclrtFree(void* address) {
  std::free(address);
  return 0;
}
extern "C" int aclrtMemcpy(void* destination, size_t destination_bytes,
                             const void* source, size_t bytes, int) {
  if (destination == nullptr || source == nullptr || bytes > destination_bytes) {
    return -1;
  }
  std::memcpy(destination, source, bytes);
  return 0;
}
#elif defined(FLUME_ACL_FIXTURE)
extern "C" int aclrtGetPhyDevIdByLogicDevId(int32_t logical, int32_t* physical) {
  *physical = logical;
  return 0;
}
#elif defined(FLUME_ACL_LEGACY_FIXTURE)
extern "C" int flume_acl_legacy_fixture_anchor() { return 0; }
#endif
