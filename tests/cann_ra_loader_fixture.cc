#include "roce_storage/cann_ra_abi.h"

using namespace flume::roce::cann;

#if defined(FLUME_RA_FIXTURE)
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
extern "C" int RaQpDestroy(void*) { return 0; }
extern "C" int RaRegisterMr(const void*, MrInfo*, void** handle) {
  *handle = reinterpret_cast<void*>(1);
  return 0;
}
extern "C" int RaDeregisterMr(const void*, void*) { return 0; }
#elif defined(FLUME_RUNTIME_FIXTURE)
extern "C" int rtSetDevice(int32_t) { return 0; }
extern "C" int rtOpenNetService(const RtNetServiceOpenArgs*) { return 0; }
extern "C" int rtCloseNetService() { return 0; }
#elif defined(FLUME_ACL_FIXTURE)
extern "C" int aclrtGetPhyDevIdByLogicDevId(int32_t logical, int32_t* physical) {
  *physical = logical;
  return 0;
}
#endif
