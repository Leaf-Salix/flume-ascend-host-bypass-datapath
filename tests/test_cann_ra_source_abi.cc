#include <sys/time.h>

#include <cstddef>

#include "network/hccp.h"
#include "roce_storage/cann_ra_abi.h"

static_assert(sizeof(TypicalQp) == sizeof(flume::roce::cann::TypicalQp),
              "Flume TypicalQp differs from the CANN source fixture");
static_assert(sizeof(MrInfoT) == sizeof(flume::roce::cann::MrInfo),
              "Flume MrInfo differs from the CANN source fixture");
static_assert(sizeof(SgList) == sizeof(flume::roce::cann::SgList),
              "Flume SgList differs from the CANN source fixture");
static_assert(sizeof(RaInitConfig) == sizeof(flume::roce::cann::RaInitConfig),
              "Flume RaInitConfig differs from the CANN source fixture");
static_assert(sizeof(rdev) == sizeof(flume::roce::cann::Rdev),
              "Flume rdev differs from the CANN source fixture");
static_assert(sizeof(RdevInitInfo) == sizeof(flume::roce::cann::RdevInitInfo),
              "Flume RdevInitInfo differs from the CANN source fixture");
static_assert(sizeof(SendWrRsp) == sizeof(flume::roce::cann::SendWrResponse),
              "Flume SendWrRsp differs from the CANN source fixture");
static_assert(sizeof(SendWr) == sizeof(flume::roce::cann::SendWr),
              "Flume SendWr differs from the CANN source fixture");
static_assert(sizeof(ibv_wc) == sizeof(flume::roce::cann::WorkCompletion),
              "Flume work completion differs from the CANN source fixture");
static_assert(offsetof(ibv_wc, status) ==
                  offsetof(flume::roce::cann::WorkCompletion, status),
              "Flume work completion status offset differs");
static_assert(offsetof(ibv_wc, byte_len) ==
                  offsetof(flume::roce::cann::WorkCompletion, byte_length),
              "Flume work completion byte length offset differs");
static_assert(offsetof(ibv_wc, wc_flags) ==
                  offsetof(flume::roce::cann::WorkCompletion, flags),
              "Flume work completion flags offset differs");

int main() { return 0; }
