#include <sys/time.h>

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

int main() { return 0; }
