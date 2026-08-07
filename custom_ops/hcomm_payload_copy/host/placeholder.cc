#include "flume_hcomm_notify_only_abi.h"

extern "C" unsigned int FlumeHcommPayloadHostAbiVersion(void) {
  return FLUME_HCOMM_NOTIFY_ONLY_VERSION;
}
