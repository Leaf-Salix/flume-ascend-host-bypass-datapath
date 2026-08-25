#ifndef FLUME_ROCE_STORAGE_CONTROL_CHANNEL_H_
#define FLUME_ROCE_STORAGE_CONTROL_CHANNEL_H_

#include <string>

#include "roce_storage/roce_storage.h"

namespace flume::roce {

enum class ControlReadResult {
  kSuccess,
  kPeerClosed,
  kError,
};

bool ControlWriteAll(int fd, const void* src, size_t bytes, std::string* error);
ControlReadResult ControlReadAll(int fd, void* dst, size_t bytes,
                                 std::string* error);
bool SendCommand(int fd, const Command& command, std::string* error);
ControlReadResult ReceiveCommand(int fd, Command* command, std::string* error);
bool SendCompletion(int fd, const Completion& completion, std::string* error);
ControlReadResult ReceiveCompletion(int fd, Completion* completion,
                                    std::string* error);

}  // namespace flume::roce

#endif  // FLUME_ROCE_STORAGE_CONTROL_CHANNEL_H_
