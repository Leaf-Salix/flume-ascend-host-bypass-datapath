#ifndef FLUME_STORAGE_POSIX_FILE_H_
#define FLUME_STORAGE_POSIX_FILE_H_

#include <stdint.h>

#include <string>
#include <vector>

namespace flume {

struct OpenedFile {
  int fd = -1;
  uint64_t size = 0;
  std::string path;
};

class PosixFileStore {
 public:
  explicit PosixFileStore(std::string root);
  ~PosixFileStore() = default;

  bool Open(const std::string& path, OpenedFile* file, std::string* error) const;
  bool Read(int fd, uint64_t offset, uint64_t length, std::vector<uint8_t>* data,
            std::string* error) const;
  void Close(int fd) const;

 private:
  std::string ResolvePath(const std::string& path, std::string* error) const;

  std::string root_;
};

}  // namespace flume

#endif  // FLUME_STORAGE_POSIX_FILE_H_
