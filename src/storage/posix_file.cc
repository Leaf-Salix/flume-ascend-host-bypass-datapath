#include "storage/posix_file.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <new>

namespace flume {

PosixFileStore::PosixFileStore(std::string root) : root_(std::move(root)) {}

bool PosixFileStore::Open(const std::string& path, OpenedFile* file,
                          std::string* error) const {
  if (file == nullptr) {
    if (error != nullptr) {
      *error = "file output pointer is null";
    }
    return false;
  }

  std::string full_path = ResolvePath(path, error);
  if (full_path.empty()) {
    return false;
  }

  int fd = open(full_path.c_str(), O_RDONLY);
  if (fd < 0) {
    if (error != nullptr) {
      *error = std::string("open failed: ") + strerror(errno);
    }
    return false;
  }

  struct stat st = {};
  if (fstat(fd, &st) != 0) {
    if (error != nullptr) {
      *error = std::string("fstat failed: ") + strerror(errno);
    }
    close(fd);
    return false;
  }
  if (!S_ISREG(st.st_mode)) {
    if (error != nullptr) {
      *error = "path is not a regular file";
    }
    close(fd);
    return false;
  }

  file->fd = fd;
  file->size = static_cast<uint64_t>(st.st_size);
  file->path = full_path;
  return true;
}

bool PosixFileStore::Read(int fd, uint64_t offset, uint64_t length,
                          std::vector<uint8_t>* data, std::string* error) const {
  if (data == nullptr) {
    if (error != nullptr) {
      *error = "data output pointer is null";
    }
    return false;
  }
  if (length > static_cast<uint64_t>(SIZE_MAX)) {
    if (error != nullptr) {
      *error = "read length exceeds size_t";
    }
    return false;
  }

  try {
    data->assign(static_cast<size_t>(length), 0);
  } catch (const std::bad_alloc&) {
    if (error != nullptr) {
      *error = "read length allocation failed";
    }
    return false;
  }
  size_t done = 0;
  while (done < data->size()) {
    ssize_t n = pread(fd, data->data() + done, data->size() - done,
                      static_cast<off_t>(offset + done));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (error != nullptr) {
        *error = std::string("pread failed: ") + strerror(errno);
      }
      return false;
    }
    if (n == 0) {
      data->resize(done);
      return true;
    }
    done += static_cast<size_t>(n);
  }
  return true;
}

void PosixFileStore::Close(int fd) const {
  if (fd >= 0) {
    close(fd);
  }
}

std::string PosixFileStore::ResolvePath(const std::string& path,
                                        std::string* error) const {
  namespace fs = std::filesystem;

  if (path.empty()) {
    if (error != nullptr) {
      *error = "path is empty";
    }
    return {};
  }
  if (path.find('\0') != std::string::npos) {
    if (error != nullptr) {
      *error = "path contains NUL byte";
    }
    return {};
  }
  fs::path requested(path);
  if (requested.is_absolute()) {
    if (error != nullptr) {
      *error = "absolute paths are not allowed";
    }
    return {};
  }
  for (const auto& part : requested) {
    if (part == "..") {
      if (error != nullptr) {
        *error = "parent path components are not allowed";
      }
      return {};
    }
  }

  fs::path root;
  fs::path full;
  try {
    root = fs::weakly_canonical(root_);
    full = fs::weakly_canonical(root / requested);
  } catch (const fs::filesystem_error& exc) {
    if (error != nullptr) {
      *error = std::string("path canonicalization failed: ") + exc.what();
    }
    return {};
  }
  auto root_str = root.string();
  auto full_str = full.string();
  if (root_str.size() > 1 && root_str.back() == '/') {
    root_str.pop_back();
  }
  if (root_str != "/" &&
      (full_str.size() < root_str.size() ||
      full_str.compare(0, root_str.size(), root_str) != 0 ||
      (full_str.size() > root_str.size() && full_str[root_str.size()] != '/'))) {
    if (error != nullptr) {
      *error = "path escapes storage root";
    }
    return {};
  }
  return full_str;
}

}  // namespace flume
