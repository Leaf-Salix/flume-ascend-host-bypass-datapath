#ifndef FLUME_TEST_UTIL_H_
#define FLUME_TEST_UTIL_H_

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <system_error>

inline void FlumeTestCheck(bool ok, const char* expr, const char* file, int line) {
  if (!ok) {
    std::cerr << file << ":" << line << ": check failed: " << expr << "\n";
    std::exit(1);
  }
}

#define FLUME_TEST_CHECK(expr) FlumeTestCheck((expr), #expr, __FILE__, __LINE__)

inline void FlumeTestRemoveAll(const std::filesystem::path& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec && ec != std::errc::no_such_file_or_directory) {
    std::cerr << "remove_all failed for " << path << ": " << ec.message()
              << "\n";
    std::exit(1);
  }
}

#endif  // FLUME_TEST_UTIL_H_
