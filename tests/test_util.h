#ifndef FLUME_TEST_UTIL_H_
#define FLUME_TEST_UTIL_H_

#include <cstdlib>
#include <iostream>

inline void FlumeTestCheck(bool ok, const char* expr, const char* file, int line) {
  if (!ok) {
    std::cerr << file << ":" << line << ": check failed: " << expr << "\n";
    std::exit(1);
  }
}

#define FLUME_TEST_CHECK(expr) FlumeTestCheck((expr), #expr, __FILE__, __LINE__)

#endif  // FLUME_TEST_UTIL_H_
