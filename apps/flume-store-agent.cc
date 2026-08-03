#include <signal.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#include "agent/storage_agent.h"

namespace {

std::atomic<bool> g_running{true};

void OnSignal(int) {
  g_running.store(false);
}

void Usage(const char* argv0) {
  std::cerr << "Usage: " << argv0
            << " [--listen 127.0.0.1:18080] [--root PATH]\n";
}

bool ParseListen(const std::string& value, std::string* host, uint16_t* port) {
  auto colon = value.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= value.size()) {
    return false;
  }
  *host = value.substr(0, colon);
  int parsed = 0;
  try {
    parsed = std::stoi(value.substr(colon + 1));
  } catch (...) {
    return false;
  }
  if (parsed <= 0 || parsed > 65535) {
    return false;
  }
  *port = static_cast<uint16_t>(parsed);
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  std::string host = "127.0.0.1";
  uint16_t port = 18080;
  std::string root = ".";

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--listen" && i + 1 < argc) {
      if (!ParseListen(argv[++i], &host, &port)) {
        Usage(argv[0]);
        return 2;
      }
    } else if (arg == "--root" && i + 1 < argc) {
      root = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      Usage(argv[0]);
      return 0;
    } else {
      Usage(argv[0]);
      return 2;
    }
  }

  signal(SIGINT, OnSignal);
  signal(SIGTERM, OnSignal);

  flume::StorageAgent agent(host, port, root);
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "failed to start agent: " << error << "\n";
    return 1;
  }

  std::cout << "flume-store-agent listening on " << agent.host() << ":"
            << agent.port() << " root=" << root << "\n";
  while (g_running.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  agent.Stop();
  return 0;
}
