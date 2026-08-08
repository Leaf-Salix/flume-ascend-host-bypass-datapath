#include "test_util.h"

#include <filesystem>
#include <string>

#include "agent/storage_agent.h"
#include "flume/flume.h"

int main() {
  flume_backend_caps_t global_caps = {};
  global_caps.size = sizeof(global_caps);
  FLUME_TEST_CHECK(flume_get_backend_caps(nullptr, &global_caps) == FLUME_OK);
  FLUME_TEST_CHECK(global_caps.size == sizeof(global_caps));
  FLUME_TEST_CHECK(global_caps.hcomm_payload_scheduler == 0);
  FLUME_TEST_CHECK(global_caps.hcomm_payload_scheduler_candidate == 0);
  FLUME_TEST_CHECK(global_caps.storage_hbm == 0);

  flume_backend_caps_t too_small = {};
  too_small.size = 1;
  FLUME_TEST_CHECK(flume_get_backend_caps(nullptr, &too_small) ==
                   FLUME_ERR_INVALID_ARGUMENT);

  namespace fs = std::filesystem;
  fs::path root = fs::temp_directory_path() / "flume-test-backend-caps";
  fs::remove_all(root);
  fs::create_directories(root);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  FLUME_TEST_CHECK(agent.Start(&error));

  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  flume_client_t* client = nullptr;
  FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), &client) == FLUME_OK);
  FLUME_TEST_CHECK(flume_attach_sim_comm(client, "caps-world", 0, 2) ==
                   FLUME_OK);

  flume_backend_caps_t sim_caps = {};
  FLUME_TEST_CHECK(flume_get_backend_caps(client, &sim_caps) == FLUME_OK);
  FLUME_TEST_CHECK(sim_caps.size == sizeof(sim_caps));
  FLUME_TEST_CHECK(sim_caps.hccl_p2p == 1);
  FLUME_TEST_CHECK(sim_caps.hcomm_channel_res == 1);
  FLUME_TEST_CHECK(sim_caps.hcomm_payload_probe == 1);
  FLUME_TEST_CHECK(sim_caps.hcomm_payload_scheduler == 1);
  FLUME_TEST_CHECK(sim_caps.hcomm_payload_scheduler_candidate == 1);
  FLUME_TEST_CHECK(sim_caps.storage_hbm == 1);
  FLUME_TEST_CHECK(sim_caps.fallback_hccl_p2p == 1);

  flume_client_close(client);
  agent.Stop();
  fs::remove_all(root);
  return 0;
}
