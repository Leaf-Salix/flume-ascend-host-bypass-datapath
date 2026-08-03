#include "test_util.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "agent/storage_agent.h"
#include "flume/flume.h"

namespace {

void OpenClient(flume::StorageAgent& agent,
                const char* comm_name,
                uint32_t rank,
                uint32_t rank_size,
                flume_client_t** out) {
  std::string endpoint = "127.0.0.1:" + std::to_string(agent.port());
  FLUME_TEST_CHECK(flume_client_open(endpoint.c_str(), out) == FLUME_OK);
  FLUME_TEST_CHECK(flume_attach_sim_comm(*out, comm_name, rank, rank_size) ==
                  FLUME_OK);
}

void AllocPair(flume_client_t* client,
               uint64_t count,
               flume_buffer_t** src,
               flume_buffer_t** dst) {
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(client, count * sizeof(float),
                                        FLUME_BUFFER_SIM_HBM, src) == FLUME_OK);
  FLUME_TEST_CHECK(flume_sim_alloc_buffer(client, count * sizeof(float),
                                        FLUME_BUFFER_SIM_HBM, dst) == FLUME_OK);
  auto* values = static_cast<float*>(flume_buffer_data(*src));
  for (uint64_t i = 0; i < count; ++i) {
    values[i] = static_cast<float>(i + 1);
  }
}

void ReleasePair(flume_buffer_t* src, flume_buffer_t* dst) {
  FLUME_TEST_CHECK(flume_buffer_release(dst) == FLUME_OK);
  FLUME_TEST_CHECK(flume_buffer_release(src) == FLUME_OK);
}

void SubmitAllReduce(flume_client_t* client,
                     flume_buffer_t* dst,
                     flume_buffer_t* src,
                     uint64_t count,
                     flume_io_t** io) {
  FLUME_TEST_CHECK(flume_allreduce_async(client, dst, 0, src, 0, count,
                                       FLUME_DTYPE_FP32, FLUME_REDUCE_SUM,
                                       nullptr, io) == FLUME_OK);
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  fs::path root = fs::temp_directory_path() / "flume-test-sim-collective-failures";
  fs::remove_all(root);
  fs::create_directories(root);

  flume::StorageAgent agent("127.0.0.1", 0, root.string());
  std::string error;
  if (!agent.Start(&error)) {
    std::cerr << "agent.Start failed: " << error << "\n";
    return 1;
  }

  {
    flume_client_t* rank0 = nullptr;
    flume_client_t* rank1 = nullptr;
    OpenClient(agent, "missing-rank-world", 0, 2, &rank0);
    OpenClient(agent, "missing-rank-world", 1, 2, &rank1);
    flume_buffer_t* src0 = nullptr;
    flume_buffer_t* dst0 = nullptr;
    flume_buffer_t* src1 = nullptr;
    flume_buffer_t* dst1 = nullptr;
    flume_io_t* io0 = nullptr;
    flume_io_t* io1 = nullptr;
    AllocPair(rank0, 4, &src0, &dst0);
    AllocPair(rank1, 4, &src1, &dst1);

    SubmitAllReduce(rank0, dst0, src0, 4, &io0);
    FLUME_TEST_CHECK(flume_wait(io0, 1) == FLUME_ERR_TIMEOUT);
    FLUME_TEST_CHECK(flume_io_release(io0) == FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_buffer_release(dst0) == FLUME_ERR_INVALID_ARGUMENT);
    SubmitAllReduce(rank1, dst1, src1, 4, &io1);
    FLUME_TEST_CHECK(flume_wait(io0, 1000) == FLUME_OK);
    FLUME_TEST_CHECK(flume_wait(io1, 1000) == FLUME_OK);
    FLUME_TEST_CHECK(flume_io_release(io1) == FLUME_OK);
    FLUME_TEST_CHECK(flume_io_release(io0) == FLUME_OK);
    ReleasePair(src1, dst1);
    ReleasePair(src0, dst0);
    flume_client_close(rank1);
    flume_client_close(rank0);
  }

  {
    flume_client_t* rank0 = nullptr;
    flume_client_t* rank1 = nullptr;
    OpenClient(agent, "mismatch-world", 0, 2, &rank0);
    OpenClient(agent, "mismatch-world", 1, 2, &rank1);
    flume_buffer_t* src0 = nullptr;
    flume_buffer_t* dst0 = nullptr;
    flume_buffer_t* src1 = nullptr;
    flume_buffer_t* dst1 = nullptr;
    flume_io_t* io0 = nullptr;
    flume_io_t* io1 = nullptr;
    AllocPair(rank0, 4, &src0, &dst0);
    AllocPair(rank1, 4, &src1, &dst1);

    SubmitAllReduce(rank0, dst0, src0, 4, &io0);
    SubmitAllReduce(rank1, dst1, src1, 2, &io1);
    FLUME_TEST_CHECK(flume_wait(io0, 1000) == FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_wait(io1, 1000) == FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_io_release(io1) == FLUME_OK);
    FLUME_TEST_CHECK(flume_io_release(io0) == FLUME_OK);
    ReleasePair(src1, dst1);
    ReleasePair(src0, dst0);
    flume_client_close(rank1);
    flume_client_close(rank0);
  }

  {
    flume_client_t* rank0 = nullptr;
    flume_client_t* rank1 = nullptr;
    OpenClient(agent, "seq-world", 0, 2, &rank0);
    OpenClient(agent, "seq-world", 1, 2, &rank1);
    flume_buffer_t* src0 = nullptr;
    flume_buffer_t* dst0 = nullptr;
    flume_buffer_t* src1 = nullptr;
    flume_buffer_t* dst1 = nullptr;
    flume_io_t* io0 = nullptr;
    flume_io_t* io1 = nullptr;
    AllocPair(rank0, 4, &src0, &dst0);
    AllocPair(rank1, 4, &src1, &dst1);

    FLUME_TEST_CHECK(flume_allreduce_async(rank0, dst0, 0, src0, 0, 0,
                                         FLUME_DTYPE_FP32, FLUME_REDUCE_SUM,
                                         nullptr, &io0) ==
                    FLUME_ERR_INVALID_ARGUMENT);
    SubmitAllReduce(rank0, dst0, src0, 4, &io0);
    SubmitAllReduce(rank1, dst1, src1, 4, &io1);
    FLUME_TEST_CHECK(flume_wait(io0, 1000) == FLUME_OK);
    FLUME_TEST_CHECK(flume_wait(io1, 1000) == FLUME_OK);
    FLUME_TEST_CHECK(flume_io_release(io1) == FLUME_OK);
    FLUME_TEST_CHECK(flume_io_release(io0) == FLUME_OK);
    ReleasePair(src1, dst1);
    ReleasePair(src0, dst0);
    flume_client_close(rank1);
    flume_client_close(rank0);
  }

  {
    flume_client_t* rank0a = nullptr;
    flume_client_t* rank0b = nullptr;
    OpenClient(agent, "duplicate-rank-world", 0, 2, &rank0a);
    OpenClient(agent, "duplicate-rank-world", 0, 2, &rank0b);
    flume_buffer_t* src0a = nullptr;
    flume_buffer_t* dst0a = nullptr;
    flume_buffer_t* src0b = nullptr;
    flume_buffer_t* dst0b = nullptr;
    flume_io_t* io0a = nullptr;
    flume_io_t* io0b = nullptr;
    AllocPair(rank0a, 4, &src0a, &dst0a);
    AllocPair(rank0b, 4, &src0b, &dst0b);

    SubmitAllReduce(rank0a, dst0a, src0a, 4, &io0a);
    SubmitAllReduce(rank0b, dst0b, src0b, 4, &io0b);
    FLUME_TEST_CHECK(flume_wait(io0a, 1000) == FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_wait(io0b, 1000) == FLUME_ERR_INVALID_ARGUMENT);
    FLUME_TEST_CHECK(flume_io_release(io0b) == FLUME_OK);
    FLUME_TEST_CHECK(flume_io_release(io0a) == FLUME_OK);
    ReleasePair(src0b, dst0b);
    ReleasePair(src0a, dst0a);
    flume_client_close(rank0b);
    flume_client_close(rank0a);
  }

  agent.Stop();
  fs::remove_all(root);
  return 0;
}
