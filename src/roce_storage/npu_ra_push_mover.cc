#include "roce_storage/npu_ra_push_mover.h"

#include <arpa/inet.h>

#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>

#include "roce_storage/cann_ra_loader.h"

namespace flume::roce {
namespace {

Endpoint ToEndpoint(const cann::TypicalQp& qp, uint8_t path_mtu) {
  Endpoint endpoint;
  std::memcpy(endpoint.gid.data(), qp.gid, endpoint.gid.size());
  endpoint.qpn = qp.qpn;
  endpoint.psn = qp.psn;
  endpoint.port = 1;
  endpoint.gid_index = static_cast<uint8_t>(qp.gid_index);
  endpoint.mtu = path_mtu;
  return endpoint;
}

cann::TypicalQp ToTypicalQp(const Endpoint& endpoint) {
  cann::TypicalQp qp{};
  qp.qpn = endpoint.qpn;
  qp.psn = endpoint.psn;
  qp.gid_index = endpoint.gid_index;
  std::memcpy(qp.gid, endpoint.gid.data(), endpoint.gid.size());
  qp.retry_count = 7;
  qp.retry_time = 14;
  return qp;
}

}  // namespace

class NpuRaPushMover::Impl {
 public:
  ~Impl() { Close(nullptr); }

  bool Open(const NpuRaPushConfig& requested, const Endpoint& peer,
            Endpoint* local, std::string* error) {
    std::lock_guard<std::mutex> lock(mu);
    if (opened || local == nullptr || peer.qpn == 0 ||
        requested.npu_rnic_ip.empty() || requested.timeout_ms == 0) {
      if (error != nullptr) *error = "invalid or repeated NPU-RA push open";
      return false;
    }
    config = requested;
    if (PathMtuBytes(config.path_mtu) < 1024) {
      if (error != nullptr) {
        *error = "NPU relay path MTU must be 1024, 2048, or 4096 bytes";
      }
      return false;
    }
    if (!api.Open(true, error)) return false;
    capability_loaded = true;

    if (api.SetDevice(static_cast<int32_t>(config.logical_device)) !=
            cann::kSuccess ||
        !api.ResolvePhysicalDevice(static_cast<int32_t>(config.logical_device),
                                   config.physical_device, &physical_device,
                                   error)) {
      if (error != nullptr && error->empty()) {
        *error = "failed to select NPU relay device";
      }
      return Fail();
    }
    effective_hdc_type = api.EffectiveHdcType(config.hdc_type);
    const int bootstrap_status = api.BootstrapNetwork(
        static_cast<int32_t>(config.logical_device), effective_hdc_type);
    if (bootstrap_status != cann::kSuccess) {
      if (error != nullptr) {
        *error = std::string("CANN network bootstrap failed via ") +
                 api.network_bootstrap_profile_name() + " (status=" +
                 std::to_string(bootstrap_status) + ")";
      }
      return Fail();
    }
    network_bootstrapped = true;
    init_config = {physical_device, cann::kNicDeploymentDevice,
                   effective_hdc_type, false};
    const int init_status = api.Init(&init_config);
    if (init_status != cann::kSuccess) {
      if (error != nullptr) {
        *error = "RaInit failed for NPU relay (status=" +
                 std::to_string(init_status) + ")";
      }
      return Fail();
    }
    ra_initialized = true;

    cann::Rdev rdev{};
    rdev.phy_id = physical_device;
    rdev.family = AF_INET;
    if (inet_pton(AF_INET, config.npu_rnic_ip.c_str(),
                  &rdev.local_ip.addr) != 1) {
      if (error != nullptr) *error = "NPU relay RNIC IP must be IPv4";
      return Fail();
    }
    cann::RdevInitInfo rdev_init{};
    rdev_init.mode = cann::kNetworkOffline;
    rdev_init.notify_type = cann::kNotify;
    if (api.RdevInit(rdev_init, rdev, &rdma_handle) != cann::kSuccess ||
        rdma_handle == nullptr) {
      if (error != nullptr) {
        *error = std::string(api.rdev_init_profile_name()) +
                 " failed for NPU relay";
      }
      return Fail();
    }
    local_qp.gid_index = config.gid_index;
    if (api.TypicalQpCreate(rdma_handle, 0, cann::kOpbaseQpMode, &local_qp,
                            &qp_handle) != cann::kSuccess ||
        qp_handle == nullptr) {
      if (error != nullptr) *error = "RaTypicalQpCreate failed for NPU relay";
      return Fail();
    }
    cann::TypicalQp peer_qp = ToTypicalQp(peer);
    local_qp.retry_count = 7;
    local_qp.retry_time = 14;
    if (api.TypicalQpModify(qp_handle, &local_qp, &peer_qp) !=
        cann::kSuccess) {
      if (error != nullptr) *error = "RaTypicalQpModify failed for NPU relay";
      return Fail();
    }
    *local = ToEndpoint(local_qp, config.path_mtu);
    opened = true;
    return true;
  }

  bool Push(void* source_hbm, size_t length, const MemoryWindow& target,
            void* acl_stream, std::string* error) {
    std::lock_guard<std::mutex> lock(mu);
    if (!opened || source_hbm == nullptr || length == 0 ||
        length > UINT32_MAX || acl_stream == nullptr ||
        target.address == 0 || target.rkey == 0 || target.length < length ||
        (target.access & kMemoryRemoteWrite) == 0) {
      if (error != nullptr) *error = "invalid NPU-RA push source or target window";
      return false;
    }
    cann::MrInfo source_mr{};
    source_mr.address = source_hbm;
    source_mr.size = length;
    source_mr.access = cann::kRaAccessLocalWrite;
    void* source_mr_handle = nullptr;
    if (api.RegisterMr(rdma_handle, &source_mr, &source_mr_handle) !=
            cann::kSuccess ||
        source_mr_handle == nullptr) {
      if (error != nullptr) *error = "RaRegisterMr failed for relay HBM";
      return false;
    }

    cann::SgList sge{reinterpret_cast<uint64_t>(source_hbm),
                     static_cast<uint32_t>(length), source_mr.lkey};
    cann::SendWr wr{&sge, 1, target.address, target.rkey,
                    cann::kRaWrRdmaWrite, cann::kRaSendSignaled};
    cann::SendWrResponse response{};
    bool ok = api.TypicalSendWr(qp_handle, &wr, &response) ==
                  cann::kSuccess &&
              api.RdmaDbSend(response.db.index, response.db.info, acl_stream) ==
                  cann::kSuccess;
    if (!ok && error != nullptr) {
      *error = "failed to submit NPU-RA RDMA Write doorbell";
    }

    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(config.timeout_ms);
    bool completion_seen = false;
    while (ok && std::chrono::steady_clock::now() < deadline) {
      cann::WorkCompletion completion{};
      const int count = api.PollCq(qp_handle, true, 1, &completion);
      if (count < 0) {
        ok = false;
        if (error != nullptr) *error = "RaPollCq failed for NPU relay send CQ";
        break;
      }
      if (count == 1) {
        completion_seen = true;
        ok = completion.status == 0;
        if (!ok && error != nullptr) {
          *error = "NPU relay send CQ reported a failed RDMA Write";
        }
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (ok && !completion_seen) {
      ok = false;
      if (error != nullptr) *error = "timed out waiting for NPU relay send CQ";
    }
    const int deregister_status =
        api.DeregisterMr(rdma_handle, source_mr_handle);
    if (deregister_status != cann::kSuccess) {
      ok = false;
      if (error != nullptr) {
        *error = "RaDeregisterMr failed for relay HBM";
      }
    }
    return ok;
  }

  bool Close(std::string*) {
    std::lock_guard<std::mutex> lock(mu);
    Cleanup();
    return true;
  }
  bool available() const { return opened; }
  bool capability_available() const { return capability_loaded; }

 private:
  bool Fail() {
    Cleanup();
    return false;
  }
  void Cleanup() {
    opened = false;
    if (qp_handle != nullptr) api.QpDestroy(qp_handle);
    qp_handle = nullptr;
    if (rdma_handle != nullptr) api.RdevDeinit(rdma_handle, cann::kNotify);
    rdma_handle = nullptr;
    if (ra_initialized) api.Deinit(&init_config);
    ra_initialized = false;
    if (network_bootstrapped) {
      api.ShutdownNetwork(static_cast<int32_t>(config.logical_device));
    }
    network_bootstrapped = false;
    api.Close();
  }

  mutable std::mutex mu;
  CannRaApi api;
  NpuRaPushConfig config;
  unsigned physical_device = 0;
  cann::RaInitConfig init_config{};
  cann::TypicalQp local_qp{};
  void* rdma_handle = nullptr;
  void* qp_handle = nullptr;
  bool network_bootstrapped = false;
  bool ra_initialized = false;
  bool capability_loaded = false;
  bool opened = false;
  int effective_hdc_type = cann::kDefaultHdcType;
};

NpuRaPushMover::NpuRaPushMover() : impl_(std::make_unique<Impl>()) {}
NpuRaPushMover::~NpuRaPushMover() = default;
bool NpuRaPushMover::Open(const NpuRaPushConfig& config, const Endpoint& peer,
                          Endpoint* local, std::string* error) {
  return impl_->Open(config, peer, local, error);
}
bool NpuRaPushMover::Push(void* source_hbm, size_t length,
                          const MemoryWindow& target, void* acl_stream,
                          std::string* error) {
  return impl_->Push(source_hbm, length, target, acl_stream, error);
}
bool NpuRaPushMover::Close(std::string* error) { return impl_->Close(error); }
bool NpuRaPushMover::available() const { return impl_->available(); }
bool NpuRaPushMover::capability_available() const {
  return impl_->capability_available();
}

}  // namespace flume::roce
