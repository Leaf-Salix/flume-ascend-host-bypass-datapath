#include "roce_storage/npu_ra_push_mover.h"

#include <arpa/inet.h>

#include <chrono>
#include <cstring>
#include <mutex>
#include <thread>

#include "roce_storage/cann_ra_loader.h"

namespace flume::roce {
namespace {

Endpoint ToEndpoint(const cann::TypicalQp& qp) {
  Endpoint endpoint;
  std::memcpy(endpoint.gid.data(), qp.gid, endpoint.gid.size());
  endpoint.qpn = qp.qpn;
  endpoint.psn = qp.psn;
  endpoint.port = 1;
  endpoint.gid_index = static_cast<uint8_t>(qp.gid_index);
  endpoint.mtu = 5;
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
    if (!api.Open(true, error)) return false;
    capability_loaded = true;

    int32_t physical = -1;
    if (api.rt_set_device(static_cast<int32_t>(config.logical_device)) !=
            cann::kSuccess ||
        api.acl_get_physical_device(static_cast<int32_t>(config.logical_device),
                                    &physical) != cann::kSuccess ||
        physical < 0) {
      if (error != nullptr) *error = "failed to select NPU relay device";
      return Fail();
    }
    physical_device = static_cast<unsigned>(physical);
    const std::string hdc_arg = "--hdcType=" + std::to_string(config.hdc_type);
    cann::RtProcExtParam parameter{hdc_arg.c_str(), hdc_arg.size()};
    cann::RtNetServiceOpenArgs open_args{&parameter, 1};
    if (api.rt_open_net_service(&open_args) != cann::kSuccess) {
      if (error != nullptr) *error = "rtOpenNetService failed for NPU relay";
      return Fail();
    }
    net_service_open = true;
    init_config = {physical_device, cann::kNicDeploymentDevice,
                   config.hdc_type, false};
    if (api.ra_init(&init_config) != cann::kSuccess) {
      if (error != nullptr) *error = "RaInit failed for NPU relay";
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
    if (api.ra_rdev_init_v2(rdev_init, rdev, &rdma_handle) != cann::kSuccess ||
        rdma_handle == nullptr) {
      if (error != nullptr) *error = "RaRdevInitV2 failed for NPU relay";
      return Fail();
    }
    local_qp.gid_index = config.gid_index;
    if (api.ra_typical_qp_create(rdma_handle, 0, cann::kOpbaseQpMode,
                                 &local_qp, &qp_handle) != cann::kSuccess ||
        qp_handle == nullptr) {
      if (error != nullptr) *error = "RaTypicalQpCreate failed for NPU relay";
      return Fail();
    }
    cann::TypicalQp peer_qp = ToTypicalQp(peer);
    local_qp.retry_count = 7;
    local_qp.retry_time = 14;
    if (api.ra_typical_qp_modify(qp_handle, &local_qp, &peer_qp) !=
        cann::kSuccess) {
      if (error != nullptr) *error = "RaTypicalQpModify failed for NPU relay";
      return Fail();
    }
    *local = ToEndpoint(local_qp);
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
    if (api.ra_register_mr(rdma_handle, &source_mr, &source_mr_handle) !=
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
    bool ok = api.ra_typical_send_wr(qp_handle, &wr, &response) ==
                  cann::kSuccess &&
              api.rt_rdma_db_send(response.db.index, response.db.info,
                                  acl_stream) == cann::kSuccess;
    if (!ok && error != nullptr) {
      *error = "failed to submit NPU-RA RDMA Write doorbell";
    }

    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(config.timeout_ms);
    bool completion_seen = false;
    while (ok && std::chrono::steady_clock::now() < deadline) {
      cann::WorkCompletion completion{};
      const int count = api.ra_poll_cq(qp_handle, true, 1, &completion);
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
    api.ra_deregister_mr(rdma_handle, source_mr_handle);
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
    if (qp_handle != nullptr) api.ra_qp_destroy(qp_handle);
    qp_handle = nullptr;
    if (rdma_handle != nullptr) api.ra_rdev_deinit(rdma_handle, cann::kNotify);
    rdma_handle = nullptr;
    if (ra_initialized) api.ra_deinit(&init_config);
    ra_initialized = false;
    if (net_service_open) api.rt_close_net_service();
    net_service_open = false;
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
  bool net_service_open = false;
  bool ra_initialized = false;
  bool capability_loaded = false;
  bool opened = false;
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
