#include "roce_storage/verbs_backend.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

#ifndef FLUME_HAVE_IBVERBS
#define FLUME_HAVE_IBVERBS 0
#endif

#if FLUME_HAVE_IBVERBS
#include <infiniband/verbs.h>
#endif

namespace flume::roce {

VerbsBackend::VerbsBackend() = default;
VerbsBackend::~VerbsBackend() { Close(); }

#if FLUME_HAVE_IBVERBS
namespace {

void SetError(std::string* error, const char* message) {
  if (error != nullptr) *error = message;
}

ibv_mtu ToMtu(uint8_t value) {
  switch (value) {
    case 1: return IBV_MTU_256;
    case 2: return IBV_MTU_512;
    case 3: return IBV_MTU_1024;
    case 4: return IBV_MTU_2048;
    default: return IBV_MTU_4096;
  }
}

uint32_t NextPsn() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  return static_cast<uint32_t>(now) & 0x00ffffffU;
}

}  // namespace

bool VerbsBackend::Open(const std::string& device_name, uint8_t port, uint8_t gid_index,
                        uint32_t queue_depth, std::string* error) {
  Close();
  if (device_name.empty() || port == 0 || queue_depth == 0) {
    SetError(error, "verbs device, port, and queue depth are required");
    return false;
  }
  int count = 0;
  ibv_device** devices = ibv_get_device_list(&count);
  if (devices == nullptr) {
    SetError(error, "ibv_get_device_list failed");
    return false;
  }
  ibv_device* selected = nullptr;
  for (int index = 0; index < count; ++index) {
    if (std::strcmp(ibv_get_device_name(devices[index]), device_name.c_str()) == 0) {
      selected = devices[index];
      break;
    }
  }
  if (selected == nullptr) {
    ibv_free_device_list(devices);
    SetError(error, "requested verbs device was not found");
    return false;
  }
  ibv_context* context = ibv_open_device(selected);
  ibv_free_device_list(devices);
  if (context == nullptr) {
    SetError(error, "ibv_open_device failed");
    return false;
  }
  ibv_pd* pd = ibv_alloc_pd(context);
  ibv_cq* cq = pd == nullptr ? nullptr : ibv_create_cq(context, static_cast<int>(queue_depth * 2), nullptr, nullptr, 0);
  ibv_qp_init_attr init{};
  init.send_cq = cq;
  init.recv_cq = cq;
  init.qp_type = IBV_QPT_RC;
  init.cap.max_send_wr = queue_depth;
  init.cap.max_recv_wr = queue_depth;
  init.cap.max_send_sge = 1;
  init.cap.max_recv_sge = 1;
  ibv_qp* qp = cq == nullptr ? nullptr : ibv_create_qp(pd, &init);
  if (qp == nullptr) {
    if (cq != nullptr) ibv_destroy_cq(cq);
    if (pd != nullptr) ibv_dealloc_pd(pd);
    ibv_close_device(context);
    SetError(error, "failed to allocate verbs PD, CQ, or RC QP");
    return false;
  }
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = port;
  attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) != 0) {
    ibv_destroy_qp(qp);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    ibv_close_device(context);
    SetError(error, "failed to move RC QP to INIT");
    return false;
  }
  ibv_gid gid{};
  ibv_port_attr port_attr{};
  if (ibv_query_gid(context, port, gid_index, &gid) != 0 || ibv_query_port(context, port, &port_attr) != 0) {
    ibv_destroy_qp(qp);
    ibv_destroy_cq(cq);
    ibv_dealloc_pd(pd);
    ibv_close_device(context);
    SetError(error, "failed to query verbs port GID or attributes");
    return false;
  }
  context_ = context;
  pd_ = pd;
  cq_ = cq;
  qp_ = qp;
  std::memcpy(endpoint_.gid.data(), gid.raw, endpoint_.gid.size());
  endpoint_.qpn = qp->qp_num;
  endpoint_.psn = NextPsn();
  endpoint_.port = port;
  endpoint_.gid_index = gid_index;
  endpoint_.mtu = static_cast<uint8_t>(port_attr.active_mtu);
  return true;
}

bool VerbsBackend::Connect(const VerbsEndpoint& peer, std::string* error) {
  if (!available() || peer.qpn == 0) {
    SetError(error, "verbs QP and peer endpoint are required");
    return false;
  }
  ibv_qp* qp = static_cast<ibv_qp*>(qp_);
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = ToMtu(std::min(endpoint_.mtu, peer.mtu));
  attr.dest_qp_num = peer.qpn;
  attr.rq_psn = peer.psn;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 12;
  attr.ah_attr.is_global = 1;
  std::memcpy(&attr.ah_attr.grh.dgid, peer.gid.data(), peer.gid.size());
  attr.ah_attr.grh.sgid_index = endpoint_.gid_index;
  attr.ah_attr.grh.hop_limit = 1;
  attr.ah_attr.port_num = endpoint_.port;
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                               IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                               IBV_QP_MIN_RNR_TIMER) != 0) {
    SetError(error, "failed to move RC QP to RTR");
    return false;
  }
  attr = {};
  attr.qp_state = IBV_QPS_RTS;
  attr.sq_psn = endpoint_.psn;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.max_rd_atomic = 1;
  if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT |
                               IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC) != 0) {
    SetError(error, "failed to move RC QP to RTS");
    return false;
  }
  return true;
}

bool VerbsBackend::Register(void* address, size_t length, bool remote_read, bool remote_write,
                            VerbsMemoryRegion* out, std::string* error) {
  if (!available() || address == nullptr || length == 0 || out == nullptr) {
    SetError(error, "verbs memory registration requires a QP, address, length, and output");
    return false;
  }
  int access = IBV_ACCESS_LOCAL_WRITE;
  if (remote_read) access |= IBV_ACCESS_REMOTE_READ;
  if (remote_write) access |= IBV_ACCESS_REMOTE_WRITE;
  ibv_mr* mr = ibv_reg_mr(static_cast<ibv_pd*>(pd_), address, length, access);
  if (mr == nullptr) {
    SetError(error, "ibv_reg_mr failed");
    return false;
  }
  out->address = address;
  out->length = length;
  out->lkey = mr->lkey;
  out->rkey = mr->rkey;
  out->opaque = mr;
  return true;
}

bool VerbsBackend::Deregister(VerbsMemoryRegion* region, std::string* error) {
  if (region == nullptr || region->opaque == nullptr) return true;
  if (ibv_dereg_mr(static_cast<ibv_mr*>(region->opaque)) != 0) {
    SetError(error, "ibv_dereg_mr failed");
    return false;
  }
  *region = {};
  return true;
}

bool VerbsBackend::PostReceive(const VerbsMemoryRegion& region, std::string* error) {
  if (!available() || region.opaque == nullptr) {
    SetError(error, "registered receive region is required");
    return false;
  }
  ibv_sge sge{};
  sge.addr = reinterpret_cast<uintptr_t>(region.address);
  sge.length = static_cast<uint32_t>(region.length);
  sge.lkey = region.lkey;
  ibv_recv_wr wr{};
  wr.sg_list = &sge;
  wr.num_sge = 1;
  ibv_recv_wr* bad = nullptr;
  if (ibv_post_recv(static_cast<ibv_qp*>(qp_), &wr, &bad) != 0) {
    SetError(error, "ibv_post_recv failed");
    return false;
  }
  return true;
}

bool VerbsBackend::WaitForReceive(uint32_t timeout_ms, std::string* error) {
  if (!available() || timeout_ms == 0) {
    SetError(error, "verbs CQ and nonzero timeout are required");
    return false;
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    ibv_wc completion{};
    const int count = ibv_poll_cq(static_cast<ibv_cq*>(cq_), 1, &completion);
    if (count < 0 || (count == 1 && (completion.status != IBV_WC_SUCCESS || completion.opcode != IBV_WC_RECV))) {
      SetError(error, "unexpected verbs receive completion");
      return false;
    }
    if (count == 1) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  SetError(error, "timed out waiting for verbs receive completion");
  return false;
}

bool VerbsBackend::Transfer(bool read, const VerbsMemoryRegion& local, uint64_t remote_address,
                            uint32_t remote_rkey, size_t length, uint32_t timeout_ms,
                            std::string* error) {
  if (!available() || local.opaque == nullptr || length == 0 || length > local.length || timeout_ms == 0) {
    SetError(error, "invalid verbs RDMA transfer arguments");
    return false;
  }
  ibv_sge sge{};
  sge.addr = reinterpret_cast<uintptr_t>(local.address);
  sge.length = static_cast<uint32_t>(length);
  sge.lkey = local.lkey;
  ibv_send_wr wr{};
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = read ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = remote_address;
  wr.wr.rdma.rkey = remote_rkey;
  ibv_send_wr* bad = nullptr;
  if (ibv_post_send(static_cast<ibv_qp*>(qp_), &wr, &bad) != 0) {
    SetError(error, "ibv_post_send RDMA transfer failed");
    return false;
  }
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    ibv_wc completion{};
    const int count = ibv_poll_cq(static_cast<ibv_cq*>(cq_), 1, &completion);
    if (count < 0 || (count == 1 && completion.status != IBV_WC_SUCCESS)) {
      SetError(error, "verbs RDMA transfer completion failed");
      return false;
    }
    if (count == 1) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  SetError(error, "timed out waiting for verbs RDMA transfer completion");
  return false;
}

bool VerbsBackend::Read(const VerbsMemoryRegion& local, uint64_t remote_address, uint32_t remote_rkey,
                        size_t length, uint32_t timeout_ms, std::string* error) {
  return Transfer(true, local, remote_address, remote_rkey, length, timeout_ms, error);
}

bool VerbsBackend::Write(const VerbsMemoryRegion& local, uint64_t remote_address, uint32_t remote_rkey,
                         size_t length, uint32_t timeout_ms, std::string* error) {
  return Transfer(false, local, remote_address, remote_rkey, length, timeout_ms, error);
}

bool VerbsBackend::available() const { return qp_ != nullptr; }
VerbsEndpoint VerbsBackend::endpoint() const { return endpoint_; }

void VerbsBackend::Close() {
  if (qp_ != nullptr) ibv_destroy_qp(static_cast<ibv_qp*>(qp_));
  if (cq_ != nullptr) ibv_destroy_cq(static_cast<ibv_cq*>(cq_));
  if (pd_ != nullptr) ibv_dealloc_pd(static_cast<ibv_pd*>(pd_));
  if (context_ != nullptr) ibv_close_device(static_cast<ibv_context*>(context_));
  context_ = nullptr;
  pd_ = nullptr;
  cq_ = nullptr;
  qp_ = nullptr;
  endpoint_ = {};
}

bool VerbsAvailable() { return true; }
const char* VerbsUnavailableReason() { return "libibverbs is available"; }

#else

bool VerbsBackend::Open(const std::string&, uint8_t, uint8_t, uint32_t, std::string* error) {
  if (error != nullptr) *error = VerbsUnavailableReason();
  return false;
}
bool VerbsBackend::Connect(const VerbsEndpoint&, std::string* error) { if (error != nullptr) *error = VerbsUnavailableReason(); return false; }
bool VerbsBackend::Register(void*, size_t, bool, bool, VerbsMemoryRegion*, std::string* error) { if (error != nullptr) *error = VerbsUnavailableReason(); return false; }
bool VerbsBackend::Deregister(VerbsMemoryRegion*, std::string*) { return true; }
bool VerbsBackend::PostReceive(const VerbsMemoryRegion&, std::string* error) { if (error != nullptr) *error = VerbsUnavailableReason(); return false; }
bool VerbsBackend::WaitForReceive(uint32_t, std::string* error) { if (error != nullptr) *error = VerbsUnavailableReason(); return false; }
bool VerbsBackend::Read(const VerbsMemoryRegion&, uint64_t, uint32_t, size_t, uint32_t, std::string* error) { if (error != nullptr) *error = VerbsUnavailableReason(); return false; }
bool VerbsBackend::Write(const VerbsMemoryRegion&, uint64_t, uint32_t, size_t, uint32_t, std::string* error) { if (error != nullptr) *error = VerbsUnavailableReason(); return false; }
bool VerbsBackend::available() const { return false; }
VerbsEndpoint VerbsBackend::endpoint() const { return {}; }
void VerbsBackend::Close() {}
bool VerbsAvailable() { return false; }
const char* VerbsUnavailableReason() { return "libibverbs headers and library were not found at CMake configure time"; }

#endif

}  // namespace flume::roce
