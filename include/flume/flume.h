#ifndef FLUME_FLUME_H_
#define FLUME_FLUME_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flume_client flume_client_t;
typedef struct flume_file flume_file_t;
typedef struct flume_buffer flume_buffer_t;
typedef struct flume_io flume_io_t;
typedef struct flume_a3_symmetric_window flume_a3_symmetric_window_t;
typedef struct flume_storage_block flume_storage_block_t;
typedef struct flume_storage_target_window flume_storage_target_window_t;
typedef struct flume_storage_direct_plan flume_storage_direct_plan_t;
typedef struct flume_roce_storage_session flume_roce_storage_session_t;

typedef enum {
  FLUME_OK = 0,
  FLUME_ERR_INVALID_ARGUMENT = 1,
  FLUME_ERR_IO = 2,
  FLUME_ERR_TIMEOUT = 3,
  FLUME_ERR_UNSUPPORTED = 4,
  FLUME_ERR_BACKEND = 5,
  FLUME_ERR_REMOTE = 6,
  FLUME_ERR_PROTOCOL = 7,
  FLUME_PENDING = 8
} flume_status_t;

typedef enum {
  FLUME_BUFFER_HOST = 0,
  FLUME_BUFFER_ASCEND_HBM = 1,
  FLUME_BUFFER_HCCL_COMM = 2,
  FLUME_BUFFER_SIM_HBM = 100,
  FLUME_BUFFER_SIM_HCCL_COMM = 101
} flume_buffer_type_t;

typedef enum {
  FLUME_PATH_AUTO = 0,
  FLUME_PATH_HCCL_HCOMM = 1,
  FLUME_PATH_RUNTIME_BASELINE = 2,
  FLUME_PATH_MOCK = 3
} flume_path_t;

typedef enum {
  FLUME_DTYPE_INT8 = 0,
  FLUME_DTYPE_INT16 = 1,
  FLUME_DTYPE_INT32 = 2,
  FLUME_DTYPE_FP16 = 3,
  FLUME_DTYPE_FP32 = 4,
  FLUME_DTYPE_INT64 = 5,
  FLUME_DTYPE_UINT64 = 6,
  FLUME_DTYPE_UINT8 = 7,
  FLUME_DTYPE_UINT16 = 8,
  FLUME_DTYPE_UINT32 = 9,
  FLUME_DTYPE_FP64 = 10,
  FLUME_DTYPE_BFP16 = 11
} flume_data_type_t;

typedef enum {
  FLUME_REDUCE_SUM = 0,
  FLUME_REDUCE_PROD = 1,
  FLUME_REDUCE_MAX = 2,
  FLUME_REDUCE_MIN = 3
} flume_reduce_op_t;

typedef enum {
  FLUME_HCOMM_ENGINE_AUTO = 0,
  FLUME_HCOMM_ENGINE_AICPU = 1,
  FLUME_HCOMM_ENGINE_AICPU_TS = 2,
  FLUME_HCOMM_ENGINE_CPU = 3,
  FLUME_HCOMM_ENGINE_CPU_TS = 4
} flume_hcomm_engine_t;

typedef enum {
  FLUME_HCOMM_PROTOCOL_AUTO = 0,
  FLUME_HCOMM_PROTOCOL_HCCS = 1,
  FLUME_HCOMM_PROTOCOL_ROCE = 2,
  FLUME_HCOMM_PROTOCOL_PCIE = 3,
  FLUME_HCOMM_PROTOCOL_SIO = 4,
  FLUME_HCOMM_PROTOCOL_HCCS_ONLY = 5
} flume_hcomm_protocol_t;

typedef enum {
  FLUME_HCOMM_PAYLOAD_COMM_BINDING_COMM_NAME = 0,
  FLUME_HCOMM_PAYLOAD_COMM_BINDING_DIAGNOSTIC_SKIP = 1,
  FLUME_HCOMM_PAYLOAD_COMM_BINDING_CHANNEL_HANDLE = 2
} flume_hcomm_payload_comm_binding_t;

typedef enum {
  FLUME_STORAGE_TRANSFER_AUTO = 0,
  FLUME_STORAGE_TRANSFER_SIM_DIRECT = 1,
  FLUME_STORAGE_TRANSFER_HOST_STAGING = 2,
  FLUME_STORAGE_TRANSFER_ROCE_STAGED = 3,
  FLUME_STORAGE_TRANSFER_ROCE_PROXY_HCCL = 4
} flume_storage_transfer_path_t;

typedef enum {
  FLUME_ROCE_POST_AUTO = 0,
  FLUME_ROCE_POST_HOST_RA = 1,
  FLUME_ROCE_POST_AICPU = 2,
  FLUME_ROCE_POST_AIV = 3
} flume_roce_post_mode_t;

typedef enum {
  FLUME_ROCE_CONTROL_TCP = 0,
  FLUME_ROCE_CONTROL_NPU_RA = 1
} flume_roce_control_mode_t;

typedef enum {
  FLUME_ROCE_TRANSFER_PUSH = 0,
  /* Reserved for a future remote-read data mover; currently unsupported. */
  FLUME_ROCE_TRANSFER_PULL = 1
} flume_roce_transfer_mode_t;

typedef enum {
  FLUME_ROCE_STORAGE_MEMORY = 0,
  FLUME_ROCE_STORAGE_POSIX = 1,
  FLUME_ROCE_STORAGE_SPDK = 2
} flume_roce_storage_backend_t;

typedef enum {
  FLUME_STORAGE_DIRECT_ROLE_AUTO = 0,
  FLUME_STORAGE_DIRECT_ROLE_SOURCE_SEND = 1,
  FLUME_STORAGE_DIRECT_ROLE_TARGET_RECV = 2,
  FLUME_STORAGE_DIRECT_ROLE_HOST_STAGING = 3
} flume_storage_direct_role_t;

typedef struct {
  uint32_t size;
  uint32_t storage_direct_sim;
  uint32_t storage_host_staging;
  uint32_t hcomm_payload_sim;
  uint32_t future_rdma_hbm;
  flume_storage_transfer_path_t default_path;
  uint32_t roce_storage_protocol;
  uint32_t roce_storage_native;
} flume_storage_transfer_caps_t;

typedef struct {
  uint32_t size;
  const char *storage_server;
  const char *npu_rnic_ip;
  const char *storage_rnic_ip;
  uint32_t npu_device;
  uint32_t gid_index;
  uint32_t bootstrap_port;
  uint32_t timeout_ms;
  flume_roce_post_mode_t post_mode;
  flume_roce_storage_backend_t storage_backend;
  uint32_t require_compute_host_bypass;
  flume_roce_control_mode_t control_mode;
  flume_roce_transfer_mode_t transfer_mode;
  // Set valid to 1 when an older CANN build cannot map a logical device id.
  // Zero keeps the runtime mapping used by current CANN releases.
  uint32_t npu_physical_device_valid;
  uint32_t npu_physical_device;
  // Zero selects the conservative 1024-byte RoCE path MTU default.
  uint32_t path_mtu_bytes;
} flume_roce_storage_options_t;

typedef struct {
  uint32_t size;
  flume_storage_transfer_path_t path;
  flume_storage_direct_role_t role;
  uint32_t peer_rank;
  uint32_t require_direct;
  uint32_t allow_host_staging;
  size_t len;
  flume_storage_target_window_t *target_window;
} flume_storage_direct_options_t;

typedef struct {
  uint32_t size;
  uint32_t notify_num;
  flume_hcomm_engine_t engine;
  flume_hcomm_protocol_t protocol;
  uint32_t require_thread_export;
  uint32_t timeout_sec;
  uint32_t disable_payload_batch_mode;
  const char *payload_batch_tag;
  uint32_t payload_recv_direct_output;
  uint32_t payload_skip_comm_acquire;
  flume_hcomm_payload_comm_binding_t payload_comm_binding;
  uint32_t payload_force_channel_fence;
  uint32_t payload_write_path;
  uint32_t payload_write_with_notify;
} flume_hcomm_channel_probe_options_t;

typedef struct {
  uint32_t size;
  uint32_t hccl_root_info;
  uint32_t hccl_init_all;
  uint32_t hccl_p2p;
  uint32_t hcomm_channel_res;
  uint32_t hcomm_primitives;
  uint32_t hcomm_thread_export;
  uint32_t hcomm_rank_graph;
  uint32_t hcomm_payload_probe;
  uint32_t hcomm_payload_scheduler;
  uint32_t storage_hbm;
  uint32_t fallback_hccl_p2p;
  uint32_t fallback_runtime_staging;
  flume_hcomm_engine_t hcomm_default_engine;
  uint32_t hcomm_payload_scheduler_candidate;
  uint32_t hcomm_payload_direct_aclrt;
  uint32_t hcomm_payload_direct_aclrt_host_args;
  uint32_t hcomm_payload_thread_notify;
  uint32_t hcomm_write_with_notify;
  uint32_t roce_storage_protocol;
  uint32_t roce_storage_native;
} flume_backend_caps_t;

const char *flume_status_string(int status);

int flume_client_open(const char *endpoint, flume_client_t **out);
int flume_client_close(flume_client_t *client);

int flume_attach_hccl_comm(flume_client_t *client, void *hccl_comm,
                          uint32_t rank, uint32_t rank_size);
int flume_attach_sim_comm(flume_client_t *client, const char *comm_name,
                         uint32_t rank, uint32_t rank_size);
int flume_get_backend_caps(flume_client_t *client, flume_backend_caps_t *out);

int flume_open(flume_client_t *client, const char *path, flume_file_t **out);
int flume_close(flume_file_t *file);

int flume_register_buffer(flume_client_t *client, void *ptr, size_t len,
                         flume_buffer_type_t type, flume_buffer_t **out);
int flume_sim_alloc_buffer(flume_client_t *client, size_t len,
                          flume_buffer_type_t type, flume_buffer_t **out);
void *flume_buffer_data(flume_buffer_t *buffer);
size_t flume_buffer_size(flume_buffer_t *buffer);
flume_buffer_type_t flume_buffer_type(flume_buffer_t *buffer);
int flume_buffer_release(flume_buffer_t *buffer);

int flume_a3_register_symmetric_memory(flume_client_t *client,
                                       flume_buffer_t *buffer,
                                       size_t offset,
                                       size_t len,
                                       flume_a3_symmetric_window_t **out);
int flume_a3_deregister_symmetric_memory(flume_a3_symmetric_window_t *window);

int flume_a3_set_memory_range(flume_client_t *client,
                             void *base_vir_ptr,
                             size_t size,
                             size_t alignment,
                             uint64_t flags);
int flume_a3_unset_memory_range(flume_client_t *client, void *base_vir_ptr);
int flume_a3_activate_comm_memory(flume_client_t *client,
                                 void *vir_ptr,
                                 size_t size,
                                 size_t offset,
                                 void *drv_mem_handle,
                                 uint64_t flags);
int flume_a3_deactivate_comm_memory(flume_client_t *client, void *vir_ptr);

int flume_pread_async(flume_file_t *file, flume_buffer_t *dst, size_t len,
                     uint64_t file_offset, size_t buffer_offset,
                     void *acl_stream, flume_io_t **out);

int flume_hbm_copy_async(flume_client_t *client,
                        flume_buffer_t *dst,
                        size_t dst_offset,
                        flume_buffer_t *src,
                        size_t src_offset,
                        size_t len,
                        void *acl_stream,
                        flume_io_t **out);

int flume_p2p_send_async(flume_client_t *client,
                        flume_buffer_t *src,
                        size_t src_offset,
                        uint64_t count,
                        flume_data_type_t data_type,
                        uint32_t dest_rank,
                        void *acl_stream,
                        flume_io_t **out);

int flume_p2p_recv_async(flume_client_t *client,
                        flume_buffer_t *dst,
                        size_t dst_offset,
                        uint64_t count,
                        flume_data_type_t data_type,
                        uint32_t src_rank,
                        void *acl_stream,
                        flume_io_t **out);

int flume_hcomm_channel_probe(flume_client_t *client,
                              uint32_t peer_rank,
                              void *acl_stream,
                              flume_io_t **out);

int flume_hcomm_channel_probe_ex(
    flume_client_t *client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t *options,
    void *acl_stream,
    flume_io_t **out);

int flume_hcomm_payload_probe(flume_client_t *client,
                              uint32_t peer_rank,
                              void *acl_stream,
                              flume_io_t **out);

int flume_hcomm_payload_probe_ex(
    flume_client_t *client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t *options,
    void *acl_stream,
    flume_io_t **out);

int flume_hcomm_custom_op_launch_smoke(flume_client_t *client,
                                       uint32_t peer_rank,
                                       void *acl_stream,
                                       flume_io_t **out);

int flume_hcomm_custom_op_launch_smoke_ex(
    flume_client_t *client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t *options,
    void *acl_stream,
    flume_io_t **out);

int flume_hcomm_resource_descriptor_smoke(flume_client_t *client,
                                          uint32_t peer_rank,
                                          void *acl_stream,
                                          flume_io_t **out);

int flume_hcomm_resource_descriptor_smoke_ex(
    flume_client_t *client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t *options,
    void *acl_stream,
    flume_io_t **out);

int flume_hcomm_notify_only_smoke(flume_client_t *client,
                                  uint32_t peer_rank,
                                  void *acl_stream,
                                  flume_io_t **out);

int flume_hcomm_notify_only_smoke_ex(
    flume_client_t *client,
    uint32_t peer_rank,
    const flume_hcomm_channel_probe_options_t *options,
    void *acl_stream,
    flume_io_t **out);

int flume_hcomm_aicpu_canary_smoke(flume_client_t *client,
                                   uint32_t peer_rank,
                                   void *acl_stream,
                                   flume_io_t **out);

int flume_hcomm_payload_send_async(flume_client_t *client,
                                  flume_buffer_t *src,
                                  size_t src_offset,
                                  uint64_t count,
                                  flume_data_type_t data_type,
                                  uint32_t dest_rank,
                                  void *acl_stream,
                                  flume_io_t **out);

int flume_hcomm_payload_send_ex(
    flume_client_t *client,
    flume_buffer_t *src,
    size_t src_offset,
    uint64_t count,
    flume_data_type_t data_type,
    uint32_t dest_rank,
    const flume_hcomm_channel_probe_options_t *options,
    void *acl_stream,
    flume_io_t **out);

int flume_hcomm_payload_recv_async(flume_client_t *client,
                                  flume_buffer_t *dst,
                                  size_t dst_offset,
                                  uint64_t count,
                                  flume_data_type_t data_type,
                                  uint32_t src_rank,
                                  void *acl_stream,
                                  flume_io_t **out);

int flume_hcomm_payload_recv_ex(
    flume_client_t *client,
    flume_buffer_t *dst,
    size_t dst_offset,
    uint64_t count,
    flume_data_type_t data_type,
    uint32_t src_rank,
    const flume_hcomm_channel_probe_options_t *options,
    void *acl_stream,
    flume_io_t **out);

int flume_prepare_storage_block_async(flume_file_t *file,
                                     uint64_t file_offset,
                                     size_t len,
                                     flume_storage_block_t **out_block,
                                     flume_io_t **out);

size_t flume_storage_block_size(flume_storage_block_t *block);
int flume_storage_block_release(flume_storage_block_t *block);

int flume_read_to_hbm_async(flume_client_t *client,
                           flume_storage_block_t *block,
                           flume_buffer_t *dst,
                           size_t dst_offset,
                           void *acl_stream,
                           flume_io_t **out);

int flume_register_storage_target_memory(
    flume_client_t *client,
    flume_buffer_t *buffer,
    size_t offset,
    size_t len,
    flume_storage_target_window_t **out);

int flume_storage_target_window_release(
    flume_storage_target_window_t *window);

int flume_get_storage_transfer_caps(flume_client_t *client,
                                    flume_storage_transfer_caps_t *out);

int flume_storage_direct_plan_create(
    flume_client_t *client,
    flume_storage_block_t *block,
    flume_buffer_t *dst,
    size_t dst_offset,
    const flume_storage_direct_options_t *options,
    flume_storage_direct_plan_t **out);

int flume_storage_direct_plan_release(flume_storage_direct_plan_t *plan);

int flume_read_storage_to_hbm_async(flume_client_t *client,
                                    flume_storage_direct_plan_t *plan,
                                    void *acl_stream,
                                    flume_io_t **out);

int flume_roce_storage_session_open(
    flume_client_t *client,
    const flume_roce_storage_options_t *options,
    flume_roce_storage_session_t **out);
int flume_roce_storage_session_close(flume_roce_storage_session_t *session);

int flume_roce_storage_read_async(
    flume_roce_storage_session_t *session,
    uint64_t object_id,
    uint64_t storage_offset,
    flume_buffer_t *dst,
    size_t dst_offset,
    size_t len,
    void *acl_stream,
    flume_io_t **out);

int flume_roce_storage_write_async(
    flume_roce_storage_session_t *session,
    uint64_t object_id,
    uint64_t storage_offset,
    flume_buffer_t *src,
    size_t src_offset,
    size_t len,
    void *acl_stream,
    flume_io_t **out);

int flume_swap_in_async(
    flume_roce_storage_session_t *session,
    uint64_t storage_offset,
    flume_buffer_t *dst_hbm,
    size_t dst_offset,
    size_t len,
    void *acl_stream,
    flume_io_t **out);

int flume_swap_out_async(
    flume_roce_storage_session_t *session,
    flume_buffer_t *src_hbm,
    size_t src_offset,
    uint64_t storage_offset,
    size_t len,
    void *acl_stream,
    flume_io_t **out);

int flume_allreduce_async(flume_client_t *client,
                         flume_buffer_t *dst,
                         size_t dst_offset,
                         flume_buffer_t *src,
                         size_t src_offset,
                         uint64_t count,
                         flume_data_type_t data_type,
                         flume_reduce_op_t op,
                         void *acl_stream,
                         flume_io_t **out);

int flume_allgather_async(flume_client_t *client,
                         flume_buffer_t *dst,
                         size_t dst_offset,
                         flume_buffer_t *src,
                         size_t src_offset,
                         uint64_t send_count,
                         flume_data_type_t data_type,
                         void *acl_stream,
                         flume_io_t **out);

int flume_wait(flume_io_t *io, int timeout_ms);
int flume_io_status(flume_io_t *io);
size_t flume_io_bytes(flume_io_t *io);
uint32_t flume_io_checksum(flume_io_t *io);
const char *flume_io_error_message(flume_io_t *io);
int flume_io_release(flume_io_t *io);

#ifdef __cplusplus
}
#endif

#endif  // FLUME_FLUME_H_
