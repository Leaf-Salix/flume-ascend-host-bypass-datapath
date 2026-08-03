# Flume 初步 Demo 设计

日期：2026-08-03

## 目标

先实现一个能在无 NPU 开发机上编译和验证控制面、API、协议、错误处理以及端到端 buffer 语义的 demo；等接入 Ascend 服务器后，Flume 主数据面切到 HCCL/HCOMM。HIXL 只作为 one-sided transfer API、内存注册和异步状态模型的参考。

初步 demo 不直接承诺 GPUDirect Storage 等价能力。它先证明：

- 上层可以用一个稳定的 `flume_pread_async` 风格接口把远端文件块读入目标 buffer。
- 控制面能够描述 file offset、size、buffer address、request id、checksum、错误码。
- 无 NPU 环境也可以用 `SIM_HCCL_COMM` 和 `SIM_HBM` 模拟 storage->communication memory->HBM 的端到端路径。
- 无 NPU 环境也可以用 `flume_attach_sim_comm` 模拟单进程多 rank collective，验证 AllReduce / AllGather 的 API 和数据布局。
- 数据面主线明确：HCCL/HCOMM 负责 NPU HBM 互通和 storage block 进入 HBM 后的搬运。
- fallback 路径明确：远端读文件到 host buffer，再 `aclrtMemcpyAsync` 进入 device buffer，只作为 baseline，不作为最终目标。

## 参考结论

### Runtime

Runtime 是 host 侧最基础依赖，demo 需要对齐这些调用模式：

- 初始化：`aclInit`、`aclrtSetDevice`、`aclFinalize`。
- Stream：`aclrtCreateStream`、`aclrtSynchronizeStream`、`aclrtDestroyStreamForce`。
- 内存：`aclrtMallocHost`、`aclrtMalloc`、`aclrtFreeHost`、`aclrtFree`。
- 异步拷贝：`aclrtMemcpyAsync`，fallback 路径使用它实现 host staging 到 device。
- 高级内存：`aclrtMallocPhysical`、`aclrtReserveMemAddress`、`aclrtMapMem`、`aclrtMemSetAccess`、跨服务器共享 handle。这些适合作为后续真直通探索，不放进第一个 demo 的硬依赖。

### HCCL/HCOMM

HCCL/HCOMM 是第一版真实数据面的优先候选，原因是它直接覆盖 NPU rank、通信域、HCCS/RoCE/PCIe、通信内存、Channel 和 Notify。HCCL 自定义 P2P 示例已经展示了可参考路径：

- Host 侧创建 engine context、Thread、Channel。
- `HcclGetHcclBuffer` 获取本端 HCCL Buffer。
- `HcclChannelGetHcclBuffer` 获取远端 HCCL Buffer。
- AICPU 侧用 `HcommLocalCopyOnThread`、`HcommReadOnThread` 和 Channel Notify 编排数据搬运。

### HIXL

HIXL 不作为主数据面，但它的接口形态值得参考：

- `Initialize(local_engine, options)`
- `RegisterMem(MemDesc, MEM_DEVICE/MEM_HOST, MemHandle&)`
- `Connect(remote_engine)`
- `TransferSync(remote_engine, READ/WRITE, vector<TransferOpDesc>)`
- `TransferAsync(...)` + `GetTransferStatus(...)`
- `DeregisterMem(...)`
- `Finalize()`

其中 `TransferOpDesc` 只有 `local_addr`、`remote_addr`、`len`，很适合参考到我们自己的 `flume_hbm_copy_async` / `flume_pread_async` API。HIXL 官方 quickstart 的 server/client、地址交换、双方注册内存、READ 拉取远端数据，也可作为 HCCL/HCOMM bridge 控制面设计的参考。

### SHMEM

SHMEM 的 stream API 值得后续做对比实现：

- `aclshmemx_putmem_on_stream`
- `aclshmemx_getmem_on_stream`
- `aclshmemx_signal_op_on_stream`
- `aclshmemx_handle_wait`

但它要求跨服务器 RDMA 场景使用 `aclshmem_malloc` 分配对称内存，不能直接拿 `aclrtMalloc` 的普通 device pointer 当跨机 RDMA buffer。因此 SHMEM 更适合做后续对比实现，而不是 demo 主路径。

### HCCL P2P baseline

HCCL P2P 的 `HcclSend`/`HcclRecv` 要严格配对和保序；`HcclBatchSendRecv` 可以批量下发本 rank 的收发任务，但通信成员仍是 HCCL rank。第一阶段可以先用公开 P2P API 建 baseline，再用 HCOMM 自定义通信接口降低中转和同步开销。

## Demo 形态

### MVP-0：无 NPU 可运行的 mock demo

两个进程：

- `flume-store-agent`：监听 TCP 控制连接，打开本地文件，根据请求读取 offset/size，并把 payload 发回。
- `flume-demo-client`：申请 host buffer，调用 `flume_pread_async`，等待完成后校验 checksum。

数据路径：

```text
client flume_pread_async
  -> control request: file_id, offset, size, request_id
  -> agent pread
  -> mock data transfer over TCP
  -> client buffer
  -> checksum verify
```

这个阶段的意义不是性能，而是把 API、请求生命周期、错误模型、trace、测试和工程结构先立住。

### MVP-1：无 NPU 可运行的 sim demo

单进程内启动 mock storage agent，并使用模拟 buffer 类型保持目标数据路径形状：

```text
storage file block
  -> flume_pread_async
  -> FLUME_BUFFER_SIM_HCCL_COMM
  -> flume_hbm_copy_async
  -> FLUME_BUFFER_SIM_HBM
```

关键点：

- `FLUME_BUFFER_SIM_HCCL_COMM` 模拟 HCCL/HCOMM 可见的通信内存。
- `FLUME_BUFFER_SIM_HBM` 模拟计算侧 NPU HBM。
- `flume_sim_alloc_buffer` 在本地分配拥有生命周期的模拟 buffer。
- `flume_hbm_copy_async` 在无 HCCL 构建中只允许模拟 buffer 之间复制。
- 测试校验 offset、size、checksum、buffer type 和最终字节内容。

这个阶段让上层 API 先按目标链路使用，后续把 sim backend 替换成 HCCL/HCOMM backend 时尽量不改应用调用。

### MVP-2：HCCL/HCOMM HBM-HBM demo

两个或多个 NPU rank：

```text
rank HBM
  -> flume_allreduce_async / flume_allgather_async
  -> base HCCL collective
  -> rank HBM
```

关键点：

- 第一版已经有 base HCCL collective wrapper：`flume_attach_hccl_comm`、`flume_register_buffer(FLUME_BUFFER_ASCEND_HBM)`、`flume_allreduce_async`、`flume_allgather_async`。
- 真机 smoke app：`flume-hccl-collective-smoke`，只在 `FLUME_ENABLE_HCCL=ON` 时构建。
- Atlas A3 HCCS 模式可加 `--a3-symmetric`：当 CMake 探测到 ACL VMM 和 HCCL symmetric window 能力时，用 ACL mapped HBM + `flume_a3_register_symmetric_memory` 包装 `HcclCommSymWinRegister`，再跑 AllReduce / AllGather。
- 目标是证明 NPU HBM collective 不经过 host memory staging；host 仍负责通信域初始化、任务下发和 stream 同步。
- 后续再跑通 `HcclSend/HcclRecv` 或 `HcclBatchSendRecv`，并参考自定义 P2P 示例封装 HCOMM Channel、Notify、HCCL Buffer。
- 输出 bandwidth、latency、CPU 占用、block size sweep。

### MVP-3：Storage Proxy -> HCCL/HCOMM demo

存储代理作为 HCCL 通信体系的一侧或旁路控制面：

```text
storage block -> HCCL/HCOMM visible buffer -> HCOMM Channel -> compute NPU HBM
```

关键点：

- 如果 storage block 进入 proxy buffer 仍经过 host，要明确标记为 partial direct。
- 先验证 HCCL/HCOMM 后半段无 host CPU 搬运。
- 后续再探索 storage RDMA/NVMe-oF 直接写入 HCCL activated comm memory。

### MVP-4：有 NPU 的 Runtime fallback demo

两个进程仍不变，但 client 目标 buffer 换成 NPU device buffer：

```text
agent pread -> TCP payload -> client pinned host buffer -> aclrtMemcpyAsync -> NPU device buffer
```

关键点：

- client 初始化 ACL Runtime。
- client `aclrtMalloc` 申请 device buffer。
- client `aclrtMallocHost` 或 `aclrtHostRegister` 准备 staging buffer。
- 数据到达 staging buffer 后，下发 `aclrtMemcpyAsync(..., stream)`。
- `flume_wait` 内部同步 stream 或 event。

这条路径可验证上层无感知、stream 同步、错误处理和性能基线，但不是主目标路径。

### MVP-5：HIXL 对比 demo

只作为对比和参考：

```text
storage/proxy rank:
  pread file block -> registered buffer
  HIXL RegisterMem
  expose remote address through control socket

compute rank:
  aclrtMalloc device buffer
  HIXL RegisterMem(MEM_DEVICE)
  HIXL Connect
  HIXL TransferSync READ(remote file-block buffer -> local NPU buffer)
  optional aclrtMemcpy D2H checksum
```

这里有一个重要限制：HIXL 不是项目主干，不应替代 HCCL/HCOMM 方案。它用于比较 one-sided API、注册内存和异步状态机设计是否合理。

## 建议工程结构

```text
.
├── CMakeLists.txt
├── include/flume/
│   ├── flume.h
│   ├── status.h
│   └── types.h
├── src/
│   ├── core/
│   │   ├── buffer.cc
│   │   ├── client.cc
│   │   ├── request.cc
│   │   └── status.cc
│   ├── protocol/
│   │   ├── framing.cc
│   │   └── messages.cc
│   ├── transport/
│   │   ├── mock_tcp.cc
│   │   ├── hccl_hcomm.cc
│   │   ├── runtime_staging.cc
│   │   └── hixl_reference.cc
│   └── storage/
│       └── posix_file.cc
├── apps/
│   ├── flume-store-agent.cc
│   ├── flume-demo-client.cc
│   ├── flume-sim-demo.cc
│   ├── flume-sim-collective-demo.cc
│   └── flume-hccl-collective-smoke.cc
└── tests/
    ├── test_protocol.cc
    ├── test_mock_pread.cc
    ├── test_sim_end_to_end.cc
    ├── test_sim_collectives.cc
    └── test_sim_a3_symmetric_memory.cc
```

## 对外 API 草案

```c
typedef struct flume_client flume_client_t;
typedef struct flume_file flume_file_t;
typedef struct flume_buffer flume_buffer_t;
typedef struct flume_io flume_io_t;
typedef struct flume_a3_symmetric_window flume_a3_symmetric_window_t;

typedef enum {
  FLUME_BUFFER_HOST = 0,
  FLUME_BUFFER_ASCEND_HBM = 1,
  FLUME_BUFFER_HCCL_COMM = 2,
  FLUME_BUFFER_SIM_HBM = 100,
  FLUME_BUFFER_SIM_HCCL_COMM = 101
} flume_buffer_type_t;

int flume_client_open(const char *endpoint, flume_client_t **out);
int flume_client_close(flume_client_t *client);

int flume_attach_hccl_comm(flume_client_t *client,
                          void *hccl_comm,
                          uint32_t rank,
                          uint32_t rank_size);
int flume_attach_sim_comm(flume_client_t *client,
                         const char *comm_name,
                         uint32_t rank,
                         uint32_t rank_size);

int flume_open(flume_client_t *client, const char *path, flume_file_t **out);
int flume_close(flume_file_t *file);

int flume_register_buffer(flume_client_t *client,
                         void *ptr,
                         size_t len,
                         flume_buffer_type_t type,
                         flume_buffer_t **out);

int flume_sim_alloc_buffer(flume_client_t *client,
                          size_t len,
                          flume_buffer_type_t type,
                          flume_buffer_t **out);

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

int flume_pread_async(flume_file_t *file,
                     flume_buffer_t *dst,
                     size_t len,
                     uint64_t file_offset,
                     size_t buffer_offset,
                     void *acl_stream,
                     flume_io_t **out);

int flume_hbm_copy_async(flume_client_t *client,
                        flume_buffer_t *dst,
                        size_t dst_offset,
                        flume_buffer_t *src,
                        size_t src_offset,
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
```

`void *acl_stream` 先保持不透明，避免无 NPU 构建时强依赖 ACL 头文件。真实 Runtime backend 内部再把它解释为 `aclrtStream`。`SIM_*` 类型只用于本地测试；真实 Ascend 环境仍以 `FLUME_BUFFER_ASCEND_HBM` 和 `FLUME_BUFFER_HCCL_COMM` 为主。

## 第一轮实现顺序

1. 初始化项目 CMake 和公共头文件。
2. 实现 `flume-store-agent` 的 POSIX `pread` 服务。
3. 实现 mock TCP transport 和 `flume-demo-client`。
4. 加一个小测试：生成临时文件，读取多个 offset，校验 checksum。
5. 增加 `SIM_HCCL_COMM` / `SIM_HBM` buffer 和 `flume-sim-demo`。
6. 增加 sim collective world、`flume-sim-collective-demo` 和 `test_sim_collectives`。
7. 增加 HCCL/HCOMM backend 的接口壳和编译开关 `FLUME_ENABLE_HCCL`。
8. 增加 base HCCL collective wrapper 与可选真机 smoke app。
9. 增加 A3 symmetric memory API wrapper、sim 回归和可选 A3 真机 smoke。
10. 有 NPU 后实现 HCOMM Channel / custom backend 的 HBM-HBM demo。
11. 再实现 Storage Proxy -> HCCL/HCOMM demo。
12. Runtime fallback 和 HIXL reference 作为对照实现。

## 测试入口

本地无 NPU：

```bash
python3 tools/flume_tool.py local
```

该命令会生成 `logs/flume-check-*`，其中 `summary.md` 指向第一处失败日志。

Ascend 主机：

```bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh
python3 tools/flume_tool.py --build-dir build-ascend ascend-probe
python3 tools/flume_tool.py --build-dir build-ascend --run-hccl-smoke --hccl-devices 0,1 ascend-probe
python3 tools/flume_tool.py --build-dir build-a3 --run-a3-symmetric-smoke --hccl-devices 0,1 ascend-probe
```

默认 `ascend-probe` 做环境、编译、链接和 mock/sim 回归探测。加 `--run-hccl-smoke` 且传入 `--hccl-devices` 后，`auto` 初始化默认走一进程一 rank 的 HCCL root-info 路径，优先复用官方 HCCL 已验证过的 bring-up 策略来运行 base HCCL AllReduce/AllGather 真机 smoke；加 `--run-a3-symmetric-smoke` 后会在 Atlas A3 HCCS 场景尝试 symmetric memory collective。rank-table 初始化暂存为未通过真机验证的诊断路径，用来继续定位 VNIC/P2P memory-share 问题。HCOMM Channel / storage->HBM 真数据面仍要等后续 backend。

## 成功标准

MVP-0：

- macOS/Linux 无 NPU 环境可编译。
- agent/client 可读 1 KiB、1 MiB、64 MiB 文件块。
- 支持 offset、size、request id、checksum 和错误返回。

MVP-1：

- `flume-sim-demo` 可在无 NPU 环境跑通 storage->SIM_HCCL_COMM->SIM_HBM。
- `test_sim_end_to_end` 校验最终模拟 HBM 内容与远端文件 offset/size 一致。
- sim path 与未来 HCCL/HCOMM path 共用 `flume_pread_async` / `flume_hbm_copy_async` API。
- `flume-sim-collective-demo` 可在无 NPU 环境跑通 4-rank AllReduce / AllGather。
- `test_sim_collectives` 校验 AllReduce 规约结果和 AllGather rank 拼接布局。
- `test_sim_a3_symmetric_memory` 校验 A3 symmetric memory API 生命周期和注册后 collective 形状。

MVP-2：

- Ascend 环境可完成 HCCL/HCOMM HBM-HBM 数据搬运。
- base HCCL collective 的数据搬运不经过 host memory staging。
- A3 HCCS 环境可验证 registered symmetric HBM window 作为 collective 输入输出。
- Channel Notify 和 stream 同步行为明确。

MVP-3：

- 远端 storage block 可以进入 HCCL/HCOMM 可见 buffer。
- compute NPU 可以通过 HCOMM Channel 拉取到 HBM。
- 明确区分 full direct 和 partial direct。

MVP-4：

- Ascend 环境可把远端文件块读入 NPU buffer。
- D2H checksum 校验通过。
- stream 同步行为明确。

MVP-5：

- HIXL `RegisterMem` + `TransferSync READ` 跑通。
- 能与 Runtime fallback 路径对比吞吐、P50/P99、CPU 占用和 copy 次数。
