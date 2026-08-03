# refer

本目录存放 Flume: Host-Bypass Data Path for Ascend NPU 的调研资料。

## 文件索引

- [flume-research.md](flume-research.md): 基于 Ascend CANN/HCCL/HCOMM、GPUDirect Storage/RDMA、NVMe-oF/RDMA 的可行性分析、架构路线、关键风险和验证计划。

## 参考源码

源码统一 clone 在 [cann-src](cann-src/) 下，当前均为 `master` 分支：

- [hccl](cann-src/hccl/)：`31f60015`
- [hcomm](cann-src/hcomm/)：`e6fcf022f`
- [runtime](cann-src/runtime/)：`a0428701d`
- [hixl](cann-src/hixl/)：`a05ab7b`
- [release-management](cann-src/release-management/)：`1133184`
- [shmem](cann-src/shmem/)：`6d9ab51`
- [asc-tools](cann-src/asc-tools/)：`f8ad529`

## 当前判断

公开资料显示，HCCL/HCOMM 已具备面向 Ascend NPU 集群通信的 RoCE、HCCS、PCIe 链路抽象，以及通信内存、Endpoint、Channel、Notify 等通信资源模型。但要复刻 NVIDIA GPUDirect Storage 的“存储/NIC 直接 DMA 到设备内存”路径，公开 API 层面的主要不确定性在于：Ascend NPU 设备内存是否能以通用 RDMA/NVMe-oF 栈可用的方式导出、pin、注册和授权访问。

因此第一阶段更适合做“可验证的桥接原型”，而不是一上来改 VFS/块层。建议先用 HCCL/HCOMM 的点对点/自定义通信算子能力和远端存储代理建立端到端基准，再决定是否进入内核或厂商驱动接口层。
