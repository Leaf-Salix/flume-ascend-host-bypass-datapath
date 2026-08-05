# Flume 测试工具

`tools/flume_tool.py` 是 Flume 当前推荐的本地测试和 Ascend 主机探测入口。它会为每次运行创建一个带时间戳的日志目录，并把每一步命令的 stdout/stderr 合并保存。

## 本地无 NPU 测试

```bash
python3 tools/flume_tool.py local
```

执行内容：

- 收集环境信息。
- 运行 CANN/HCCL 布局检查，作为 optional 步骤。
- 以 `FLUME_ENABLE_HCCL=OFF` 配置 CMake。
- 编译项目。
- 执行 `ctest --output-on-failure`。
- 执行 `flume-sim-demo`。
- 执行 `flume-sim-collective-demo`，其中会注册 sim A3 symmetric memory window 后再跑 collective。

macOS 或无 Ascend Linux 上的预期结果：HCCL 布局检查可以失败，但 configure、build、CTest 和 sim demo 应通过。普通步骤默认 600 秒超时，可用 `--step-timeout-sec` 调整。

## Ascend 主机探测

先加载 CANN 环境：

```bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh
python3 tools/flume_tool.py --build-dir build-ascend ascend-probe
```

执行内容：

- 收集环境信息。
- 强制检查 CANN/HCCL 布局。
- 以 `FLUME_ENABLE_HCCL=ON` 配置 CMake。
- 编译并链接 HCCL/HCOMM；HCOMM public header 依赖的 `securec.h` 也需要在 CANN include 路径下可见。
- 在 CMake configure 日志里打印可选能力位，例如 `FLUME_HAVE_HCCL_SYM_WINDOW`、`FLUME_HAVE_HCCL_COMM_MEMORY`、`FLUME_HAVE_ACL_VMM`。
- 执行当前 CTest、storage sim demo 和 collective sim demo。

默认边界：`ascend-probe` 只验证环境发现、编译和链接，以及当前 mock/sim 回归。它不会默认跑真实 HCCL 数据面，也不会要求 A3 试用 API 必须存在。

可选真机 collective smoke：

```bash
python3 tools/flume_tool.py --build-dir build-ascend --run-hccl-smoke --hccl-devices 0,1 ascend-probe
```

该 smoke 会构建并运行 `flume-hccl-collective-smoke`。如果传入 `--hccl-devices <device-a>,<device-b>`，工具默认设置 `ASCEND_RT_VISIBLE_DEVICES=<device-a>,<device-b>`，并让每个 rank 进程使用逻辑设备 `0,1`。随后通过 Flume API 在 Ascend HBM buffer 上提交 AllReduce 和 AllGather，并用 D2H 结果校验 correctness。H2D/D2H 只用于初始化和校验，collective 本身不经过 host memory staging。

可选 Stage 2 HCCL P2P copy smoke：

```bash
python3 tools/flume_tool.py --build-dir build-p2p --run-hccl-p2p-smoke --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

该模式会在同一个 `flume-hccl-collective-smoke` 中追加 `--p2p-copy`，先跑 base AllReduce / AllGather，再通过 Flume API 调用 `HcclSend` / `HcclRecv` 做 rank0 HBM 到 rank1 HBM 的配对拷贝并校验 rank1 结果。它是当前 Stage 2 的公开 HCCL baseline；HCOMM Channel / Notify / HCCL Buffer 自定义 P2P backend 仍保留为下一步。

`--run-hccl-p2p-smoke` 目前不能和 `--run-a3-symmetric-smoke` 组合。CMake 会探测 `FLUME_HAVE_HCCL_P2P`，如果当前 HCCL 头文件或库没有导出 `HcclSend` / `HcclRecv`，smoke 会直接报告不可用。

推荐真机打通顺序：

1. `auto` / `root-info`：默认推荐路径。工具让 rank0 通过 `HcclGetRootInfo` 生成 root info 文件，再启动一进程一 rank 的 `HcclCommInitRootInfo`，尽量贴近官方 HCCL root-info 测试。
2. `all` / `init-all`：单进程多卡对照路径，直接使用 `HcclCommInitAll`。
3. `rank-table`：实验诊断路径，暂存代码中用于定位 HCCL rank-table、VNIC 和 P2P memory-share 行为；当前单机 HCCS_SW 真机测试未通过，不作为首选打通路径。

如果 HCCL 控制面自动选到了 NPU 内部 vNIC，例如日志里出现 `listen on ip[192.x.x.x]`，请显式指定 Host 侧真实网卡。`--hccl-host-ifname` 填 Linux 网卡名，不是 IP；`--hccl-host-ip` 填该网卡上的 Host IP：

```bash
python3 tools/flume_tool.py --build-dir build-ascend --run-hccl-smoke --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

工具会在 HCCL smoke 步骤设置 `HCCL_SOCKET_IFNAME` 和 `HCCL_IF_IP`。如果同时使用自动生成的 rank table，工具还会把 `host_ip` 写入 `hccl-rank-table.json`，用于约束 Host TCP 控制面地址。

显式测试 root-info 初始化：

```bash
python3 tools/flume_tool.py --build-dir build-rootinfo --run-hccl-smoke --hccl-init-mode root-info --hccl-devices <device-a>,<device-b> ascend-probe
```

`root-info` 模式是当前首选 fabric bring-up：它主要验证 CANN/HCCL 运行时、device 绑定、Host TCP 控制面与基础 collective 是否可用，并复用官方 HCCL root-info 测试已经证明可走通的初始化策略。它仍只把 host 用于初始协商和结果校验，不把 collective 数据通过 host memory staging。

显式测试 `HcclCommInitAll` 对照路径：

```bash
python3 tools/flume_tool.py --build-dir build-initall --run-hccl-smoke --hccl-init-mode all --hccl-devices <device-a>,<device-b> ascend-probe
```

该路径适合判断单进程多卡初始化在现场 CANN/driver 组合上是否可用。若 `root-info` 通过但 `all` 失败，优先把 `all` 当成兼容性差异，而不是 Flume 数据面错误。

显式测试 Ascend 910 v1 rank table 初始化：

```bash
python3 tools/flume_tool.py --build-dir build-ranktable --run-hccl-smoke --hccl-init-mode rank-table --hccl-devices <device-a>,<device-b> ascend-probe
```

rank-table 模式会在日志目录生成单机 `hccl-rank-table.json`，并通过 `tools/flume_hccl_multiproc.py` 启动一进程一 rank。rank table 中的 `device_id` 保持物理卡号；如果启用默认 visible remap，每个 rank 进程仍使用逻辑卡号。单机场景默认按 HCCL 示例把 `device_ip` 留空，让 HCCL 优先使用机内链路；生成摘要会写到同一日志目录的 `hccl-rank-table-summary.txt`。这条路径当前保留为未通过真机验证的诊断分支：在已测单机 HCCS_SW die 对上会进入 VNIC/P2P memory-share 建链，并可能卡在 `rt enableP2P` 或 `P2PConnected timeout`。

单机 rank-table 模式下，即使加了 `--hccl-link-mode roce`，HCCL 也可能按 NPU topology 选择 HCCS/VNIC+P2P，日志里会表现为 `GetIsUsedRdma: isUsedRdma[0]`。因此单机 rank-table 不能作为 RoCE RDMA 数据面的严格证明；要验证 RoCE RDMA，优先使用多节点 rank table。

如果要显式测试设备网卡/RoCE rank table，可以加 `--hccl-rank-table-net device`。工具会从 `/etc/hccn.conf` 读取 `address_<physical_device_id>`，再尝试 `hccn_tool -i <device> -ip -g`。若 `hccn_tool` 需要 `sudo` 或现场已经查到了 IP，可以手动传入物理卡号到 HCCN IP 的映射：

```bash
python3 tools/flume_tool.py --build-dir build-ranktable-roce --run-hccl-smoke --hccl-init-mode rank-table --hccl-link-mode roce --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> --hccl-device-ips <device-a>=<hccn-ip>,<device-b>=<hccn-ip> ascend-probe
```

`--hccl-link-mode roce` 会设置 `HCCL_INTRA_PCIE_ENABLE=0` 和 `HCCL_INTRA_ROCE_ENABLE=1`，并让生成的 rank table 填入 `device_ip`。如果要显式固定 PCIe/HCCS 路径，可以使用 `--hccl-link-mode pcie`。

RoCE 模式下优先选择同一 HCCN 平面/同一 IPv4 `/24` 前缀的卡，例如现场 `4=<hccn-ip>` 时，可以优先尝试同为 `<hccn-subnet>` 的另一张卡。`hccl-rank-table-summary.txt` 会打印生成 rank table 中的 `device_ip_ipv4_prefix24`。

如果请求真实 HCCL smoke 且系统存在 `npu-smi`，工具会在 smoke 前额外运行 `npu-topo-check`，收集 `npu-smi info -t topo` 以及所选 device 对应 chip 的 `npu-smi info -t hccs`。这一步是 optional：它用于把 HCCS topology、lane health、error/retry 计数留进日志，避免把物理 fabric、驱动 P2P 策略和 Flume 调用路径混在一起判断。

遇到 `HCCL_E_INTERNAL` / `HCCL_E_UNAVAIL` 但普通日志不足以定位时，可以追加 `--hccl-debug-logs`，工具会设置 `ASCEND_GLOBAL_LOG_LEVEL=0` 和 `ASCEND_SLOG_PRINT_TO_STDOUT=1`，尽量把 CANN/HCCL 内部日志收入 `07-hccl-collective-smoke.log`。

如果 HCCL smoke 失败，工具会额外生成 `HCCL_SMOKE_DIAGNOSTICS.txt`，其中包含命令头、判读提示、关键 HCCL 信号、前若干条 error-like 日志和末尾日志。优先看这个摘要，再回到完整 smoke log。

常见判读：

- `LoadOpBinary` / `dynamic_*.o failed`：先检查 `ASCEND_HOME_PATH` 是否指向 CANN toolkit 根目录，例如 `/usr/local/Ascend/cann-8.5/cann-8.5.0`，不要指到 `aarch64-linux` 子目录，也不要落到错误的 `/usr/local/Ascend/cann` 软链。
- `Get available Vnic info success ... Vnic ip[192.x.x.x]`：`192.x.x.x` 是 RA 返回的 NPU socket VNIC 地址，不是 Host IP，也不是 rank table 里的 RoCE `device_ip`。
- `GetIsUsedRdma ... isUsedRdma[0]`：当前 HCCL 没走 RoCE RDMA 数据面。
- `rt enableP2P fail` / `P2PConnected timeout` / `Wait Enable P2P Failed`：P2P memory-share enable 失败；若 `npu-topo-check` 显示 HCCS health OK，则更像驱动/固件 P2P 策略或运行时状态问题。

如果现场已有框架生成的 rank table，也可以直接传入：

```bash
python3 tools/flume_tool.py --build-dir build-ranktable --run-hccl-smoke --hccl-init-mode rank-table --hccl-devices <device-a>,<device-b> --hccl-rank-table /path/to/rank_table.json ascend-probe
```

Atlas A3 HCCS symmetric-memory smoke：

```bash
python3 tools/flume_tool.py --build-dir build-a3 --run-a3-symmetric-smoke --hccl-devices 0,1 ascend-probe
```

该模式会让 `flume-hccl-collective-smoke` 追加 `--a3-symmetric`：每个 rank 通过 `aclrtReserveMemAddress` / `aclrtMallocPhysical` / `aclrtMapMem` 构造 mapped HBM，再调用 `flume_a3_register_symmetric_memory` 包装 `HcclCommSymWinRegister`，最后在同一块对称窗口内做 AllReduce 和 AllGather。它只建议在 Atlas A3 HCCS 环境使用；如果当前 CANN/HCCL 头文件没有 ACL VMM、symmetric window 或 symmetric-window config 字段，工具会直接报告该 smoke unavailable。

真实 HCCL smoke 默认 600 秒超时。排查大规模 rank 或慢初始化时可以调整：

```bash
python3 tools/flume_tool.py --build-dir build-a3 --run-a3-symmetric-smoke --hccl-smoke-timeout-sec 1200 --hccl-devices 0,1 ascend-probe
```

## 日志

每次运行会生成：

```text
logs/flume-check-YYYYMMDD-HHMMSS/
├── 00-environment.txt
├── 01-hccl-env-check.log
├── 02-npu-smi-info-m.log           # only when npu-smi exists
├── 03-npu-topo-check.log           # only when HCCL smoke requests devices
├── ...
├── NN-hccl-collective-smoke.log    # only when requested
├── hccl-root-info.bin              # only for root-info multi-process smoke
├── hccl-rank-logs/                 # child process logs, one file per rank
├── HCCL_SMOKE_DIAGNOSTICS.txt      # only when smoke fails
├── HCCL_SMOKE_SETUP_NOTES.txt      # only when setup warnings apply
└── summary.md
```

失败时先看 `summary.md`。它会标出第一个 required 失败步骤和对应日志文件。
