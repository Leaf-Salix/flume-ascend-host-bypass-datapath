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
- 在 CMake configure 日志里打印可选能力位，例如 `FLUME_HAVE_HCCL_SYM_WINDOW`、`FLUME_HAVE_HCCL_P2P`、`FLUME_HAVE_HCOMM_CHANNEL_RES`、`FLUME_HAVE_HCOMM_THREAD_EXPORT`、`FLUME_HAVE_HCOMM_PRIMITIVES`、`FLUME_HAVE_HCOMM_RANK_GRAPH`、`FLUME_HAVE_ACL_VMM`。
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

该模式会在同一个 `flume-hccl-collective-smoke` 中追加 `--p2p-copy`，先跑 base AllReduce / AllGather，再通过 Flume API 调用 `HcclSend` / `HcclRecv` 做 rank0 HBM 到 rank1 HBM 的配对拷贝并校验 rank1 结果。它是当前 Stage 2 的公开 HCCL payload baseline。

`--run-hccl-p2p-smoke` 目前不能和 `--run-a3-symmetric-smoke` 组合。CMake 会探测 `FLUME_HAVE_HCCL_P2P`，如果当前 HCCL 头文件或库没有导出 `HcclSend` / `HcclRecv`，smoke 会直接报告不可用。

可选 Stage 2 HCOMM Channel resource probe：

```bash
python3 tools/flume_tool.py --build-dir build-hcomm --run-hcomm-channel-probe --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

该模式会在 base AllReduce / AllGather 之后追加 `--hcomm-channel-probe`。probe 会调用 Flume 的 `flume_hcomm_channel_probe_ex`，在已 attach 的 HCCL comm 上尝试获取本端 HCCL Buffer、CPU_TS/AICPU_TS thread resource、可选 thread export、HCOMM Channel，以及 peer rank 的远端 HCCL Buffer。它优先通过 `HcclRankGraphGetLinks` 构造官方 link descriptor；如果当前 CANN 没有 rank graph 能力，才退回 legacy `remoteRank/channelProtocol/notifyNum` descriptor，并在 smoke 日志里标明。它验证的是 HCOMM 自定义 P2P backend 的 channel resource 准备阶段；当前还不执行 AICPU kernel，也不调用 `HcommReadOnThread` 搬 payload。

默认 HCOMM probe 使用 `engine=auto`、`protocol=hccs`、`channel notify_num=2`。`auto` 会按 CANN 能力选择最终 engine：如果探测到 `hccl_res_expt.h` / thread-export 能力，则选择 `aicpu-ts`；如果像 CANN 8.5 一样没有该扩展头，则选择 `cpu-ts`，只验证 channel resource path，不声明 AICPU thread-export-ready。需要定位现场差异时可以显式切换 channel 策略：

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-sio --run-hcomm-channel-probe --hccl-devices <device-a>,<device-b> --hcomm-channel-protocol sio ascend-probe
python3 tools/flume_tool.py --build-dir build-hcomm-cpu-ts --run-hcomm-channel-probe --hccl-devices <device-a>,<device-b> --hcomm-channel-engine cpu-ts --hcomm-channel-protocol hccs ascend-probe
python3 tools/flume_tool.py --build-dir build-hcomm-strict --run-hcomm-channel-probe --hcomm-require-thread-export --hccl-devices <device-a>,<device-b> ascend-probe
```

`--hcomm-require-thread-export` 是严格 AICPU thread-export 前置检查：它要求当前 CANN 同时支持 thread export，并且最终 engine 是 `aicpu` / `aicpu-ts`。CANN 8.5 缺少 `hccl_res_expt.h` 是正常版本差异，该模式应清晰返回 unsupported，而不是编译失败或误报 success。

可选 Stage 2.5 HCOMM payload readiness smoke：

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-payload --run-hcomm-payload-smoke --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

该模式会追加 `--hcomm-payload-smoke`。默认模式仍是 readiness probe：它会复用 HCOMM Channel resource probe，并报告当前 CANN/包状态是否足够进入 payload scheduler。未安装 payload kernel 时，预期结果是清晰的 readiness/unsupported 诊断：

```text
hcomm payload smoke unsupported ... fallback=hccl-p2p
detail="... stage3b_plan=pair-copy ..."
```

这表示：Channel 前置资源可探测，Flume 也已经生成 pair-copy primitive 编排计划，但当前环境还没有完成 payload scheduler。默认情况下这不会被当作 CANN 环境失败。要验证真正 HCOMM payload copy，追加严格模式：

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-payload-strict --run-hcomm-payload-smoke --hcomm-require-payload-copy --hccl-devices <device-a>,<device-b> ascend-probe
```

严格模式会调用 `flume_hcomm_payload_send_async` / `flume_hcomm_payload_recv_async`，rank0 走 `HcommLocalCopyOnThread(input -> local_hccl_buffer) + Notify`，rank1 走 `Notify + HcommReadOnThread(remote_hccl_buffer -> local_hccl_buffer) + HcommLocalCopyOnThread(local_hccl_buffer -> output)`，并校验 rank1 HBM 内容。完整成功需要 rank0/rank1 都打印 passed，且 marker 同时包含 `stage3b3e_payload_copy=passed`、`stage3b3e_direct_aclrt_payload_launch=passed`、`stage3b3e_payload_sync=passed`、`payload_kernel_status=success`、`payload_failure_step=none`、`payload_status_word=0`、`payload_kernel_hcomm_ret=0`、`payload_status_schema=v2`、`payload_status_word_count=8`、`payload_echo=passed`、`payload_thread_notify_order=...`、`payload_pattern=strict-v1`、source/received/expected checksum match、`payload_verify=passed` 和 `fallback=none`。如果 payload custom-op package 或 kernel 函数缺失，严格模式应失败并输出 precise unsupported reason。

已有日志也可以离线复核 strict-positive 门禁：

```bash
python3 tools/flume_tool.py hcomm-payload-verify-logs logs/flume-check-<timestamp>
```

该命令会重建 `ASCEND_FULL_MATRIX_DECISION_TREE.md`，并且只有在完整看到 rank0/rank1 passed、Stage 3B.3E launch/sync passed、kernel status success、`payload_failure_step=none`、status word 0、kernel HCOMM ret 0、`payload_status_schema=v2`、`payload_status_word_count=8`、`payload_echo=passed`、`payload_pattern=strict-v1`、rank0 source checksum、rank1 received/expected checksum 且三者一致、rank1 `payload_verify=passed` 和 `fallback=none` 时才返回 0。缺任意一个证据都会返回非 0，用于防止把 package load、canary、notify-only 或 fallback 路径误判成真正 HCOMM payload copy。
若 strict-positive 失败，decision tree 会按 rank 输出 `rankN suggested action`，
把 `comm-acquire`、`local-copy`、`ready-notify-wait`、`remote-read`、
`output-copy`、`batch-end` 等 kernel failure step 映射到具体排查方向。
它还会输出 host descriptor fingerprint 和 HCOMM resource fingerprint，
用于核对 payload bytes、local/remote HCCL Buffer size、engine/protocol、
Channel desc source、channel count 和 notify 数量是否符合预期。

payload completion 语义会用 `payload_completion_mode` 标出：HCCS/SIO 路径使用 `ordered-notify`，RoCE 路径使用 `channel-fence`，后者会在 recv kernel 的 `HcommReadOnThread` 后调用公开 `HcommChannelFenceOnThread` 再 record done，避免把“读请求已提交”误当成“payload 已落到目标 HBM”。ABI 常量名里保留 `CHANNEL_DRAIN` 是历史兼容命名，runtime marker 以 `channel-fence` 为准。
成功日志还会包含 `payload_batch_mode=on` 和
`payload_kernel_status=success`。payload kernel 使用 HCOMM 空 tag 临时批量任务，
避免 AICPU+TS 非空 tag 缓存语义影响 pair-copy smoke。如果 CANN 暴露 host/AICPU thread-export，
日志还会包含
`payload_thread_notify=host-aicpu payload_completion=thread-notify+stream-sync+status-word`；
kernel 会在 `HcommBatchModeEnd` 返回之后才 record host completion notify，
避免 host wakeup 早于 HCOMM batch 执行。对应日志 marker 是
`payload_thread_notify_order=batch-end-before-host-notify`。
否则会保留 direct ACL 路线并标记
`payload_thread_notify=unavailable payload_completion=stream-sync+status-word`
和 `payload_thread_notify_order=not-used`。
默认 batch tag 为空，保持 HCOMM temporary batch 语义。如果 batch-enabled
strict gate 失败但 no-batch 诊断通过，可以加
`--hcomm-payload-batch-tag=flume-payload-v1` 跑一次非空 tag 对照；成功日志会用
`payload_desc_batch_tag=empty|set` 标出本次 descriptor 传入的 tag 形态。
如果 direct ACL launch
和 stream sync 都通过但该字段不是 `success`，说明包加载/launch
已经不是问题，下一步应检查 descriptor 字段或 AICPU kernel 里的 HCOMM
primitive 调用。该字段会细分为 `local-copy-failed`、
`ready-notify-record-failed`、`ready-notify-wait-failed`、
`remote-read-failed`、`done-notify-record-failed`、`batch-start-failed`、
`batch-end-failed` 或 `thread-notify-*-failed` 等阶段；失败时还会打印
`payload_kernel_hcomm_ret=<ret>`，表示对应 HCOMM primitive 的原始返回码。
若同时看到 `payload_primitive_state=pending` 和
`payload_kernel_hcomm_ret=4294967295`，表示 kernel 已进入
`payload_failure_step` 对应的 HCOMM primitive，但 status read 时该
primitive 仍未返回，通常应按 timeout/hang 定位该阶段，而不是把
`4294967295` 当成真实 HCOMM 返回码。
如果看到 `payload_kernel_status=not-written` 或
`payload_status_word=4294967295`，说明 kernel 没有写回 device-visible
status word，应优先检查 descriptor handoff、status pointer 和 kernel
是否实际执行，而不是先定位 HCOMM primitive。

构建 Flume custom-op package 有三条路径。Host B 这类有 CANN toolkit、但没有
HCCL source packaging flow 的环境，优先试 direct-build：

```bash
# 推荐优先：直接用已安装 CANN toolkit 编出 JSON/tar，并导出隔离 runtime layout
python3 tools/flume_tool.py \
  --custom-op-build-mode payload \
  --custom-op-export-root <temporary-custom-op-root> \
  hcomm-custom-op-direct-build

# 可选：由 Flume 工具调用 HCCL source custom-op packaging flow，并自动做产物 preflight
python3 tools/flume_tool.py \
  --hccl-source-root <path-to-cann-hccl-source> \
  --custom-op-build-mode payload \
  hcomm-custom-op-build

# 构建后立即安装，并验证安装后的 package 能被 Flume runtime 扫描到
python3 tools/flume_tool.py \
  --hccl-source-root <path-to-cann-hccl-source> \
  --custom-op-build-mode payload \
  --install-custom-op-package \
  hcomm-custom-op-build

# 不修改系统 CANN/OPP：把已通过 preflight 的 JSON/tar 导出成 runtime layout
python3 tools/flume_tool.py \
  --custom-op-json <path-to-libflume_hcomm_payload_aicpu_kernel.json> \
  --custom-op-aicpu-tar <path-to-aicpu_flume_hcomm_payload.tar.gz> \
  --custom-op-build-mode payload \
  --custom-op-export-root <temporary-custom-op-root> \
  hcomm-custom-op-export-runtime

# 如只想构建 no-internal-header canary kernel，用于 Stage 3B.3D
python3 tools/flume_tool.py \
  --hccl-source-root <path-to-cann-hccl-source> \
  --custom-op-build-mode canary \
  hcomm-custom-op-build

# 等价的底层 HCCL build.sh 形式：
bash build.sh --vendor=flume --ops=hcomm_payload \
  --custom_ops_path=<flume-repo>/custom_ops/hcomm_payload_copy

FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD=ON \
bash build.sh --vendor=flume --ops=hcomm_payload \
  --custom_ops_path=<flume-repo>/custom_ops/hcomm_payload_copy
```

`hcomm-custom-op-direct-build` 会查找 `ASCEND_HOME_PATH` 或标准 CANN layout
下的 `hcomm_primitives.h` 和 `libhcomm.so`，直接编
`libflume_hcomm_payload_aicpu_kernel.so`，打包
`aicpu_flume_hcomm_payload.tar.gz`，复制匹配的 JSON，执行 package
preflight；如果传了 `--custom-op-export-root`，还会把通过 preflight 的产物
导出到 `<temporary-custom-op-root>/opp/vendors/<vendor>/aicpu/{config,kernel}`。
这条路径不需要 `hccl/hccl_launch.h`，也不需要 HCCL source `build.sh`，但
payload 模式需要当前 toolkit 的 `libhcomm.so` 导出 HCOMM primitive 符号。

`hcomm-custom-op-build` 默认使用 `payload` 模式，也就是打开
`FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD=ON`，生成 Stage 3B.3E 所需的
HCOMM primitive payload package。该模式需要目标 CANN/HCCL 源码构建环境能提供 HCOMM primitive 头和
device-side `ccl_kernel` 链接能力，但 direct ACL payload 路线不需要
`hccl/hccl_launch.h`。只有在目标 CANN 明确暴露 `hccl_launch.h` 且需要测试
legacy public-HCCL-launch notify-only 入口时，才额外传
`--build-public-hccl-launch` 或打开
`FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH=ON`。如果 Host B 上还没有 HCCL
source tree，该命令会在 build 前清晰报 `missing HCCL source build.sh`，
这表示缺 packaging toolchain，不是 Flume runtime 或 HCOMM payload kernel
逻辑失败。`--install-custom-op-package` 会执行生成的 `.run --install` 并在
安装后再跑一次 installed-package preflight；它是显式 opt-in，因为会修改目标
CANN/OPP 安装状态。安装前必须先通过 build artifact preflight；如果 JSON、
AICPU tar、V4 payload entrypoint 或 primitive-payload marker 不完整，工具会拒绝安装。
`hcomm-custom-op-export-runtime` 是不污染系统安装的替代路径：它只把已通过
preflight 的 JSON/tar 复制到
`<temporary-custom-op-root>/opp/vendors/<vendor>/aicpu/{config,kernel}`，
后续 strict-positive 命令传 `--custom-op-root <temporary-custom-op-root>` 即可。
如果只想临时验证 loose build artifacts，也可以在 strict-positive smoke
中同时传 `--custom-op-json <json>` 和 `--custom-op-aicpu-tar <tar>`；工具会把
两者都转发给 runtime，避免 package preflight 通过但 C++ launcher 因找不到
matching AICPU tar 而降级为 not-ready。

安装包后可以先做不依赖 NPU 的包体自检：

```bash
# 检查默认 canary package
python3 tools/flume_tool.py hcomm-custom-op-package

# 检查 Stage 3B.3E payload-copy kernel 是否也在包里
python3 tools/flume_tool.py --require-hcomm-payload-kernel \
  hcomm-custom-op-package

# 如未安装 run 包，也可以直接检查某个 CANN/OPP root 或 build 产物
python3 tools/flume_tool.py --custom-op-root <cann-root-or-opp-root> \
  --require-hcomm-payload-kernel hcomm-custom-op-package
python3 tools/flume_tool.py \
  --custom-op-json <path-to-libflume_hcomm_payload_aicpu_kernel.json> \
  --custom-op-aicpu-tar <path-to-aicpu_flume_hcomm_payload.tar.gz> \
  --require-hcomm-payload-kernel hcomm-custom-op-package
```

如果第二条失败并出现 `payload_direct_aclrt ... missing`，说明当前安装的是
canary-only 包或 payload 包不完整；如果同时出现
`reason.payload_direct_aclrt=legacy-entrypoint-present`，说明当前安装的是
旧 payload 包。当前 payload-ready 要求 JSON 声明
`FlumeHcommPayloadCopyDirectAclrtKernelV4`、`FlumeHcommPayloadCopyAbiVersion4`
、`FlumeHcommPayloadCopySemanticVersion` 和
`FlumeHcommPayloadCopyRequiresCommAcquire`，以及当前 device-visible status ABI
marker `FlumeHcommPayloadStatusSchemaVersion` 和
`FlumeHcommPayloadStatusWordCount`。旧包只声明
`FlumeHcommPayloadCopyDirectAclrtKernel` 或缺 semantic v5 marker 时会被明确判为
stale，需要用 `FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD=ON` 重新打包安装后再跑
strict payload smoke。该检查还会确认 AICPU tar
是否可读、是否包含 `libflume_hcomm_payload_aicpu_kernel.so`，并在
`readelf` 或 `nm` 可用时检查 tar 内 SO 是否真的导出
`FlumeHcommCanaryDirectAclrtKernel`、
`FlumeHcommPayloadCopyDirectAclrtKernelV4` 和
`FlumeHcommPayloadBuildModeInternalPayload`，并要求
`FlumeHcommPayloadCopyAbiVersion4` 和
`FlumeHcommPayloadCopySemanticVersion` 以及
`FlumeHcommPayloadCopyRequiresCommAcquire`、`FlumeHcommPayloadStatusSchemaVersion`
和 `FlumeHcommPayloadStatusWordCount` 同时出现在 JSON 和 SO 里，作为当前
descriptor ABI、payload success-status schema/word-count 与 HCOMM comm
acquire/release 语义 marker。默认 canary-only
包可能为了 JSON/SO 兼容导出 V4 stub；没有 internal build-mode marker、
ABI v4 marker、semantic v5 marker、comm-acquire marker 或 status schema marker 时不会被判为
payload-ready，避免把空包、坏包、stub 包、旧 ABI 包、旧语义包、缺 HCOMM
comm acquire/release、旧 status ABI 的包或 JSON/SO 不一致的包误判为可跑 strict payload。

`ascend-probe` 在运行 `--run-hcomm-payload-smoke` 或
`--run-hcomm-notify-only-smoke` 时会自动追加
`hcomm-custom-op-package-preflight` 诊断步骤；`ascend-full-matrix` 会默认用
payload-required 模式检查包体，并在
`ASCEND_FULL_MATRIX_DECISION_TREE.md` 里标记 package 是 `not-ready`、
`canary-ready` 还是 `payload-ready`。
`--custom-op-root`、`--custom-op-json`、`--custom-op-aicpu-tar` 和
`--custom-op-vendor` 也会传给真实 HCOMM smoke runtime；`--custom-op-json`
是 authoritative，路径写错时 runtime 不会悄悄回退到系统安装目录。
`--custom-op-aicpu-tar` 不作为 `aclrtBinaryLoadFromFile` 的输入，但会作为
loose build artifacts 的 runtime readiness tar，因此 preflight 与 C++
launcher 不会使用两套不同的 package 判断。

未安装 primitive payload 包时，严格模式预期失败并返回 unsupported。推荐把 `--run-hcomm-payload-smoke` 与 `--run-hccl-p2p-smoke` 一起跑，以同时验证 fallback：

```bash
python3 tools/flume_tool.py --build-dir build-stage25 --run-hccl-p2p-smoke --run-hcomm-payload-smoke --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

如果要验证 Stage 3B custom-op/AICPU scheduler 分支已经进入编译产物，可以追加：

```bash
python3 tools/flume_tool.py --build-dir build-stage3b-customop --build-hcomm-custom-op --run-hcomm-payload-smoke --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

当前预期仍是 unsupported，但 detail 应从默认的 `custom-op/AICPU scheduler build disabled` 变成 `stage3b3b_launcher_router=selected:unsupported`，并列出 `public_hccl_launch`、`direct_aclrt`、`thread_export`、`hcomm_primitives` 和 `custom_op_package` 状态。`thread_export=off` 只表示不能使用 host/AICPU thread notify completion 增强；direct ACL payload candidate 仍可用 `stream-sync+status-word` 作为完成证明。如果当前 CANN 暴露 direct ACL runtime launch API，还会追加 `stage3b3c_direct_aclrt_loader`、`stage3b3c_descriptor_handoff` 和 `stage3b3c_direct_aclrt_launch`，用于区分 custom-op 包缺失、函数解析失败、descriptor ABI 失败和真实 launch 失败。Stage 3B.3D 还会追加 `stage3b3d_no_internal_headers=on` 和 `stage3b3d_direct_aclrt_canary_*` marker，用于验证不依赖 HCCL/HCOMM 内部头的 direct ACL custom-op canary 路径。

也可以只跑 Stage 3B.1 no-op launch readiness，不跑 payload readiness：

```bash
python3 tools/flume_tool.py --build-dir build-stage3b1 --run-hcomm-custom-op-launch-smoke --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

成功诊断 marker 是 `hcomm custom-op launch smoke unsupported` 加 `stage3b1_launch_plan=noop-custom-op`。当前 unsupported 是预期结果；它说明 Flume 已经完成 channel resource 和 no-op launch plan 的诊断链路，但真实 custom-op/AICPU launcher 还没有实现。

Stage 3B.2 resource descriptor smoke：

```bash
python3 tools/flume_tool.py --build-dir build-stage3b2 --run-hcomm-resource-descriptor-smoke --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

当前预期仍是 unsupported，但 detail 应包含 `stage3b2_resource_descriptor=host-packaged` 和 `stage3b2_descriptor_handoff=missing`。这表示 HCOMM Channel、local/remote HCCL Buffer、notify 索引、rank 元数据、engine/protocol 和 desc source 已经被整理成稳定 descriptor；下一步缺的是把该 descriptor 传给 custom-op/AICPU kernel 并执行 notify no-op。

Stage 3B.2-complete / 3B.3-prep notify-only smoke：

```bash
python3 tools/flume_tool.py --build-dir build-stage3b2-notify --run-hcomm-notify-only-smoke --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

当前预期仍是 unsupported，但 detail 应包含 `stage3b2_notify_only_plan=channel-notify`、`stage3b2_kernel_consume=missing`、`stage3b3b_launcher_router=selected:unsupported` 和 direct ACL readiness marker。若未安装 Flume custom-op package，预期是 `stage3b3c_direct_aclrt_loader=unsupported`、`stage3b3c_descriptor_handoff=blocked`、`stage3b3c_direct_aclrt_launch=not-attempted`，同时 Stage 3B.3D canary 预期停在 `stage3b3d_direct_aclrt_canary_loader=unsupported` / `stage3b3d_direct_aclrt_canary_launch=not-attempted`。若已安装只含 canary 的 Flume custom-op package，则可期待 `stage3b3d_direct_aclrt_canary=passed canary_status_word=0 canary_observed_token=1128357465`，表示 no-internal-header custom-op kernel 已消费 descriptor 并写回 device-visible token，但这仍不代表 HCOMM notify-only 已经完成。若安装了 primitive payload 包，notify-only direct ACL 成功还应包含 `stage3b2_kernel_consume=passed notify_kernel_status=success notify_status_word=0`；否则应优先根据 `notify_kernel_hcomm_ret=<ret>` 定位 in-kernel HCOMM Notify 调用。

可选 Stage 3A storage proxy HBM smoke：

```bash
python3 tools/flume_tool.py \
  --build-dir build-storage-hbm \
  --run-storage-hbm-smoke \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --storage-smoke-file /path/on/local-ssd/flume-storage-smoke.bin \
  --storage-smoke-bytes 4096 \
  ascend-probe
```

该模式验证 Stage 3A fallback 实路径，不声明 full direct。数据路径是：

```text
local SSD file slice
  -> rank0 host pinned buffer
  -> rank0 proxy HBM
  -> HcclSend / HcclRecv
  -> rank1 compute HBM
  -> checksum verification
```

rank1 的期望 checksum 由 `flume_tool.py` 在启动前根据同一个文件切片计算，并通过 `--storage-smoke-checksum` 传给 rank 进程，因此 rank1 不需要再读取 SSD 来获得期望数据。成功 marker：

```text
rank 1 storage HBM smoke passed: storage_hbm=hccl-p2p-staging ...
```

如果不传 `--storage-smoke-file`，工具会在本次 `logs/flume-check-*/storage-smoke-input.bin` 自动生成确定性输入文件；这适合快速验证工具链。要测试远端主机本地 SSD，请显式传入 SSD 路径。`--storage-smoke-bytes` 必须小于等于 `--hccl-count * 4`，因为当前 smoke 复用 FP32 collective 的每 rank HBM buffer 做 byte payload staging；例如传 `--storage-smoke-bytes 16777216` 时，应设置 `--hccl-count 4194304` 或更大。

`pcie` 对当前 HCCL `HcclChannelAcquire` probe 默认判为 unsupported，保留这个值只是为了把误用场景诊断清楚；推荐优先测试 `hccs` 或现场拓扑对应的 `sio`。`hccs-only` 是 Flume 侧保留的诊断别名，在当前 CANN 8.5/9.0 头文件里会映射到 `COMM_PROTOCOL_HCCS`。

可选值：

| 参数 | 默认 | 可选值 |
| --- | --- | --- |
| `--build-hcomm-custom-op` | off | 配置 `FLUME_BUILD_HCOMM_CUSTOM_OP=ON`，只打开 Stage 3B custom-op/AICPU scheduler 编译分支；当前 launcher 仍预期返回 unsupported |
| `--run-hcomm-custom-op-launch-smoke` | off | 运行 Stage 3B.1 no-op custom-op launch readiness smoke |
| `--run-hcomm-resource-descriptor-smoke` | off | 运行 Stage 3B.2 resource descriptor packaging smoke |
| `--run-hcomm-notify-only-smoke` | off | 运行 Stage 3B.2-complete / 3B.3-prep notify-only kernel-consume readiness smoke |
| `--hcomm-channel-engine` | `auto` | `auto`, `aicpu`, `aicpu-ts`, `cpu`, `cpu-ts` |
| `--hcomm-channel-protocol` | `hccs` | `auto`, `hccs`, `hccs-only`, `roce`, `pcie`, `sio` |
| `--hcomm-notify-num` | `2` | `1..64`，设置 `HcclChannelDesc.notifyNum` |
| `--hcomm-timeout-sec` | `60` | HCOMM kernel 内 notify / payload wait 超时；应小于 rank 级 HCCL smoke 超时 |
| `--hcomm-require-thread-export` | off | 严格要求 thread-export / AICPU thread-export-ready 前置能力 |
| `--hcomm-require-payload-copy` | off | 严格要求真实 HCOMM payload copy；当前 Stage 2.5 skeleton 预期 unsupported |

可以和 P2P baseline 合在一起跑：

```bash
python3 tools/flume_tool.py --build-dir build-stage2 --run-hccl-p2p-smoke --run-hcomm-channel-probe --hccl-devices <device-a>,<device-b> --hccl-host-ifname <host-ifname> --hccl-host-ip <host-ip> ascend-probe
```

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

HCCL smoke 日志会打印 `FLUME_BACKEND_CAPS ...`，用于快速判断当前 CANN/HCCL/HCOMM backend 能力，例如 `hcomm_default_engine=cpu-ts`、`hcomm_aicpu_thread_export=off`、`hcomm_payload_probe=on`、`hcomm_payload_scheduler=not-implemented`、`hcomm_payload_scheduler_candidate=on|off`、`hcomm_payload_direct_aclrt=on|off`、`hcomm_payload_thread_notify=on|off`、`hcomm_launcher_public_hccl=off`、`hcomm_launcher_direct_aclrt=on|off`、`hcomm_payload=not-implemented`。CANN 8.5 下 `hcomm_aicpu_thread_export=off` 是正常版本差异，不代表 HCOMM Channel resource path 不支持，也不阻止 direct ACL payload route 使用 `stream-sync+status-word` completion。`hcomm_primitives=off` 是 host-side primitive probe 结果；direct ACL payload 路径的 primitive 调用发生在已安装 custom-op package 内部，因此 package-ready + strict smoke 才是最终证据。`hcomm_payload_scheduler_candidate=on` 只表示当前 build 具备 direct ACL payload scheduler 候选路径；真正成功仍以 strict payload smoke 同时输出两 rank passed、`stage3b3e_payload_copy=passed`、direct ACL payload launch/sync passed、`payload_kernel_status=success`、`payload_failure_step=none`、`payload_status_word=0`、`payload_kernel_hcomm_ret=0`、`payload_status_schema=v2`、`payload_status_word_count=8`、`payload_echo=passed`、`payload_thread_notify_order=...`、`payload_pattern=strict-v1`、source/received/expected checksum match、`payload_verify=passed` 和 `fallback=none` 为准。

完整两卡矩阵建议用：

```bash
python3 tools/flume_tool.py --build-dir build-full \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-full-matrix
```

`ascend-full-matrix` 会一次构建，然后跑 local tests/sim、HCCL collective、
HCCL P2P baseline、HCOMM Channel probe、HCOMM payload readiness，并追加
`--hcomm-require-payload-copy`。如果 package preflight 显示
`payload-ready`，strict payload copy 会作为 required positive 执行；如果
package 还没 ready，则该步骤保留为 optional expected negative。当前未安装
payload package 时预期 readiness 返回 `unsupported` / `fallback=hccl-p2p`，
strict negative 失败但在 summary 中标为 optional；这说明缺的是可安装的
Flume custom-op/AICPU payload package，而不是 HCCL collective 或 HCCL P2P
baseline。

payload package 已经通过 preflight 后，可以用更窄的严格正例入口，只验证
Stage 3B.3E 真实 HCOMM payload copy：

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-payload-positive \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  --auto-build-hcomm-payload-package \
  --auto-run-hcomm-payload-nobatch-diagnostic \
  --auto-run-hcomm-payload-tagged-diagnostic \
  --collect-cann-compat-label host-b-cann \
  hcomm-payload-strict-positive
```

这个入口会自动启用 `FLUME_BUILD_HCOMM_CUSTOM_OP=ON`，先 required 检查
custom-op package 是否 `payload-ready`，再跑 HCCL P2P baseline 和
`--hcomm-require-payload-copy`。完整成功必须同时看到 rank0/rank1
`hcomm payload smoke passed`，以及 `stage3b3e_payload_copy=passed`、
`stage3b3e_direct_aclrt_payload_launch=passed`、`stage3b3e_payload_sync=passed`、
`payload_kernel_status=success`、`payload_failure_step=none`、`payload_status_word=0`、
`payload_kernel_hcomm_ret=0`、`payload_status_schema=v2`、`payload_status_word_count=8`、`payload_echo=passed`、`payload_thread_notify_order=...`、`payload_pattern=strict-v1`、source/received/expected checksum match、`payload_verify=passed` 和 `fallback=none`。
如果 preflight 失败，这个入口会在 launch 前停止，避免把 canary-only 包或旧
entrypoint 包误判成 payload copy 失败。

Stage 3B.4 storage-over-HCOMM focused gate：

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-storage-positive \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  --auto-build-hcomm-payload-package \
  --auto-run-hcomm-payload-nobatch-diagnostic \
  --auto-run-hcomm-payload-tagged-diagnostic \
  --collect-cann-compat-label host-b-cann \
  hcomm-storage-strict-positive
```

这个入口同样要求 custom-op package `payload-ready`，并先要求 Stage 3B.3E
strict payload copy 证据完整，再运行 storage HBM smoke。完整成功除了
strict-positive 全套 marker，还必须看到 rank1 storage 校验通过并打印
`storage_hbm=hcomm-payload-staging`。这条路径仍然是
`file -> host -> proxy_hbm -> HCOMM payload -> compute_hbm`，用于验证
storage proxy 已接到 HCOMM payload scheduler；它还不是 full
storage-direct DMA。

已有日志可以离线验证：

```bash
python3 tools/flume_tool.py hcomm-storage-verify-logs logs/flume-check-...
```

该命令只有在 strict-positive payload 证据完整且 storage smoke 走
`storage_hbm=hcomm-payload-staging` 时返回 0。

如果要同时采集 CANN fixture：

```bash
python3 tools/flume_tool.py --build-dir build-full \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --collect-cann-compat-label cann-8.5.0-aarch64 \
  ascend-full-matrix
```

如果需要把现场 CANN 8.5 能力固化成文本 fixture，先跑一次 `ascend-probe`，再执行：

```bash
python3 tools/collect_cann_compat.py --label cann-8.5.0-aarch64 --flume-log-dir logs/flume-check-YYYYMMDD-HHMMSS --devices <device-a>,<device-b>
```

采集结果写入 `refer/cann-compat/cann-8.5.0-aarch64/`，默认被 Git 忽略；它只包含 header/lib/symbol/log 文本清单，不复制 CANN 二进制。

Stage 3B.3E payload copy 调试时，优先查看这些新增 fixture：

- `hcomm-primitive-headers.txt`：真实 `hcomm_primitives.h` 中和 Flume payload kernel 有关的声明摘录。
- `hcomm-primitive-call-shape-probe.txt`：只编译、不链接、不运行，验证当前 `HcommAcquireComm`、`HcommLocalCopyOnThread`、`HcommReadOnThread`、Notify、Batch 调用形状是否被真实 CANN 头文件接受；`status: PASS` 才说明 ABI 形状匹配。
- `hcomm-primitive-symbols.txt`：`libhcomm` 中这些 primitive 的目标符号是否存在。

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

真实 HCCL smoke 默认 600 秒进程级超时，root-info / rank-table 多进程
launcher 会给 rank 子进程预留 5 秒收尾窗口。HCOMM kernel 内 notify /
payload wait 默认 60 秒超时。`--hcomm-timeout-sec` 应小于 rank 级 HCCL
smoke 超时，这样 HCOMM primitive 或 notify 等待失败时，kernel 有机会先写回
`payload_kernel_status` / `payload_status_word`，而不是被外层进程 timeout
直接杀掉。排查大规模 rank 或慢初始化时可以调整：

```bash
python3 tools/flume_tool.py --build-dir build-a3 --run-a3-symmetric-smoke \
  --hccl-smoke-timeout-sec 1200 --hcomm-timeout-sec 90 \
  --hccl-devices <device-a>,<device-b> ascend-probe
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
