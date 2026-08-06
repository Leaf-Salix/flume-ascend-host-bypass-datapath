# CANN 8.5 Compatibility Strategy

日期：2026-08-06

## 1. 结论

Flume 的 Ascend backend 应以 **CANN 8.5 作为 baseline ABI**。高版本 CANN 可以提供增强路径，但不能让 8.5 主路径退化成不可用。

这意味着：

- CANN 8.5 是当前必须适配的主目标。
- 高版本接口只通过 feature probe 启用。
- 缺少高版本扩展时必须走 fallback，而不是编译失败或误报成功。
- 兼容判断依据是实际 header、`.so` symbol 和 runtime smoke，不是单纯版本号。

```text
CANN 8.5 baseline
  -> root-info HCCL bring-up
  -> HCCL Send/Recv P2P payload baseline
  -> HCOMM Channel resource with cpu-ts
  -> HCOMM payload backend without hccl_res_expt.h
  -> storage proxy / HCCL Buffer path

Higher CANN optional extensions
  -> AICPU_TS thread export
  -> rank graph descriptors
  -> A3 symmetric memory
  -> future direct registration capabilities
```

## 2. 为什么不先找 driver 8.5 源码

当前不把 driver 源码作为首要 refer 仓库，原因是：

| 层级 | 对 Flume 的价值 | 策略 |
| --- | --- | --- |
| HCCL/HCOMM headers | 直接决定可编译 API | 必须采集和适配 |
| HCCL/HCOMM `.so` symbols | 直接决定可链接/可运行 API | 必须采集和适配 |
| Runtime/ACL headers and symbols | 决定 stream、HBM、VMM、device 管理能力 | 必须采集和适配 |
| driver/firmware/HDK | 决定实际 P2P/VNIC/HCCS 策略，但通常不是可改的用户态 API | 当黑盒，通过日志和 smoke 诊断 |
| master 开源源码 | 有助于理解未来能力和实现思路 | 只作参考，不当作 8.5 API 真相 |

对当前代码编写最有用的不是 driver 源码，而是 **CANN 8.5 安装环境实际暴露的 ABI fixture**。

## 3. 兼容矩阵

| 能力 | CANN 8.5 期望 | 当前 Flume 策略 | 高版本增强 |
| --- | --- | --- | --- |
| HCCL root-info | 必须支持 | `auto` 初始化优先 root-info | 无 |
| HCCL init-all | 应支持，作为对照 | `--hccl-init-mode all` | 无 |
| rank-table / cluster-info | 保留诊断 | 不作为单机 HCCS_SW 主路径 | 后续按真实需求再启用 |
| HCCL AllReduce/AllGather | 必须支持 | base collective smoke | 无 |
| HCCL Send/Recv P2P | 必须支持 | Stage 2 payload baseline | 可扩展 batch/多 pair |
| HCOMM Channel resource | 必须支持 | 默认 `engine=auto -> cpu-ts` | 有 thread-export 时可选 `aicpu-ts` |
| HCOMM rank graph desc | 可选 | 有 `hccl_rank_graph.h` 时优先使用，否则 legacy desc | 高版本优先 |
| `hccl_res_expt.h` / thread export | 8.5 可缺失 | `--hcomm-require-thread-export` 返回 unsupported | 有则启用 AICPU thread-export path |
| HCOMM primitives payload | 目标必须支持 8.5 fallback | 下一阶段实现不依赖 `hccl_res_expt.h` 的路径 | 有扩展时优化 |
| A3 symmetric memory | 非 8.5 baseline | 能力位存在才启用 | A3/HCCS 增强 |
| storage -> HBM | 后续目标 | 先走 storage proxy / HCCL Buffer | direct registration 能力存在时增强 |

## 4. Backend Capability 输出

真机 smoke 会打印一行机器可解析的能力：

```text
FLUME_BACKEND_CAPS hccl_root_info=on hccl_init_all=on hccl_p2p=on hcomm_channel=on hcomm_default_engine=cpu-ts hcomm_rank_graph=off hcomm_aicpu_thread_export=off hcomm_primitives=on hcomm_payload=not-implemented storage_hbm=not-implemented cann85_baseline=feature-probed
```

判读：

| 字段 | 含义 |
| --- | --- |
| `hccl_p2p=on` | 当前 build 可以使用 `HcclSend` / `HcclRecv` baseline |
| `hcomm_channel=on` | 当前 build 可以跑 HCOMM Channel resource probe |
| `hcomm_default_engine=cpu-ts` | 当前默认 HCOMM probe 走 CANN 8.5 baseline engine |
| `hcomm_aicpu_thread_export=off` | 当前没有高版本 thread-export 扩展，CANN 8.5 正常 |
| `hcomm_rank_graph=off` | 没有 rank graph 时应 fallback legacy descriptor |
| `hcomm_payload=not-implemented` | 还没有真正 HCOMM primitive payload copy |
| `storage_hbm=not-implemented` | 还没有 storage -> HBM direct path |

`hcomm_aicpu_thread_export=off` 不是 CANN 8.5 不支持的信号。它只说明不能走高版本 AICPU thread-export path。

## 5. 远端采集流程

先跑默认 HCOMM Channel probe：

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-cann85 \
  --run-hcomm-channel-probe \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

然后采集 CANN 8.5 fixture：

```bash
python3 tools/collect_cann_compat.py \
  --label cann-8.5.0-aarch64 \
  --flume-log-dir logs/flume-check-YYYYMMDD-HHMMSS \
  --devices <device-a>,<device-b>
```

生成目录：

```text
refer/cann-compat/cann-8.5.0-aarch64/
  VERSION.txt
  env.txt
  include-manifest.txt
  include-feature-presence.txt
  lib-manifest.txt
  lib-symbols/
  cmake-feature-probe.txt
  flume-backend-caps.txt
  npu-smi.txt
  hccn-ips.txt
  summary.md
```

这些文件是文本清单，不包含 CANN 二进制。默认不提交具体机器采集结果。

## 6. CANN 8.5 主测试与预期

### 6.1 HCCL P2P baseline

```bash
python3 tools/flume_tool.py --build-dir build-p2p-baseline \
  --run-hccl-p2p-smoke \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

预期：

```text
hccl collective smoke passed ... p2p_copy=on
```

这证明公开 HCCL `Send/Recv` 能完成 HBM -> HBM payload baseline。

### 6.2 HCOMM Channel resource baseline

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-cann85 \
  --run-hcomm-channel-probe \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

预期：

```text
FLUME_HAVE_HCOMM_CHANNEL_RES: 1
FLUME_HAVE_HCOMM_THREAD_EXPORT: 0
FLUME_BACKEND_CAPS ... hcomm_channel=on ... hcomm_default_engine=cpu-ts ... hcomm_aicpu_thread_export=off ...
```

如果 probe 成功：

```text
hcomm channel probe passed ...
detail="resolved_engine=cpu-ts ... channel_desc=rank-graph|legacy-desc ... thread_export=not-required"
```

这证明 CANN 8.5 的 HCOMM Channel resource path 可用。

### 6.3 AICPU thread-export 扩展检查

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-thread-export-check \
  --run-hcomm-channel-probe \
  --hcomm-require-thread-export \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

CANN 8.5 预期返回 unsupported：

```text
HCOMM thread export is unavailable in this CANN build
```

这不是 CANN 8.5 主路径失败，只说明高版本 AICPU thread-export 扩展不可用。

## 7. 后续实现原则

后续 Stage 2.5 / Stage 3 应按这个顺序推进：

1. 先把 `cpu-ts` HCOMM Channel resource 在 CANN 8.5 真机上跑通。
2. 再查 CANN 8.5 中可用的 HCOMM primitive/on-thread API，找不依赖 `hccl_res_expt.h` 的 payload copy 路径。
3. 对高版本 CANN 添加 AICPU thread-export path，但保持 8.5 fallback。
4. 对每条数据面都打印 backend caps 和 fallback reason。
5. `FLUME_REQUIRE_NO_HOST_COPY=1` 时，任何 host staging fallback 都必须返回 unsupported，不能静默降级。

项目的长期目标仍然是 host-bypass storage -> NPU HBM，但短期必须先把 CANN 8.5 的 HCCL/HCOMM HBM-HBM 后半段做扎实。
