# Stage 3B HCOMM Custom-Op Payload Scheduler Plan

Stage 3B 的目标是把 Stage 3A 的 `storage_hbm=hccl-p2p-staging`
fallback，逐步替换成 Flume 自己的 HCOMM primitive payload scheduler。
它不是重新实现 HCCL collective，而是把 HCCL 不覆盖的 storage payload
路径接入 NPU HBM 数据面。

## Why Stage 3B Exists

当前已经通过真机验证的 Stage 3A 路径是：

```text
local SSD file slice
  -> rank0 host pinned buffer
  -> rank0 proxy HBM
  -> HcclSend / HcclRecv
  -> rank1 compute HBM
  -> checksum verification
```

这证明 storage payload 可以进入 Flume 的 NPU fabric pipeline，但仍有两点不满足最终目标：

| Gap | Current Stage 3A | Stage 3B Target |
| --- | --- | --- |
| HBM-to-HBM payload transport | HCCL public P2P fallback | HCOMM Channel + primitive scheduler |
| Scheduler ownership | HCCL internal send/recv | Flume custom-op/AICPU launch path |
| Storage-direct claim | Not claimed | Still not claimed until storage/RDMA registration is proven |
| Host CPU payload participation | rank0 read + H2D staging remains | progressively move payload work into NPU-side scheduler |

因此 Stage 3B 先解决 HCOMM primitive 是否能由 Flume 调度的问题；
Stage 4 再解决 storage/RDMA 如何直接进入 NPU-visible memory。

## Stage Breakdown

| Stage | Goal | Success Marker | Expected Failure Before Done |
| --- | --- | --- | --- |
| 3B.1 | no-op custom-op/AICPU launch readiness | `hcomm custom-op launch smoke passed` | `custom-op/AICPU scheduler build disabled` or `custom-op/AICPU scheduler launch missing` |
| 3B.2 | package HCOMM resource descriptor and prepare custom-op handoff | `stage3b2_resource_descriptor=host-packaged` | `custom-op/AICPU descriptor handoff is missing` |
| 3B.2-complete / 3B.3-prep | consume descriptor and run notify-only channel sync | `stage3b2_kernel_consume=passed` and `stage3b2_notify_only_plan=channel-notify` | `stage3b2_kernel_consume=missing` |
| 3B.3 | execute HCOMM pair-copy primitives | `hcomm_payload_scheduler=custom-op-aicpu` and checksum pass | primitive call failure / stream sync failure |
| 3B.4 | wire scheduler into storage HBM path | `storage_hbm=hcomm-payload-staging` | fallback remains `hccl-p2p` |

## Stage 3B.1: Custom-Op Launch Readiness

3B.1 deliberately does not move payload. It proves the launcher boundary:

```text
Flume process
  -> HCCL comm attached
  -> HCOMM Channel resource acquired
  -> no-op custom-op launch plan generated
  -> scheduler status reported
```

The implementation surface is:

| Layer | File/API | Role |
| --- | --- | --- |
| Public API | `flume_hcomm_custom_op_launch_smoke_ex` | callable launch-readiness probe |
| Plan model | `BuildCustomOpLaunchSmokePlan` | stable no-op launch step plan |
| Smoke app | `--hcomm-custom-op-launch-smoke` | true NPU observable marker |
| Tooling | `--run-hcomm-custom-op-launch-smoke` | one-command remote validation |
| Build switch | `--build-hcomm-custom-op` | toggles `FLUME_BUILD_HCOMM_CUSTOM_OP=ON` |

The current implementation has two honest outcomes:

| Build Mode | Expected Result | Meaning |
| --- | --- | --- |
| default | unsupported, `custom-op/AICPU scheduler build disabled` | normal development build; launcher branch is off |
| `--build-hcomm-custom-op` | unsupported, `custom-op/AICPU scheduler launch missing` | compile branch is present; real launcher still needs implementation |

3B.1 is complete when a future build can report:

```text
hcomm custom-op launch smoke passed
stage3b1_launch_plan=noop-custom-op
```

That would prove the no-op custom-op can be launched and synchronized on the
caller stream. It still would not prove payload copy.

## Stage 3B.2: Resource Descriptor Smoke

3B.2 packages the resource descriptor needed by the payload kernel:

```text
HCCL comm
HCOMM channel
local HCCL buffer
remote HCCL buffer
ready/done notify indices
rank metadata
```

The current 3B.2 implementation stops at host-side packaging and reports the
handoff boundary honestly:

```text
hcomm resource descriptor smoke unsupported
stage3b2_resource_descriptor=host-packaged
stage3b2_descriptor_handoff=missing
```

This means HCOMM Channel acquisition and descriptor packaging worked, but the
descriptor has not yet been consumed by a custom-op/AICPU kernel.

The next 3B.2 completion step is for the kernel to perform a notify no-op:

```text
rank0: record ready -> wait done
rank1: wait ready -> record done
```

Success means resources survive the host-to-custom-op boundary. Failure at this
stage should not be interpreted as a storage or payload-copy failure.

The repository now has a 3B.2-complete / 3B.3-prep notify-only plan:

| Rank | Steps |
| --- | --- |
| send rank | consume resource descriptor -> `HcommChannelNotifyRecordOnThread(ready)` -> `HcommChannelNotifyWaitOnThread(done)` |
| recv rank | consume resource descriptor -> `HcommChannelNotifyWaitOnThread(ready)` -> `HcommChannelNotifyRecordOnThread(done)` |

Current true Ascend backend expected result:

```text
hcomm notify-only smoke unsupported
stage3b2_resource_descriptor=host-packaged
stage3b2_notify_only_plan=channel-notify
stage3b2_kernel_consume=missing
```

This is the last synchronization-only boundary before enabling
`HcommLocalCopyOnThread` / `HcommReadOnThread` payload movement.

## Stage 3B.3: HCOMM Pair-Copy Primitive Scheduler

3B.3 executes the current pair-copy plan:

| Rank | Steps |
| --- | --- |
| send rank | `HcommLocalCopyOnThread(input -> local_hccl_buffer)` -> `HcommChannelNotifyRecordOnThread(ready)` -> `HcommChannelNotifyWaitOnThread(done)` |
| recv rank | `HcommChannelNotifyWaitOnThread(ready)` -> `HcommReadOnThread(remote_hccl_buffer -> output)` -> `HcommChannelNotifyRecordOnThread(done)` |

Success changes backend capability semantics from:

```text
hcomm_payload_scheduler=not-implemented
fallback=hccl-p2p
```

to:

```text
hcomm_payload_scheduler=custom-op-aicpu
hcomm_payload=on
fallback=none
```

## Stage 3B.4: Storage HBM Integration

Once the HCOMM pair-copy scheduler works, Stage 3A can be rewired:

```text
file/storage block
  -> proxy HBM or HCCL buffer
  -> HCOMM payload scheduler
  -> compute HBM
  -> checksum verification
```

This is still a staging path unless storage/RDMA can DMA directly into
NPU-visible memory. Full host-bypass requires Stage 4 storage registration work.

## Remote Validation Commands

Default 3B.1 readiness check:

```bash
python3 tools/flume_tool.py \
  --build-dir build-stage3b1 \
  --run-hcomm-custom-op-launch-smoke \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

Expected current result:

```text
hcomm custom-op launch smoke unsupported
stage3b1_launch_plan=noop-custom-op
custom-op/AICPU scheduler build disabled
```

Compile-branch readiness check:

```bash
python3 tools/flume_tool.py \
  --build-dir build-stage3b1-customop \
  --build-hcomm-custom-op \
  --run-hcomm-custom-op-launch-smoke \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

Expected current result:

```text
hcomm custom-op launch smoke unsupported
stage3b1_launch_plan=noop-custom-op
custom-op/AICPU scheduler launch missing
```

This second result is useful: it proves the build switch reached the runtime
diagnostic path, so the next missing piece is the real custom-op/AICPU launcher.

Stage 3B.2 resource descriptor smoke:

```bash
python3 tools/flume_tool.py \
  --build-dir build-stage3b2 \
  --run-hcomm-resource-descriptor-smoke \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

Expected current result:

```text
hcomm resource descriptor smoke unsupported
stage3b2_resource_descriptor=host-packaged
stage3b2_descriptor_handoff=missing
```

Compile-branch descriptor check:

```bash
python3 tools/flume_tool.py \
  --build-dir build-stage3b2-customop \
  --build-hcomm-custom-op \
  --run-hcomm-resource-descriptor-smoke \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

Expected current result still reports unsupported, but the detail should include
`custom-op/AICPU scheduler launch missing`, proving the descriptor path reached
the custom-op build branch.

Stage 3B.2-complete notify-only smoke:

```bash
python3 tools/flume_tool.py \
  --build-dir build-stage3b2-notify \
  --run-hcomm-notify-only-smoke \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

Expected current result:

```text
hcomm notify-only smoke unsupported
stage3b2_notify_only_plan=channel-notify
stage3b2_kernel_consume=missing
```

Compile-branch notify-only check:

```bash
python3 tools/flume_tool.py \
  --build-dir build-stage3b2-notify-customop \
  --build-hcomm-custom-op \
  --run-hcomm-notify-only-smoke \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-probe
```

The current expected result remains unsupported, but should include
`custom-op/AICPU scheduler launch missing`. A future complete 3B.2 build should
turn this into `hcomm notify-only smoke passed` with
`stage3b2_kernel_consume=passed`.
