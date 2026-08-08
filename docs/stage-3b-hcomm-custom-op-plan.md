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
| 3B.3A | true notify-only AICPU launch | `stage3b3a_kernel_launch=passed stage3b2_kernel_consume=passed notify_kernel_status=success` | public launch API or custom-op package missing |
| 3B.3B | route launch capability across public HCCL and direct ACL paths | `stage3b3b_launcher_router=selected:<backend>` | `selected:unsupported` with precise missing reasons |
| 3B.3C | direct ACL custom-op loader / descriptor ABI / launch readiness | `stage3b3c_direct_aclrt_launch=passed` | `custom_op_package=missing` or direct ABI handoff blocked |
| 3B.3D | no-internal-header direct ACL custom-op canary | `stage3b3d_direct_aclrt_canary=passed` | canary package missing or direct ACL launch unavailable |
| 3B.3E / 3B.3F | execute HCOMM pair-copy primitives through direct ACL custom-op | two ranks passed, `stage3b3e_payload_copy=passed`, payload launch/sync passed, `payload_kernel_status=success`, `payload_failure_step=none`, `payload_status_word=0`, `payload_kernel_hcomm_ret=0`, `payload_status_schema=v4`, `payload_status_word_count=14`, `payload_echo=passed`, `payload_descriptor_fingerprint=passed`, `payload_data_probe=observed`, `payload_data_flow=passed`, `payload_primitive_state=completed`, `payload_trace=passed`, `payload_trace_schema=v2`, `payload_trace_word_count=80`, `payload_trace_event=kernel-exit`, `payload_trace_order=passed`, `payload_trace_ret_order=passed`, `payload_trace_primitive_path=send-local-copy\|recv-read-*\|send-write\|recv-write-local-copy`, `payload_transfer_mode=read\|write`, `payload_trace_result=success`, `payload_comm_binding=comm-name` + `payload_comm_acquire=default` or explicit `payload_comm_binding=channel-handle`, `payload_desc_batch_tag=default\|custom`, `payload_recv_path=local-buffer\|direct-output`, `payload_semantic_v6=present`, `payload_semantic_v7=present`, `payload_semantic_v8=present`, `payload_semantic_v9=present`, `payload_semantic_v10=present`, `payload_semantic_v11=present`, `payload_thread_notify_order=...`, `payload_pattern=strict-v1`, source/received/expected checksum match, `payload_verify=passed`, `fallback=none` | payload kernel missing / primitive call failure / stream sync failure |
| 3B.3 | stabilize HCOMM pair-copy scheduler as default payload backend | `hcomm_payload_scheduler=custom-op-aicpu` | environment-specific fallback remains required |
| 3B.4 | wire scheduler into storage HBM path | `storage_hbm=hcomm-payload-staging` | fallback remains `hccl-p2p` |

Current implementation note: 3B.3E has a candidate kernel and strict-positive
runtime gate in the repository. It is not considered complete until an installed
payload-ready custom-op package passes `hcomm-payload-strict-positive` on
Ascend hardware with `fallback=none`. Build-output JSON/tar pairs are useful for
preflight, but strict-positive runtime launch requires the installed
`aicpu/config` JSON and its matching `aicpu/kernel` tar.

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

## Stage 3B.3A/3B: Launcher Capability Router

3B.3A introduced the notify-only AICPU kernel entrypoint and the public HCCL
launch route:

```text
public_hccl_launch:
  HcclAicpuKernelLaunch(...)
  -> FlumeHcommNotifyOnlyAicpuKernel(...)
```

Some CANN packages do not expose `hccl_launch.h` / `HcclAicpuKernelLaunch` as a
public C ABI even when internal HCOMM code contains an AICPU launch mechanism.
Stage 3B.3B therefore turns launch into a router instead of a single attempt:

```text
detect:
  public HcclAicpuKernelLaunch
  direct ACL runtime custom-op launch APIs
  HCOMM thread export / engine context
  HCOMM primitive APIs
  installed Flume custom-op package

select:
  public_hccl_launch
  unsupported with precise reason
```

Stage 3B.3C continues the direct ACL route by probing three sub-boundaries:

```text
direct_aclrt:
  aclrtBinaryLoadFromFile(...)
  -> aclrtBinaryGetFunction(FlumeHcommNotifyOnlyDirectAclrtKernel)
  -> aclrtKernelArgsAppend(flume_hcomm_notify_only_desc_v1)
  -> aclrtLaunchKernelWithConfig(...)
```

Current target hosts that expose ACL runtime launch but do not have the Flume
custom-op package installed should produce an honest diagnostic:

```text
stage3b3a_kernel_launch=unsupported
stage3b3b_launcher_router=selected:unsupported
public_hccl_launch=off
direct_aclrt=on|off
custom_op_package=missing
stage3b3c_direct_aclrt_loader=unsupported
stage3b3c_descriptor_handoff=blocked
stage3b3c_direct_aclrt_launch=not-attempted
```

This is still a useful Stage 3B.3C result: it distinguishes CANN packaging/API
gaps from channel-resource, descriptor ABI, launch, or HCCL P2P failures.

Stage 3B.3D removes the short-term dependency on non-public HCCL/HCOMM headers
for the first real custom-op launch canary. It does not replace the future
HCOMM notify or pair-copy kernel. It verifies that a Flume-owned AICPU/custom-op
kernel can be packaged, loaded, receive a Flume-owned descriptor, launch through
public ACL runtime APIs, and complete on the stream:

```text
direct_aclrt_canary:
  aclrtBinaryLoadFromFile(...)
  -> aclrtBinaryGetFunction(FlumeHcommCanaryDirectAclrtKernel)
  -> aclrtMalloc(canary status word)
  -> aclrtKernelArgsAppend(flume_hcomm_canary_desc_v1)
  -> aclrtLaunchKernelWithConfig(...)
  -> aclrtSynchronizeStream(...)
  -> aclrtMemcpy(canary status word D2H)
```

The canary kernel includes only `flume_hcomm_notify_only_abi.h`. It deliberately
does not include `hccl_launch.h`, `hcomm_primitives.h`, `hccl_res_expt.h`, or
headers from `pkg_inc`. The direct ACL notify-only kernel is separate from the
legacy public-HCCL-launch notify-only kernel, so payload-package builds can use
HCOMM primitives without requiring `hccl/hccl_launch.h`.

Expected no-package diagnostic:

```text
stage3b3d_no_internal_headers=on
direct_aclrt_canary_candidate=blocked
stage3b3d_direct_aclrt_canary_loader=unsupported
stage3b3d_direct_aclrt_canary_handoff=blocked
stage3b3d_direct_aclrt_canary_launch=not-attempted
```

Expected installed-canary diagnostic:

```text
stage3b3d_no_internal_headers=on
direct_aclrt_canary_candidate=available
stage3b3d_direct_aclrt_canary_loader=passed
stage3b3d_direct_aclrt_canary_handoff=passed
stage3b3d_direct_aclrt_canary_launch=passed
stage3b3d_direct_aclrt_canary_sync=passed
canary_status_word=0
canary_observed_token=1128357465
stage3b3d_direct_aclrt_canary=passed
```

This success marker means the public direct ACL custom-op canary path works and
the kernel consumed the Flume descriptor strongly enough to write back the
expected device-visible status/token. It does not mean
`HcommChannelNotifyRecordOnThread`, `HcommChannelNotifyWaitOnThread`, or payload
copy has executed.

Stage 3B.3E wires the same direct ACL launch surface to the actual pair-copy
primitive plan. Host code still does not call HCOMM primitives and does not
reimplement HCCL collective behavior; it packages a byte-copy descriptor and
launches the Flume custom-op kernel:

```text
send rank:
  flume_hcomm_payload_send_async(src_hbm)
  -> package flume_hcomm_payload_copy_desc_v1
  -> FlumeHcommPayloadCopyDirectAclrtKernel
  -> HcommLocalCopyOnThread(src_hbm -> local_hccl_buffer)
  -> HcommChannelNotifyRecordOnThread(ready)
  -> HcommChannelNotifyWaitOnThread(done)

recv rank:
  flume_hcomm_payload_recv_async(dst_hbm)
  -> package flume_hcomm_payload_copy_desc_v1
  -> FlumeHcommPayloadCopyDirectAclrtKernel
  -> HcommChannelNotifyWaitOnThread(ready)
  -> HcommReadOnThread(remote_hccl_buffer -> local_hccl_buffer)
  -> HcommLocalCopyOnThread(local_hccl_buffer -> dst_hbm)
  -> HcommChannelNotifyRecordOnThread(done)

write-path candidate:
  send rank additionally calls HcommWriteOnThread(local_hccl_buffer -> remote_hccl_buffer)
  recv rank waits ready and local-copies its HCCL Buffer into dst_hbm
```

The strict smoke path `--run-hcomm-payload-smoke --hcomm-require-payload-copy`
now calls `flume_hcomm_payload_send_async` / `flume_hcomm_payload_recv_async`
instead of only running readiness probe. A passing result must include:

```text
rank 0 hcomm payload smoke passed
rank 1 hcomm payload smoke passed
stage3b3e_payload_copy=passed
stage3b3e_direct_aclrt_payload_loader=passed
stage3b3e_payload_descriptor_handoff=passed
stage3b3e_direct_aclrt_payload_launch=passed
stage3b3e_payload_sync=passed
payload_batch_mode=on|off
payload_kernel_status=success
payload_failure_step=none
payload_status_word=0
payload_kernel_hcomm_ret=0
payload_status_schema=v4
payload_status_word_count=14
payload_echo=passed payload_descriptor_fingerprint=passed
payload_primitive_state=completed
payload_trace=passed
payload_trace_schema=v2
payload_trace_word_count=80
payload_trace_event=kernel-exit
payload_trace_order=passed
payload_trace_ret_order=passed
payload_trace_primitive_path=send-local-copy|recv-read-*|send-write|recv-write-local-copy
payload_transfer_mode=read|write
payload_trace_result=success
payload_comm_acquire=default
payload_comm_binding=comm-name
payload_desc_batch_tag=default|custom
payload_recv_path=local-buffer|direct-output
payload_semantic_v6=present
payload_semantic_v7=present
payload_semantic_v8=present payload_semantic_v9=present payload_semantic_v10=present payload_semantic_v11=present
payload_thread_notify_order=host-notify-before-batch-end|not-used
payload_verify=passed
payload_source_checksum=<fnv32>
payload_checksum=<fnv32>
payload_expected_checksum=<fnv32>
payload_usable_hccl_buffer_bytes=<bytes>
payload_channel_desc=rank-graph|legacy-desc
payload_resolved_engine=aicpu-ts
payload_resolved_protocol=hccs|roce
payload_thread_notify=host-aicpu|unavailable
payload_completion=thread-notify+stream-sync+status-word|stream-sync+status-word
fallback=none
```

This is the first marker that should be treated as a real HCOMM payload-copy
attempt. `stage3b3d_direct_aclrt_canary=passed` remains only a launcher canary.
If launch and stream sync pass but `payload_kernel_status` is
not `success`, the direct ACL package route is working and the next debugging
target is the descriptor fields or HCOMM primitive execution inside the AICPU
kernel. The kernel reports per-step failures such as
`local-copy-failed`, `ready-notify-record-failed`,
`ready-notify-wait-failed`, `remote-read-failed`,
`done-notify-record-failed`, `batch-start-failed`, `batch-end-failed`, and
`thread-notify-*-failed`. On those failures, `payload_kernel_hcomm_ret`
contains the raw return code from the failed HCOMM primitive.
If `payload_kernel_status=not-written` or
`payload_status_word=4294967295`, the kernel did not update the device-visible
status word; check descriptor handoff, status pointer validity, and whether the
kernel body executed before debugging the HCOMM primitive sequence.

The official HCOMM custom-op example also uses host CPU thread to AICPU thread
notifications around kernel launch. Flume now models that boundary explicitly:
when `HcclThreadExportToCommEngine` is available, the host records into the
AICPU thread before launch and waits for the AICPU completion notify; the
kernel waits for that host notify before running primitives and records
completion before returning. On CANN builds without thread-export, Flume keeps
the direct ACL route usable and reports `payload_thread_notify=unavailable`;
the host-visible completion proof is then stream synchronization plus the
device-visible `status_word`.
Therefore, thread-export is an optional completion enhancement for the direct
ACL payload route, not a hard scheduler-candidate requirement.

Before running strict smoke, either build/export a runtime package or inspect
an already installed custom-op package. The direct build route is the preferred
first attempt on hosts that expose `hcomm_primitives.h` and `libhcomm.so` but
do not have the HCCL source `build.sh` packaging flow:

```bash
python3 tools/flume_tool.py \
  --custom-op-build-mode payload \
  --custom-op-export-root <temporary-custom-op-root> \
  hcomm-custom-op-direct-build
```

Then pass `--custom-op-root <temporary-custom-op-root>` to
`hcomm-payload-strict-positive` or `ascend-full-matrix`.

For a focused one-command Stage 3B.3E attempt, strict-positive can now build
that same direct ACL payload package automatically if the initial package
preflight fails:

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-payload-positive \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  --auto-build-hcomm-payload-package \
  --auto-run-hcomm-payload-channel-handle-candidate \
  --auto-run-hcomm-payload-write-path-candidate \
  --auto-run-hcomm-payload-channel-fence-diagnostic \
  --auto-run-hcomm-payload-nobatch-diagnostic \
  --auto-run-hcomm-payload-tagged-diagnostic \
  --collect-cann-compat-label host-b-cann \
  hcomm-payload-strict-positive
```

The auto path exports the package under the current log directory and retries
the strict gate with that isolated runtime root. It does not install into the
system CANN/OPP tree, so it is the preferred first attempt on shared hosts.
If the batch-enabled strict gate fails, the optional no-batch rerun writes
`HCOMM_PAYLOAD_NOBATCH_DIAGNOSTIC.md`, the optional tagged-batch rerun writes
`HCOMM_PAYLOAD_TAGGED_DIAGNOSTIC.md`, and the CANN fixture adds
`hcomm-primitive-call-shape-probe.txt` so the same log bundle can distinguish
ABI call-shape problems, default-tag batch compatibility, batch submit/ordering
problems, and primitive-copy problems. If a no-batch, tagged-batch, or
direct-output rerun produces the full strict-positive marker set with checksum
match and `fallback=none`, the tools accept that log as real HCOMM payload-copy
evidence and record the variant explicitly in the decision tree.

If the default read-path fails around `HcommReadOnThread`, enable
`--auto-run-hcomm-payload-write-path-candidate`. That path tries the send-side
`HcommWriteOnThread(local_hccl_buffer -> remote_hccl_buffer)` transfer mode
instead of recv-side remote read. The auto runner writes
`HCOMM_PAYLOAD_WRITE_PATH_CANDIDATE_MATRIX.md`; with the channel-handle,
channel-fence, and no-batch auto flags enabled, the matrix covers plain write,
write + channel-handle, write + channel-handle + channel-fence, write +
channel-handle + no-batch, and write + channel-handle + no-batch +
channel-fence. It strips recv direct-output from write-path commands because
direct-output is read-path-only. A selected write-path candidate must still show
`payload_transfer_mode=write`, full rank trace evidence, matching checksums,
and `fallback=none`.

If the recv side appears to need stronger read completion semantics, pass
`--hcomm-payload-channel-fence` or enable
`--auto-run-hcomm-payload-channel-fence-diagnostic`. That variant keeps the same
payload path but forces `HcommChannelFenceOnThread` after `HcommReadOnThread`,
including on HCCS/SIO protocols where the default is ordered notify. A complete
channel-fence pass is accepted as HCOMM payload-copy evidence and is recorded as
`payload_completion_mode=channel-fence`.
When channel-handle auto candidates are enabled, the same diagnostic also tries
the enabled channel-handle cross-product candidates, including
`hcomm-payload-channel-handle-channel-fence-candidate`,
`hcomm-payload-channel-handle-direct-output-channel-fence-candidate`,
`hcomm-payload-channel-handle-nobatch-channel-fence-candidate`, and
`hcomm-payload-channel-handle-nobatch-direct-output-channel-fence-candidate`.
Those variants let one log bundle cover comm-binding, read-completion,
recv-output, and batch-submit hypotheses without weakening the strict evidence
gate. The tool also writes
`HCOMM_PAYLOAD_CHANNEL_HANDLE_CANDIDATE_MATRIX.md`, which summarizes each tried
candidate's failure step, HCOMM return code, recv path, completion mode, rank
evidence, fallback marker, and selected status.

For failed payload kernels, host-side trace parsing also emits
`payload_trace_first_error_event`, `payload_trace_first_error_ret`, and
`payload_trace_first_error_index`. These markers summarize the first non-zero
device-side HCOMM primitive return from the trace ring. They are diagnostic
only; strict-positive success still requires full rank evidence, checksum
match, and `fallback=none`.

If the batch-enabled strict gate fails inside the payload kernel, use the
diagnostic no-batch variant to isolate HCOMM primitive execution from
`HcommBatchModeStart/End` submit semantics:

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-payload-nobatch \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  --custom-op-root <temporary-custom-op-root> \
  --hcomm-payload-disable-batch \
  hcomm-payload-strict-positive
```

This is a valid HCOMM payload-copy success path when strict-positive also sees
both ranks passed, checksum match, complete trace evidence, and `fallback=none`;
it simply does not validate HcommBatchModeStart/End. A no-batch rank log that reaches
`stage3b3e_payload_copy=passed` means the custom-op descriptor, channel,
notify, `HcommLocalCopyOnThread`, and `HcommReadOnThread` path are viable, and
any remaining batch-enabled failure is HCOMM batch submit/ordering. A no-batch failure points
directly at the failing primitive through `payload_failure_step=...`.

If the default path fails at `payload_failure_step=comm-acquire`, run the
diagnostic no-comm-acquire variant to test whether the already-acquired
`ChannelHandle` can drive Notify/Read without the extra in-kernel
`HcommAcquireComm` / `HcommReleaseComm` pair:

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-payload-no-comm \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  --custom-op-root <temporary-custom-op-root> \
  --hcomm-payload-skip-comm-acquire \
  hcomm-payload-strict-positive
```

This is also deliberately not a final success path: it is marked
`payload_comm_binding=diagnostic-skip`. A no-comm-acquire pass means the Channel
Notify, `HcommLocalCopyOnThread`, and `HcommReadOnThread` path is viable and the
remaining issue is comm-name binding or `HcommAcquireComm` / `HcommReleaseComm`
semantics for the selected CANN build. To test the same ChannelHandle-only
behavior as a candidate backend rather than a diagnostic, run the strict smoke
with `--hcomm-payload-comm-binding=channel-handle`; that path can satisfy the
final gate only if both ranks pass with checksum match and `fallback=none`.
For one-shot remote collection, add
`--auto-run-hcomm-payload-channel-handle-candidate`; if the default
`comm-name` run fails, the tool reruns the same strict smoke with
`channel-handle` binding and writes
`HCOMM_PAYLOAD_CHANNEL_HANDLE_CANDIDATE.md`.

Before running strict smoke against an existing package, inspect it:

```bash
python3 tools/flume_tool.py hcomm-custom-op-package
python3 tools/flume_tool.py --require-hcomm-payload-kernel \
  hcomm-custom-op-package

# Optional no-install checks against an explicit root or build artifact pair.
python3 tools/flume_tool.py --custom-op-root <cann-root-or-opp-root> \
  --require-hcomm-payload-kernel hcomm-custom-op-package
python3 tools/flume_tool.py \
  --custom-op-json <path-to-libflume_hcomm_payload_aicpu_kernel.json> \
  --custom-op-aicpu-tar <path-to-aicpu_flume_hcomm_payload.tar.gz> \
  --require-hcomm-payload-kernel hcomm-custom-op-package
```

The first command verifies the canary package; the second verifies that the
installed JSON declares `FlumeHcommPayloadCopyDirectAclrtKernelV4` and that the
AICPU package tar is readable and contains
`libflume_hcomm_payload_aicpu_kernel.so`. When `readelf` or `nm` is available,
the preflight also checks exported SO symbols.

If installing into the system CANN/OPP tree is not desirable, export a
preflight-passing artifact pair into an isolated runtime root:

```bash
python3 tools/flume_tool.py \
  --custom-op-json <path-to-libflume_hcomm_payload_aicpu_kernel.json> \
  --custom-op-aicpu-tar <path-to-aicpu_flume_hcomm_payload.tar.gz> \
  --custom-op-build-mode payload \
  --custom-op-export-root <temporary-custom-op-root> \
  hcomm-custom-op-export-runtime
```

Then pass `--custom-op-root <temporary-custom-op-root>` to
`hcomm-payload-strict-positive` or `ascend-full-matrix`. This does not build
the kernel by itself; it makes already-built JSON/tar artifacts visible through
the same runtime layout that direct ACL loading uses.
The preflight also verifies that the SO inside the tar exports the required
entrypoints, so a JSON/SO mismatch fails before strict payload smoke. If the
second check fails, the next action is to rebuild the package with
`FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD=ON`, not to debug HCOMM payload
execution yet. On CANN packages that do not expose `hccl/hccl_launch.h`, keep
`FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH=OFF`; the direct ACL payload
package does not need that legacy public-HCCL-launch entrypoint.

The legacy `FlumeHcommPayloadCopyDirectAclrtKernel` entrypoint is still exported
as a compatibility wrapper, but Flume's payload-ready preflight requires the V4
entrypoint so that stale packages do not silently skip the extended status
diagnostic ABI, descriptor echo words, and the HCCL comm-name handoff. It also requires the SO symbol
`FlumeHcommPayloadCopyAbiVersion4`, and the same function must be declared in
the JSON so the runtime loader can verify the package before descriptor
handoff. This marks the current payload descriptor semantic ABI after the
kernel descriptor gained `comm_name` for `HcommAcquireComm` /
`HcommReleaseComm`. The preflight also requires
`FlumeHcommPayloadCopyRequiresCommAcquire` so a package built before the
communicator acquire/release handoff cannot be misclassified as payload-ready.
It also requires `FlumeHcommPayloadStatusSchemaVersion` and
`FlumeHcommPayloadStatusWordCount`, in both JSON and the tar-contained SO, so
the host can reject packages that predate the current device-visible status
schema before attempting a strict payload launch. The same gate requires
`FlumeHcommPayloadTraceSchemaVersion` and
`FlumeHcommPayloadTraceWordCount` so a package that cannot emit the ordered
device-side primitive trace is not misclassified as payload-ready.
When the package JSON declares only
the legacy entrypoint, the preflight reports
`reason.payload_direct_aclrt=legacy-entrypoint-present` and
`action.payload_direct_aclrt=rebuild-with-current-flume`; treat that as a
package refresh problem, not as evidence that HCOMM payload primitives failed.
When the V4 entrypoint and primitive-payload marker exist but
`FlumeHcommPayloadCopyAbiVersion4` is missing, rebuild the package with current
Flume headers before running strict smoke.
When `FlumeHcommPayloadCopyRequiresCommAcquire` is missing, rebuild from the
current Flume tree before running strict smoke; that package predates the
HCOMM communicator acquire/release contract.
When `FlumeHcommPayloadStatusSchemaVersion` or
`FlumeHcommPayloadStatusWordCount` is missing, rebuild from the current tree;
that package predates the current success-status schema and cannot prove
`payload_kernel_hcomm_ret=0` with the expected status word layout.
When `FlumeHcommPayloadTraceSchemaVersion` or
`FlumeHcommPayloadTraceWordCount` is missing, rebuild from the current tree;
that package predates the ordered device-side primitive trace required by the
strict-positive evidence gate.

`ascend-probe` records the same check as
`hcomm-custom-op-package-preflight` when payload or notify-only smoke is
requested. `ascend-full-matrix` runs the preflight in payload-required mode and
adds the package state to `ASCEND_FULL_MATRIX_DECISION_TREE.md`: `not-ready`
means rebuild/install the Stage 3B.3E package first, while `payload-ready`
means the next meaningful test is strict payload smoke.
The `--custom-op-root`, `--custom-op-json`, `--custom-op-aicpu-tar`, and
`--custom-op-vendor` options are also propagated into the real smoke runtime.
`--custom-op-json` is authoritative: if it is set and missing, the runtime
reports the package as missing instead of falling back to a system install.
`--custom-op-aicpu-tar` is not the `aclrtBinaryLoadFromFile` input, but it is
forwarded as the explicit runtime readiness tar for loose build artifacts so
preflight and the C++ launcher do not disagree about package readiness.

Host B validation has confirmed this expected diagnostic on a CANN 9.0 beta
toolkit: the required HCCL/HCOMM smoke flow passes, `direct_aclrt=on`, and the
direct ACL route stops cleanly at `custom_op_package=missing` with
`stage3b3c_direct_aclrt_loader=unsupported`,
`stage3b3c_descriptor_handoff=blocked`, and
`stage3b3c_direct_aclrt_launch=not-attempted`.
When the direct ACL notify-only kernel is installed and executes successfully,
the status-word proof adds `notify_kernel_status=success` and
`notify_status_word=0`. If launch/sync pass but this status is not success,
the custom-op route is alive and the next debugging target is the in-kernel
HCOMM Notify sequence.

A separate no-install toolkit package inspection found the same public-launch
limitation in a newer CANN 9.0 package: public `hccl_launch.h` /
`HcclAicpuKernelLaunch` was still absent, while internal HCOMM package headers
showed additional thread-export / primitive / launch-adjacent interfaces. That
keeps the short-term focus on the direct ACL route and custom-op package
installation rather than waiting for public HCCL launch exposure.

## Stage 3B.3: HCOMM Pair-Copy Primitive Scheduler

3B.3 executes the current pair-copy plan:

| Rank | Steps |
| --- | --- |
| send rank | `HcommLocalCopyOnThread(input -> local_hccl_buffer)` -> `HcommChannelNotifyRecordOnThread(ready)` -> `HcommChannelNotifyWaitOnThread(done)` |
| recv rank | `HcommChannelNotifyWaitOnThread(ready)` -> `HcommReadOnThread(remote_hccl_buffer -> local_hccl_buffer)` -> `HcommLocalCopyOnThread(local_hccl_buffer -> output)` -> `HcommChannelNotifyRecordOnThread(done)` |

The default recv path keeps the local HCCL Buffer staging step because it is the
stricter HCOMM Buffer-constrained path. Stage 3B.3E also has a default-off
diagnostic switch, `--hcomm-payload-recv-direct-output`, that changes only the
recv descriptor to `HcommReadOnThread(remote_hccl_buffer -> output)`. Its marker
is `payload_recv_path=direct-output`; the default marker is
`payload_recv_path=local-buffer`. This diagnostic is useful when the default
path reaches `HcommReadOnThread` but fails at the final output local-copy.

Success changes backend capability semantics from:

```text
hcomm_payload_scheduler=not-implemented
hcomm_payload_scheduler_candidate=on
hcomm_payload_direct_aclrt=on
fallback=hccl-p2p
```

to:

```text
hcomm_payload_scheduler=custom-op-aicpu
hcomm_payload=on
fallback=none
```

`hcomm_payload_scheduler_candidate=on` is not a success marker by itself. It
means the Flume binary was built with the direct ACL custom-op launcher needed
to attempt the scheduler. Host-side `hcomm_primitives=off` does not block this
route because the HCOMM primitive calls live inside the installed custom-op
package. Runtime readiness still depends on that package and is proven only by
strict payload smoke passing on both ranks with `stage3b3e_payload_copy=passed`,
direct ACL payload launch/sync passed, `payload_kernel_status=success`,
`payload_failure_step=none`, `payload_status_word=0`, `payload_kernel_hcomm_ret=0`,
`payload_status_schema=v4`, `payload_status_word_count=14`,
`payload_echo=passed`, `payload_descriptor_fingerprint=passed`, `payload_data_probe=observed`, `payload_data_flow=passed`, `payload_primitive_state=completed`,
`payload_trace=passed`, `payload_trace_schema=v2`, `payload_trace_word_count=80`, `payload_trace_event=kernel-exit`,
`payload_trace_order=passed`, `payload_trace_ret_order=passed`,
`payload_trace_primitive_path=send-local-copy|recv-read-*|send-write|recv-write-local-copy`,
`payload_transfer_mode=read|write`, `payload_trace_result=success`,
`payload_comm_binding=comm-name` plus `payload_comm_acquire=default`, or
explicit `payload_comm_binding=channel-handle`,
`payload_desc_batch_tag=default|custom`,
`payload_recv_path=local-buffer|direct-output`, `payload_semantic_v6=present`,
`payload_semantic_v7=present`, `payload_semantic_v8=present`,
`payload_semantic_v9=present`, `payload_semantic_v10=present`, `payload_semantic_v11=present`,
`payload_thread_notify_order=...`, `payload_pattern=strict-v1`,
source/received/expected checksum match, `payload_verify=passed`, and
`fallback=none`. `thread_export=off` changes the
completion marker to `payload_completion=stream-sync+status-word`; it does not
by itself block the direct ACL payload candidate.

## Stage 3B.4: Storage HBM Integration

Stage 3A now has two storage-HBM smoke paths. The default remains the proven
`storage_hbm=hccl-p2p-staging` fallback. When the strict HCOMM payload-copy
gate is explicitly required, the same storage smoke uses the HCOMM payload
scheduler and reports `storage_hbm=hcomm-payload-staging`:

```text
file/storage block
  -> proxy HBM or HCCL buffer
  -> HCOMM payload scheduler
  -> compute HBM
  -> checksum verification
```

This is still a staging path unless storage/RDMA can DMA directly into
NPU-visible memory. Full host-bypass requires Stage 4 storage registration work.
The `hcomm-payload-staging` marker should only be considered valid after
Stage 3B.3E itself passes with `fallback=none` and checksum verification.
Use the focused gate once the payload package is installed:

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-storage-positive \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  --auto-build-hcomm-payload-package \
  --auto-run-hcomm-payload-channel-handle-candidate \
  --auto-run-hcomm-payload-write-path-candidate \
  --auto-run-hcomm-payload-channel-fence-diagnostic \
  --auto-run-hcomm-payload-nobatch-diagnostic \
  --auto-run-hcomm-payload-tagged-diagnostic \
  --collect-cann-compat-label host-b-cann \
  hcomm-storage-strict-positive
```

The command records both `hcomm-payload-strict-evidence` and
`hcomm-storage-strict-evidence`; the second gate requires strict-positive
payload copy plus `storage_hbm=hcomm-payload-staging`. If the storage-over-HCOMM
gate fails after package preflight, the optional write-path matrix, no-batch,
tagged-batch, and channel-fence reruns collect the same A/B evidence as the
focused payload gate, but the final storage gate still requires the HCOMM
payload path, complete trace/checksum evidence, `fallback=none`, and
`storage_hbm=hcomm-payload-staging`; batch-enabled mode remains a separate
compatibility signal when a no-batch path is the first one to pass.

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
stage3b3b_launcher_router=selected:unsupported
```

This second result is useful: it proves the build switch reached the runtime
diagnostic path. The router detail then identifies whether the blocker is public
HCCL launch, direct ACL runtime launch, thread export, HCOMM primitives, or
custom-op package installation.

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
`stage3b3b_launcher_router=selected:unsupported`, proving the descriptor path
reached the custom-op build branch and produced precise launcher diagnostics.

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
`stage3b3b_launcher_router=selected:unsupported` and the Stage 3B.3C direct ACL
markers. With no installed Flume custom-op package, the expected loader result is
`stage3b3c_direct_aclrt_loader=unsupported`. A future complete 3B.2 build should
turn this into `hcomm notify-only smoke passed` with
`stage3b2_kernel_consume=passed`.
