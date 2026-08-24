# CANN HCOMM Version Adaptation

## Scope

Flume supports CANN installations that expose HCOMM resource APIs through
different public-header layouts. This document separates compile/link
capability from device-side execution support and records the adaptation path
for CANN 9.0 beta-era toolkits without publishing machine-specific details.

## Observed compatibility shape

| Capability | Compatibility target | Flume interpretation |
|---|---|---|
| `HcclThreadExportToCommEngine` | Declared in `hccl_res.h`; no `hccl_res_expt.h`; symbol exported by `libhcomm` | Supported after a compile-and-link probe |
| Channel fence | Legacy `HcommChannelFence(channel)` only | Build-compatible fallback; device support must be verified separately |
| Channel/thread OnThread primitives | Declarations and symbols present | Candidate for AICPU_TS payload execution |
| Direct ACL custom-op launch | Loader, descriptor handoff, and launch accepted | Launcher pipeline is available |
| Host-RA storage | Requires a storage RNIC and NPU HCCN/RoCE reachability | Hardware-topology dependent |

`hccl_res_expt.h` is optional. Some CANN packages keep
`HcclThreadExportToCommEngine` in that header; others expose the same API from
`hccl_res.h`. Flume includes the experimental header when present and probes
the function call itself, rather than treating the optional header as the
capability.

## Why the current device error is not yet attributed

The direct ACL loader, descriptor handoff, and kernel launch can pass while
stream synchronization returns an AICPU error. If the device-visible status
and trace words remain at their initial sentinel values, there is no evidence
that the payload function body reached its first status write. That outcome
does not identify `HcommLocalCopyOnThread`, channel notify, channel fence,
batch mode, or communication-domain acquire as the failing operation.

In particular, a missing `hccl_res_expt.h` previously caused Flume to disable
thread export at compile time. That also disabled the host/AICPU launch-order
handshake. A rerun after the corrected probe is required before changing the
payload algorithm.

## API selection rules

| API family | A2/A3 direction | 950 Host-RoCE direction | Selection rule |
|---|---|---|---|
| `Hcomm*OnThread` | Publicly documented path with acquired CPU_TS/AICPU_TS thread resources | Also available where documented | Preferred for the current HCCS payload kernel |
| `HcommChannelFence` | Not documented as supported | Experimental Host CPU/RoCE API | Never infer AICPU support from symbol presence |
| `HcommChannelNotifyRecord/Wait` | Not documented as supported | Experimental Host CPU/RoCE API | Do not use as an A2/A3 AICPU fallback |
| `HcommReadNbi/WriteNbi` | Not documented as supported | Host CPU/RoCE and registered communication memory | Keep as a separate Host-RoCE backend candidate |
| `HcommLocalCopyOnThread` | Supported for device-memory local copy | AICPU_TS device path where documented | Still requires a valid thread resource |

The exact product support matrix must come from the CANN documentation shipped
with the target installation. Header and ELF symbol presence prove ABI
availability, not product or execution-context support.

## Adaptation sequence

1. Probe thread export from `hccl_res.h` plus optional `hccl_res_expt.h`, and
   require the matching `libhcomm` symbol.
2. Acquire CPU_TS and AICPU_TS resources, export both directions, and require
   nonzero handles before enabling `payload_thread_notify=host-aicpu`.
3. Rerun the existing official-P2P strict payload shape. It already disables
   batch mode, binds the acquired channel directly, skips communication-domain
   acquire, and writes device status before payload primitives.
4. If synchronization still fails, run isolated kernels in this order:
   entry/status write, local copy, channel notify pair, remote read/write, and
   completion fence. Each probe must use a separate launch and status word so
   an AICPU process error can be assigned to one primitive family.
5. Add a non-OnThread backend only when the target product documentation and a
   real device probe both confirm that execution context. It is not a generic
   substitute for AICPU_TS on A2/A3.

## Acceptance markers

The corrected resource path should first report:

```text
hcomm_thread_export=1
payload_thread_notify=host-aicpu
host_thread_notify_ready=on
```

Full payload success still requires:

```text
stage3b3e_direct_aclrt_payload_launch=passed
stage3b3e_payload_sync=passed
payload_kernel_status=success
payload_trace_result=success
payload_verify=passed
fallback=none
```

If thread export is declared but the runtime call fails, Flume must report the
actual HCCL return code and keep the payload backend unsupported. It must not
silently select a legacy API based only on a successful compile.

## Remote validation order

Use a fresh build directory so the revised CMake capability probe cannot be
hidden by an older cache. Substitute site-specific values only on the test
host; do not commit them.

1. Collect the installed ABI and verify the strict resource path:

   ```bash
   python3 tools/flume_tool.py \
     --build-dir build-hcomm-api-layout \
     --hccl-devices <device-a>,<device-b> \
     --hccl-host-ifname <host-ifname> \
     --hccl-host-ip <host-ip> \
     --run-hcomm-channel-probe \
     --hcomm-require-thread-export \
     --collect-cann-compat-label hcomm-api-layout \
     ascend-probe
   ```

   Required evidence is `FLUME_HAVE_HCOMM_THREAD_EXPORT=1`, a successful
   channel probe with `resolved_engine=aicpu-ts`, and
   `hcomm-thread-export-probe.txt` reporting the declaration source and
   `libhcomm_symbol: present`.

2. Run the no-internal-header canary and notify-only smoke with the isolated
   payload package. The canary must pass before interpreting a notify failure.
   A canary failure means package load, relocation, entry, descriptor, or
   status-memory plumbing is still broken; it is not a notify result.

3. Run the focused strict payload gate:

   ```bash
   python3 tools/flume_tool.py \
     --build-dir build-hcomm-payload-positive \
     --hccl-devices <device-a>,<device-b> \
     --hccl-host-ifname <host-ifname> \
     --hccl-host-ip <host-ip> \
     --hccl-debug-logs \
     --auto-build-hcomm-payload-package \
     --auto-run-hcomm-payload-candidate-matrix \
     --collect-cann-compat-label hcomm-payload-positive \
     hcomm-payload-strict-positive
   ```

4. Classify the result without guessing:

   | Evidence | Classification | Next action |
   |---|---|---|
   | Canary fails or status remains sentinel | Kernel entry/package dependency failure | Inspect AICPU loader and relocation logs |
   | Canary passes, notify-only writes a nonzero HCOMM return | Notify/thread-resource failure | Inspect exported handles and that primitive's documented execution context |
   | Notify-only passes, payload trace identifies first failing primitive | Payload primitive-specific failure | Adapt only that primitive family |
   | Payload/checksum/trace all pass | Real HCOMM payload path | Keep `fallback=none` and proceed to storage integration |

The legacy channel fence is retained as a compile-time compatibility branch.
It is not accepted as device support until the strict payload checksum and
trace gate passes on the target product.
