# Flume: Host-Bypass Data Path for Ascend NPU

Flume is an experimental C++/C project for exploring host-bypass data movement from storage into Ascend NPU HBM, then across NPU HBM through CANN HCCL/HCOMM. The current implementation is HCCL/HCOMM-first: host CPU remains responsible for setup, capability probing, task submission, logs, and fallback decisions, while the target payload path avoids host memory staging whenever the backend supports it.

The public C ABI uses the `flume_` prefix.

## What Flume Adds Beyond HCCL

HCCL already solves NPU tensor communication. For normal collective or point-to-point communication, HCCL can move payload directly between NPU HBM buffers over HCCS/RoCE/PCIe without staging tensor data through host memory. Flume does not try to reimplement AllReduce, AllGather, or HCCL Send/Recv.

Flume focuses on the part HCCL does not cover: making storage data become part of the NPU data path and exposing that through a stable, capability-aware runtime.

| Area | HCCL provides | Flume responsibility |
| --- | --- | --- |
| HBM-to-HBM collectives | Yes: `HcclAllReduce`, `HcclAllGather`, topology-selected HCCS/RoCE/PCIe payload path | Reuse as correctness/performance baseline and framework-facing fallback |
| HBM-to-HBM P2P copy | Yes: public `HcclSend` / `HcclRecv` where exported | Reuse as Stage 2 verified baseline |
| HCOMM Channel resources | Low-level resource APIs for custom communication operators | Probe, wrap, diagnose, and turn into a future payload backend |
| Storage block -> NPU HBM | Not a HCCL feature | Core Flume target: storage proxy, HCCL/HCOMM visible buffers, future RDMA/storage direct path |
| CANN version fallback | Partly exposed by headers/libraries | Feature-probed runtime that supports CANN 8.5 baseline and high-version optional paths |
| Application transparency | Framework-specific | Stable `flume_` ABI and future framework integration hooks |

In short: HCCL is the communication substrate; Flume is the storage-aware host-bypass data-path layer built around that substrate.

## Status

Implemented and testable on macOS/Linux without NPU hardware:

- TCP storage-agent control plane for `open` / `pread` / `close`.
- Local mock pread path with offset, length, checksum, and error handling.
- Simulated communication-memory and HBM buffers:
  - `FLUME_BUFFER_SIM_HCCL_COMM`
  - `FLUME_BUFFER_SIM_HBM`
- Simulated HBM copy through `flume_hbm_copy_async`.
- Simulated multi-rank `AllReduce` / `AllGather`.
- Simulated paired P2P send/recv through `flume_p2p_send_async` / `flume_p2p_recv_async`.
- Simulated HCOMM Channel resource probe through `flume_hcomm_channel_probe`.
- Structured backend capability query through `flume_get_backend_caps`.
- Simulated HCOMM payload pair copy through `flume_hcomm_payload_send_async` / `flume_hcomm_payload_recv_async`.
- Simulated storage partial-direct path:
  `file offset -> SIM_HCCL_COMM staging -> SIM_HBM`.
- Simulated A3 symmetric-memory lifecycle checks.
- Tooling for local and Ascend-host validation under `tools/`.

Implemented and validated on Ascend hardware in the current test environment:

- HCCL-enabled build path with CANN/HCCL/HCOMM/ACL discovery.
- `flume_attach_hccl_comm` for reusing an external `HcclComm`.
- `flume_allreduce_async` / `flume_allgather_async` wrappers over `HcclAllReduce` / `HcclAllGather`.
- `flume_p2p_send_async` / `flume_p2p_recv_async` wrappers over `HcclSend` / `HcclRecv` when those APIs are exported by the installed HCCL headers and library.
- `flume_hcomm_channel_probe` / `flume_hcomm_channel_probe_ex` wrappers for the HCOMM Channel resource stage: local HCCL Buffer, CPU_TS/AICPU_TS thread resources, optional thread export, configurable channel engine/protocol, channel acquire, and remote HCCL Buffer query. On CANN 8.5 the default `auto` engine resolves to `cpu-ts`; strict thread-export is an optional high-version check.
- Optional single-node multi-card HCCL smoke test on Ascend HBM buffers. Verified with `root-info` and `init-all`.
- Optional rank0-to-rank1 HCCL P2P copy smoke on Ascend HBM buffers. Verified with `p2p_copy=on`.
- Optional rank0/rank1 HCOMM Channel resource probe smoke. Verified as channel-resource readiness; it does not yet move payload with HCOMM primitives.
- Optional Stage 2.5 HCOMM payload readiness smoke. It probes HCOMM Channel resources plus primitive symbol availability, then reports `unsupported` / `fallback=hccl-p2p` until the custom-op/AICPU payload scheduler is implemented.
- Optional Stage 3B.1 HCOMM custom-op no-op launch readiness smoke. It distinguishes default `custom-op/AICPU scheduler build disabled` from `--build-hcomm-custom-op` `custom-op/AICPU scheduler launch missing`.
- Optional Stage 3B.2 HCOMM resource descriptor smoke. It packages channel, buffer, notify, rank, engine, protocol, and descriptor-source metadata, then reports expected unsupported until descriptor handoff into custom-op/AICPU is implemented.
- Optional Stage 3B.2-complete / 3B.3-prep HCOMM notify-only smoke. It fixes the send/recv ready-done Channel Notify plan and reports expected unsupported until the custom-op/AICPU kernel can consume the descriptor.
- Stage 3B.3B HCOMM launcher capability router. It reports whether Flume can use public `HcclAicpuKernelLaunch`, direct ACL runtime custom-op launch APIs, HCOMM thread export, HCOMM primitives, and an installed Flume custom-op package; unsupported results include precise missing reasons.
- Stage 3B.3C direct ACL custom-op readiness. It probes installed package loading, direct function lookup, descriptor ABI handoff, and `aclrtLaunchKernelWithConfig` separately.
- Stage 3B.3D no-internal-header direct ACL canary path. It verifies that a Flume custom-op package can be launched through the public ACL runtime route without relying on unpublished `hccl_launch.h` headers.
- Stage 3B.3E HCOMM payload-copy candidate. The custom-op package exports the V2 payload ABI and contains a kernel that consumes the Flume descriptor, calls `HcommLocalCopyOnThread`, `HcommReadOnThread`, and HCOMM Channel Notify primitives, then reports device-visible status words. This is implemented as a strict-positive candidate, but still requires an installed payload-ready custom-op package and remote NPU evidence before it is considered complete.
- One-shot Ascend matrix command for collecting collective, HCCL P2P, HCOMM channel, HCOMM custom-op launch readiness, HCOMM resource descriptor readiness, HCOMM notify-only readiness, HCOMM payload readiness, Stage 3A storage-HBM fallback, and strict expected-negative logs in one run. Verified on Host B (CANN 9.0) with HCCS_SW device pairs; the strict payload-copy step is an optional expected negative while the custom-op/AICPU scheduler launch is not implemented.
- Optional Atlas A3 HCCS symmetric-memory smoke using ACL mapped HBM and `HcclCommSymWinRegister` when those APIs are exposed by the installed CANN/HCCL headers.

Not complete yet:

- Strict-positive validation of the HCOMM primitive / custom-op payload backend for direct HBM-to-HBM copy. The code path and package ABI exist; completion requires `hcomm-payload-strict-positive` to pass with `stage3b3e_payload_copy=passed`, `payload_kernel_status=success`, `payload_status_word=0`, `payload_kernel_hcomm_ret=0`, `payload_verify=passed`, and `fallback=none` on Ascend hardware.
- Storage proxy rank backed by HCCL/HCOMM communication memory.
- Full RDMA / NVMe-oF / SPDK to NPU HBM data path.
- Transparent framework integration.

## Repository Layout

```text
include/                 public C ABI
src/                     core library, protocol, storage agent
apps/                    demo and smoke-test binaries
tests/                   local unit and simulation tests
tools/                   validation helper and logs guide
scripts/                 environment checks
docs/                    architecture and feasibility notes
refer/                   research notes; cloned CANN references are ignored
```

## Local Validation

Use this path on macOS or Linux without Ascend hardware:

```bash
python3 tools/flume_tool.py local
```

The helper writes command logs and a summary under `logs/flume-check-*`.

Manual equivalent:

```bash
cmake -S . -B build -DFLUME_BUILD_TESTS=ON
cmake --build build -j4
ctest --test-dir build --output-on-failure
./build/flume-sim-demo
./build/flume-sim-collective-demo
```

## Ascend Validation

Source CANN first:

```bash
source /usr/local/Ascend/ascend-toolkit/set_env.sh
python3 tools/flume_tool.py --build-dir build-ascend ascend-probe
```

`ascend-probe` checks CANN/HCCL discovery, CMake configuration, linking, and local mock/sim regression tests. It does not run a real HCCL data-plane smoke unless requested.

During CMake configuration the build prints feature flags for optional HCCL/A3 APIs such as `FLUME_HAVE_HCCL_SYM_WINDOW`, `FLUME_HAVE_HCCL_COMM_MEMORY`, and `FLUME_HAVE_ACL_VMM`. If a trial A3 API is missing, Flume still builds the base HCCL path and the corresponding A3 wrapper returns `FLUME_ERR_UNSUPPORTED`.

Single-node multi-card HCCL collective smoke:

```bash
python3 tools/flume_tool.py --build-dir build-ascend --run-hccl-smoke --hccl-devices 0,1 ascend-probe
```

Stage 2 HCCL P2P copy smoke:

```bash
python3 tools/flume_tool.py --build-dir build-p2p --run-hccl-p2p-smoke --hccl-devices 0,1 ascend-probe
```

This adds a paired `HcclSend` / `HcclRecv` check from rank 0 HBM to rank 1 HBM after the base collective smoke. It is the current public-HCCL baseline for HBM-to-HBM P2P copy.

Stage 2 HCOMM Channel resource probe:

```bash
python3 tools/flume_tool.py --build-dir build-hcomm --run-hcomm-channel-probe --hccl-devices 0,1 ascend-probe
```

This keeps the same base collective smoke, then asks Flume to acquire the HCOMM resources needed by the future custom backend: HCCL Buffer, CPU_TS/AICPU_TS thread resources, an HCOMM Channel, and the peer HCCL Buffer. It prefers rank-graph link descriptors when available and falls back to a legacy descriptor only when rank graph is unavailable. It does not yet launch an AICPU kernel or move payload with `HcommReadOnThread`.

The default HCOMM probe uses `--hcomm-channel-engine auto`, `--hcomm-channel-protocol hccs`, and `--hcomm-notify-num 2`. On CANN builds without `hccl_res_expt.h`, such as CANN 8.5, `auto` resolves to `cpu-ts` and validates the channel-resource path without claiming AICPU thread-export readiness. Add `--hcomm-require-thread-export` only when you want a strict future AICPU thread-export check; CANN 8.5 is expected to report unsupported for that stricter extension check.

When `--hccl-devices` is set, the default `auto` mode uses the HCCL
`root-info` initialization strategy and launches one process per rank. This is
the bring-up path closest to the official HCCL root-info test and should be the
first real hardware baseline for Flume. `--hccl-init-mode all` is kept as a
single-process comparison path.

`--hccl-init-mode rank-table` is currently retained as an experimental
diagnostic path. On tested single-node HCCS_SW pairs it can enter HCCL
VNIC/P2P memory-share setup and has not passed hardware validation yet.
`tools/README.md` documents host NIC pinning, rank-table caveats, topology
collection, and `HCCL_SMOKE_DIAGNOSTICS.txt`.

Stage 2.5 HCOMM payload readiness smoke:

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-payload --run-hcomm-payload-smoke --hccl-devices 0,1 ascend-probe
```

This probes whether the current CANN build exposes the HCOMM primitive symbols needed by the future payload backend. The current implementation intentionally does not claim payload copy success; it should report `hcomm payload smoke unsupported ... fallback=hccl-p2p` unless a future custom-op/AICPU scheduler is implemented. Add `--hcomm-require-payload-copy` only when testing that future strict path.

Stage 3A storage proxy HBM smoke:

```bash
python3 tools/flume_tool.py \
  --build-dir build-storage-hbm \
  --run-storage-hbm-smoke \
  --hccl-devices 0,1 \
  --storage-smoke-file /path/on/local-ssd/input.bin \
  --storage-smoke-bytes 4096 \
  ascend-probe
```

This validates the first real storage-integrated fallback path. Rank 0 acts as the storage proxy: it reads a file slice from local storage, copies that slice into proxy-rank HBM, then uses `HcclSend` to send bytes to rank 1 compute HBM. Rank 1 receives with `HcclRecv` and verifies the checksum that `flume_tool.py` computed before launch. The success marker is `storage_hbm=hccl-p2p-staging`. This is not full storage-direct DMA; host CPU still performs the SSD file read and H2D staging into proxy HBM.

If `--storage-smoke-file` is omitted, `flume_tool.py` generates a deterministic input file in the run log directory. `--storage-smoke-bytes` must fit in the per-rank smoke HBM buffer, so either keep it at the default 4096 bytes or set `--hccl-count >= ceil(bytes / 4)`.

Status: Host B (CANN 9.0) has validated this path with a local SSD input file and a 16 MiB byte payload. The marker `storage_hbm=hccl-p2p-staging` appeared on both ranks and the rank1 checksum matched the source slice.

Stage 3B HCOMM payload scheduler skeleton:

```text
send rank:
  HcommLocalCopyOnThread(input -> local HCCL Buffer)
  HcommChannelNotifyRecordOnThread(ready)
  HcommChannelNotifyWaitOnThread(done)

recv rank:
  HcommChannelNotifyWaitOnThread(ready)
  HcommReadOnThread(remote HCCL Buffer -> output)
  HcommChannelNotifyRecordOnThread(done)
```

The current code now has a library-level plan model for this pair-copy scheduler and a reserved `custom_ops/hcomm_payload_copy/` implementation surface. Stage 3B.1 adds `--run-hcomm-custom-op-launch-smoke` for the no-op custom-op launch readiness boundary. Stage 3B.2 adds `--run-hcomm-resource-descriptor-smoke` to package the HCOMM resource descriptor on the host side and clearly mark the custom-op/AICPU descriptor handoff as missing. Stage 3B.2-complete / 3B.3-prep adds `--run-hcomm-notify-only-smoke` for the descriptor-consume plus Channel Notify record/wait plan. Stage 3B.3B adds a launcher router so unsupported results identify whether the missing piece is public HCCL launch, direct ACL runtime launch, thread export, HCOMM primitives, or custom-op package installation. Stage 3B.3C starts the direct ACL route by probing package load, direct function lookup, descriptor ABI handoff, and launch separately. Stage 3B.3D proves the direct ACL canary route without unpublished HCCL launch headers. Stage 3B.3E adds the real payload-copy candidate: rank0 copies source HBM into the HCCL Buffer, rank1 reads from the remote HCCL Buffer into destination HBM, and both ranks synchronize with HCOMM Channel Notify. This path is only accepted as complete when the strict-positive smoke passes with no fallback. The full sub-stage plan is in [docs/stage-3b-hcomm-custom-op-plan.md](docs/stage-3b-hcomm-custom-op-plan.md).

Full two-rank Ascend matrix:

```bash
python3 tools/flume_tool.py \
  --build-dir build-full \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --hccl-debug-logs \
  ascend-full-matrix
```

This builds once, runs local regression tests, then runs HCCL collective,
HCCL P2P baseline, HCOMM Channel probe, HCOMM payload readiness, and Stage 3A
storage proxy HBM fallback in one two-rank smoke. It also runs a strict
payload-copy check as an optional expected negative until an installed
payload-ready custom-op package is present and the strict-positive gate passes.
The log directory includes
`ASCEND_FULL_MATRIX_DECISION_TREE.md`.

Atlas A3 HCCS symmetric-memory smoke:

```bash
python3 tools/flume_tool.py --build-dir build-a3 --run-a3-symmetric-smoke --hccl-devices 0,1 ascend-probe
```

This optional smoke requires the installed CANN runtime and HCCL headers to expose ACL VMM/mapped-HBM APIs, `HcclCommInitRootInfoConfig`, `HcclCommConfig.hcclSymWinMaxMemSizePerRank`, and `HcclCommSymWinRegister`.

## Dependencies

Local mock/sim builds:

- CMake 3.20+
- C++17 compiler
- Python 3

HCCL-enabled builds additionally require a sourced CANN environment with:

- HCCL headers and library
- HCOMM headers and library
- ACL headers
- `securec.h`
- `libc_sec.so` / `libc_sec.dylib` (`c_sec`)
- `libascendcl`

## Documentation

- [Implementation flow summary](docs/implementation-flow-summary.md)
- [CANN 8.5 compatibility strategy](docs/cann-8.5-compatibility.md)
- [Architecture](docs/architecture.md)
- [Demo plan](docs/demo-plan.md)
- [Single-node multi-card HBM/HCCL analysis](docs/single-node-hbm-hccl-analysis.md)
- [Research notes](refer/flume-research.md)
