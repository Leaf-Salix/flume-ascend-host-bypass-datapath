# Flume: Host-Bypass Data Path for Ascend NPU

Flume is an experimental C++/C project for exploring host-bypass data movement into and across Ascend NPU HBM. The current implementation is CANN HCCL/HCOMM-first: host CPU remains responsible for setup and task submission, while the target data path avoids host memory staging whenever the backend supports it.

The public C ABI uses the `flume_` prefix.

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
- Simulated A3 symmetric-memory lifecycle checks.
- Tooling for local and Ascend-host validation under `tools/`.

Implemented but still requires Ascend hardware validation:

- HCCL-enabled build path with CANN/HCCL/HCOMM/ACL discovery.
- `flume_attach_hccl_comm` for reusing an external `HcclComm`.
- `flume_allreduce_async` / `flume_allgather_async` wrappers over `HcclAllReduce` / `HcclAllGather`.
- `flume_p2p_send_async` / `flume_p2p_recv_async` wrappers over `HcclSend` / `HcclRecv` when those APIs are exported by the installed HCCL headers and library.
- `flume_hcomm_channel_probe` / `flume_hcomm_channel_probe_ex` wrappers for the HCOMM Channel resource stage: local HCCL Buffer, CPU_TS/AICPU_TS thread resources, optional thread export, configurable channel engine/protocol, channel acquire, and remote HCCL Buffer query.
- Optional single-node multi-card HCCL smoke test on Ascend HBM buffers.
- Optional rank0-to-rank1 HCCL P2P copy smoke on Ascend HBM buffers.
- Optional rank0/rank1 HCOMM Channel resource probe smoke.
- Optional Atlas A3 HCCS symmetric-memory smoke using ACL mapped HBM and `HcclCommSymWinRegister` when those APIs are exposed by the installed CANN/HCCL headers.

Not implemented yet:

- HCOMM primitive / custom-op payload backend for direct HBM-to-HBM copy.
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

The default HCOMM probe uses `--hcomm-channel-engine auto`, `--hcomm-channel-protocol hccs`, and `--hcomm-notify-num 2`. On CANN builds without `hccl_res_expt.h`, such as CANN 8.5, `auto` resolves to `cpu-ts` and validates the channel-resource path without claiming payload readiness. Add `--hcomm-require-thread-export` only when you want a strict future AICPU/HCOMM payload-ready check; CANN 8.5 is expected to report unsupported for that stricter mode.

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
- [Architecture](docs/architecture.md)
- [Demo plan](docs/demo-plan.md)
- [Single-node multi-card HBM/HCCL analysis](docs/single-node-hbm-hccl-analysis.md)
- [Research notes](refer/flume-research.md)
