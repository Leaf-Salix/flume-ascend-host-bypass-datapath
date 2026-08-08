# HCOMM Payload Copy Custom Op Skeleton

This directory is the reserved implementation surface for Stage 3B.

Current status:

- Not built by default.
- `FLUME_BUILD_HCOMM_CUSTOM_OP=ON` only changes the library diagnostic from
  `custom-op/AICPU scheduler build disabled` to
  a Stage 3B.3B launcher-router decision for notify-only smoke.
- Stage 3B.3A adds a kernel-side notify-only entrypoint:
  `FlumeHcommNotifyOnlyAicpuKernel`.
- Stage 3B.3B detects public `HcclAicpuKernelLaunch`, direct ACL runtime
  custom-op launch APIs, HCOMM thread export, HCOMM primitives, and installed
  Flume custom-op package state before selecting a launcher.
- Stage 3B.3C adds the direct ACL runtime entrypoint:
  `FlumeHcommNotifyOnlyDirectAclrtKernel`, which consumes
  `flume_hcomm_notify_only_desc_v1` directly through `aclrtKernelArgsAppend`.
- Stage 3B.3D adds a no-internal-header direct ACL canary entrypoint:
  `FlumeHcommCanaryDirectAclrtKernel`. The default device kernel build now
  compiles this canary without `pkg_inc`, `hccl_launch.h`,
  `hcomm_primitives.h`, or `hccl_res_expt.h`.
- Stage 3B.3E adds the first true HCOMM pair-copy kernel:
  `FlumeHcommPayloadCopyDirectAclrtKernel`. It is behind
  `FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD=ON` because the kernel itself
  calls public `hcomm_primitives.h` APIs: `HcommLocalCopyOnThread`,
  `HcommReadOnThread`, and Channel Notify primitives. This direct ACL payload
  package does not require `hccl_launch.h` or `pkg_inc`.
- Payload copy now has an experimental kernel path, but it is not part of the
  default no-internal-header canary build and still requires remote validation
  with the HCOMM primitive payload package enabled.
- The legacy public HCCL-launch notify-only entrypoint that consumes
  `HcclP2pKernelParam` is optional and guarded by
  `FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH=ON`; keep it off on CANN
  packages that do not expose `hccl_launch.h`. Package builds export stubs for
  JSON-declared functions that are not enabled in the current mode, so the
  static JSON and SO remain loader-compatible. Direct ACL payload smoke does
  not use those stubs.

Target data-plane plan:

```text
send rank:
  HcommLocalCopyOnThread(input -> local HCCL Buffer)
  HcommChannelNotifyRecordOnThread(ready)
  HcommChannelNotifyWaitOnThread(done)

recv rank:
  HcommChannelNotifyWaitOnThread(ready)
  HcommReadOnThread(remote HCCL Buffer -> local HCCL Buffer)
  HcommLocalCopyOnThread(local HCCL Buffer -> output)
  HcommChannelNotifyRecordOnThread(done)
```

The default recv path intentionally stages through the local HCCL Buffer before
copying into user HBM. For diagnostics, Flume can set
`FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT` in the descriptor, making the recv
kernel call `HcommReadOnThread(remote HCCL Buffer -> output HBM)` directly and
skip the final local copy. The smoke marker is
`payload_recv_path=direct-output`; the default marker is
`payload_recv_path=local-buffer`.

Stage 3B.2-complete notify-only plan:

```text
send rank:
  consume serialized resource descriptor
  HcommChannelNotifyRecordOnThread(ready)
  HcommChannelNotifyWaitOnThread(done)

recv rank:
  consume serialized resource descriptor
  HcommChannelNotifyWaitOnThread(ready)
  HcommChannelNotifyRecordOnThread(done)
```

Stage 3B.3A true-launch path:

```text
host:
  acquire AICPU_TS Thread and HCOMM Channel
  package flume_hcomm_notify_only_desc_v1
  call HcclAicpuKernelLaunch(...)

AICPU kernel:
  receive HcclP2pKernelParam
  consume flume_hcomm_notify_only_desc_v1 from opParams
  rank0: HcommChannelNotifyRecordOnThread(ready)
         HcommChannelNotifyWaitOnThread(done)
  rank1: HcommChannelNotifyWaitOnThread(ready)
         HcommChannelNotifyRecordOnThread(done)
```

Stage 3B.3B launcher-router path:

```text
host:
  probe public HCCL launch ABI
  probe direct ACL runtime custom-op launch APIs
  probe HCOMM thread export and primitive APIs
  check installed Flume custom-op package
  select public_hccl_launch or unsupported with precise reason
```

Stage 3B.3C direct ACL readiness path:

```text
host:
  aclrtBinaryLoadFromFile(installed Flume custom-op JSON)
  aclrtBinaryGetFunction(FlumeHcommNotifyOnlyDirectAclrtKernel)
  aclrtKernelArgsAppend(flume_hcomm_notify_only_desc_v1)
  aclrtLaunchKernelWithConfig(...)

AICPU kernel:
  consume flume_hcomm_notify_only_desc_v1 directly
  rank0/rank1 run the same ready/done notify-only protocol
```

Stage 3B.3D no-internal-header canary path:

```text
host:
  aclrtBinaryLoadFromFile(installed Flume custom-op JSON)
  aclrtBinaryGetFunction(FlumeHcommCanaryDirectAclrtKernel)
  aclrtMalloc(canary status word)
  aclrtKernelArgsAppend(flume_hcomm_canary_desc_v1)
  aclrtLaunchKernelWithConfig(...)
  aclrtSynchronizeStream(...)
  aclrtMemcpy(canary status word D2H)

AICPU/custom-op kernel:
  include only flume_hcomm_notify_only_abi.h
  validate flume_hcomm_canary_desc_v1
  record a device-visible canary status/token
```

Stage 3B.3E payload-copy path:

```text
send rank kernel:
  HcommLocalCopyOnThread(src_hbm -> local HCCL Buffer)
  HcommChannelNotifyRecordOnThread(ready)
  HcommChannelNotifyWaitOnThread(done)

recv rank kernel:
  HcommChannelNotifyWaitOnThread(ready)
  HcommReadOnThread(remote HCCL Buffer -> local HCCL Buffer)
  HcommLocalCopyOnThread(local HCCL Buffer -> dst_hbm)
  HcommChannelNotifyRecordOnThread(done)
```

Optional recv direct-output diagnostic:

```text
recv rank kernel:
  HcommChannelNotifyWaitOnThread(ready)
  HcommReadOnThread(remote HCCL Buffer -> dst_hbm)
  HcommChannelNotifyRecordOnThread(done)
```

Expected markers:

- success: `stage3b3a_kernel_launch=passed stage3b2_kernel_consume=passed`
- capability/version block: `stage3b3a_kernel_launch=unsupported`
- router: `stage3b3b_launcher_router=selected:<backend>`
- direct ACL loader: `stage3b3c_direct_aclrt_loader=passed|unsupported|failed`
- direct ACL handoff: `stage3b3c_descriptor_handoff=passed|blocked|failed`
- direct ACL launch: `stage3b3c_direct_aclrt_launch=passed|not-attempted|failed`
- direct ACL notify status: `notify_kernel_status=success notify_status_word=0`
- no-internal canary:
  `stage3b3d_no_internal_headers=on stage3b3d_direct_aclrt_canary=passed`
- payload copy:
  `stage3b3e_payload_copy=passed stage3b3e_payload_sync=passed payload_batch_mode=on payload_kernel_status=success`
- payload completion:
  `payload_thread_notify=host-aicpu payload_completion=thread-notify+stream-sync+status-word`
  when CANN exposes thread-export handles; otherwise
  `payload_thread_notify=unavailable payload_completion=stream-sync+status-word`
- real launch failure: `stage3b3a_kernel_launch=failed`

The AICPU kernel must still be packaged and deployed through the CANN/HCCL
custom-op packaging flow before a target host can load
`libflume_hcomm_payload_aicpu_kernel.so`.

Packaging sketch:

```bash
cd <flume-repo>
python3 tools/flume_tool.py \
  --hccl-source-root <hccl-source-root> \
  --custom-op-build-mode canary \
  hcomm-custom-op-build

cd <hccl-source-root>
bash build.sh \
  --vendor=flume \
  --ops=hcomm_payload \
  --custom_ops_path=<flume-repo>/custom_ops/hcomm_payload_copy

./build_out/cann-hccl_custom_hcomm_payload_linux-<arch>.run --install
```

By default this builds the no-internal-header canary package. To build the
experimental HCOMM primitive payload package, enable primitive payload mode
through the Flume helper or through the environment before invoking HCCL
`build.sh`. The old `FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY` variable is
still accepted as a compatibility alias, but new scripts should use
`FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD`:

```bash
cd <flume-repo>
# Preferred when the target host has CANN toolkit headers/libs but no HCCL
# source packaging tree: build JSON/tar directly and export an isolated
# runtime layout.
python3 tools/flume_tool.py \
  --custom-op-build-mode payload \
  --custom-op-export-root <temporary-custom-op-root> \
  hcomm-custom-op-direct-build

python3 tools/flume_tool.py \
  --hccl-source-root <hccl-source-root> \
  --custom-op-build-mode payload \
  hcomm-custom-op-build

# Optional: install the generated .run package and verify installed visibility.
python3 tools/flume_tool.py \
  --hccl-source-root <hccl-source-root> \
  --custom-op-build-mode payload \
  --install-custom-op-package \
  hcomm-custom-op-build

# Optional: export preflight-passing build artifacts into an isolated runtime
# layout instead of installing into the system CANN/OPP tree.
python3 tools/flume_tool.py \
  --custom-op-json <path-to-libflume_hcomm_payload_aicpu_kernel.json> \
  --custom-op-aicpu-tar <path-to-aicpu_flume_hcomm_payload.tar.gz> \
  --custom-op-build-mode payload \
  --custom-op-export-root <temporary-custom-op-root> \
  hcomm-custom-op-export-runtime

cd <hccl-source-root>
FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD=ON \
bash build.sh \
  --vendor=flume \
  --ops=hcomm_payload \
  --custom_ops_path=<flume-repo>/custom_ops/hcomm_payload_copy

./build_out/cann-hccl_custom_hcomm_payload_linux-<arch>.run --install
```

The export command writes the runtime-loadable layout expected by Flume under
`<temporary-custom-op-root>/opp/vendors/flume/aicpu/{config,kernel}` and then
runs package preflight against that root. It is useful when the target machine
should not be modified by a `.run --install` step.

`--install-custom-op-package` only installs after the build artifact preflight
passes. If the JSON, AICPU tar, V4 payload entrypoint, or primitive-payload
build marker is missing, the helper stops before touching the target CANN/OPP
install.

The primitive payload package is the one required for
`stage3b3e_payload_copy=passed`.
It still uses the direct ACL runtime launcher and intentionally avoids the
legacy `hccl_launch.h` path. Only enable
`FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH=ON` when the target CANN package
actually exposes `hccl/hccl_launch.h` and you specifically want to test the
older public-HCCL-launch notify-only entrypoint.
The package preflight treats the build as payload-ready only when the AICPU SO
exports `FlumeHcommPayloadBuildModeInternalPayload`; a canary-only package may
export the V4 payload function as a compatibility stub, but it is not accepted
as a real payload package. Current payload-ready packages also export
`FlumeHcommPayloadCopyAbiVersion4`,
`FlumeHcommPayloadCopySemanticVersion`,
`FlumeHcommPayloadCopySemanticVersion5`,
`FlumeHcommPayloadCopySemanticVersion6`,
`FlumeHcommPayloadCopyRequiresCommAcquire`,
`FlumeHcommPayloadStatusSchemaVersion`, and
`FlumeHcommPayloadStatusWordCount`. These mark the descriptor ABI with HCCL
comm-name handoff, descriptor echo words, the success-status schema where the
second status word is written as `payload_kernel_hcomm_ret=0`, the expected
status word count, and the requirement that the kernel acquires/releases the
HCOMM communicator by name. Payload-ready preflight also requires the AICPU SO
to reference the HCOMM primitive symbols used by the payload path, including
`HcommLocalCopyOnThread`, `HcommReadOnThread`, HCOMM Channel Notify, Batch, and
Comm Acquire/Release APIs. A marker-only SO is therefore not accepted as a real
payload package.
The packaging CMake installs a mode-specific JSON under the same runtime name:
canary builds use `libflume_hcomm_payload_aicpu_kernel_canary.json`, while
payload builds use `libflume_hcomm_payload_aicpu_kernel_payload.json` and
rename it to `libflume_hcomm_payload_aicpu_kernel.json` in the installed OPP
layout.

After installation, run Flume with `--build-hcomm-custom-op` and
`--run-hcomm-notify-only-smoke`. A successful Stage 3B.3A run prints
`stage3b3a_kernel_launch=passed stage3b2_kernel_consume=passed`.

For the real Stage 3B.3E payload-copy gate, prefer an installed or exported OPP
runtime layout. Flume launches the kernel with `aclrtBinaryLoadFromFile(JSON)`,
so an explicit `--custom-op-json` should normally point at
`aicpu/config/libflume_hcomm_payload_aicpu_kernel.json`, with the matching
`aicpu/kernel/aicpu_flume_hcomm_payload.tar.gz` in the same runtime layout. For
loose build artifacts, pass both `--custom-op-json <json>` and
`--custom-op-aicpu-tar <tar>`; the JSON remains the ACL runtime load source, and
the tar path is forwarded to runtime readiness checks so strict-positive does
not silently fall back to an unrelated installed package.

```bash
python3 tools/flume_tool.py --build-dir build-hcomm-payload-positive \
  --build-hcomm-custom-op \
  --custom-op-json <installed-opp-root>/vendors/flume/aicpu/config/libflume_hcomm_payload_aicpu_kernel.json \
  --custom-op-aicpu-tar <installed-opp-root>/vendors/flume/aicpu/kernel/aicpu_flume_hcomm_payload.tar.gz \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  hcomm-payload-strict-positive
```

Strict-positive success must include
`stage3b3e_payload_copy=passed`, `stage3b3e_direct_aclrt_payload_launch=passed`,
`stage3b3e_payload_sync=passed`, `payload_kernel_status=success`,
`payload_failure_step=none`, `payload_status_word=0`, `payload_kernel_hcomm_ret=0`,
`payload_status_schema=v2`, `payload_status_word_count=8`,
`payload_echo=passed`, `payload_desc_batch_tag=default|custom`,
`payload_thread_notify_order=...`, `payload_pattern=strict-v1`, source/received/expected checksum match,
`payload_verify=passed`, and `fallback=none` on both ranks.
