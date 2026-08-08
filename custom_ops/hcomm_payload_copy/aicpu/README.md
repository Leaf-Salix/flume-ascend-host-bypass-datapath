# AICPU Kernel

This directory contains the Stage 3B custom-op kernel entrypoints.

The default Stage 3B.3D canary path is:

- `direct_acl_canary_kernel.cc`
- exported function: `FlumeHcommCanaryDirectAclrtKernel`
- includes only `flume_hcomm_notify_only_abi.h`

It verifies direct ACL custom-op package load, function lookup, descriptor
handoff, launch, and stream completion without relying on HCCL/HCOMM internal
headers.

The Stage 3B.3C direct ACL notify-only path is:

- `notify_only_direct_acl_kernel.cc`
- exported function: `FlumeHcommNotifyOnlyDirectAclrtKernel`
- includes `hcomm_primitives.h`

When `FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH=OFF`, this file also
exports a stub `FlumeHcommNotifyOnlyAicpuKernel` so the static package JSON
stays consistent with the shared object. The stub only preserves loader
compatibility; it is not a working public-HCCL-launch kernel.

The older Stage 3B.3A public-HCCL-launch notify-only experiment is:

- `notify_only_kernel.cc`
- exported function: `FlumeHcommNotifyOnlyAicpuKernel`

It is preserved behind `FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH=ON`
because it requires `hccl/hccl_launch.h`.

The Stage 3B.3E payload-copy experiment is behind
`FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY=ON`:

- `payload_copy_kernel.cc`
- exported functions: `FlumeHcommPayloadCopyDirectAclrtKernelV2` and the
  legacy wrapper `FlumeHcommPayloadCopyDirectAclrtKernel`

It consumes `flume_hcomm_payload_copy_desc_v1`, then runs a pair-copy plan
instead of a collective:

```text
send rank: HcommLocalCopyOnThread(src_hbm -> local_hccl_buffer)
           HcommChannelNotifyRecordOnThread(ready)
           HcommChannelNotifyWaitOnThread(done)

recv rank: HcommChannelNotifyWaitOnThread(ready)
           HcommReadOnThread(remote_hccl_buffer -> dst_hbm)
           HcommChannelNotifyRecordOnThread(done)
```

The descriptor also carries a batch tag and a device-visible `status_word`.
The kernel wraps the pair-copy primitive sequence in
`HcommBatchModeStart/End(batch_tag)`, then writes a concrete status such as
`success`, `local-copy-failed`, `remote-read-failed`, or
`ready-notify-wait-failed` before returning so host-side strict smoke can
distinguish a package/launch problem from a specific HCOMM primitive execution
problem.

The legacy public-HCCL-launch notify-only kernel consumes `HcclP2pKernelParam`, decodes
`flume_hcomm_notify_only_desc_v1` from `opParams`, then runs:

```text
send rank: HcommChannelNotifyRecordOnThread(ready)
           HcommChannelNotifyWaitOnThread(done)

recv rank: HcommChannelNotifyWaitOnThread(ready)
           HcommChannelNotifyRecordOnThread(done)
```

The kernel source is intentionally not compiled by the default local project
build. It must be packaged through the CANN/HCCL custom-op packaging flow before
the target host can load `libflume_hcomm_payload_aicpu_kernel.so`.
