# AICPU Kernel

This directory contains two Stage 3B custom-op kernel entrypoints.

The default Stage 3B.3D canary path is:

- `direct_acl_canary_kernel.cc`
- exported function: `FlumeHcommCanaryDirectAclrtKernel`
- includes only `flume_hcomm_notify_only_abi.h`

It verifies direct ACL custom-op package load, function lookup, descriptor
handoff, launch, and stream completion without relying on HCCL/HCOMM internal
headers.

The older Stage 3B.3A notify-only experiment is:

- `notify_only_kernel.cc`
- exported functions: `FlumeHcommNotifyOnlyAicpuKernel` and
  `FlumeHcommNotifyOnlyDirectAclrtKernel`

It is preserved behind `FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY=ON`.

The Stage 3B.3E payload-copy experiment is also behind that option:

- `payload_copy_kernel.cc`
- exported function: `FlumeHcommPayloadCopyDirectAclrtKernel`

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
`HcommBatchModeStart/End(batch_tag)`, then writes `success`,
`invalid-argument`, or `hcomm-error` before returning so host-side strict smoke
can distinguish a package/launch problem from an HCOMM primitive execution
problem.

The notify-only kernel consumes `HcclP2pKernelParam`, decodes
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
