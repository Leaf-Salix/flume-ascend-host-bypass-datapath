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

It consumes `HcclP2pKernelParam`, decodes
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
