# AICPU Kernel

This directory contains the Stage 3B.3A notify-only AICPU kernel entrypoint:

- `notify_only_kernel.cc`
- exported function: `FlumeHcommNotifyOnlyAicpuKernel`

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
