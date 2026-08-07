# HCOMM Payload Copy Custom Op Skeleton

This directory is the reserved implementation surface for Stage 3B.

Current status:

- Not built by default.
- `FLUME_BUILD_HCOMM_CUSTOM_OP=ON` only changes the library diagnostic from
  `custom-op/AICPU scheduler build disabled` to
  an experimental `HcclAicpuKernelLaunch` attempt for notify-only smoke.
- Stage 3B.3A adds a kernel-side notify-only entrypoint:
  `FlumeHcommNotifyOnlyAicpuKernel`.
- Payload copy is still not implemented in this custom-op path.

Target data-plane plan:

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

Expected markers:

- success: `stage3b3a_kernel_launch=passed stage3b2_kernel_consume=passed`
- capability/version block: `stage3b3a_kernel_launch=unsupported`
- real launch failure: `stage3b3a_kernel_launch=failed`

The AICPU kernel must still be packaged and deployed through the CANN/HCCL
custom-op packaging flow before a target host can load
`libflume_hcomm_payload_aicpu_kernel.so`.
