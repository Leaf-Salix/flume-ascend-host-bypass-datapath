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

Expected markers:

- success: `stage3b3a_kernel_launch=passed stage3b2_kernel_consume=passed`
- capability/version block: `stage3b3a_kernel_launch=unsupported`
- router: `stage3b3b_launcher_router=selected:<backend>`
- direct ACL loader: `stage3b3c_direct_aclrt_loader=passed|unsupported|failed`
- direct ACL handoff: `stage3b3c_descriptor_handoff=passed|blocked|failed`
- direct ACL launch: `stage3b3c_direct_aclrt_launch=passed|not-attempted|failed`
- real launch failure: `stage3b3a_kernel_launch=failed`

The AICPU kernel must still be packaged and deployed through the CANN/HCCL
custom-op packaging flow before a target host can load
`libflume_hcomm_payload_aicpu_kernel.so`.

Packaging sketch:

```bash
cd <hccl-source-root>
bash build.sh \
  --vendor=flume \
  --ops=hcomm_payload \
  --custom_ops_path=<flume-repo>/custom_ops/hcomm_payload_copy

./build_out/cann-hccl_custom_hcomm_payload_linux-<arch>.run --install
```

After installation, run Flume with `--build-hcomm-custom-op` and
`--run-hcomm-notify-only-smoke`. A successful Stage 3B.3A run prints
`stage3b3a_kernel_launch=passed stage3b2_kernel_consume=passed`.
