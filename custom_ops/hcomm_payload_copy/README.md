# HCOMM Payload Copy Custom Op Skeleton

This directory is the reserved implementation surface for Stage 3B.

Current status:

- Not built by default.
- `FLUME_BUILD_HCOMM_CUSTOM_OP=ON` only changes the library diagnostic from
  `custom-op/AICPU scheduler build disabled` to
  `custom-op/AICPU scheduler launch missing`.
- No real AICPU kernel is launched yet.

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

The host-side launcher must create/acquire HCOMM Thread, Channel, Notify, and
HCCL Buffer resources, serialize the resource context, submit the AICPU kernel,
and synchronize completion back to the caller's ACL stream.
