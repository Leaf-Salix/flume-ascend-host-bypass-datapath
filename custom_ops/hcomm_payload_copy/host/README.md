# Host Launcher

Stage 3B.3A currently launches through the library-side
`HcclAicpuKernelLaunch` path in `src/core/client.cc`.

The launcher path:

```text
ProbeHcommChannelResources(AICPU_TS)
  -> package flume_hcomm_notify_only_desc_v1
  -> HcclAicpuKernelLaunch(...)
  -> marker: stage3b3a_kernel_launch=passed|unsupported|failed
```

This directory remains reserved for a future standalone custom-op host library
or packaging glue if we decide to export a separate Flume custom-op API.
