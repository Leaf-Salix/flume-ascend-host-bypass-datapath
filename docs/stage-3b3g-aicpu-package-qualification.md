# Stage 3B.3G: AICPU Device Package Qualification

Stage 3B.3G separates package structure, device candidacy, runtime admission,
and kernel execution. A host loader or symbol check must not be used as proof
that the AICPU OS can load a custom kernel.

## Qualification levels

| Level | Required evidence | Meaning |
|---|---|---|
| `unqualified` | Package missing or unreadable | No usable package evidence |
| `structural` | JSON, tar, kernel SO, required symbols | Host-side inspection only |
| `device-candidate` | HCCL device package manifest, AArch64 ELF, no known host runtime dependencies | Eligible for a real device canary |
| `runtime-admitted` | Runtime and AICPU loader accept the package | Kernel entry can be loaded |
| `kernel-executed` | Standalone canary writes the expected device-visible token | Device execution proved |

`hcomm-custom-op-direct-build` always produces
`package_provenance=host-diagnostic package_qualification=structural`. It is
useful for ABI and symbol diagnostics but cannot satisfy strict payload gates.

## Official source bootstrap

Flume does not copy HCCL implementation code. The tool can prepare a pinned
official HCCL checkout as an external build dependency:

```bash
python3 tools/flume_tool.py \
  --build-dir build-aicpu-package \
  hcomm-custom-op-source-prepare
```

The default source is `https://gitcode.com/cann/hccl.git` at the revision
recorded by `HCCL_SOURCE_REVISION` in `tools/flume_tool.py`. The checkout lives
under `<build-dir>/deps/hccl` when no explicit source root or local ignored
reference checkout exists.

During package build Flume copies its own custom-op source into a unique,
marker-owned directory below the HCCL checkout. This keeps
`add_subdirectory()` inside the HCCL source tree for CMake 4.x compatibility.
The staging directory is removed after `build.sh` returns, including failed
builds; Flume refuses to remove directories without its ownership marker.
The host packaging configure maps its custom-op binary directory relative to
the host build root onto the matching device build directory. The resulting
`signed_aicpu_install_source` therefore follows CANN's per-subdirectory
`add_cann_sign_file` output instead of assuming a fixed
`build_device/signatures` location.

The device package can then be built with:

```bash
python3 tools/flume_tool.py \
  --build-dir build-aicpu-package \
  --prepare-hccl-source \
  --custom-op-build-mode payload \
  hcomm-custom-op-build
```

Static qualification only yields `device-candidate`. Runtime security policy,
AICPU package admission, standalone canary execution, notify-only execution,
and strict payload transfer remain separate gates.

For CANN layouts without a link-time `libccl_kernel.so`, the custom-op build
reuses the official HCCL `generate_stub(ccl_kernel)` helper. Configure output
records `ccl_kernel_link_mode` as `existing-target`,
`installed-library`, or `generated-stub`. The stub only satisfies the package
build boundary; the standalone canary and payload gates still decide whether
the device runtime can resolve and execute the HCOMM primitives.

## Read-only runtime gate

Flume can inspect the package whitelist and query the driver custom-op
signature properties without changing either one:

```bash
python3 tools/flume_tool.py \
  --cann-package-root <cann-root> \
  --hccl-devices <device-a>,<device-b> \
  hcomm-aicpu-runtime-preflight
```

The tool never runs `npu-smi set` and never edits
`ascend_package_load.ini`. A missing whitelist entry or a known blocking
signature policy stops the device gate and produces
`AICPU_RUNTIME_ADMIN_ACTIONS.txt`.

After an administrator-approved device package is installed or supplied, the
one-shot gate is:

```bash
python3 tools/flume_tool.py \
  --build-dir build-aicpu-gate \
  --hccl-devices <device-a>,<device-b> \
  --hccl-host-ifname <host-ifname> \
  --hccl-host-ip <host-ip> \
  --build-hcomm-custom-op \
  hcomm-aicpu-qualification-gate
```

It requires static `device-candidate` evidence, performs the read-only policy
preflight, and then starts three independent smoke processes:

```text
standalone canary -> notify-only -> strict payload
```

No later process is started when an earlier gate fails.
