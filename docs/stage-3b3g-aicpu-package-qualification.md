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
