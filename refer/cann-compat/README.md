# CANN Compatibility Fixtures

This directory is for text-only compatibility fixtures collected from real CANN
installations. Generated fixture directories are ignored by Git by default.

Use the collector on an Ascend host:

```bash
python3 tools/collect_cann_compat.py \
  --label cann-8.5.0-aarch64 \
  --flume-log-dir logs/flume-check-YYYYMMDD-HHMMSS \
  --devices 10,12
```

The generated fixture records header presence, library manifests, exported
symbols, Flume feature probes, backend capability lines, and optional `npu-smi`
output. It does not copy CANN binaries.
