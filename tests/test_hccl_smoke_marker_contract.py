#!/usr/bin/env python3
"""Keep the HCCL smoke app's strict payload markers aligned with the tool gate."""

from __future__ import annotations

import sys
from pathlib import Path


REQUIRED_MARKERS = (
    "payload_status_schema=v4",
    "payload_status_word_count=14",
    "payload_echo=passed",
    "payload_descriptor_fingerprint=passed",
    "payload_data_probe=observed",
    "payload_data_user_entry_fingerprint=",
    "payload_data_local_exit_fingerprint=",
    "payload_data_user_exit_fingerprint=",
    "payload_data_sample_bytes=",
    "payload_primitive_state=completed",
    "payload_trace=passed",
    "payload_trace_schema=v2",
    "payload_trace_word_count=80",
    "payload_trace_event=kernel-exit",
    "payload_trace_order=passed",
    "payload_trace_ret_order=passed",
    "payload_semantic_v10=present",
    "payload_semantic_v11=present",
)

STALE_MARKERS = (
    "payload_status_schema=v3",
    "payload_status_word_count=10",
)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: test_hccl_smoke_marker_contract.py <repo-root>",
              file=sys.stderr)
        return 2
    repo = Path(sys.argv[1]).resolve()
    source = repo / "apps" / "flume-hccl-collective-smoke.cc"
    text = source.read_text(encoding="utf-8")
    missing = [marker for marker in REQUIRED_MARKERS if marker not in text]
    if missing:
        print("missing strict smoke marker(s):", file=sys.stderr)
        for marker in missing:
            print(f"  {marker}", file=sys.stderr)
        return 1
    stale = [marker for marker in STALE_MARKERS if marker in text]
    if stale:
        print("stale strict smoke marker(s):", file=sys.stderr)
        for marker in stale:
            print(f"  {marker}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
