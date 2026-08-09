#!/usr/bin/env python3
"""Reject HCCL payload API shortcuts in HCOMM custom-op source files."""

from __future__ import annotations

import re
import sys
from pathlib import Path


FORBIDDEN_HCCL_PAYLOAD_APIS = (
    "HcclSend",
    "HcclRecv",
    "HcclBatchSendRecv",
    "HcclBatchSendRecvV2",
    "HcclAllReduce",
    "HcclAllReduceV2",
    "HcclAllGather",
    "HcclAllGatherV",
    "HcclReduceScatter",
    "HcclReduceScatterV",
    "HcclBroadcast",
    "HcclBroadcastV2",
    "HcclReduce",
    "HcclGather",
    "HcclScatter",
    "HcclAlltoAll",
    "HcclAlltoAllV",
    "HcclAlltoAllVC",
    "HcclBarrier",
    "HcclBatchPut",
    "HcclBatchGet",
    "HcclBatchRead",
    "HcclBatchWrite",
)

SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp"}


def source_files(repo_root: Path) -> list[Path]:
    roots = [
        repo_root / "custom_ops" / "hcomm_payload_copy",
        repo_root / "src" / "hcomm_payload",
    ]
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file() and path.suffix in SOURCE_SUFFIXES:
                files.append(path)
    return sorted(files)


def main() -> int:
    repo_root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else (
        Path(__file__).resolve().parents[1])
    pattern = re.compile(
        r"\b(" + "|".join(re.escape(name)
                           for name in FORBIDDEN_HCCL_PAYLOAD_APIS) + r")\b")
    violations: list[str] = []
    files = source_files(repo_root)
    for path in files:
        text = path.read_text(encoding="utf-8", errors="replace")
        for line_no, line in enumerate(text.splitlines(), start=1):
            match = pattern.search(line)
            if match is None:
                continue
            rel = path.relative_to(repo_root)
            violations.append(f"{rel}:{line_no}: forbidden {match.group(1)}")

    if violations:
        print("HCOMM payload source gate failed:")
        print("\n".join(violations))
        print("")
        print("HCOMM payload kernels must use HCOMM primitives such as "
              "HcommReadOnThread/HcommWriteOnThread, not HCCL payload, "
              "collective, or one-sided APIs.")
        return 1

    print("hcomm_payload_source_no_hccl_payload_api=passed")
    print(f"files_scanned={len(files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
