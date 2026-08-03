#!/usr/bin/env python3
"""Check whether this machine has a CANN/HCCL layout usable by CMake."""

from __future__ import annotations

import os
import platform
from pathlib import Path


def main() -> int:
    ascend_home = os.environ.get("ASCEND_HOME_PATH")
    print(f"platform: {platform.system()} {platform.machine()}")
    if not ascend_home:
        print("ASCEND_HOME_PATH: not set")
        print("HCCL status: unavailable")
        print("hint: source the CANN set_env.sh on an Ascend Linux host")
        return 1

    root = Path(ascend_home)
    include_candidates = [
        root / "hccl" / "include",
        root / "include",
        root / "latest" / "hccl" / "include",
    ]
    lib_candidates = [
        root / "hccl" / "lib64",
        root / "lib64",
        root / "latest" / "hccl" / "lib64",
    ]

    include = next((p for p in include_candidates if p.exists()), None)
    lib = next((p for p in lib_candidates if p.exists()), None)

    print(f"ASCEND_HOME_PATH: {root}")
    print(f"HCCL include: {include if include else 'not found'}")
    print(f"HCCL lib64: {lib if lib else 'not found'}")

    if include and lib:
        print("HCCL status: available for FLUME_ENABLE_HCCL=ON")
        return 0

    print("HCCL status: incomplete CANN/HCCL layout")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
