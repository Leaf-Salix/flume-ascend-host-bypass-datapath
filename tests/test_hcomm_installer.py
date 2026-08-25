#!/usr/bin/env python3
"""Fail-closed tests for the parked Stage 3B.3G installer."""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: test_hcomm_installer.py <repo-root>", file=sys.stderr)
        return 2
    repo = Path(sys.argv[1]).resolve()
    installer = (repo / "custom_ops" / "hcomm_payload_copy" / "scripts" /
                 "flume_install.sh")
    source = installer.read_text(encoding="utf-8")
    for forbidden in ("npu-smi set", "flume_base_install", "cp ", "mv ",
                      "rm ", "ascend_package_load.ini"):
        assert forbidden not in source

    cmake = (repo / "custom_ops" / "hcomm_payload_copy" /
             "CMakeLists.txt").read_text(encoding="utf-8")
    assert "flume_custom_installer=parked-todo-v1" in cmake
    assert "RENAME flume_base_install.sh" not in cmake

    with tempfile.TemporaryDirectory(prefix="flume-parked-installer-") as tmp:
        tmp_path = Path(tmp)
        sentinel = tmp_path / "system-state"
        sentinel.write_text("unchanged\n", encoding="utf-8")
        for operation in ("--install", "--uninstall"):
            result = subprocess.run(
                ["bash", str(installer), operation], text=True,
                capture_output=True, check=False)
            assert result.returncode != 0
            assert "flume_installer=parked-todo-v1" in result.stdout
            assert "status=TODO" in result.stdout
            assert "stage3b3g_system_install=parked" in result.stdout
            assert "action=none" in result.stdout
            assert "system_changes=none" in result.stdout
            assert "fallback=hccl-p2p" in result.stdout
            assert sentinel.read_text(encoding="utf-8") == "unchanged\n"

        tool_result = subprocess.run(
            [
                sys.executable, str(repo / "tools" / "flume_tool.py"),
                f"--log-root={tmp_path / 'logs'}",
                f"--build-dir={tmp_path / 'build'}",
                "--hccl-source-root=/definitely/missing",
                "--install-custom-op-package",
                "hcomm-custom-op-build",
            ],
            cwd=repo, text=True, capture_output=True, check=False)
        assert tool_result.returncode == 0
        assert "hcomm-custom-op-install-parked" in tool_result.stdout
        assert "summary" in tool_result.stdout
        logs = sorted((tmp_path / "logs").glob("flume-check-*"))
        assert len(logs) == 1
        parked_log = next(logs[0].glob("*hcomm-custom-op-install-parked.log"))
        parked_text = parked_log.read_text(encoding="utf-8")
        assert "status=TODO" in parked_text
        assert "action=none" in parked_text
        assert "system_changes=none" in parked_text
        assert "fallback=hccl-p2p" in parked_text
        assert not (tmp_path / "build").exists()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
