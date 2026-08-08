#!/usr/bin/env python3
"""Synthetic tests for Flume HCOMM custom-op package preflight."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path


KERNEL_SO = "libflume_hcomm_payload_aicpu_kernel.so"


def compile_kernel(tmp: Path, include_abi_v2: bool) -> Path:
    source = tmp / ("kernel_v2.c" if include_abi_v2 else "kernel_legacy.c")
    lines = [
        "unsigned int FlumeHcommCanaryDirectAclrtKernel(void *p) "
        "{ (void)p; return 0; }",
        "unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV2(void *p) "
        "{ (void)p; return 0; }",
        "unsigned int FlumeHcommPayloadCopyDirectAclrtKernel(void *p) "
        "{ (void)p; return FlumeHcommPayloadCopyDirectAclrtKernelV2(p); }",
        "unsigned int FlumeHcommPayloadBuildModeInternalPayload(void) "
        "{ return 1; }",
        "unsigned int FlumeHcommPayloadCopyAbiVersion(void) { return 2; }",
    ]
    if include_abi_v2:
        lines.append(
            "unsigned int FlumeHcommPayloadCopyAbiVersion2(void) { return 1; }"
        )
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output = tmp / ("kernel_v2.so" if include_abi_v2 else "kernel_legacy.so")
    if platform.system() == "Darwin":
        command = ["cc", "-dynamiclib", "-o", str(output), str(source)]
    else:
        command = ["cc", "-shared", "-fPIC", "-o", str(output), str(source)]
    subprocess.run(command, check=True)
    return output


def write_package(tmp: Path, include_abi_v2: bool) -> tuple[Path, Path]:
    so_path = compile_kernel(tmp, include_abi_v2)
    tar_path = tmp / ("pkg_v2.tar.gz" if include_abi_v2 else "pkg_legacy.tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(so_path, arcname=f"aicpu_kernels_device/{KERNEL_SO}")

    payload = {
        "FlumeHcommCanaryDirectAclrtKernel": {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": KERNEL_SO,
                "functionName": "FlumeHcommCanaryDirectAclrtKernel",
            }
        },
        "FlumeHcommPayloadCopyDirectAclrtKernelV2": {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": KERNEL_SO,
                "functionName": "FlumeHcommPayloadCopyDirectAclrtKernelV2",
            }
        },
    }
    if include_abi_v2:
        payload["FlumeHcommPayloadCopyAbiVersion2"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": KERNEL_SO,
                "functionName": "FlumeHcommPayloadCopyAbiVersion2",
            }
        }
    json_path = tmp / ("pkg_v2.json" if include_abi_v2 else "pkg_legacy.json")
    json_path.write_text(json.dumps(payload), encoding="utf-8")
    return json_path, tar_path


def run_preflight(repo: Path, json_path: Path, tar_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(repo / "tools" / "flume_tool.py"),
            f"--custom-op-json={json_path}",
            f"--custom-op-aicpu-tar={tar_path}",
            "--require-hcomm-payload-kernel",
            "hcomm-custom-op-package",
        ],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: test_hcomm_package_preflight.py <repo-root>", file=sys.stderr)
        return 2
    repo = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="flume-package-preflight-") as tmp_text:
        tmp = Path(tmp_text)

        legacy_json, legacy_tar = write_package(tmp, include_abi_v2=False)
        legacy = run_preflight(repo, legacy_json, legacy_tar)
        if legacy.returncode == 0:
            print(legacy.stdout)
            print(legacy.stderr, file=sys.stderr)
            raise AssertionError("legacy package without ABI v2 marker passed")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=missing" in legacy.stdout
        assert "function_so.payload_abi_version_v2.FlumeHcommPayloadCopyAbiVersion2=missing" in legacy.stdout
        assert "reason=payload kernel package is missing the payload ABI version marker" in legacy.stdout

        v2_json, v2_tar = write_package(tmp, include_abi_v2=True)
        v2 = run_preflight(repo, v2_json, v2_tar)
        if v2.returncode != 0:
            print(v2.stdout)
            print(v2.stderr, file=sys.stderr)
            raise AssertionError("ABI v2 package did not pass")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=present" in v2.stdout
        assert "function_so.payload_abi_version_v2.FlumeHcommPayloadCopyAbiVersion2=present" in v2.stdout
        assert "status=PASS" in v2.stdout

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
