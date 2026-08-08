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


def compile_kernel(tmp: Path, mode: str) -> Path:
    source = tmp / f"kernel_{mode}.c"
    lines = [
        "unsigned int FlumeHcommCanaryDirectAclrtKernel(void *p) "
        "{ (void)p; return 0; }",
        "unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV2(void *p) "
        "{ (void)p; return 0; }",
        "unsigned int FlumeHcommPayloadCopyDirectAclrtKernel(void *p) "
        "{ (void)p; return FlumeHcommPayloadCopyDirectAclrtKernelV2(p); }",
        "unsigned int FlumeHcommPayloadCopyAbiVersion(void) { return 2; }",
    ]
    if mode == "canary":
        lines.append(
            "unsigned int FlumeHcommPayloadBuildModeCanaryOnly(void) "
            "{ return 1; }"
        )
    else:
        lines.append(
            "unsigned int FlumeHcommPayloadBuildModeInternalPayload(void) "
            "{ return 1; }"
        )
    if mode != "legacy":
        lines.append(
            "unsigned int FlumeHcommPayloadCopyAbiVersion2(void) { return 1; }"
        )
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output = tmp / f"kernel_{mode}.so"
    if platform.system() == "Darwin":
        command = ["cc", "-dynamiclib", "-o", str(output), str(source)]
    else:
        command = ["cc", "-shared", "-fPIC", "-o", str(output), str(source)]
    subprocess.run(command, check=True)
    return output


def write_package(tmp: Path, mode: str) -> tuple[Path, Path]:
    so_path = compile_kernel(tmp, mode)
    tar_path = tmp / f"pkg_{mode}.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(so_path, arcname=f"aicpu_kernels_device/{KERNEL_SO}")

    kernel_so = (
        "libwrong_flume_hcomm_payload_aicpu_kernel.so"
        if mode == "wrong_so" else KERNEL_SO
    )
    payload = {
        "FlumeHcommCanaryDirectAclrtKernel": {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommCanaryDirectAclrtKernel",
            }
        },
        "FlumeHcommPayloadCopyDirectAclrtKernelV2": {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyDirectAclrtKernelV2",
            }
        },
    }
    if mode != "legacy":
        payload["FlumeHcommPayloadCopyAbiVersion2"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyAbiVersion2",
            }
        }
    json_path = tmp / f"pkg_{mode}.json"
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

        legacy_json, legacy_tar = write_package(tmp, mode="legacy")
        legacy = run_preflight(repo, legacy_json, legacy_tar)
        if legacy.returncode == 0:
            print(legacy.stdout)
            print(legacy.stderr, file=sys.stderr)
            raise AssertionError("legacy package without ABI v2 marker passed")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=missing" in legacy.stdout
        assert "function_so.payload_abi_version_v2.FlumeHcommPayloadCopyAbiVersion2=missing" in legacy.stdout
        assert "reason=payload kernel package is missing the payload ABI version marker" in legacy.stdout

        canary_json, canary_tar = write_package(tmp, mode="canary")
        canary = run_preflight(repo, canary_json, canary_tar)
        if canary.returncode == 0:
            print(canary.stdout)
            print(canary.stderr, file=sys.stderr)
            raise AssertionError("canary-only package passed as payload-ready")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=present" in canary.stdout
        assert "function_so.payload_abi_version_v2.FlumeHcommPayloadCopyAbiVersion2=present" in canary.stdout
        assert "function_so.build_mode.canary_only.FlumeHcommPayloadBuildModeCanaryOnly=present" in canary.stdout
        assert "function_so.build_mode.internal_payload.FlumeHcommPayloadBuildModeInternalPayload=missing" in canary.stdout
        assert "reason=payload kernel package is canary-only" in canary.stdout

        wrong_so_json, wrong_so_tar = write_package(tmp, mode="wrong_so")
        wrong_so = run_preflight(repo, wrong_so_json, wrong_so_tar)
        if wrong_so.returncode == 0:
            print(wrong_so.stdout)
            print(wrong_so.stderr, file=sys.stderr)
            raise AssertionError("package with mismatched JSON kernelSo passed")
        assert "aicpu_tar_so.libflume_hcomm_payload_aicpu_kernel.so=present" in wrong_so.stdout
        assert "function.canary_direct_aclrt.FlumeHcommCanaryDirectAclrtKernel=missing" in wrong_so.stdout
        assert (
            "function.payload_direct_aclrt."
            "FlumeHcommPayloadCopyDirectAclrtKernelV2=missing"
        ) in wrong_so.stdout
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=missing" in wrong_so.stdout
        assert "status=FAIL" in wrong_so.stdout

        v2_json, v2_tar = write_package(tmp, mode="v2")
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
