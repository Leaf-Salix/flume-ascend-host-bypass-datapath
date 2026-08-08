#!/usr/bin/env python3
"""Synthetic tests for Flume HCOMM custom-op package preflight."""

from __future__ import annotations

import json
import importlib.util
import platform
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from types import SimpleNamespace


KERNEL_SO = "libflume_hcomm_payload_aicpu_kernel.so"
KERNEL_JSON = "libflume_hcomm_payload_aicpu_kernel.json"
AICPU_TAR = "aicpu_flume_hcomm_payload.tar.gz"


def load_flume_tool(repo: Path):
    spec = importlib.util.spec_from_file_location(
        "flume_tool_under_test", repo / "tools" / "flume_tool.py")
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load tools/flume_tool.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


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
    if mode not in ("legacy", "stale_v2"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion(void) "
            "{ return 3; }"
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
    if mode not in ("legacy", "stale_v2"):
        payload["FlumeHcommPayloadCopySemanticVersion"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion",
            }
        }
    if mode != "canary":
        payload["FlumeHcommPayloadBuildModeInternalPayload"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadBuildModeInternalPayload",
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
        flume_tool = load_flume_tool(repo)
        static_json_path = (
            repo / "custom_ops" / "hcomm_payload_copy" / "aicpu" /
            KERNEL_JSON
        )
        static_payload = json.loads(static_json_path.read_text(encoding="utf-8"))
        for label, function_name in flume_tool.HCOMM_CUSTOM_OP_FUNCTIONS.items():
            assert flume_tool.JsonDeclaresFunction(
                static_payload, function_name, KERNEL_SO), label
        assert flume_tool.JsonDeclaresFunction(
            static_payload, flume_tool.HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT,
            KERNEL_SO)
        static_payload_json_path = (
            repo / "custom_ops" / "hcomm_payload_copy" / "aicpu" /
            "libflume_hcomm_payload_aicpu_kernel_payload.json"
        )
        assert static_payload_json_path.read_text(
            encoding="utf-8") == static_json_path.read_text(encoding="utf-8")
        static_canary_json_path = (
            repo / "custom_ops" / "hcomm_payload_copy" / "aicpu" /
            "libflume_hcomm_payload_aicpu_kernel_canary.json"
        )
        static_canary = json.loads(static_canary_json_path.read_text(
            encoding="utf-8"))
        for label in ("canary_direct_aclrt", "payload_direct_aclrt",
                      "payload_abi_v2", "payload_semantic"):
            assert flume_tool.JsonDeclaresFunction(
                static_canary, flume_tool.HCOMM_CUSTOM_OP_FUNCTIONS[label],
                KERNEL_SO), label
        assert not flume_tool.JsonDeclaresFunction(
            static_canary,
            flume_tool.HCOMM_CUSTOM_OP_FUNCTIONS["build_mode_internal"],
            KERNEL_SO)

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
        assert "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=present" in canary.stdout
        assert "function.build_mode_internal.FlumeHcommPayloadBuildModeInternalPayload=missing" in canary.stdout
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

        stale_v2_json, stale_v2_tar = write_package(tmp, mode="stale_v2")
        stale_v2 = run_preflight(repo, stale_v2_json, stale_v2_tar)
        if stale_v2.returncode == 0:
            print(stale_v2.stdout)
            print(stale_v2.stderr, file=sys.stderr)
            raise AssertionError("stale ABI v2 package without semantic marker passed")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=present" in stale_v2.stdout
        assert "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=missing" in stale_v2.stdout
        assert "function.build_mode_internal.FlumeHcommPayloadBuildModeInternalPayload=present" in stale_v2.stdout
        assert "function_so.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=missing" in stale_v2.stdout
        assert "reason=payload kernel package is missing the payload semantic marker" in stale_v2.stdout

        v2_json, v2_tar = write_package(tmp, mode="v2")
        v2 = run_preflight(repo, v2_json, v2_tar)
        if v2.returncode != 0:
            print(v2.stdout)
            print(v2.stderr, file=sys.stderr)
            raise AssertionError("ABI v2 package did not pass")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=present" in v2.stdout
        assert "function_so.payload_abi_version_v2.FlumeHcommPayloadCopyAbiVersion2=present" in v2.stdout
        assert "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=present" in v2.stdout
        assert "function_so.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=present" in v2.stdout
        assert "function.build_mode_internal.FlumeHcommPayloadBuildModeInternalPayload=present" in v2.stdout
        assert "status=PASS" in v2.stdout

        installed_json = (
            tmp / "runtime" / "opp" / "vendors" / "flume" / "aicpu" /
            "config" / KERNEL_JSON
        )
        installed_tar = installed_json.parents[1] / "kernel" / AICPU_TAR
        installed_json.parent.mkdir(parents=True)
        installed_tar.parent.mkdir(parents=True)
        installed_json.write_text(v2_json.read_text(encoding="utf-8"),
                                  encoding="utf-8")
        installed_tar.write_bytes(v2_tar.read_bytes())
        ok, message = flume_tool.ValidateRuntimeCustomOpJson(
            SimpleNamespace(custom_op_json=str(installed_json)))
        assert ok, message
        found_json, found_tar = flume_tool.FindInstalledCustomOpRuntimeArtifacts(
            "flume", [str(tmp / "runtime")])
        assert found_json == installed_json
        assert found_tar == installed_tar
        next_steps = flume_tool.WriteCustomOpInstallNextSteps(
            tmp, "flume", found_json, found_tar)
        next_steps_text = next_steps.read_text(encoding="utf-8")
        assert str(installed_json) in next_steps_text
        assert "hcomm-payload-strict-positive" in next_steps_text
        build_steps = flume_tool.WritePayloadPackageBuildNextSteps(
            tmp, SimpleNamespace(hccl_source_root=str(repo / "refer" /
                                                      "cann-src" / "hccl")))
        build_steps_text = build_steps.read_text(encoding="utf-8")
        assert "--custom-op-build-mode payload" in build_steps_text
        assert "--install-custom-op-package" in build_steps_text
        assert "hcomm-custom-op-build" in build_steps_text
        inferred_tar_preflight = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--custom-op-json={installed_json}",
                "--require-hcomm-payload-kernel",
                "hcomm-custom-op-package",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if inferred_tar_preflight.returncode != 0:
            print(inferred_tar_preflight.stdout)
            print(inferred_tar_preflight.stderr, file=sys.stderr)
            raise AssertionError("installed JSON did not infer matching AICPU tar")
        assert f"aicpu_tar_path={installed_tar}" in inferred_tar_preflight.stdout
        assert "status=PASS" in inferred_tar_preflight.stdout

        ok, message = flume_tool.ValidateRuntimeCustomOpJson(
            SimpleNamespace(custom_op_json=str(v2_json)))
        assert not ok
        assert (
            "strict-positive runtime launches use "
            "aclrtBinaryLoadFromFile(JSON)"
        ) in message

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
