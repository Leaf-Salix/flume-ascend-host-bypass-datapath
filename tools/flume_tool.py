#!/usr/bin/env python3
"""Convenience runner for Flume local and Ascend probes."""

from __future__ import annotations

import argparse
import copy
import datetime as _dt
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUILD_JOBS = min(os.cpu_count() or 4, 32)
HCOMM_CUSTOM_OP_JSON = "libflume_hcomm_payload_aicpu_kernel.json"
HCOMM_CUSTOM_OP_TAR = "aicpu_flume_hcomm_payload.tar.gz"
HCOMM_CUSTOM_OP_KERNEL_SO = "libflume_hcomm_payload_aicpu_kernel.so"
HCOMM_CUSTOM_OP_FUNCTIONS = {
    "notify_hccl_launch": "FlumeHcommNotifyOnlyAicpuKernel",
    "notify_direct_aclrt": "FlumeHcommNotifyOnlyDirectAclrtKernel",
    "canary_direct_aclrt": "FlumeHcommCanaryDirectAclrtKernel",
    "payload_direct_aclrt": "FlumeHcommPayloadCopyDirectAclrtKernelV4",
    "payload_direct_aclrt_v2": "FlumeHcommPayloadCopyDirectAclrtKernelV2",
    "payload_direct_aclrt_v3": "FlumeHcommPayloadCopyDirectAclrtKernelV3",
    "payload_abi_v2": "FlumeHcommPayloadCopyAbiVersion2",
    "payload_abi_v3": "FlumeHcommPayloadCopyAbiVersion3",
    "payload_abi_v4": "FlumeHcommPayloadCopyAbiVersion4",
    "payload_semantic": "FlumeHcommPayloadCopySemanticVersion",
    "payload_semantic_v5": "FlumeHcommPayloadCopySemanticVersion5",
    "payload_semantic_v6": "FlumeHcommPayloadCopySemanticVersion6",
    "payload_semantic_v7": "FlumeHcommPayloadCopySemanticVersion7",
    "payload_semantic_v8": "FlumeHcommPayloadCopySemanticVersion8",
    "payload_requires_comm_acquire": "FlumeHcommPayloadCopyRequiresCommAcquire",
    "payload_status_schema": "FlumeHcommPayloadStatusSchemaVersion",
    "payload_status_word_count": "FlumeHcommPayloadStatusWordCount",
    "build_mode_internal": "FlumeHcommPayloadBuildModeInternalPayload",
}
HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT = "FlumeHcommPayloadCopyDirectAclrtKernel"
HCOMM_PAYLOAD_BUILD_MODE_CANARY_ONLY = "FlumeHcommPayloadBuildModeCanaryOnly"
HCOMM_PAYLOAD_BUILD_MODE_INTERNAL = "FlumeHcommPayloadBuildModeInternalPayload"
HCOMM_PAYLOAD_COPY_ABI_VERSION = "FlumeHcommPayloadCopyAbiVersion"
HCOMM_PAYLOAD_COPY_ABI_VERSION_V2 = "FlumeHcommPayloadCopyAbiVersion2"
HCOMM_PAYLOAD_COPY_ABI_VERSION_V3 = "FlumeHcommPayloadCopyAbiVersion3"
HCOMM_PAYLOAD_COPY_ABI_VERSION_V4 = "FlumeHcommPayloadCopyAbiVersion4"
HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION = "FlumeHcommPayloadCopySemanticVersion"
HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5 = "FlumeHcommPayloadCopySemanticVersion5"
HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6 = "FlumeHcommPayloadCopySemanticVersion6"
HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7 = "FlumeHcommPayloadCopySemanticVersion7"
HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8 = "FlumeHcommPayloadCopySemanticVersion8"
HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE = "FlumeHcommPayloadCopyRequiresCommAcquire"
HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION = "FlumeHcommPayloadStatusSchemaVersion"
HCOMM_PAYLOAD_STATUS_WORD_COUNT = "FlumeHcommPayloadStatusWordCount"
HCOMM_PAYLOAD_PRIMITIVE_SYMBOLS = (
    "HcommAcquireComm",
    "HcommReleaseComm",
    "HcommBatchModeStart",
    "HcommBatchModeEnd",
    "HcommLocalCopyOnThread",
    "HcommReadOnThread",
    "HcommChannelNotifyRecordOnThread",
    "HcommChannelNotifyWaitOnThread",
    "HcommChannelFenceOnThread",
    "HcommThreadNotifyRecordOnThread",
    "HcommThreadNotifyWaitOnThread",
)
HCOMM_CUSTOM_OP_NAME = "hcomm_payload"
HCOMM_CUSTOM_OP_PATH = REPO_ROOT / "custom_ops" / "hcomm_payload_copy"


def ResolveHcclInitMode(args: argparse.Namespace) -> str:
    init_mode = args.hccl_init_mode
    if init_mode == "auto":
        init_mode = "root-info" if args.hccl_devices or args.run_a3_symmetric_smoke else "all"
    if init_mode == "init-all":
        init_mode = "all"
    return init_mode


def Checksum32(data: bytes) -> int:
    checksum = 2166136261
    for item in data:
        checksum ^= item
        checksum = (checksum * 16777619) & 0xFFFFFFFF
    return checksum


def UpdateChecksum32(checksum: int, data: bytes) -> int:
    for item in data:
        checksum ^= item
        checksum = (checksum * 16777619) & 0xFFFFFFFF
    return checksum


def AscendHomeCandidates(extra_roots: Optional[Iterable[str]] = None) -> list[Path]:
    candidates: list[Path] = []
    if extra_roots:
        for value in extra_roots:
            value = value.strip()
            if value:
                candidates.append(Path(value))
    for key in ("ASCEND_HOME_PATH", "ASCEND_CUSTOM_OPP_PATH", "ASCEND_OPP_PATH"):
        value = os.environ.get(key, "").strip()
        if value:
            path = Path(value)
            if path.name == "opp":
                path = path.parent
            candidates.append(path)
    candidates.append(Path("/usr/local/Ascend/cann"))
    seen: set[str] = set()
    unique: list[Path] = []
    for path in candidates:
        if path.name == "opp":
            path = path.parent
        text = str(path)
        if text not in seen:
            seen.add(text)
            unique.append(path)
    return unique


def RuntimeAicpuTarForJson(json_path: Path) -> Optional[Path]:
    parts = json_path.parts
    if len(parts) < 3:
        return None
    if parts[-3:] != ("aicpu", "config", HCOMM_CUSTOM_OP_JSON):
        return None
    return Path(*parts[:-2]) / "kernel" / HCOMM_CUSTOM_OP_TAR


def HcommCustomOpPackageCandidates(
        vendors: list[str], extra_roots: Optional[Iterable[str]] = None,
        explicit_json: str = "", explicit_tar: str = ""
) -> list[tuple[Path, str, Optional[Path], Optional[Path]]]:
    candidates: list[tuple[Path, str, Optional[Path], Optional[Path]]] = []
    if explicit_json or explicit_tar:
        json_path = Path(explicit_json) if explicit_json else None
        tar_path = Path(explicit_tar) if explicit_tar else None
        if tar_path is None and json_path is not None:
            tar_path = RuntimeAicpuTarForJson(json_path)
        candidates.append((
            Path("<explicit>"),
            "explicit",
            json_path,
            tar_path,
        ))
    for root in AscendHomeCandidates(extra_roots):
        for vendor in vendors:
            base = root / "opp" / "vendors" / vendor
            json_path = base / "aicpu" / "config" / HCOMM_CUSTOM_OP_JSON
            tar_path = base / "aicpu" / "kernel" / HCOMM_CUSTOM_OP_TAR
            candidates.append((root, vendor, json_path, tar_path))
    return candidates


def JsonDeclaresFunction(payload: object, function_name: str,
                         kernel_so: str = "") -> bool:
    def kernel_matches(node: object) -> bool:
        if not kernel_so:
            return True
        if not isinstance(node, dict):
            return False
        op_info = node.get("opInfo")
        if isinstance(op_info, dict):
            return op_info.get("kernelSo") == kernel_so
        return node.get("kernelSo") == kernel_so

    if isinstance(payload, dict):
        direct = payload.get(function_name)
        if isinstance(direct, dict):
            op_info = direct.get("opInfo", {})
            if isinstance(op_info, dict) and (
                    op_info.get("functionName") == function_name and
                    kernel_matches(direct)):
                return True
        if payload.get("functionName") == function_name and kernel_matches(payload):
            return True
        return any(JsonDeclaresFunction(value, function_name, kernel_so)
                   for value in payload.values())
    if isinstance(payload, list):
        return any(JsonDeclaresFunction(item, function_name, kernel_so)
                   for item in payload)
    return False


def InspectAicpuTar(tar_path: Optional[Path]) -> tuple[str, str, int, str]:
    if tar_path is None or not tar_path.exists():
        return ("missing", "missing", 0, "")
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            names = tar.getnames()
    except (OSError, tarfile.TarError) as exc:
        return ("unreadable", "unknown", 0, str(exc))
    has_kernel_so = any(Path(name).name == HCOMM_CUSTOM_OP_KERNEL_SO
                        for name in names)
    return ("present", "present" if has_kernel_so else "missing",
            len(names), "")


def _RunSymbolTool(path: Path) -> tuple[str, str]:
    commands: list[list[str]] = []
    if shutil.which("readelf"):
        commands.append(["readelf", "-Ws", str(path)])
    if shutil.which("nm"):
        commands.append(["nm", "-D", str(path)])
        commands.append(["nm", "-g", str(path)])
    if not commands:
        return ("unknown", "no readelf or nm available")
    errors: list[str] = []
    for command in commands:
        try:
            result = subprocess.run(
                command, text=True, capture_output=True, timeout=20,
                check=False)
        except (OSError, subprocess.SubprocessError) as exc:
            errors.append(f"{command[0]}: {exc}")
            continue
        if result.returncode == 0:
            return ("present", result.stdout + "\n" + result.stderr)
        errors.append(f"{' '.join(command)} rc={result.returncode}: "
                      f"{result.stderr.strip()}")
    return ("unreadable", "; ".join(errors))


def InspectAicpuTarSymbols(
        tar_path: Optional[Path], function_names: Iterable[str]
) -> tuple[str, dict[str, bool], str]:
    if tar_path is None or not tar_path.exists():
        return ("not-checked", {}, "tar missing")
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            member = next((item for item in tar.getmembers()
                           if Path(item.name).name == HCOMM_CUSTOM_OP_KERNEL_SO),
                          None)
            if member is None:
                return ("not-checked", {}, "kernel so missing")
            extracted = tar.extractfile(member)
            if extracted is None:
                return ("unreadable", {}, "kernel so member is not a file")
            with tempfile.TemporaryDirectory(prefix="flume-aicpu-symbols-") as tmp:
                so_path = Path(tmp) / HCOMM_CUSTOM_OP_KERNEL_SO
                so_path.write_bytes(extracted.read())
                symbol_state, output = _RunSymbolTool(so_path)
    except (OSError, tarfile.TarError) as exc:
        return ("unreadable", {}, str(exc))
    if symbol_state != "present":
        return (symbol_state, {}, output)
    symbols = {name: bool(re.search(rf"(^|\s)_?{re.escape(name)}($|\s)",
                                    output))
               for name in function_names}
    return ("present", symbols, "")


def HcommCustomOpPackageCommand(args: argparse.Namespace,
                                require_payload: bool) -> list[str]:
    command = [sys.executable, "tools/flume_tool.py"]
    if args.custom_op_vendor:
        command.append(f"--custom-op-vendor={args.custom_op_vendor}")
    if args.custom_op_root:
        command.append(f"--custom-op-root={args.custom_op_root}")
    if args.custom_op_json:
        command.append(f"--custom-op-json={args.custom_op_json}")
    if args.custom_op_aicpu_tar:
        command.append(f"--custom-op-aicpu-tar={args.custom_op_aicpu_tar}")
    if require_payload:
        command.append("--require-hcomm-payload-kernel")
    command.append("hcomm-custom-op-package")
    return command


def HcommPayloadAutoDirectBuildCommand(args: argparse.Namespace,
                                       runner: Runner,
                                       export_root: Path) -> list[str]:
    build_root = Path(args.build_dir) / "hcomm-payload-auto-direct-build"
    command = [
        sys.executable,
        "tools/flume_tool.py",
        f"--build-dir={build_root}",
        f"--log-root={runner.run_dir / 'auto-direct-build-logs'}",
        f"--jobs={args.jobs}",
        f"--custom-op-vendor={args.custom_op_vendor}",
        "--custom-op-build-mode=payload",
        f"--custom-op-export-root={export_root}",
    ]
    if args.cann_package_root:
        command.append(f"--cann-package-root={args.cann_package_root}")
    command.append("hcomm-custom-op-direct-build")
    return command


def MaybeAutoBuildPayloadPackage(
        runner: Runner,
        args: argparse.Namespace,
        original_package_result: StepResult) -> tuple[argparse.Namespace,
                                                     StepResult]:
    if (original_package_result.returncode == 0 or
            not getattr(args, "auto_build_hcomm_payload_package", False)):
        return args, original_package_result

    export_root = runner.run_dir / "hcomm-payload-auto-runtime"
    build_result = runner.run(
        "hcomm-payload-auto-direct-build",
        HcommPayloadAutoDirectBuildCommand(args, runner, export_root),
        required=True,
        timeout_seconds=args.hccl_smoke_timeout_sec,
    )
    if build_result.returncode != 0:
        return args, original_package_result

    auto_args = copy.copy(args)
    auto_args.custom_op_root = str(export_root)
    auto_args.custom_op_json = ""
    auto_args.custom_op_aicpu_tar = ""
    package_result = runner.run(
        "hcomm-custom-op-package-preflight-autobuilt",
        HcommCustomOpPackageCommand(auto_args, require_payload=True),
        required=True,
        timeout_seconds=args.step_timeout_sec,
    )
    if package_result.returncode != 0:
        return args, package_result
    note = runner.run_dir / "HCOMM_PAYLOAD_AUTO_PACKAGE.txt"
    rerun_command = [
        "python3 tools/flume_tool.py",
        f"  --build-dir {Path(args.build_dir)}",
        "  --hccl-devices <device-a>,<device-b>",
        "  --hccl-host-ifname <host-ifname>",
        "  --hccl-host-ip <host-ip>",
        "  --build-hcomm-custom-op",
        f"  --custom-op-root {export_root}",
        "  --hcomm-require-payload-copy",
        "  --hccl-debug-logs",
        "  hcomm-payload-strict-positive",
    ]
    note.write_text(
        "Flume auto-built an isolated direct ACL HCOMM payload package for "
        "this run.\n"
        f"custom_op_root: {export_root}\n"
        "package_preflight: payload-ready\n"
        "The package was built with hcomm-custom-op-direct-build and exported "
        "under this log directory; no system CANN/OPP installation was "
        "modified. Reuse it with --custom-op-root if the strict smoke needs "
        "to be rerun against the same artifacts.\n"
        "\n"
        "Focused rerun command:\n"
        + " \\\n".join(rerun_command) + "\n"
        "\n"
        "The run is only a true HCOMM payload-copy success when the strict "
        "decision tree reports both ranks passed with fallback=none, "
        "stage3b3e_payload_copy=passed, payload_semantic_v8=present, and "
        "payload_trace_order=passed.\n",
        encoding="utf-8",
    )
    print(f"[ok] payload auto package -> {note}")
    return auto_args, package_result


def PackageRequirementBlocks(package_text: str) -> list[tuple[set[str], str]]:
    blocks: list[tuple[set[str], str]] = []
    current_required: Optional[set[str]] = None
    for raw_line in package_text.splitlines():
        line = raw_line.strip()
        if line.startswith("required="):
            required = line.split("=", 1)[1].split(",")
            current_required = {item.strip() for item in required
                                if item.strip()}
            continue
        if line.startswith("status=") and current_required is not None:
            blocks.append((current_required, line.split("=", 1)[1].strip()))
            current_required = None
    return blocks


def PackageTextPayloadReady(package_text: str) -> bool:
    payload_required = {
        "canary_direct_aclrt",
        "payload_direct_aclrt",
        "payload_abi_v4",
        "payload_semantic",
        "payload_semantic_v5",
        "payload_semantic_v6",
        "payload_semantic_v7",
        "payload_semantic_v8",
        "payload_requires_comm_acquire",
        "payload_status_schema",
        "payload_status_word_count",
        "payload_primitive_deps",
        "build_mode_internal",
    }
    return any(status == "PASS" and payload_required.issubset(required_set)
               for required_set, status in PackageRequirementBlocks(
                   package_text))


def PackageTextLooksPayloadRequired(package_text: str) -> bool:
    return any("payload_direct_aclrt" in required_set
               for required_set, _status in PackageRequirementBlocks(
                   package_text))


def PackageTextCanaryReady(package_text: str) -> bool:
    return any(
        status == "PASS" and "canary_direct_aclrt" in required_set and
        "payload_direct_aclrt" not in required_set
        for required_set, status in PackageRequirementBlocks(package_text))


def PackageTextReason(package_text: str) -> str:
    match = re.search(r"^reason=(.+)$", package_text, re.MULTILINE)
    reason = match.group(1).strip() if match else "missing"
    if reason.endswith("missing or incomplete"):
        if re.search(r"^aicpu_tar=missing$", package_text, re.MULTILINE):
            return "custom-op AICPU tar missing"
        if re.search(r"^aicpu_tar_readable=(missing|unreadable)$",
                     package_text, re.MULTILINE):
            return "custom-op AICPU tar unreadable"
        if re.search(
                rf"^aicpu_tar_so\.{re.escape(HCOMM_CUSTOM_OP_KERNEL_SO)}=missing$",
                package_text, re.MULTILINE):
            return "custom-op AICPU tar missing kernel SO"
        if re.search(
                r"^aicpu_tar_so_symbols=(missing|unreadable|not-checked)$",
                package_text, re.MULTILINE):
            return "custom-op AICPU tar symbols unavailable"
    return reason


def PackageTextNextAction(package_text: str) -> str:
    reason = PackageTextReason(package_text)
    if "stale legacy entrypoint" in reason:
        return ("rebuild/reinstall the Stage 3B.3E payload package with the "
                "current V4 payload entrypoint")
    if "canary-only" in reason:
        return ("rebuild/reinstall custom-op package in payload mode; "
                "installed package is canary/stub-only")
    if "ABI version marker" in reason:
        return ("rebuild/reinstall payload custom-op package with current "
                "Flume V4 ABI headers")
    if "semantic marker" in reason:
        return ("rebuild/reinstall the Stage 3B.3E payload custom-op package "
                "from current Flume; installed package has stale semantics")
    if "comm-acquire marker" in reason:
        return ("rebuild/reinstall the Stage 3B.3E payload custom-op package "
                "from current Flume; installed package predates HCOMM comm "
                "acquire handoff")
    if "primitive dependencies" in reason:
        return ("rebuild/reinstall the Stage 3B.3E payload custom-op package "
                "from current Flume; installed package does not reference "
                "the required HCOMM primitive APIs")
    if "status schema marker" in reason:
        return ("rebuild/reinstall the Stage 3B.3E payload custom-op package "
                "from current Flume; installed package predates the current "
                "payload status schema")
    if "AICPU tar" in reason:
        return ("rebuild/export the custom-op runtime package so the JSON and "
                "matching AICPU tar are both present in the runtime layout")
    if "no Flume HCOMM custom-op JSON found" in reason:
        return "install the Stage 3B.3E primitive payload custom-op package"
    if "missing or incomplete" in reason:
        return "rebuild/reinstall the Stage 3B.3E primitive payload custom-op package"
    return "inspect hcomm-custom-op-package-preflight failure"


def ValidateRuntimeCustomOpJson(args: argparse.Namespace) -> tuple[bool, str]:
    if not args.custom_op_json:
        return (True, "")
    json_path = Path(args.custom_op_json).expanduser()
    runtime_tar = RuntimeAicpuTarForJson(json_path)
    if runtime_tar is not None and runtime_tar.exists():
        return (True, "")
    custom_op_aicpu_tar = getattr(args, "custom_op_aicpu_tar", "")
    if custom_op_aicpu_tar:
        explicit_tar = Path(custom_op_aicpu_tar).expanduser()
        if explicit_tar.exists():
            return (True, "")
    expected = (str(runtime_tar) if runtime_tar is not None
                else "<custom-op-root>/opp/vendors/<vendor>/aicpu/kernel/" +
                HCOMM_CUSTOM_OP_TAR)
    message = (
        "explicit --custom-op-json is not an installed/runtime-loadable "
        "Flume custom-op JSON and no matching AICPU tar was found. "
        "Strict-positive runtime launches use aclrtBinaryLoadFromFile(JSON), "
        "while --custom-op-aicpu-tar supplies the package readiness tar for "
        "loose build artifacts. Install/export the package, pass both "
        "--custom-op-json and --custom-op-aicpu-tar, or point --custom-op-json "
        f"at the installed aicpu/config JSON with matching AICPU tar at {expected}")
    return (False, message)


def HcclHeaderSyntaxCommand() -> Optional[list[str]]:
    cxx = os.environ.get("CXX", "c++")
    if shutil.which(cxx) is None:
        return None
    include_roots = [
        REPO_ROOT / "include",
        REPO_ROOT / "src",
        REPO_ROOT / "custom_ops" / "hcomm_payload_copy" / "include",
        REPO_ROOT / "refer" / "cann-src" / "hccl" / "include",
        REPO_ROOT / "refer" / "cann-src" / "hcomm" / "include",
        REPO_ROOT / "refer" / "cann-src" / "runtime" / "include" / "external",
        REPO_ROOT / "refer" / "cann-src" / "hcomm" / "test" / "stub" /
        "depends" / "include",
    ]
    if not all(path.exists() for path in include_roots[3:]):
        return None
    macros = [
        "FLUME_ENABLE_HCCL=1",
        "FLUME_HAVE_HCCL_ROOT_INFO=1",
        "FLUME_HAVE_HCCL_ROOT_INFO_CONFIG=1",
        "FLUME_HAVE_HCCL_COMM_INIT_ALL=1",
        "FLUME_HAVE_HCCL_P2P=1",
        "FLUME_HAVE_HCCL_COMM_NAME=1",
        "FLUME_HAVE_HCOMM_CHANNEL_RES=1",
        "FLUME_HAVE_HCOMM_PRIMITIVES=1",
        "FLUME_HAVE_ACLRT_CUSTOM_OP_LAUNCH=1",
        "FLUME_BUILD_HCOMM_CUSTOM_OP=1",
    ]
    command = [cxx, "-std=c++17", "-fsyntax-only"]
    command.extend(f"-D{macro}" for macro in macros)
    command.extend(f"-I{path}" for path in include_roots)
    command.append(str(REPO_ROOT / "apps" / "flume-hccl-collective-smoke.cc"))
    return command


def HcommPayloadKernelSyntaxCommand() -> Optional[list[str]]:
    cxx = os.environ.get("CXX", "c++")
    if shutil.which(cxx) is None:
        return None
    include_roots = [
        REPO_ROOT / "custom_ops" / "hcomm_payload_copy" / "include",
        REPO_ROOT / "refer" / "cann-src" / "hcomm" / "include",
        REPO_ROOT / "refer" / "cann-src" / "runtime" / "include" / "external",
        REPO_ROOT / "refer" / "cann-src" / "hcomm" / "test" / "stub" /
        "depends" / "include",
    ]
    if not all(path.exists() for path in include_roots[1:]):
        return None
    command = [cxx, "-std=c++17", "-fsyntax-only"]
    command.extend(f"-I{path}" for path in include_roots)
    command.append(str(REPO_ROOT / "custom_ops" / "hcomm_payload_copy" /
                       "aicpu" / "payload_copy_kernel.cc"))
    return command


def ResolveHcclSourceRoot(args: argparse.Namespace) -> Path:
    if args.hccl_source_root:
        return Path(args.hccl_source_root).expanduser().resolve()
    env_root = os.environ.get("FLUME_HCCL_SOURCE_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return (REPO_ROOT / "refer" / "cann-src" / "hccl").resolve()


def CannToolkitCustomOpTemplateBuildScripts(
        extra_roots: Optional[Iterable[str]] = None) -> list[Path]:
    scripts: list[Path] = []
    roots = AscendHomeCandidates(extra_roots)
    roots.extend(sorted(Path("/usr/local/Ascend").glob("cann-*")))
    for root in roots:
        for rel in (
                "tools/new_op_project_template/custom_op/build.sh",
                "tools/op_project_templates/op_project_tmpl/build.sh",
                "tools/op_project_templates/ascendc/aclnn/build.sh",
                "tools/op_project_templates/ascendc/customize/build.sh"):
            candidate = root / rel
            if candidate.exists():
                scripts.append(candidate)
    seen: set[str] = set()
    unique: list[Path] = []
    for script in scripts:
        text = str(script)
        if text not in seen:
            seen.add(text)
            unique.append(script)
    return unique[:16]


def _LibraryExists(lib_dir: Path, name: str) -> bool:
    return any((lib_dir / f"lib{name}{suffix}").exists()
               for suffix in (".so", ".a", ".dylib"))


def HcommPrimitivesHeaderCandidates(cann_binary_root: Path) -> list[Path]:
    include = cann_binary_root / "include"
    return [
        include / "hccl" / "hcomm_primitives.h",
        include / "hcomm" / "hcomm_primitives.h",
        include / "hcomm_primitives.h",
    ]


def FindHcommPrimitivesHeader(cann_binary_root: Path) -> Optional[Path]:
    for candidate in HcommPrimitivesHeaderCandidates(cann_binary_root):
        if candidate.exists():
            return candidate
    return None


def ResolveCannRootPair(extra_root: str = "") -> Optional[tuple[Path, Path]]:
    roots = AscendHomeCandidates([extra_root])
    roots.extend(sorted(Path("/usr/local/Ascend").glob("cann-*")))
    roots.extend(sorted(Path("/usr/local/Ascend").glob("ascend-toolkit*")))
    seen: set[str] = set()
    for root in roots:
        candidates = [root / "aarch64-linux", root / "x86_64-linux", root]
        for candidate in candidates:
            text = str(candidate)
            if text in seen:
                continue
            seen.add(text)
            include = candidate / "include"
            lib64 = candidate / "lib64"
            if include.exists() and lib64.exists():
                package_root = (
                    candidate.parent
                    if candidate.name in ("aarch64-linux", "x86_64-linux")
                    else candidate)
                return (package_root, candidate)
    return None


def ResolveCannBinaryRoot(extra_root: str = "") -> Optional[Path]:
    roots = ResolveCannRootPair(extra_root)
    return roots[1] if roots is not None else None


def CannRuntimeEnvUpdates(args: argparse.Namespace) -> dict[str, str]:
    roots = ResolveCannRootPair(getattr(args, "cann_package_root", ""))
    if roots is None:
        return {}
    cann_package_root, cann_binary_root = roots
    updates = {"ASCEND_HOME_PATH": str(cann_package_root)}
    lib_paths = []
    for path in (
            cann_binary_root / "lib64",
            Path("/usr/local/Ascend/driver/lib64"),
            Path("/usr/local/Ascend/driver/lib64/common")):
        if path.exists():
            lib_paths.append(str(path))
    if lib_paths:
        existing = os.environ.get("LD_LIBRARY_PATH", "")
        updates["LD_LIBRARY_PATH"] = (
            ":".join(lib_paths + ([existing] if existing else [])))
    return updates


@dataclass
class StepResult:
    name: str
    command: list[str]
    returncode: int
    seconds: float
    log_path: Path
    required: bool


@dataclass
class CommandSpec:
    name: str
    command: list[str]
    required: bool
    env_updates: dict[str, str]


class Runner:
    def __init__(self, log_root: Path) -> None:
        stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.run_dir = log_root / f"flume-check-{stamp}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.results: list[StepResult] = []
        self._step_index = 0

    def run(self, name: str, command: Iterable[str], *, required: bool = True,
            timeout_seconds: int = 0,
            env_updates: Optional[dict[str, str]] = None) -> StepResult:
        self._step_index += 1
        command_list = list(command)
        env = os.environ.copy()
        if env_updates:
            env.update(env_updates)
        log_path = self.run_dir / f"{self._step_index:02d}-{name}.log"
        start = time.monotonic()
        output = ""
        returncode = 0
        try:
            proc = subprocess.run(
                command_list,
                cwd=REPO_ROOT,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                env=env,
                timeout=timeout_seconds if timeout_seconds > 0 else None,
            )
            output = proc.stdout or ""
            returncode = proc.returncode
        except subprocess.TimeoutExpired as exc:
            returncode = 124
            if isinstance(exc.stdout, bytes):
                output = exc.stdout.decode("utf-8", errors="replace")
            else:
                output = exc.stdout or ""
            output += f"\nTIMEOUT after {timeout_seconds} seconds\n"
        except OSError as exc:
            returncode = 127
            output = f"failed to start command: {exc}\n"
        seconds = time.monotonic() - start
        with log_path.open("w", encoding="utf-8") as f:
            f.write(f"$ {' '.join(command_list)}\n")
            f.write(f"cwd: {REPO_ROOT}\n")
            if env_updates:
                for key, value in sorted(env_updates.items()):
                    f.write(f"env {key}={value}\n")
            if timeout_seconds > 0:
                f.write(f"timeout_seconds: {timeout_seconds}\n")
            f.write(f"returncode: {returncode}\n")
            f.write(f"duration_seconds: {seconds:.3f}\n")
            f.write("\n")
            f.write(output)
        result = StepResult(name, command_list, returncode, seconds, log_path, required)
        self.results.append(result)
        marker = "ok" if result.returncode == 0 else "failed"
        optional = "" if required else " optional"
        print(f"[{marker}] {name}{optional} -> {log_path}")
        return result

    def record_static(self, name: str, lines: Iterable[str], *,
                      returncode: int, required: bool = True) -> StepResult:
        self._step_index += 1
        log_path = self.run_dir / f"{self._step_index:02d}-{name}.log"
        with log_path.open("w", encoding="utf-8") as f:
            f.write("$ internal evidence gate\n")
            f.write(f"cwd: {REPO_ROOT}\n")
            f.write(f"returncode: {returncode}\n")
            f.write("duration_seconds: 0.000\n\n")
            for line in lines:
                f.write(line)
                if not line.endswith("\n"):
                    f.write("\n")
        result = StepResult(name, ["internal", name], returncode, 0.0,
                            log_path, required)
        self.results.append(result)
        marker = "ok" if result.returncode == 0 else "failed"
        optional = "" if required else " optional"
        print(f"[{marker}] {name}{optional} -> {log_path}")
        return result

    def write_env_report(self) -> None:
        report = self.run_dir / "00-environment.txt"
        lines = [
            f"repo: {REPO_ROOT}",
            f"platform: {platform.system()} {platform.release()} {platform.machine()}",
            f"python: {sys.version.split()[0]} ({sys.executable})",
            f"cmake: {shutil.which('cmake') or 'not found'}",
            f"ctest: {shutil.which('ctest') or 'not found'}",
            f"c++: {shutil.which('c++') or 'not found'}",
            f"ASCEND_HOME_PATH: {os.environ.get('ASCEND_HOME_PATH', 'not set')}",
            f"ASCEND_RT_VISIBLE_DEVICES: {os.environ.get('ASCEND_RT_VISIBLE_DEVICES', 'not set')}",
            f"HCCL_IF_IP: {os.environ.get('HCCL_IF_IP', 'not set')}",
            f"HCCL_SOCKET_IFNAME: {os.environ.get('HCCL_SOCKET_IFNAME', 'not set')}",
            f"HCCL_CONNECT_TIMEOUT: {os.environ.get('HCCL_CONNECT_TIMEOUT', 'not set')}",
            f"HCCL_INTRA_PCIE_ENABLE: {os.environ.get('HCCL_INTRA_PCIE_ENABLE', 'not set')}",
            f"HCCL_INTRA_ROCE_ENABLE: {os.environ.get('HCCL_INTRA_ROCE_ENABLE', 'not set')}",
            f"ASCEND_GLOBAL_LOG_LEVEL: {os.environ.get('ASCEND_GLOBAL_LOG_LEVEL', 'not set')}",
            f"ASCEND_SLOG_PRINT_TO_STDOUT: {os.environ.get('ASCEND_SLOG_PRINT_TO_STDOUT', 'not set')}",
            f"LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH', 'not set')}",
            f"DYLD_LIBRARY_PATH: {os.environ.get('DYLD_LIBRARY_PATH', 'not set')}",
        ]
        report.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"[ok] environment -> {report}")

    def write_summary(self) -> int:
        summary = self.run_dir / "summary.md"
        failures = [r for r in self.results if r.required and r.returncode != 0]
        with summary.open("w", encoding="utf-8") as f:
            f.write("# Flume Check Summary\n\n")
            f.write(f"- repo: `{REPO_ROOT}`\n")
            f.write(f"- log dir: `{self.run_dir}`\n")
            f.write(f"- status: {'FAILED' if failures else 'PASSED'}\n\n")
            f.write("| Step | Required | Return | Seconds | Log |\n")
            f.write("| --- | --- | --- | --- | --- |\n")
            for r in self.results:
                rel_log = r.log_path.relative_to(self.run_dir)
                f.write(
                    f"| `{r.name}` | {'yes' if r.required else 'no'} | "
                    f"{r.returncode} | {r.seconds:.2f} | `{rel_log}` |\n"
                )
            if failures:
                f.write("\n## First Required Failure\n\n")
                first = failures[0]
                f.write(f"- step: `{first.name}`\n")
                f.write(f"- command: `{' '.join(first.command)}`\n")
                f.write(f"- log: `{first.log_path}`\n")
        print(f"[ok] summary -> {summary}")
        return 1 if failures else 0


def WriteHcclSmokeDiagnostics(run_dir: Path, source_log: Path) -> Path:
    diag = run_dir / "HCCL_SMOKE_DIAGNOSTICS.txt"
    try:
        text = source_log.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        diag.write_text(f"failed to read smoke log: {exc}\n", encoding="utf-8")
        print(f"[ok] hccl smoke diagnostics -> {diag}")
        return diag

    lines = text.splitlines()
    error_re = re.compile(
        r"(\[ERROR\]|HCCL_E_|EI[0-9]{4}|Hccl.*failed|HCCL.*failed|"
        r"HCCP.*failed|failed,| failed| error)",
        re.IGNORECASE,
    )
    matches: list[tuple[int, str]] = []
    for lineno, line in enumerate(lines, 1):
        if error_re.search(line):
            matches.append((lineno, line))

    signal_specs = [
        (
            "Transport Decision",
            re.compile(
                r"(GetIsUsedRdma|createLink para|isUsedRdma|isInterServer|"
                r"isUsedInterHccsMode|isConnectedWithPcie)",
                re.IGNORECASE,
            ),
        ),
        (
            "RA/VNIC Addressing",
            re.compile(
                r"(Get available Vnic info success|Vnic ip|vnicIp|"
                r"VNIC_TYPE|StartVnic|CreateVnicSocketHandle)",
                re.IGNORECASE,
            ),
        ),
        (
            "P2P Enable",
            re.compile(
                r"(rt enableP2P.*fail|Enable P2P Failed|P2PConnected.*timeout|"
                r"Wait Enable P2P Failed)",
                re.IGNORECASE,
            ),
        ),
        (
            "Socket Establish",
            re.compile(
                r"(connect destRank.*fail|wait socket establish timeout|"
                r"LINK_ERROR_INFO|socket is connecting)",
                re.IGNORECASE,
            ),
        ),
        (
            "CANN Binary Path",
            re.compile(
                r"(LoadOpBinary|open op binary file\[.*\.o\].*failed|"
                r"dynamic_.*\.o)",
                re.IGNORECASE,
            ),
        ),
        (
            "HCOMM Resource Probe",
            re.compile(
                r"(hcomm channel probe|HcclGetHcclBuffer|HcclThreadAcquire|"
                r"HcclThreadExportToCommEngine|HcclChannelAcquire|"
                r"HcclChannelGetHcclBuffer|hcomm custom-op launch smoke|"
                r"hcomm resource descriptor smoke|stage3b1_launch|"
                r"stage3b2_resource_descriptor|descriptor_handoff|"
                r"hcomm notify-only smoke|stage3b2_notify_only|"
                r"stage3b2_kernel_consume|notify_kernel|notify_status_word|"
                r"stage3b3a_kernel_launch|"
                r"stage3b3b_launcher_router|direct_aclrt|custom_op_package|"
                r"payload_package|payload_package_reason|package_aicpu_tar|"
                r"package_json_path|package_aicpu_tar_path|"
                r"aicpu_tar|aicpu_tar_so|"
                r"stage3b3c_direct_aclrt_loader|stage3b3c_descriptor_handoff|"
                r"stage3b3c_direct_aclrt_launch|"
                r"stage3b3d_no_internal_headers|direct_aclrt_canary_candidate|"
                r"stage3b3d_direct_aclrt_canary|"
                r"canary_status_word|canary_observed_token|"
                r"stage3b3e_payload_copy|stage3b3e_direct_aclrt_payload|"
                r"stage3b3e_payload_descriptor_handoff|stage3b3e_payload_sync|"
                r"payload_kernel|payload_status_word|hcomm_timeout_sec|"
                r"aclrt_custom_op_launch|HcclAicpuKernelLaunch)",
                re.IGNORECASE,
            ),
        ),
        (
            "HCOMM Payload Probe",
            re.compile(
                r"(hcomm payload smoke|HCOMM payload|HcommLocalCopyOnThread|"
                r"HcommReadOnThread|HCOMM primitive|payload scheduler|"
                r"fallback=hccl-p2p)",
                re.IGNORECASE,
            ),
        ),
        (
            "Storage HBM Smoke",
            re.compile(
                r"(storage HBM smoke|storage_hbm=hccl-p2p-staging|"
                r"storage_hbm=hcomm-payload-staging|"
                r"storage-smoke-input)",
                re.IGNORECASE,
            ),
        ),
    ]
    signal_matches: list[tuple[str, list[tuple[int, str]]]] = []
    for title, pattern in signal_specs:
        hits: list[tuple[int, str]] = []
        for lineno, line in enumerate(lines, 1):
            if pattern.search(line):
                hits.append((lineno, line))
        signal_matches.append((title, hits))

    joined = "\n".join(lines)
    hints: list[str] = []
    if (re.search(r"env HCCL_INTRA_ROCE_ENABLE=1", joined) and
            re.search(r"isUsedRdma\[0\]", joined)):
        hints.append(
            "HCCL reported isUsedRdma[0] even though RoCE env was requested. "
            "On single-node rank-table runs, HCCL may still select "
            "HCCS/VNIC+P2P by topology; use a multi-node rank table to test "
            "RoCE RDMA explicitly."
        )
    if re.search(r"Get available Vnic info success.*Vnic ip\[192\.", joined,
                 re.IGNORECASE):
        hints.append(
            "192.x.x.x addresses came from the RA socket VNIC query. They are "
            "not host_nic_ip or rank-table device_ip values."
        )
    if re.search(r"(rt enableP2P.*fail|P2PConnected.*timeout|Wait Enable P2P Failed)",
                 joined, re.IGNORECASE):
        hints.append(
            "P2P memory-share enable timed out or failed. If npu-smi HCCS "
            "health is OK, this points at driver/firmware P2P policy or "
            "runtime state rather than a Flume API error."
        )
    if (re.search(r"(connect destRank.*fail|wait socket establish timeout)",
                  joined, re.IGNORECASE) and
            re.search(r"192\.\d+\.\d+\.\d+", joined)):
        hints.append(
            "Socket establishment timed out on a 192.x VNIC endpoint. This is "
            "the HCCL device VNIC/BUS path, not the host TCP control-plane "
            "path controlled by HCCL_SOCKET_IFNAME."
        )
    if re.search(r"(LoadOpBinary|open op binary file\[.*\.o\].*failed)",
                 joined, re.IGNORECASE):
        hints.append(
            "HCCL failed while loading its operator binary. Check that "
            "ASCEND_HOME_PATH points at the CANN toolkit root that contains "
            "lib64/*.o, not an architecture subdirectory or stale symlink."
        )
    if re.search(r"payload scheduler.*not implemented|hcomm payload smoke unsupported",
                 joined, re.IGNORECASE):
        hints.append(
            "HCOMM payload smoke reached the Stage 2.5 readiness probe but "
            "Flume has not implemented the custom-op/AICPU payload scheduler "
            "yet. This is an expected unsupported/fallback result for the "
            "current skeleton, not a CANN environment failure."
        )
    if re.search(r"storage_hbm=hccl-p2p-staging", joined, re.IGNORECASE):
        hints.append(
            "Stage 3A storage smoke transferred a file slice through the "
            "fallback path file->host->proxy HBM->HCCL P2P->compute HBM. "
            "This validates storage integration plumbing, not full direct "
            "storage DMA into HBM."
        )
    if re.search(r"storage_hbm=hcomm-payload-staging", joined, re.IGNORECASE):
        hints.append(
            "Storage smoke transferred a file slice through the HCOMM payload "
            "scheduler path file->host->proxy HBM->HCOMM payload->compute HBM. "
            "This validates Stage 3B integration with the storage proxy; it is "
            "still not full storage DMA into HBM."
        )

    with diag.open("w", encoding="utf-8") as f:
        f.write(f"source_log: {source_log}\n")
        f.write(f"total_lines: {len(lines)}\n")
        f.write(f"matched_error_lines: {len(matches)}\n\n")
        f.write("## Command Header\n\n")
        for line in lines[:16]:
            f.write(line + "\n")
        f.write("\n## Decision Hints\n\n")
        if hints:
            for hint in hints:
                f.write(f"- {hint}\n")
        else:
            f.write("- No targeted HCCL decision hint matched.\n")
        f.write("\n## Targeted HCCL Signals\n\n")
        for title, hits in signal_matches:
            f.write(f"### {title}\n\n")
            if hits:
                for lineno, line in hits[:80]:
                    f.write(f"{lineno}: {line}\n")
                if len(hits) > 80:
                    f.write(f"... {len(hits) - 80} more matching lines omitted.\n")
            else:
                f.write("No matching lines.\n")
            f.write("\n")
        f.write("\n## First Error-like Lines\n\n")
        if matches:
            for lineno, line in matches[:160]:
                f.write(f"{lineno}: {line}\n")
        else:
            f.write("No error-like lines matched. Check the tail section below.\n")
        f.write("\n## Final Log Lines\n\n")
        for line in lines[-120:]:
            f.write(line + "\n")
    print(f"[ok] hccl smoke diagnostics -> {diag}")
    return diag


def CollectHcclSmokeSetupNotes(args: argparse.Namespace,
                               init_mode: str) -> list[str]:
    notes: list[str] = []
    if init_mode == "rank-table":
        notes.append(
            "Rank-table smoke is kept as an experimental diagnostic path. "
            "On current single-node HCCS_SW testing, HcclCommInitClusterInfo "
            "can enter HCCL VNIC/P2P memory-share bring-up and has not passed "
            "hardware validation yet. Prefer auto/root-info for first fabric "
            "bring-up."
        )
    if init_mode == "rank-table" and args.hccl_link_mode == "roce":
        prefix = ("Generated rank-table mode is single-server."
                  if not args.hccl_rank_table
                  else "Rank-table mode is server-internal for this smoke.")
        notes.append(
            f"{prefix} On single-node HCCL runs, HCCL may select "
            "HCCS/VNIC+P2P by NPU topology "
            "even when --hccl-link-mode roce sets "
            "HCCL_INTRA_ROCE_ENABLE=1; logs may still show isUsedRdma[0]. "
            "Use a multi-node rank table to validate RoCE RDMA explicitly."
        )
    if args.run_storage_hbm_smoke:
        if args.hcomm_require_payload_copy:
            notes.append(
                "Storage HBM smoke is running through the HCOMM payload "
                "scheduler path. Rank0 still reads a local file slice through "
                "the host into proxy-rank HBM, then sends it to rank1 compute "
                "HBM with Flume HCOMM payload copy. This validates Stage 3B.4 "
                "storage-proxy wiring, not full storage-direct DMA."
            )
        else:
            notes.append(
                "Storage HBM smoke is Stage 3A fallback validation. Rank0 "
                "reads a file slice from local storage, copies it into "
                "proxy-rank HBM, then sends it to rank1 HBM with "
                "HcclSend/HcclRecv. This is not full storage direct; it keeps "
                "the API/test surface ready for a future HCOMM/RDMA backend."
            )
    return notes


def WriteHcclSmokeSetupNotes(run_dir: Path, notes: list[str]) -> Optional[Path]:
    if not notes:
        return None
    path = run_dir / "HCCL_SMOKE_SETUP_NOTES.txt"
    path.write_text("\n".join(f"WARNING: {note}" for note in notes) + "\n",
                    encoding="utf-8")
    for note in notes:
        print(f"[warning] {note}")
    print(f"[ok] hccl setup notes -> {path}")
    return path


def ParseDeviceList(spec: str) -> list[str]:
    devices = [item.strip() for item in spec.split(",") if item.strip()]
    for device in devices:
        if not device.isdigit():
            raise ValueError(f"invalid device id: {device}")
    return devices


def GenerateStorageSmokeFile(path: Path, total_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    chunk_size = 1024 * 1024
    written = 0
    with path.open("wb") as f:
        while written < total_bytes:
            n = min(chunk_size, total_bytes - written)
            data = bytes(((written + i) * 131 + 17) & 0xFF for i in range(n))
            f.write(data)
            written += n


def FileSliceChecksum(path: Path, offset: int, size: int) -> int:
    checksum = 2166136261
    remaining = size
    with path.open("rb") as f:
        f.seek(offset)
        while remaining > 0:
            data = f.read(min(1024 * 1024, remaining))
            if not data:
                raise RuntimeError(
                    f"short storage smoke file read: {path} offset={offset} "
                    f"bytes={size}"
                )
            checksum = UpdateChecksum32(checksum, data)
            remaining -= len(data)
    return checksum


def ResolveStorageSmokeInput(args: argparse.Namespace,
                             run_dir: Path) -> tuple[Path, int]:
    if args.storage_smoke_bytes <= 0:
        raise RuntimeError("--storage-smoke-bytes must be greater than 0")
    if args.storage_smoke_offset < 0:
        raise RuntimeError("--storage-smoke-offset must be >= 0")
    total_bytes = args.storage_smoke_offset + args.storage_smoke_bytes
    if args.storage_smoke_file:
        path = Path(args.storage_smoke_file)
        if not path.exists():
            raise RuntimeError(f"--storage-smoke-file does not exist: {path}")
    else:
        path = run_dir / "storage-smoke-input.bin"
        GenerateStorageSmokeFile(path, total_bytes)
    try:
        stat_size = path.stat().st_size
    except OSError as exc:
        raise RuntimeError(f"failed to stat storage smoke file {path}: {exc}") from exc
    if stat_size < total_bytes:
        raise RuntimeError(
            f"storage smoke file is too small: {path} size={stat_size} "
            f"needs offset+bytes={total_bytes}"
        )
    checksum = FileSliceChecksum(path, args.storage_smoke_offset,
                                 args.storage_smoke_bytes)
    summary = run_dir / "storage-smoke-input-summary.txt"
    summary.write_text(
        "\n".join([
            f"path: {path}",
            f"offset: {args.storage_smoke_offset}",
            f"bytes: {args.storage_smoke_bytes}",
            f"checksum32: {checksum}",
            "mode: generated" if not args.storage_smoke_file else "mode: user-file",
        ]) + "\n",
        encoding="utf-8",
    )
    return path, checksum


def ParseDeviceIpMap(spec: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if not spec:
        return mapping
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"invalid device IP mapping: {item}")
        device, ip = [part.strip() for part in item.split("=", 1)]
        if not device.isdigit() or not ip:
            raise ValueError(f"invalid device IP mapping: {item}")
        mapping[device] = ip
    return mapping


def LoadHccnDeviceIps(path: Path = Path("/etc/hccn.conf")) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        if path == Path("/etc/hccn.conf") and not path.exists():
            return {}
        raise RuntimeError(f"failed to read {path}: {exc}") from exc
    ips: dict[str, str] = {}
    pattern = re.compile(r"^address_(\d+)\s*=\s*(\S+)\s*$")
    for line in text.splitlines():
        match = pattern.match(line.strip())
        if match:
            ips[match.group(1)] = match.group(2)
    return ips


def QueryHccnToolDeviceIp(device: str) -> Optional[str]:
    tool = shutil.which("hccn_tool")
    if tool is None:
        return None
    try:
        proc = subprocess.run(
            [tool, "-i", device, "-ip", "-g"],
            cwd=REPO_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    ip_pattern = re.compile(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)")
    for match in ip_pattern.finditer(proc.stdout or ""):
        ip = match.group(0)
        if ip != "0.0.0.0":
            return ip
    return None


def DiscoverHccnDeviceIps(physical_devices: list[str],
                          manual_device_ips: dict[str, str]) -> dict[str, str]:
    device_ips = LoadHccnDeviceIps()
    device_ips.update(manual_device_ips)
    for device in physical_devices:
        if device not in device_ips:
            ip = QueryHccnToolDeviceIp(device)
            if ip is not None:
                device_ips[device] = ip
    return device_ips


def WriteHcclRankTableV1(run_dir: Path, physical_devices: list[str],
                         manual_device_ips: dict[str, str],
                         include_device_ips: bool,
                         server_id: str,
                         host_ip: str = "") -> Path:
    if not physical_devices:
        raise RuntimeError("--hccl-init-mode rank-table requires --hccl-devices")
    device_ips: dict[str, str] = {}
    if include_device_ips:
        device_ips = DiscoverHccnDeviceIps(physical_devices, manual_device_ips)
        missing = [device for device in physical_devices if device not in device_ips]
        if missing:
            raise RuntimeError(
                "missing HCCN device IP for physical device(s): " + ",".join(missing) +
                "; checked /etc/hccn.conf address_<device> and "
                "`hccn_tool -i <device> -ip -g`. Provide IPs with "
                "--hccl-device-ips, provide an existing rank table with "
                "--hccl-rank-table, or verify HCCN IP configuration on the host."
            )
    devices = []
    for rank, device in enumerate(physical_devices):
        devices.append({
            "device_id": device,
            "device_ip": device_ips.get(device, ""),
            "rank_id": str(rank),
        })
    server = {
        "server_id": server_id,
        "host_nic_ip": "reserve",
        "device": devices,
    }
    if host_ip:
        server["host_ip"] = host_ip
    table = {
        "status": "completed",
        "version": "1.0",
        "server_count": "1",
        "server_list": [server],
    }
    path = run_dir / "hccl-rank-table.json"
    path.write_text(json.dumps(table, indent=2) + "\n", encoding="utf-8")
    summary = run_dir / "hccl-rank-table-summary.txt"
    lines = [
        f"path: {path}",
        "format: v1 single-server",
        f"server_id: {server_id}",
        f"host_ip: {host_ip if host_ip else 'not set'}",
        f"device_ip_mode: {'device' if include_device_ips else 'none'}",
        "rank,physical_device_id,rank_table_device_id,rank_table_device_ip",
    ]
    for rank, device in enumerate(physical_devices):
        lines.append(f"{rank},{device},{device},{device_ips.get(device, '')}")
    prefixes = sorted({
        ".".join(ip.split(".")[:3])
        for ip in device_ips.values()
        if ip.count(".") == 3
    })
    if prefixes:
        lines.append("device_ip_ipv4_prefix24: " + ",".join(prefixes))
    summary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def build_commands(args: argparse.Namespace, enable_hccl: bool,
                   run_dir: Optional[Path] = None) -> list[CommandSpec]:
    build_dir = args.build_dir
    cmake_env_updates = CannRuntimeEnvUpdates(args) if enable_hccl else {}
    configure = [
        "cmake",
        "-S",
        ".",
        "-B",
        build_dir,
        f"-DFLUME_BUILD_TESTS={'ON' if not args.skip_tests else 'OFF'}",
        f"-DFLUME_ENABLE_HCCL={'ON' if enable_hccl else 'OFF'}",
        f"-DFLUME_BUILD_HCOMM_CUSTOM_OP={'ON' if args.build_hcomm_custom_op else 'OFF'}",
    ]
    commands: list[CommandSpec] = [
        CommandSpec("cmake-configure", configure, True, cmake_env_updates),
        CommandSpec("cmake-build",
                    ["cmake", "--build", build_dir, "-j", str(args.jobs)],
                    True, cmake_env_updates),
    ]
    if not args.skip_tests:
        commands.append(CommandSpec(
            "ctest", ["ctest", "--test-dir", build_dir, "--output-on-failure"],
            True, cmake_env_updates))
    sim_demo = str(Path(build_dir) / "flume-sim-demo")
    commands.append(CommandSpec("sim-demo", [sim_demo], True,
                                cmake_env_updates))
    sim_collective_demo = str(Path(build_dir) / "flume-sim-collective-demo")
    commands.append(CommandSpec("sim-collective-demo", [sim_collective_demo],
                                True, cmake_env_updates))
    if enable_hccl and (args.run_hccl_smoke or args.run_a3_symmetric_smoke or
                        args.run_hccl_p2p_smoke or
                        args.run_hcomm_channel_probe or
                        args.run_hcomm_custom_op_launch_smoke or
                        args.run_hcomm_resource_descriptor_smoke or
                        args.run_hcomm_notify_only_smoke or
                        args.run_hcomm_payload_smoke or
                        args.run_storage_hbm_smoke):
        hccl_smoke = str(Path(build_dir) / "flume-hccl-collective-smoke")
        init_mode = ResolveHcclInitMode(args)
        command = [hccl_smoke, f"--count={args.hccl_count}",
                   f"--init={init_mode}"]
        storage_smoke_input: Optional[Path] = None
        storage_smoke_checksum: Optional[int] = None
        if args.run_storage_hbm_smoke:
            if run_dir is None:
                raise RuntimeError("storage HBM smoke requires a log run directory")
            if args.storage_smoke_bytes > args.hccl_count * 4:
                raise RuntimeError(
                    "--storage-smoke-bytes exceeds the per-rank smoke HBM "
                    "buffer; increase --hccl-count or reduce "
                    "--storage-smoke-bytes"
                )
            storage_smoke_input, storage_smoke_checksum = ResolveStorageSmokeInput(
                args, run_dir)
        env_updates: dict[str, str] = CannRuntimeEnvUpdates(args)
        if args.hccl_host_ifname:
            env_updates["HCCL_SOCKET_IFNAME"] = args.hccl_host_ifname
        if args.hccl_host_ip:
            env_updates["HCCL_IF_IP"] = args.hccl_host_ip
        if args.hccl_link_mode == "pcie":
            env_updates["HCCL_INTRA_PCIE_ENABLE"] = "1"
            env_updates["HCCL_INTRA_ROCE_ENABLE"] = "0"
        elif args.hccl_link_mode == "roce":
            env_updates["HCCL_INTRA_PCIE_ENABLE"] = "0"
            env_updates["HCCL_INTRA_ROCE_ENABLE"] = "1"
        if args.hccl_debug_logs:
            env_updates["ASCEND_GLOBAL_LOG_LEVEL"] = "0"
            env_updates["ASCEND_SLOG_PRINT_TO_STDOUT"] = "1"
        if args.custom_op_vendor:
            env_updates["FLUME_HCOMM_CUSTOM_OP_VENDOR"] = args.custom_op_vendor
        if args.custom_op_root:
            env_updates["FLUME_HCOMM_CUSTOM_OP_ROOT"] = args.custom_op_root
        if args.custom_op_json:
            env_updates["FLUME_HCOMM_CUSTOM_OP_JSON"] = args.custom_op_json
        if args.custom_op_aicpu_tar:
            env_updates["FLUME_HCOMM_CUSTOM_OP_AICPU_TAR"] = (
                args.custom_op_aicpu_tar)
        hccl_devices = ParseDeviceList(args.hccl_devices) if args.hccl_devices else []
        manual_device_ips = ParseDeviceIpMap(args.hccl_device_ips)
        if init_mode == "rank-table":
            if args.hccl_rank_table:
                rank_table = Path(args.hccl_rank_table)
                if not rank_table.exists():
                    raise RuntimeError(f"--hccl-rank-table does not exist: {rank_table}")
            else:
                if run_dir is None:
                    raise RuntimeError("rank-table mode requires a log run directory")
                include_device_ips = (args.hccl_rank_table_net == "device" or
                                      args.hccl_link_mode == "roce")
                rank_table = WriteHcclRankTableV1(
                    run_dir, hccl_devices, manual_device_ips,
                    include_device_ips, args.hccl_server_id,
                    args.hccl_host_ip)
            if args.hccl_visible_remap:
                env_updates["ASCEND_RT_VISIBLE_DEVICES"] = ",".join(hccl_devices)
                smoke_devices = ",".join(str(i) for i in range(len(hccl_devices)))
            else:
                smoke_devices = ",".join(hccl_devices)
            command = [
                sys.executable,
                "tools/flume_hccl_multiproc.py",
                f"--binary={hccl_smoke}",
                f"--count={args.hccl_count}",
                "--init=rank-table",
                f"--rank-table={rank_table}",
                f"--devices={smoke_devices}",
                f"--rank-log-dir={run_dir / 'hccl-rank-logs'}",
            ]
            if args.hccl_smoke_timeout_sec > 5:
                command.append(f"--timeout-sec={args.hccl_smoke_timeout_sec - 5}")
        elif init_mode == "root-info" and hccl_devices:
            if args.hccl_visible_remap:
                env_updates["ASCEND_RT_VISIBLE_DEVICES"] = ",".join(hccl_devices)
                smoke_devices = ",".join(str(i) for i in range(len(hccl_devices)))
            else:
                smoke_devices = ",".join(hccl_devices)
            if run_dir is None:
                raise RuntimeError("root-info multi-process mode requires a log run directory")
            root_info_file = run_dir / "hccl-root-info.bin"
            command = [
                sys.executable,
                "tools/flume_hccl_multiproc.py",
                f"--binary={hccl_smoke}",
                f"--count={args.hccl_count}",
                "--init=root-info",
                f"--root-info-file={root_info_file}",
                f"--devices={smoke_devices}",
                f"--rank-log-dir={run_dir / 'hccl-rank-logs'}",
            ]
            if args.hccl_smoke_timeout_sec > 5:
                command.append(f"--timeout-sec={args.hccl_smoke_timeout_sec - 5}")
        elif hccl_devices and args.hccl_visible_remap:
            env_updates["ASCEND_RT_VISIBLE_DEVICES"] = ",".join(hccl_devices)
            logical_devices = ",".join(str(i) for i in range(len(hccl_devices)))
            command.append(f"--devices={logical_devices}")
        elif hccl_devices:
            command.append(f"--devices={','.join(hccl_devices)}")
        if args.run_a3_symmetric_smoke:
            command.append("--a3-symmetric")
            command.append(f"--sym-win-gb={args.hccl_sym_win_gb}")
        if args.run_hccl_p2p_smoke:
            command.append("--p2p-copy")
        if args.run_hcomm_channel_probe:
            command.append("--hcomm-channel-probe")
        if args.run_hcomm_custom_op_launch_smoke:
            command.append("--hcomm-custom-op-launch-smoke")
        if args.run_hcomm_resource_descriptor_smoke:
            command.append("--hcomm-resource-descriptor-smoke")
        if args.run_hcomm_notify_only_smoke:
            command.append("--hcomm-notify-only-smoke")
        if args.run_hcomm_payload_smoke:
            command.append("--hcomm-payload-smoke")
        if args.run_storage_hbm_smoke:
            assert storage_smoke_input is not None
            assert storage_smoke_checksum is not None
            command.append("--storage-hbm-smoke")
            command.append(f"--storage-smoke-file={storage_smoke_input}")
            command.append(f"--storage-smoke-offset={args.storage_smoke_offset}")
            command.append(f"--storage-smoke-bytes={args.storage_smoke_bytes}")
            command.append(f"--storage-smoke-checksum={storage_smoke_checksum}")
        if (args.run_hcomm_channel_probe or
                args.run_hcomm_custom_op_launch_smoke or
                args.run_hcomm_resource_descriptor_smoke or
                args.run_hcomm_notify_only_smoke or
                args.run_hcomm_payload_smoke or
                (args.run_storage_hbm_smoke and args.hcomm_require_payload_copy)):
            command.append(f"--hcomm-channel-engine={args.hcomm_channel_engine}")
            command.append(f"--hcomm-channel-protocol={args.hcomm_channel_protocol}")
            command.append(f"--hcomm-notify-num={args.hcomm_notify_num}")
            command.append(f"--hcomm-timeout-sec={args.hcomm_timeout_sec}")
            if args.hcomm_require_thread_export:
                command.append("--hcomm-require-thread-export")
            if args.hcomm_require_payload_copy:
                command.append("--hcomm-require-payload-copy")
            if args.hcomm_payload_disable_batch:
                command.append("--hcomm-payload-disable-batch")
            if args.hcomm_payload_recv_direct_output:
                command.append("--hcomm-payload-recv-direct-output")
            if args.hcomm_payload_skip_comm_acquire:
                command.append("--hcomm-payload-skip-comm-acquire")
            if args.hcomm_payload_comm_binding:
                command.append(
                    f"--hcomm-payload-comm-binding={args.hcomm_payload_comm_binding}")
            if args.hcomm_payload_batch_tag:
                command.append(
                    f"--hcomm-payload-batch-tag={args.hcomm_payload_batch_tag}")
        commands.append(CommandSpec("hccl-collective-smoke", command, True,
                                    env_updates))
    return commands


def run_local(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=False, timeout_seconds=args.step_timeout_sec)
    for spec in build_commands(args, enable_hccl=False, run_dir=runner.run_dir):
        runner.run(spec.name, spec.command, required=spec.required,
                   timeout_seconds=args.step_timeout_sec,
                   env_updates=spec.env_updates)
    syntax_command = HcclHeaderSyntaxCommand()
    if syntax_command is not None:
        runner.run("hccl-header-syntax", syntax_command, required=True,
                   timeout_seconds=args.step_timeout_sec)
    payload_kernel_syntax_command = HcommPayloadKernelSyntaxCommand()
    if payload_kernel_syntax_command is not None:
        runner.run("hcomm-payload-kernel-syntax",
                   payload_kernel_syntax_command, required=True,
                   timeout_seconds=args.step_timeout_sec)
    return runner.write_summary()


def run_ascend_probe(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=True, timeout_seconds=args.step_timeout_sec,
               env_updates=CannRuntimeEnvUpdates(args))
    requested_hccl_smoke = (args.run_hccl_smoke or args.run_a3_symmetric_smoke or
                            args.run_hccl_p2p_smoke or
                            args.run_hcomm_channel_probe or
                            args.run_hcomm_custom_op_launch_smoke or
                            args.run_hcomm_resource_descriptor_smoke or
                            args.run_hcomm_notify_only_smoke or
                            args.run_hcomm_payload_smoke or
                            args.run_storage_hbm_smoke)
    hccl_devices = ParseDeviceList(args.hccl_devices) if args.hccl_devices else []
    if shutil.which("npu-smi"):
        runner.run("npu-smi-info-m", ["npu-smi", "info", "-m"],
                   required=False, timeout_seconds=args.step_timeout_sec)
        if requested_hccl_smoke and hccl_devices:
            runner.run(
                "npu-topo-check",
                [sys.executable, "tools/flume_npu_topo_check.py",
                 f"--devices={','.join(hccl_devices)}"],
                required=False,
                timeout_seconds=args.step_timeout_sec,
            )
    if args.run_hcomm_payload_smoke or args.run_hcomm_notify_only_smoke:
        runner.run(
            "hcomm-custom-op-package-preflight",
            HcommCustomOpPackageCommand(
                args, require_payload=args.hcomm_require_payload_copy),
            required=False,
            timeout_seconds=args.step_timeout_sec,
        )
    try:
        command_specs = build_commands(args, enable_hccl=True,
                                       run_dir=runner.run_dir)
    except RuntimeError as exc:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(str(exc) + "\n", encoding="utf-8")
        print(f"[failed] command setup -> {setup_log}")
        return 1
    if requested_hccl_smoke:
        WriteHcclSmokeSetupNotes(
            runner.run_dir,
            CollectHcclSmokeSetupNotes(args, ResolveHcclInitMode(args)),
        )
    for spec in command_specs:
        timeout = (args.hccl_smoke_timeout_sec if spec.name == "hccl-collective-smoke"
                   else args.step_timeout_sec)
        result = runner.run(spec.name, spec.command, required=spec.required,
                            timeout_seconds=timeout, env_updates=spec.env_updates)
        if spec.name == "hccl-collective-smoke" and result.returncode != 0:
            WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)
    note = runner.run_dir / "ASCEND_PROBE_SCOPE.txt"
    note.write_text(
        "This probe validates CANN/HCCL discovery, compile/link, and the current "
        "mock/sim tests. The base HCCL collective path enqueues AllReduce and "
        "AllGather on caller-provided Ascend HBM pointers; stream execution still "
        "requires validation on Ascend hardware. CMake feature-detects optional "
        "A3 APIs; missing symmetric-window or communication-memory APIs should "
        "not block the base HCCL build. Pass --run-hccl-smoke to run "
        "the optional true HCCL single-node collective smoke test; when "
        "--hccl-devices is set, auto init uses the root-info path to mirror "
        "the official HCCL bring-up baseline. Pass "
        "--run-a3-symmetric-smoke on Atlas A3 HCCS hosts to allocate ACL mapped "
        "HBM and register it through HcclCommSymWinRegister. Pass "
        "--run-hccl-p2p-smoke to add a rank0-to-rank1 HcclSend/HcclRecv "
        "HBM copy check after the collective smoke. Pass "
        "--run-hcomm-channel-probe to validate the HCOMM Channel resource "
        "stage used by the future AICPU/HCOMM primitive data path; "
        "--hcomm-channel-engine, --hcomm-channel-protocol, and "
        "--hcomm-notify-num select the resource probe strategy. The default "
        "HCOMM probe validates channel resources only and the smoke log prints "
        "a FLUME_BACKEND_CAPS line. Pass --run-hcomm-custom-op-launch-smoke "
        "to run the Stage 3B.1 no-op custom-op launch readiness check; it "
        "is expected to report unsupported with a Stage 3B.3B launcher-router "
        "decision until a supported public HCCL launch or direct ACL runtime "
        "custom-op launch route is available. Pass "
        "--run-hcomm-resource-descriptor-smoke to run "
        "the Stage 3B.2 resource descriptor packaging check; it is expected "
        "to report unsupported until descriptor handoff into custom-op/AICPU "
        "is implemented. Pass --run-hcomm-notify-only-smoke to run the "
        "Stage 3B.2-complete notify-only kernel-consume check; it is "
        "expected to report unsupported until the custom-op/AICPU kernel "
        "can consume the descriptor and perform channel notify record/wait. "
        "Pass --run-hcomm-payload-smoke to run the "
        "Stage 2.5 payload-readiness probe: it checks Channel resources and "
        "HCOMM primitive capability, then reports unsupported/fallback clearly "
        "until Flume implements the custom-op/AICPU payload scheduler. "
        "The tool also runs hcomm-custom-op-package-preflight before payload "
        "or notify-only smoke so the log distinguishes missing/incomplete "
        "custom-op packages from runtime launch or primitive failures. Add "
        "--hcomm-require-thread-export for a strict AICPU thread-export "
        "prerequisite check, which is expected to report unsupported on CANN "
        "builds without hccl_res_expt.h such as CANN 8.5. Add "
        "--hcomm-require-payload-copy only when a future build is expected to "
        "complete real HCOMM payload copy. Pass --run-storage-hbm-smoke to "
        "run the Stage 3A storage proxy fallback path: rank0 reads a local "
        "file slice, copies it to proxy HBM, and sends it to rank1 compute "
        "HBM with HcclSend/HcclRecv. The marker is "
        "storage_hbm=hccl-p2p-staging; this is not full storage-direct DMA.\n",
        encoding="utf-8",
    )
    print(f"[ok] scope note -> {note}")
    return runner.write_summary()


STRICT_PAYLOAD_RANK_MARKERS = (
    "stage3b3e_payload_copy=passed",
    "stage3b3e_direct_aclrt_payload_loader=passed",
    "stage3b3e_payload_descriptor_handoff=passed",
    "stage3b3e_direct_aclrt_payload_launch=passed",
    "stage3b3e_payload_sync=passed",
    "payload_kernel_status=success",
    "payload_failure_step=none",
    "payload_status_word=0",
    "payload_kernel_hcomm_ret=0",
    "payload_status_schema=v2",
    "payload_status_word_count=8",
    "payload_echo=passed",
    "payload_primitive_state=completed",
    "payload_trace=passed",
    "payload_trace_event=kernel-exit",
    "payload_trace_order=passed",
    "payload_trace_primitive_path=",
    "payload_trace_result=success",
    "payload_role=",
    "payload_batch_mode=on",
    "payload_comm_acquire=default",
    "payload_comm_binding=comm-name",
    "payload_desc_batch_tag=",
    "payload_recv_path=",
    "payload_semantic_v6=present",
    "payload_semantic_v7=present",
    "payload_semantic_v8=present",
    "payload_thread_notify_order=",
    "payload_pattern=strict-v1",
    "fallback=none",
)


def StrictPayloadRankMarkersForCommBinding(binding: str) -> tuple[str, ...]:
    if binding == "comm-name":
        return STRICT_PAYLOAD_RANK_MARKERS
    if binding == "channel-handle":
        return tuple(
            "payload_comm_acquire=skipped"
            if item == "payload_comm_acquire=default" else (
                "payload_comm_binding=channel-handle"
                if item == "payload_comm_binding=comm-name" else item)
            for item in STRICT_PAYLOAD_RANK_MARKERS)
    if binding == "diagnostic-skip":
        return tuple(
            "payload_comm_acquire=skipped"
            if item == "payload_comm_acquire=default" else (
                "payload_comm_binding=diagnostic-skip"
                if item == "payload_comm_binding=comm-name" else item)
            for item in STRICT_PAYLOAD_RANK_MARKERS)
    raise ValueError(f"unsupported payload comm binding marker: {binding}")


def ExtractStrictPayloadRankLines(strict: str) -> dict[int, str]:
    rank_lines = {0: "", 1: ""}
    for line in strict.splitlines():
        for rank in rank_lines:
            if re.search(rf"\brank {rank} hcomm payload smoke "
                         r"(passed|unsupported|failed)\b", line):
                rank_lines[rank] = line
    return rank_lines


def StrictPayloadRankEvidencePassed(strict: str) -> tuple[bool, bool, bool]:
    rank_lines = ExtractStrictPayloadRankLines(strict)
    accepted_marker_sets = (
        StrictPayloadRankMarkersForCommBinding("comm-name"),
        StrictPayloadRankMarkersForCommBinding("channel-handle"),
    )
    rank0_binding = MarkerValueFromLine(rank_lines[0], "payload_comm_binding")
    rank1_binding = MarkerValueFromLine(rank_lines[1], "payload_comm_binding")
    binding_ok = (
        rank0_binding == rank1_binding and
        rank0_binding in ("comm-name", "channel-handle"))
    rank0_ok = bool(rank_lines[0]) and any(
        all(marker in rank_lines[0] for marker in markers)
        for markers in accepted_marker_sets) and (
            "payload_role=send" in rank_lines[0] and
            "payload_trace_primitive_path=send-local-copy" in rank_lines[0])
    rank1_ok = (bool(rank_lines[1]) and any(
        all(marker in rank_lines[1] for marker in markers)
        for markers in accepted_marker_sets) and
                "payload_role=recv" in rank_lines[1] and
                "payload_trace_primitive_path=recv-read" in rank_lines[1] and
                "payload_verify=passed" in rank_lines[1])
    source_match = re.search(r"\bpayload_source_checksum=([^\s\"]+)",
                             rank_lines[0])
    payload_match = re.search(r"\bpayload_checksum=([^\s\"]+)",
                              rank_lines[1])
    expected_match = re.search(r"\bpayload_expected_checksum=([^\s\"]+)",
                               rank_lines[1])
    checksum_ok = (
        source_match is not None and payload_match is not None and
        expected_match is not None and source_match.group(1) ==
        payload_match.group(1) == expected_match.group(1))
    return (rank0_ok and rank1_ok and binding_ok and checksum_ok,
            rank0_ok, rank1_ok)


def StrictPayloadLogPassed(log_path: Optional[Path]) -> bool:
    if log_path is None:
        return False
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return StrictPayloadRankEvidencePassed(text)[0]


def StorageHbmRank1Passed(text: str) -> bool:
    return re.search(r"\brank 1 storage HBM smoke passed\b", text) is not None


def StorageHbmRank1Path(text: str) -> str:
    for line in text.splitlines():
        if re.search(r"\brank 1 storage HBM smoke passed\b", line):
            return MarkerValueFromLine(line, "storage_hbm")
    return "missing"


def StorageHbmHcommPathPassed(text: str) -> bool:
    for line in text.splitlines():
        if (re.search(r"\brank 1 storage HBM smoke passed\b", line) and
                "storage_hbm=hcomm-payload-staging" in line):
            return True
    return False


def MarkerValueFromLine(line: str, name: str) -> str:
    match = re.search(rf"\b{re.escape(name)}=([^\s\"]+)", line)
    return match.group(1) if match else "missing"


def MarkerValue(text: str, name: str) -> str:
    match = re.search(rf"\b{re.escape(name)}=([^\s\"]+)", text)
    return match.group(1) if match else "missing"


def StrictPayloadNoBatchDiagnosticPassed(text: str) -> tuple[bool, bool, bool]:
    rank_lines = ExtractStrictPayloadRankLines(text)
    no_batch_markers = tuple(
        "payload_batch_mode=off" if item == "payload_batch_mode=on" else item
        for item in StrictPayloadRankMarkersForCommBinding("comm-name"))
    rank0_ok = bool(rank_lines[0]) and all(
        marker in rank_lines[0] for marker in no_batch_markers) and (
            "payload_role=send" in rank_lines[0])
    rank1_ok = (bool(rank_lines[1]) and
                all(marker in rank_lines[1]
                    for marker in no_batch_markers) and
                "payload_role=recv" in rank_lines[1] and
                "payload_verify=passed" in rank_lines[1])
    source = MarkerValueFromLine(rank_lines[0], "payload_source_checksum")
    payload = MarkerValueFromLine(rank_lines[1], "payload_checksum")
    expected = MarkerValueFromLine(rank_lines[1], "payload_expected_checksum")
    checksum_ok = (
        source != "missing" and source == payload and payload == expected)
    return (rank0_ok and rank1_ok and checksum_ok, rank0_ok, rank1_ok)


def StrictPayloadNoCommAcquireDiagnosticPassed(
        text: str) -> tuple[bool, bool, bool]:
    rank_lines = ExtractStrictPayloadRankLines(text)
    no_comm_markers = StrictPayloadRankMarkersForCommBinding("diagnostic-skip")
    rank0_ok = bool(rank_lines[0]) and all(
        marker in rank_lines[0] for marker in no_comm_markers) and (
            "payload_role=send" in rank_lines[0])
    rank1_ok = (bool(rank_lines[1]) and
                all(marker in rank_lines[1]
                    for marker in no_comm_markers) and
                "payload_role=recv" in rank_lines[1] and
                "payload_verify=passed" in rank_lines[1])
    source = MarkerValueFromLine(rank_lines[0], "payload_source_checksum")
    payload = MarkerValueFromLine(rank_lines[1], "payload_checksum")
    expected = MarkerValueFromLine(rank_lines[1], "payload_expected_checksum")
    checksum_ok = (
        source != "missing" and source == payload and payload == expected)
    return (rank0_ok and rank1_ok and checksum_ok, rank0_ok, rank1_ok)


def WriteHcommPayloadNoBatchDiagnostic(
        run_dir: Path,
        default_log: Optional[Path],
        no_batch_log: Optional[Path]) -> Path:
    note = run_dir / "HCOMM_PAYLOAD_NOBATCH_DIAGNOSTIC.md"
    try:
        default_text = (default_log.read_text(encoding="utf-8",
                                              errors="replace")
                        if default_log is not None else "")
    except OSError as exc:
        default_text = f"failed to read default log: {exc}"
    try:
        no_batch_text = (no_batch_log.read_text(encoding="utf-8",
                                                errors="replace")
                         if no_batch_log is not None else "")
    except OSError as exc:
        no_batch_text = f"failed to read no-batch log: {exc}"

    default_rank_lines = ExtractStrictPayloadRankLines(default_text)
    no_batch_rank_lines = ExtractStrictPayloadRankLines(no_batch_text)
    no_batch_ok, no_batch_rank0_ok, no_batch_rank1_ok = (
        StrictPayloadNoBatchDiagnosticPassed(no_batch_text))
    default_rank0_failure_step = MarkerValueFromLine(
        default_rank_lines[0], "payload_failure_step")
    default_rank1_failure_step = MarkerValueFromLine(
        default_rank_lines[1], "payload_failure_step")
    default_failure_step = (
        default_rank1_failure_step
        if default_rank1_failure_step not in ("missing", "none") else
        default_rank0_failure_step)
    no_batch_rank0_failure_step = MarkerValueFromLine(
        no_batch_rank_lines[0], "payload_failure_step")
    no_batch_rank1_failure_step = MarkerValueFromLine(
        no_batch_rank_lines[1], "payload_failure_step")
    no_batch_failure_step = (
        no_batch_rank1_failure_step
        if no_batch_rank1_failure_step not in ("missing", "none") else
        no_batch_rank0_failure_step)
    no_batch_kernel = MarkerValueFromLine(
        no_batch_rank_lines[1], "payload_kernel_status")
    if no_batch_kernel == "missing":
        no_batch_kernel = MarkerValueFromLine(
            no_batch_rank_lines[0], "payload_kernel_status")
    no_batch_hcomm_ret = MarkerValueFromLine(
        no_batch_rank_lines[1], "payload_kernel_hcomm_ret")
    if no_batch_hcomm_ret == "missing":
        no_batch_hcomm_ret = MarkerValueFromLine(
            no_batch_rank_lines[0], "payload_kernel_hcomm_ret")
    no_batch_batch_mode = MarkerValueFromLine(
        no_batch_rank_lines[1], "payload_batch_mode")
    if no_batch_batch_mode == "missing":
        no_batch_batch_mode = MarkerValueFromLine(
            no_batch_rank_lines[0], "payload_batch_mode")

    if no_batch_ok:
        decision = (
            "no-batch HCOMM payload copy and checksum verification passed; "
            "the remaining issue is likely HcommBatchModeStart/End submit or "
            "ordering in the batch-enabled strict path")
        next_action = (
            "inspect default strict-positive batch-start/batch-end failure "
            "and HCOMM batch mode compatibility for the selected engine")
    elif no_batch_failure_step != "missing":
        decision = (
            "no-batch diagnostic reached the payload kernel but failed inside "
            f"`{no_batch_failure_step}`")
        next_action = StrictPayloadFailureAction(1, no_batch_failure_step)
    elif no_batch_kernel != "missing":
        decision = (
            "no-batch diagnostic launched but did not produce complete rank "
            "evidence")
        next_action = "inspect no-batch rank logs and payload kernel status"
    else:
        decision = "no-batch diagnostic did not reach payload kernel evidence"
        next_action = "inspect direct ACL loader/package/descriptor handoff"

    lines = [
        "# HCOMM Payload No-Batch Diagnostic",
        "",
        f"- default_strict_log: `{default_log}`",
        f"- no_batch_log: `{no_batch_log}`",
        f"- default_failure_step: `{default_failure_step}`",
        f"- default_rank0_failure_step: `{default_rank0_failure_step}`",
        f"- default_rank1_failure_step: `{default_rank1_failure_step}`",
        f"- no_batch_kernel_status: `{no_batch_kernel}`",
        f"- no_batch_failure_step: `{no_batch_failure_step}`",
        f"- no_batch_rank0_failure_step: `{no_batch_rank0_failure_step}`",
        f"- no_batch_rank1_failure_step: `{no_batch_rank1_failure_step}`",
        f"- no_batch_hcomm_ret: `{no_batch_hcomm_ret}`",
        f"- no_batch_batch_mode: `{no_batch_batch_mode}`",
        f"- no_batch_rank0_evidence: `{'passed' if no_batch_rank0_ok else 'missing'}`",
        f"- no_batch_rank1_evidence: `{'passed' if no_batch_rank1_ok else 'missing'}`",
        f"- no_batch_payload_copy_and_verify: `{'passed' if no_batch_ok else 'not-passed'}`",
        "",
        f"decision: {decision}",
        f"next_action: {next_action}",
        "",
        "The no-batch path is diagnostic only. It intentionally cannot satisfy "
        "the final strict-positive gate, which still requires "
        "`payload_batch_mode=on`.",
        "",
        "## Rank Evidence",
        "",
        f"- default_rank0: `{default_rank_lines[0]}`",
        f"- default_rank1: `{default_rank_lines[1]}`",
        f"- no_batch_rank0: `{no_batch_rank_lines[0]}`",
        f"- no_batch_rank1: `{no_batch_rank_lines[1]}`",
    ]
    note.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok] hcomm payload no-batch diagnostic -> {note}")
    return note


def RunHcommPayloadNoBatchDiagnostic(
        runner: Runner,
        base_command: list[str],
        env_updates: Optional[dict[str, str]],
        timeout_seconds: int,
        default_log: Optional[Path]) -> StepResult:
    command = list(base_command)
    if "--hcomm-payload-disable-batch" not in command:
        command.append("--hcomm-payload-disable-batch")
    result = runner.run(
        "hcomm-payload-nobatch-diagnostic",
        command,
        required=False,
        timeout_seconds=timeout_seconds,
        env_updates=env_updates,
    )
    if result.returncode != 0:
        WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)
    WriteHcommPayloadNoBatchDiagnostic(
        runner.run_dir, default_log, result.log_path)
    return result


def WriteHcommPayloadNoCommAcquireDiagnostic(
        run_dir: Path,
        default_log: Optional[Path],
        no_comm_log: Optional[Path]) -> Path:
    note = run_dir / "HCOMM_PAYLOAD_NO_COMM_ACQUIRE_DIAGNOSTIC.md"
    try:
        default_text = (default_log.read_text(encoding="utf-8",
                                              errors="replace")
                        if default_log is not None else "")
    except OSError as exc:
        default_text = f"failed to read default log: {exc}"
    try:
        no_comm_text = (no_comm_log.read_text(encoding="utf-8",
                                              errors="replace")
                        if no_comm_log is not None else "")
    except OSError as exc:
        no_comm_text = f"failed to read no-comm-acquire log: {exc}"

    default_rank_lines = ExtractStrictPayloadRankLines(default_text)
    no_comm_rank_lines = ExtractStrictPayloadRankLines(no_comm_text)
    no_comm_ok, no_comm_rank0_ok, no_comm_rank1_ok = (
        StrictPayloadNoCommAcquireDiagnosticPassed(no_comm_text))
    default_rank0_failure_step = MarkerValueFromLine(
        default_rank_lines[0], "payload_failure_step")
    default_rank1_failure_step = MarkerValueFromLine(
        default_rank_lines[1], "payload_failure_step")
    default_failure_step = (
        default_rank1_failure_step
        if default_rank1_failure_step not in ("missing", "none") else
        default_rank0_failure_step)
    no_comm_rank0_failure_step = MarkerValueFromLine(
        no_comm_rank_lines[0], "payload_failure_step")
    no_comm_rank1_failure_step = MarkerValueFromLine(
        no_comm_rank_lines[1], "payload_failure_step")
    no_comm_failure_step = (
        no_comm_rank1_failure_step
        if no_comm_rank1_failure_step not in ("missing", "none") else
        no_comm_rank0_failure_step)
    no_comm_kernel = MarkerValue(no_comm_text, "payload_kernel_status")
    no_comm_hcomm_ret = MarkerValue(no_comm_text, "payload_kernel_hcomm_ret")
    no_comm_acquire = MarkerValue(no_comm_text, "payload_comm_acquire")
    no_comm_binding = MarkerValue(no_comm_text, "payload_comm_binding")

    if no_comm_ok:
        decision = (
            "no-comm-acquire HCOMM payload copy and checksum verification "
            "passed; the default failure is likely isolated to "
            "HcommAcquireComm/HcommReleaseComm or comm-name binding")
        next_action = (
            "inspect HcclGetCommName output, HcommAcquireComm requirements, "
            "and whether the selected CANN build expects ChannelHandle-only "
            "payload primitives")
    elif no_comm_failure_step != "missing":
        decision = (
            "no-comm-acquire diagnostic reached the payload kernel but failed "
            f"inside `{no_comm_failure_step}`")
        next_action = StrictPayloadFailureAction(1, no_comm_failure_step)
    elif no_comm_kernel != "missing":
        decision = (
            "no-comm-acquire diagnostic launched but did not produce complete "
            "rank evidence")
        next_action = "inspect no-comm-acquire rank logs and payload kernel status"
    else:
        decision = (
            "no-comm-acquire diagnostic did not reach payload kernel evidence")
        next_action = "inspect direct ACL loader/package/descriptor handoff"

    lines = [
        "# HCOMM Payload No-Comm-Acquire Diagnostic",
        "",
        f"- default_strict_log: `{default_log}`",
        f"- no_comm_acquire_log: `{no_comm_log}`",
        f"- default_failure_step: `{default_failure_step}`",
        f"- default_rank0_failure_step: `{default_rank0_failure_step}`",
        f"- default_rank1_failure_step: `{default_rank1_failure_step}`",
        f"- no_comm_kernel_status: `{no_comm_kernel}`",
        f"- no_comm_failure_step: `{no_comm_failure_step}`",
        f"- no_comm_rank0_failure_step: `{no_comm_rank0_failure_step}`",
        f"- no_comm_rank1_failure_step: `{no_comm_rank1_failure_step}`",
        f"- no_comm_hcomm_ret: `{no_comm_hcomm_ret}`",
        f"- no_comm_acquire_marker: `{no_comm_acquire}`",
        f"- no_comm_binding_marker: `{no_comm_binding}`",
        f"- no_comm_rank0_evidence: `{'passed' if no_comm_rank0_ok else 'missing'}`",
        f"- no_comm_rank1_evidence: `{'passed' if no_comm_rank1_ok else 'missing'}`",
        f"- no_comm_payload_copy_and_verify: `{'passed' if no_comm_ok else 'not-passed'}`",
        "",
        f"decision: {decision}",
        f"next_action: {next_action}",
        "",
        "The no-comm-acquire path is diagnostic only. It intentionally cannot "
        "satisfy the final strict-positive gate, which still requires "
        "either `payload_comm_binding=comm-name` with "
        "`payload_comm_acquire=default`, or an explicit "
        "`payload_comm_binding=channel-handle` backend run.",
        "",
        "## Rank Evidence",
        "",
        f"- default_rank0: `{default_rank_lines[0]}`",
        f"- default_rank1: `{default_rank_lines[1]}`",
        f"- no_comm_rank0: `{no_comm_rank_lines[0]}`",
        f"- no_comm_rank1: `{no_comm_rank_lines[1]}`",
    ]
    note.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok] hcomm payload no-comm-acquire diagnostic -> {note}")
    return note


def RunHcommPayloadNoCommAcquireDiagnostic(
        runner: Runner,
        base_command: list[str],
        env_updates: Optional[dict[str, str]],
        timeout_seconds: int,
        default_log: Optional[Path]) -> StepResult:
    command = list(base_command)
    if "--hcomm-payload-skip-comm-acquire" not in command:
        command.append("--hcomm-payload-skip-comm-acquire")
    result = runner.run(
        "hcomm-payload-no-comm-acquire-diagnostic",
        command,
        required=False,
        timeout_seconds=timeout_seconds,
        env_updates=env_updates,
    )
    if result.returncode != 0:
        WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)
    WriteHcommPayloadNoCommAcquireDiagnostic(
        runner.run_dir, default_log, result.log_path)
    return result


def PayloadCommandWithoutCommBinding(command: list[str]) -> list[str]:
    return [
        item for item in command
        if item != "--hcomm-payload-skip-comm-acquire" and
        not item.startswith("--hcomm-payload-comm-binding=")
    ]


def CommandUsesChannelHandleBinding(command: list[str]) -> bool:
    return "--hcomm-payload-comm-binding=channel-handle" in command


def WriteHcommPayloadChannelHandleCandidate(
        run_dir: Path,
        default_log: Optional[Path],
        channel_log: Optional[Path]) -> Path:
    note = run_dir / "HCOMM_PAYLOAD_CHANNEL_HANDLE_CANDIDATE.md"
    try:
        default_text = (default_log.read_text(encoding="utf-8",
                                              errors="replace")
                        if default_log is not None else "")
    except OSError as exc:
        default_text = f"failed to read default log: {exc}"
    try:
        channel_text = (channel_log.read_text(encoding="utf-8",
                                              errors="replace")
                        if channel_log is not None else "")
    except OSError as exc:
        channel_text = f"failed to read channel-handle log: {exc}"

    default_rank_lines = ExtractStrictPayloadRankLines(default_text)
    channel_rank_lines = ExtractStrictPayloadRankLines(channel_text)
    channel_ok, channel_rank0_ok, channel_rank1_ok = (
        StrictPayloadRankEvidencePassed(channel_text))
    default_rank0_failure_step = MarkerValueFromLine(
        default_rank_lines[0], "payload_failure_step")
    default_rank1_failure_step = MarkerValueFromLine(
        default_rank_lines[1], "payload_failure_step")
    default_failure_step = (
        default_rank1_failure_step
        if default_rank1_failure_step not in ("missing", "none") else
        default_rank0_failure_step)
    channel_rank0_failure_step = MarkerValueFromLine(
        channel_rank_lines[0], "payload_failure_step")
    channel_rank1_failure_step = MarkerValueFromLine(
        channel_rank_lines[1], "payload_failure_step")
    channel_failure_step = (
        channel_rank1_failure_step
        if channel_rank1_failure_step not in ("missing", "none") else
        channel_rank0_failure_step)
    channel_binding = MarkerValue(channel_text, "payload_comm_binding")
    channel_acquire = MarkerValue(channel_text, "payload_comm_acquire")
    channel_hcomm_ret = MarkerValue(channel_text, "payload_kernel_hcomm_ret")

    if channel_ok and channel_binding == "channel-handle":
        decision = (
            "channel-handle HCOMM payload copy and checksum verification "
            "passed; this run provides strict-positive evidence for the "
            "ChannelHandle/ThreadHandle backend without in-kernel comm acquire")
        next_action = (
            "use --hcomm-payload-comm-binding=channel-handle for the current "
            "CANN environment and proceed to the storage HCOMM strict gate")
    elif channel_failure_step != "missing":
        decision = (
            "channel-handle candidate reached the payload kernel but failed "
            f"inside `{channel_failure_step}`")
        next_action = StrictPayloadFailureAction(1, channel_failure_step)
    else:
        decision = (
            "channel-handle candidate did not produce complete payload "
            "evidence")
        next_action = "inspect direct ACL loader/package/descriptor handoff"

    lines = [
        "# HCOMM Payload Channel-Handle Candidate",
        "",
        f"- default_strict_log: `{default_log}`",
        f"- channel_handle_log: `{channel_log}`",
        f"- default_failure_step: `{default_failure_step}`",
        f"- default_rank0_failure_step: `{default_rank0_failure_step}`",
        f"- default_rank1_failure_step: `{default_rank1_failure_step}`",
        f"- channel_binding_marker: `{channel_binding}`",
        f"- channel_acquire_marker: `{channel_acquire}`",
        f"- channel_failure_step: `{channel_failure_step}`",
        f"- channel_rank0_failure_step: `{channel_rank0_failure_step}`",
        f"- channel_rank1_failure_step: `{channel_rank1_failure_step}`",
        f"- channel_hcomm_ret: `{channel_hcomm_ret}`",
        f"- channel_rank0_evidence: `{'passed' if channel_rank0_ok else 'missing'}`",
        f"- channel_rank1_evidence: `{'passed' if channel_rank1_ok else 'missing'}`",
        f"- channel_payload_copy_and_verify: `{'passed' if channel_ok else 'not-passed'}`",
        "",
        f"decision: {decision}",
        f"next_action: {next_action}",
        "",
        "This is not the diagnostic-skip path. It is an explicit backend "
        "candidate and can satisfy the strict-positive gate only when both "
        "ranks pass with `payload_comm_binding=channel-handle`, checksum "
        "match, complete trace evidence, and `fallback=none`.",
        "",
        "## Rank Evidence",
        "",
        f"- default_rank0: `{default_rank_lines[0]}`",
        f"- default_rank1: `{default_rank_lines[1]}`",
        f"- channel_rank0: `{channel_rank_lines[0]}`",
        f"- channel_rank1: `{channel_rank_lines[1]}`",
    ]
    note.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok] hcomm payload channel-handle candidate -> {note}")
    return note


def RunHcommPayloadChannelHandleCandidate(
        runner: Runner,
        base_command: list[str],
        env_updates: Optional[dict[str, str]],
        timeout_seconds: int,
        default_log: Optional[Path]) -> StepResult:
    command = PayloadCommandWithoutCommBinding(base_command)
    command.append("--hcomm-payload-comm-binding=channel-handle")
    result = runner.run(
        "hcomm-payload-channel-handle-candidate",
        command,
        required=False,
        timeout_seconds=timeout_seconds,
        env_updates=env_updates,
    )
    if result.returncode != 0:
        WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)
    WriteHcommPayloadChannelHandleCandidate(
        runner.run_dir, default_log, result.log_path)
    return result


def RunHcommPayloadTaggedDiagnostic(
        runner: Runner,
        base_command: list[str],
        env_updates: Optional[dict[str, str]],
        timeout_seconds: int,
        default_log: Optional[Path],
        batch_tag: str) -> Optional[StepResult]:
    if not batch_tag:
        return None
    command = [
        item for item in base_command
        if not item.startswith("--hcomm-payload-batch-tag=") and
        item != "--hcomm-payload-disable-batch"
    ]
    command.append(f"--hcomm-payload-batch-tag={batch_tag}")
    result = runner.run(
        "hcomm-payload-tagged-diagnostic",
        command,
        required=False,
        timeout_seconds=timeout_seconds,
        env_updates=env_updates,
    )
    if result.returncode != 0:
        WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)
    WriteHcommPayloadTaggedDiagnostic(
        runner.run_dir, default_log, result.log_path, batch_tag)
    return result


def WriteHcommPayloadTaggedDiagnostic(
        run_dir: Path,
        default_log: Optional[Path],
        tagged_log: Optional[Path],
        batch_tag: str) -> Path:
    note = run_dir / "HCOMM_PAYLOAD_TAGGED_DIAGNOSTIC.md"
    try:
        default_text = (default_log.read_text(encoding="utf-8",
                                              errors="replace")
                        if default_log is not None else "")
    except OSError as exc:
        default_text = f"failed to read default log: {exc}"
    try:
        tagged_text = (tagged_log.read_text(encoding="utf-8",
                                            errors="replace")
                       if tagged_log is not None else "")
    except OSError as exc:
        tagged_text = f"failed to read tagged log: {exc}"

    tagged_ok, tagged_rank0_ok, tagged_rank1_ok = (
        StrictPayloadRankEvidencePassed(tagged_text))
    default_failure_step = MarkerValue(default_text, "payload_failure_step")
    tagged_failure_step = MarkerValue(tagged_text, "payload_failure_step")
    tagged_batch_mode = MarkerValue(tagged_text, "payload_batch_mode")
    tagged_batch_tag = MarkerValue(tagged_text, "payload_desc_batch_tag")
    tagged_hcomm_ret = MarkerValue(tagged_text, "payload_kernel_hcomm_ret")
    tagged_rank_lines = ExtractStrictPayloadRankLines(tagged_text)

    if tagged_ok:
        decision = (
            "tagged batch HCOMM payload copy and checksum verification passed; "
            "the default batch tag path is the likely compatibility problem")
        next_action = (
            "rerun strict-positive with the same --hcomm-payload-batch-tag and "
            "then wire Stage 3B.4 storage once the strict gate is green")
    elif tagged_failure_step != "missing":
        decision = (
            "tagged batch diagnostic reached the payload kernel but failed "
            f"inside `{tagged_failure_step}`")
        next_action = StrictPayloadFailureAction(1, tagged_failure_step)
    else:
        decision = "tagged batch diagnostic did not reach complete payload evidence"
        next_action = "inspect tagged direct ACL loader/package/descriptor handoff"

    lines = [
        "# HCOMM Payload Tagged Batch Diagnostic",
        "",
        f"- default_strict_log: `{default_log}`",
        f"- tagged_log: `{tagged_log}`",
        f"- requested_batch_tag: `{batch_tag}`",
        f"- default_failure_step: `{default_failure_step}`",
        f"- tagged_batch_mode: `{tagged_batch_mode}`",
        f"- tagged_descriptor_batch_tag: `{tagged_batch_tag}`",
        f"- tagged_failure_step: `{tagged_failure_step}`",
        f"- tagged_hcomm_ret: `{tagged_hcomm_ret}`",
        f"- tagged_rank0_evidence: `{'passed' if tagged_rank0_ok else 'missing'}`",
        f"- tagged_rank1_evidence: `{'passed' if tagged_rank1_ok else 'missing'}`",
        f"- tagged_payload_copy_and_verify: `{'passed' if tagged_ok else 'not-passed'}`",
        "",
        f"decision: {decision}",
        f"next_action: {next_action}",
        "",
        "The tagged batch path is diagnostic unless the user explicitly reruns "
        "the strict-positive gate with the same tag. It does not make a "
        "failed default strict run pass retroactively.",
        "",
        "## Rank Evidence",
        "",
        f"- tagged_rank0: `{tagged_rank_lines[0]}`",
        f"- tagged_rank1: `{tagged_rank_lines[1]}`",
    ]
    note.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok] hcomm payload tagged diagnostic -> {note}")
    return note


def WriteHcommPayloadDirectOutputDiagnostic(
        run_dir: Path,
        default_log: Optional[Path],
        direct_log: Optional[Path]) -> Path:
    note = run_dir / "HCOMM_PAYLOAD_DIRECT_OUTPUT_DIAGNOSTIC.md"
    try:
        default_text = (default_log.read_text(encoding="utf-8",
                                              errors="replace")
                        if default_log is not None else "")
    except OSError as exc:
        default_text = f"failed to read default log: {exc}"
    try:
        direct_text = (direct_log.read_text(encoding="utf-8",
                                            errors="replace")
                       if direct_log is not None else "")
    except OSError as exc:
        direct_text = f"failed to read direct-output log: {exc}"

    default_rank_lines = ExtractStrictPayloadRankLines(default_text)
    direct_rank_lines = ExtractStrictPayloadRankLines(direct_text)
    direct_ok, direct_rank0_ok, direct_rank1_ok = (
        StrictPayloadRankEvidencePassed(direct_text))
    default_recv_path = MarkerValue(default_text, "payload_recv_path")
    direct_recv_path = MarkerValue(direct_text, "payload_recv_path")
    direct_path_ok = direct_recv_path == "direct-output"
    default_failure_step = MarkerValue(default_text, "payload_failure_step")
    direct_rank0_failure_step = MarkerValueFromLine(
        direct_rank_lines[0], "payload_failure_step")
    direct_rank1_failure_step = MarkerValueFromLine(
        direct_rank_lines[1], "payload_failure_step")
    direct_failure_step = (
        direct_rank1_failure_step
        if direct_rank1_failure_step not in ("missing", "none") else
        direct_rank0_failure_step)
    direct_kernel = MarkerValue(direct_text, "payload_kernel_status")
    direct_hcomm_ret = MarkerValue(direct_text, "payload_kernel_hcomm_ret")
    direct_source = MarkerValueFromLine(
        direct_rank_lines[0], "payload_source_checksum")
    direct_payload = MarkerValueFromLine(
        direct_rank_lines[1], "payload_checksum")
    direct_expected = MarkerValueFromLine(
        direct_rank_lines[1], "payload_expected_checksum")
    direct_checksum_match = (
        direct_source != "missing" and direct_source == direct_payload and
        direct_payload == direct_expected)

    if direct_ok and direct_path_ok:
        decision = (
            "direct-output HCOMM payload copy and checksum verification "
            "passed; the default local-buffer path is likely failing in the "
            "recv output local-copy stage")
        next_action = (
            "rerun hcomm-payload-strict-positive explicitly with "
            "--hcomm-payload-recv-direct-output, then use that green strict "
            "gate as the Stage 3B.3E evidence before wiring Stage 3B.4 storage")
    elif direct_failure_step != "missing":
        decision = (
            "direct-output diagnostic reached the payload kernel but failed "
            f"inside `{direct_failure_step}`")
        next_action = StrictPayloadFailureAction(1, direct_failure_step)
    elif direct_kernel != "missing":
        decision = (
            "direct-output diagnostic launched but did not produce complete "
            "rank evidence")
        next_action = "inspect direct-output rank logs and payload kernel status"
    else:
        decision = "direct-output diagnostic did not reach payload kernel evidence"
        next_action = "inspect direct ACL loader/package/descriptor handoff"

    lines = [
        "# HCOMM Payload Direct-Output Diagnostic",
        "",
        f"- default_strict_log: `{default_log}`",
        f"- direct_output_log: `{direct_log}`",
        f"- default_recv_path: `{default_recv_path}`",
        f"- direct_recv_path: `{direct_recv_path}`",
        f"- default_failure_step: `{default_failure_step}`",
        f"- direct_kernel_status: `{direct_kernel}`",
        f"- direct_failure_step: `{direct_failure_step}`",
        f"- direct_rank0_failure_step: `{direct_rank0_failure_step}`",
        f"- direct_rank1_failure_step: `{direct_rank1_failure_step}`",
        f"- direct_hcomm_ret: `{direct_hcomm_ret}`",
        f"- direct_rank0_evidence: `{'passed' if direct_rank0_ok else 'missing'}`",
        f"- direct_rank1_evidence: `{'passed' if direct_rank1_ok else 'missing'}`",
        f"- direct_checksum_match: `{'yes' if direct_checksum_match else 'no'}`",
        f"- direct_payload_copy_and_verify: `{'passed' if direct_ok and direct_path_ok else 'not-passed'}`",
        "",
        f"decision: {decision}",
        f"next_action: {next_action}",
        "",
        "The direct-output path is diagnostic unless the user explicitly reruns "
        "the strict-positive gate with --hcomm-payload-recv-direct-output. It "
        "does not make a failed default strict run pass retroactively.",
        "",
        "## Rank Evidence",
        "",
        f"- default_rank0: `{default_rank_lines[0]}`",
        f"- default_rank1: `{default_rank_lines[1]}`",
        f"- direct_rank0: `{direct_rank_lines[0]}`",
        f"- direct_rank1: `{direct_rank_lines[1]}`",
    ]
    note.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok] hcomm payload direct-output diagnostic -> {note}")
    return note


def RunHcommPayloadDirectOutputDiagnostic(
        runner: Runner,
        base_command: list[str],
        env_updates: Optional[dict[str, str]],
        timeout_seconds: int,
        default_log: Optional[Path]) -> StepResult:
    command = [
        item for item in base_command
        if item != "--hcomm-payload-recv-direct-output"
    ]
    command.append("--hcomm-payload-recv-direct-output")
    result = runner.run(
        "hcomm-payload-direct-output-diagnostic",
        command,
        required=False,
        timeout_seconds=timeout_seconds,
        env_updates=env_updates,
    )
    if result.returncode != 0:
        WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)
    WriteHcommPayloadDirectOutputDiagnostic(
        runner.run_dir, default_log, result.log_path)
    return result


def StrictPayloadFailureAction(rank: int, failure_step: str,
                               primitive_state: str = "missing") -> str:
    prefix = f"inspect rank {rank} "
    if primitive_state == "pending":
        return (prefix + "pending HCOMM primitive timeout/hang at " +
                failure_step)
    actions = {
        "invalid-argument":
            "payload descriptor fields and ABI/status schema",
        "comm-acquire":
            "HcommAcquireComm path, HCCL comm name, and payload package "
            "libhcomm linkage",
        "batch-start":
            "HcommBatchModeStart compatibility for the selected AICPU_TS "
            "engine and batch tag",
        "batch-end":
            "HcommBatchModeEnd completion after host completion notify record",
        "host-aicpu-thread-notify-wait":
            "host/AICPU thread notify wait handles and launch ordering",
        "host-aicpu-thread-notify-record":
            "host/AICPU completion notify record handles after batch end",
        "local-copy":
            "HcommLocalCopyOnThread input HBM to local HCCL Buffer path",
        "ready-notify-record":
            "HCOMM ready notify record index and Channel descriptor",
        "ready-notify-wait":
            "HCOMM ready notify wait index, peer rank launch, and role pairing",
        "remote-read":
            "HcommReadOnThread remote HCCL Buffer to local HCCL Buffer path",
        "channel-fence":
            "HcommChannelFenceOnThread completion for RoCE/channel-drain mode",
        "output-copy":
            "local HCCL Buffer to user HBM output copy",
        "done-notify-record":
            "HCOMM done notify record index after recv-side output copy",
        "done-notify-wait":
            "HCOMM done notify wait index and recv rank completion",
        "comm-release":
            "HcommReleaseComm cleanup after payload primitives complete",
    }
    if failure_step in actions:
        return prefix + actions[failure_step]
    if failure_step == "none":
        return "inspect strict payload success markers and checksum evidence"
    return prefix + "in-kernel HCOMM primitive failure at " + failure_step


def AnalyzeNpuRuntimeDiagnostics(run_dir: Path,
                                  combined_smoke_text: str) -> tuple[str, str, str]:
    """Classify pre-payload NPU runtime failures without exposing host details."""
    texts = [combined_smoke_text]
    for pattern in ("*-npu-smi-info-m.log", "*-npu-topo-check.log"):
        for path in sorted(run_dir.glob(pattern)):
            texts.append(path.read_text(encoding="utf-8", errors="replace"))
    rank_dir = run_dir / "hccl-rank-logs"
    if rank_dir.is_dir():
        for path in sorted(rank_dir.glob("rank-*.log")):
            texts.append(path.read_text(encoding="utf-8", errors="replace"))
    joined = "\n".join(texts)

    driver_patterns = (
        r"dcmi module initialize failed",
        r"DrvMngGetConsoleLogLevel failed",
        r"get platform info failed",
        r"DestroyRuntimeImpl: soHandle or rt is nullptr",
    )
    if any(re.search(pattern, joined, re.IGNORECASE)
           for pattern in driver_patterns):
        return (
            "driver-runtime-unavailable",
            "`dcmi module initialize failed` or driver platform-info failure",
            "fix NPU driver/runtime visibility before rerunning strict-positive",
        )
    if re.search(r"root-info.*(missing|not produced|timeout)",
                 joined, re.IGNORECASE):
        return (
            "root-info-bringup-failed",
            "`root-info` bring-up did not complete",
            "inspect HCCL root-info bring-up before payload kernel evidence",
        )
    if rank_dir.is_dir() and (rank_dir / "rank-0.log").exists() and not (
            rank_dir / "rank-1.log").exists():
        return (
            "rank-launch-incomplete",
            "rank0 log exists but rank1 log is missing",
            "inspect multiprocess rank launch and device visibility",
        )
    if "hcomm payload smoke passed" in combined_smoke_text:
        return (
            "payload-smoke-ran",
            "`hcomm payload smoke passed` marker",
            "inspect strict payload markers",
        )
    if "hcomm payload smoke unsupported" in combined_smoke_text:
        return (
            "payload-smoke-unsupported",
            "`hcomm payload smoke unsupported` marker",
            "inspect payload package and launcher capability",
        )
    return ("unknown", "no NPU runtime preflight signal", "continue strict-positive")


def FindCannCompatFixtureDir(run_dir: Path) -> Optional[Path]:
    for log in sorted(run_dir.glob("*-collect-cann-compat.log"), reverse=True):
        try:
            text = log.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        match = re.search(r"CANN compatibility fixture -> (.+)", text)
        if not match:
            continue
        path = Path(match.group(1).strip())
        if path.exists():
            return path
    return None


def HcommPrimitiveAbiFixtureStatus(
        run_dir: Path) -> tuple[str, str, str, str]:
    fixture = FindCannCompatFixtureDir(run_dir)
    if fixture is None:
        return (
            "not-collected",
            "missing `collect-cann-compat` fixture",
            "run `tools/collect_cann_compat.py` with the latest strict log",
            "missing",
        )
    probe_path = fixture / "hcomm-primitive-call-shape-probe.txt"
    symbols_path = fixture / "hcomm-primitive-symbols.txt"
    try:
        probe_text = probe_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return (
            "missing",
            f"failed to read `{probe_path.name}`: {exc}",
            "rerun CANN compatibility fixture collection",
            str(fixture),
        )
    status_match = re.search(r"^status:\s*(\S+)", probe_text, re.MULTILINE)
    status = status_match.group(1) if status_match else "missing"
    if status == "PASS":
        result = "call-shape-pass"
        next_action = (
            "ABI call shape matches; continue debugging HCOMM primitive "
            "runtime ordering/batch semantics")
    elif status == "FAIL":
        result = "call-shape-fail"
        next_action = (
            "fix AICPU payload kernel primitive call signatures before "
            "rerunning strict-positive")
    else:
        result = "call-shape-unknown"
        next_action = "inspect hcomm-primitive-call-shape-probe.txt"
    symbol_summary = "missing"
    try:
        symbols_text = symbols_path.read_text(encoding="utf-8",
                                              errors="replace")
        missing = []
        present = []
        for name in (
                "HcommAcquireComm", "HcommLocalCopyOnThread",
                "HcommReadOnThread", "HcommBatchModeStart",
                "HcommBatchModeEnd"):
            state_match = re.search(rf"^{re.escape(name)}:\s*(\S+)",
                                    symbols_text, re.MULTILINE)
            state = state_match.group(1) if state_match else "missing"
            if state == "present":
                present.append(name)
            elif state == "missing":
                missing.append(name)
        symbol_summary = (
            f"present={','.join(present) or 'none'}; "
            f"missing={','.join(missing) or 'none'}")
    except OSError:
        symbol_summary = "hcomm-primitive-symbols.txt missing"
    evidence = (
        f"`{probe_path.name}` status `{status}`; {symbol_summary}; "
        f"fixture `{fixture}`")
    return result, evidence, next_action, str(fixture)


def RunCannCompatCollection(
        runner: Runner,
        args: argparse.Namespace,
        hccl_devices: list[str]) -> Optional[StepResult]:
    if not args.collect_cann_compat_label:
        return None
    return runner.run(
        "collect-cann-compat",
        [sys.executable, "tools/collect_cann_compat.py",
         "--label", args.collect_cann_compat_label,
         "--flume-log-dir", str(runner.run_dir),
         "--devices", ",".join(hccl_devices)],
        required=False,
        timeout_seconds=args.step_timeout_sec,
        env_updates=CannRuntimeEnvUpdates(args),
    )


def WriteMatrixDecisionTree(run_dir: Path, smoke_log: Optional[Path],
                             strict_log: Optional[Path],
                             package_log: Optional[Path]) -> Path:
    def read(path: Optional[Path]) -> str:
        if path is None:
            return ""
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def marker_state(text: str, name: str) -> str:
        match = re.search(rf"\b{re.escape(name)}=([^\s\"]+)", text)
        return match.group(1) if match else "missing"

    def marker_value(text: str, name: str) -> str:
        match = re.search(rf"\b{re.escape(name)}=([^\s\"]+)", text)
        return match.group(1) if match else "missing"

    def marker_value_from_line(line: str, name: str) -> str:
        match = re.search(rf"\b{re.escape(name)}=([^\s\"]+)", line)
        return match.group(1) if match else "missing"

    smoke = read(smoke_log)
    strict = read(strict_log)
    package = read(package_log)
    no_batch_log = FindStepLog(run_dir, ["hcomm-payload-nobatch-diagnostic"])
    no_batch = read(no_batch_log)
    tagged_log = FindStepLog(run_dir, ["hcomm-payload-tagged-diagnostic"])
    tagged = read(tagged_log)
    direct_output_log = FindStepLog(
        run_dir, ["hcomm-payload-direct-output-diagnostic"])
    direct_output = read(direct_output_log)
    no_comm_log = FindStepLog(
        run_dir, ["hcomm-payload-no-comm-acquire-diagnostic"])
    no_comm = read(no_comm_log)
    combined = smoke + "\n" + strict
    npu_runtime_status, npu_runtime_evidence, npu_runtime_next_action = (
        AnalyzeNpuRuntimeDiagnostics(run_dir, combined))
    hcomm_abi_status, hcomm_abi_evidence, hcomm_abi_next_action, _fixture = (
        HcommPrimitiveAbiFixtureStatus(run_dir))
    lines = [
        "# Flume Ascend Full Matrix Decision Tree",
        "",
        "| Check | Result | Evidence |",
        "| --- | --- | --- |",
    ]
    hccl_ok = "hccl collective smoke passed" in combined
    p2p_ok = ("p2p_copy=on" in combined and
              "hccl collective smoke passed" in combined)
    hcomm_channel_ok = "hcomm channel probe passed" in combined
    hcomm_custom_op_launch_ok = "hcomm custom-op launch smoke passed" in combined
    hcomm_custom_op_launch_unsupported = (
        "hcomm custom-op launch smoke unsupported" in combined)
    hcomm_resource_descriptor_ok = (
        "hcomm resource descriptor smoke passed" in combined)
    hcomm_resource_descriptor_unsupported = (
        "hcomm resource descriptor smoke unsupported" in combined)
    hcomm_notify_only_ok = "hcomm notify-only smoke passed" in combined
    hcomm_notify_only_unsupported = (
        "hcomm notify-only smoke unsupported" in combined)
    hcomm_payload_ok = "hcomm payload smoke passed" in combined
    hcomm_payload_unsupported = "hcomm payload smoke unsupported" in combined
    storage_hbm_ok = StorageHbmRank1Passed(combined)
    storage_hbm_path = ("hcomm-payload-staging" if
                        StorageHbmHcommPathPassed(combined) else
                        StorageHbmRank1Path(combined))
    strict_positive_ok, strict_rank0_ok, strict_rank1_ok = (
        StrictPayloadRankEvidencePassed(strict))
    strict_rank_lines = ExtractStrictPayloadRankLines(strict)
    strict_source_checksum = marker_value_from_line(
        strict_rank_lines[0], "payload_source_checksum")
    strict_payload_checksum = marker_value_from_line(
        strict_rank_lines[1], "payload_checksum")
    strict_expected_checksum = marker_value_from_line(
        strict_rank_lines[1], "payload_expected_checksum")
    strict_checksum_match = "missing"
    if (strict_source_checksum != "missing" and
            strict_payload_checksum != "missing" and
            strict_expected_checksum != "missing"):
        strict_checksum_match = (
            "yes" if strict_source_checksum == strict_payload_checksum ==
            strict_expected_checksum else "no")
    strict_negative_expected = (
        "hcomm-payload-strict-negative" in strict and
        ("HCOMM payload copy required but unavailable" in strict or
         "unsupported" in strict))
    caps_match = re.search(r"FLUME_BACKEND_CAPS .+", combined)
    caps = caps_match.group(0) if caps_match else "missing FLUME_BACKEND_CAPS"
    primitives = "unknown"
    if "hcomm_primitives=on" in caps:
        primitives = "present"
    elif "hcomm_primitives=off" in caps:
        primitives = "absent"
    scheduler_candidate = "hcomm_payload_scheduler_candidate=on" in caps
    scheduler_missing = (not hcomm_payload_ok and
                         ("hcomm_payload_scheduler=not-implemented" in caps or
                          "custom-op/AICPU scheduler" in combined or
                          "custom-op launch" in combined))
    package_payload_ready = PackageTextPayloadReady(package)
    package_canary_ready = PackageTextCanaryReady(package)
    package_status = "payload-ready" if package_payload_ready else (
        "canary-ready" if package_canary_ready else (
            "not-ready" if package else "not-checked"))
    package_reason = PackageTextReason(package) if package else "missing"
    package_source = marker_value(package, "root")
    package_vendor = marker_value(package, "vendor")
    package_tar = marker_value(package, "aicpu_tar")
    package_tar_so = marker_value(
        package, f"aicpu_tar_so.{HCOMM_CUSTOM_OP_KERNEL_SO}")
    strict_loader = marker_state(strict, "stage3b3e_direct_aclrt_payload_loader")
    strict_handoff = marker_state(strict, "stage3b3e_payload_descriptor_handoff")
    strict_launch = marker_state(strict, "stage3b3e_direct_aclrt_payload_launch")
    strict_sync = marker_state(strict, "stage3b3e_payload_sync")
    strict_resource_acquire = marker_value(strict, "payload_resource_acquire")
    strict_resource_step = marker_value(strict, "payload_resource_step")
    strict_resource_status = marker_value(strict, "payload_resource_status")
    strict_kernel = marker_value(strict, "payload_kernel_status")
    strict_failure_step = marker_value(strict, "payload_failure_step")
    strict_status_word = marker_value(strict, "payload_status_word")
    strict_hcomm_ret = marker_value(strict, "payload_kernel_hcomm_ret")
    strict_status_schema = marker_value(strict, "payload_status_schema")
    strict_status_word_count = marker_value(strict, "payload_status_word_count")
    strict_echo = marker_value(strict, "payload_echo")
    strict_trace = marker_value(strict, "payload_trace")
    strict_trace_event = marker_value(strict, "payload_trace_event")
    strict_trace_order = marker_value(strict, "payload_trace_order")
    strict_trace_result = marker_value(strict, "payload_trace_result")
    strict_rank0_role = marker_value_from_line(strict_rank_lines[0],
                                               "payload_role")
    strict_rank1_role = marker_value_from_line(strict_rank_lines[1],
                                               "payload_role")
    strict_pattern = marker_value(strict, "payload_pattern")
    strict_primitive_state = marker_value(strict, "payload_primitive_state")
    strict_desc_bytes = marker_value(strict, "payload_desc_bytes")
    strict_desc_ready_notify = marker_value(strict,
                                            "payload_desc_ready_notify_idx")
    strict_desc_done_notify = marker_value(strict,
                                           "payload_desc_done_notify_idx")
    strict_desc_completion = marker_value(strict,
                                          "payload_desc_completion_mode")
    strict_desc_thread_notify = marker_value(
        strict, "payload_desc_thread_notify_mode")
    strict_desc_local_buffer = marker_value(
        strict, "payload_desc_local_hccl_buffer_bytes")
    strict_desc_remote_buffer = marker_value(
        strict, "payload_desc_remote_hccl_buffer_bytes")
    strict_resolved_engine = marker_value(strict, "payload_resolved_engine")
    strict_resolved_protocol = marker_value(strict, "payload_resolved_protocol")
    strict_channel_desc = marker_value(strict, "payload_channel_desc")
    strict_channel_count = marker_value(strict, "payload_channel_count")
    strict_notify_num = marker_value(strict, "payload_notify_num")
    strict_usable_buffer = marker_value(
        strict, "payload_usable_hccl_buffer_bytes")
    strict_local_buffer = marker_value(strict, "payload_local_hccl_buffer_bytes")
    strict_remote_buffer = marker_value(
        strict, "payload_remote_hccl_buffer_bytes")
    strict_semantic = marker_value(strict, "payload_semantic")
    strict_semantic_v5 = marker_value(strict, "payload_semantic_v5")
    strict_semantic_v6 = marker_value(strict, "payload_semantic_v6")
    strict_semantic_v7 = marker_value(strict, "payload_semantic_v7")
    strict_semantic_v8 = marker_value(strict, "payload_semantic_v8")
    strict_build_mode = marker_value(strict, "payload_build_mode")
    strict_runtime_package_source = marker_value(strict, "package_source")
    strict_runtime_package_tar = marker_value(strict, "package_aicpu_tar")
    strict_runtime_package_tar_readable = marker_value(
        strict, "package_aicpu_tar_readable")
    strict_verify = marker_value(strict, "payload_verify")
    strict_fallback = marker_value(strict, "fallback")
    strict_batch_mode = marker_value(strict, "payload_batch_mode")
    strict_desc_batch_tag = marker_value(strict, "payload_desc_batch_tag")
    strict_recv_path = marker_value(strict, "payload_recv_path")
    no_batch_ok, no_batch_rank0_ok, no_batch_rank1_ok = (
        StrictPayloadNoBatchDiagnosticPassed(no_batch))
    no_batch_result = (
        "passed" if no_batch_ok else (
            "partial" if no_batch_rank0_ok or no_batch_rank1_ok else (
                "not-run" if not no_batch else "not-passed")))
    no_batch_failure_step = marker_value(no_batch, "payload_failure_step")
    no_batch_hcomm_ret = marker_value(no_batch, "payload_kernel_hcomm_ret")
    tagged_ok, tagged_rank0_ok, tagged_rank1_ok = (
        StrictPayloadRankEvidencePassed(tagged))
    tagged_result = (
        "passed" if tagged_ok else (
            "partial" if tagged_rank0_ok or tagged_rank1_ok else (
                "not-run" if not tagged else "not-passed")))
    tagged_failure_step = marker_value(tagged, "payload_failure_step")
    tagged_batch_tag = marker_value(tagged, "payload_desc_batch_tag")
    tagged_hcomm_ret = marker_value(tagged, "payload_kernel_hcomm_ret")
    direct_output_ok, direct_output_rank0_ok, direct_output_rank1_ok = (
        StrictPayloadRankEvidencePassed(direct_output))
    direct_output_recv_path = marker_value(direct_output, "payload_recv_path")
    direct_output_effective_ok = (
        direct_output_ok and direct_output_recv_path == "direct-output")
    direct_output_result = (
        "passed" if direct_output_effective_ok else (
            "partial" if direct_output_rank0_ok or direct_output_rank1_ok else (
                "not-run" if not direct_output else "not-passed")))
    direct_output_failure_step = marker_value(
        direct_output, "payload_failure_step")
    direct_output_hcomm_ret = marker_value(
        direct_output, "payload_kernel_hcomm_ret")
    no_comm_ok, no_comm_rank0_ok, no_comm_rank1_ok = (
        StrictPayloadNoCommAcquireDiagnosticPassed(no_comm))
    no_comm_result = (
        "passed" if no_comm_ok else (
            "partial" if no_comm_rank0_ok or no_comm_rank1_ok else (
                "not-run" if not no_comm else "not-passed")))
    no_comm_failure_step = marker_value(no_comm, "payload_failure_step")
    no_comm_hcomm_ret = marker_value(no_comm, "payload_kernel_hcomm_ret")
    no_comm_acquire = marker_value(no_comm, "payload_comm_acquire")
    rank_status = {}
    for rank in (0, 1):
        rank_line = strict_rank_lines[rank]
        rank_status[rank] = {
            "kernel": marker_value_from_line(rank_line, "payload_kernel_status"),
            "failure_step": marker_value_from_line(rank_line,
                                                   "payload_failure_step"),
            "status_word": marker_value_from_line(rank_line,
                                                  "payload_status_word"),
            "hcomm_ret": marker_value_from_line(rank_line,
                                                "payload_kernel_hcomm_ret"),
            "primitive_state": marker_value_from_line(
                rank_line, "payload_primitive_state"),
            "fallback": marker_value_from_line(rank_line, "fallback"),
        }
        rank_status[rank]["action"] = StrictPayloadFailureAction(
            rank, rank_status[rank]["failure_step"],
            rank_status[rank]["primitive_state"])

    lines.append(
        f"| HCCL collective ok? | {'yes' if hccl_ok else 'no'} | `{caps}` |")
    lines.append(
        f"| HCCL P2P fallback ok? | {'yes' if p2p_ok else 'no'} | `p2p_copy=on` marker |")
    lines.append(
        f"| HCOMM channel ok? | {'yes' if hcomm_channel_ok else 'no'} | `hcomm channel probe passed` marker |")
    lines.append(
        f"| HCOMM custom-op launch readiness? | "
        f"{'pass' if hcomm_custom_op_launch_ok else ('unsupported' if hcomm_custom_op_launch_unsupported else 'no signal')} | "
        "`hcomm custom-op launch smoke` marker |")
    lines.append(
        f"| HCOMM resource descriptor handoff readiness? | "
        f"{'pass' if hcomm_resource_descriptor_ok else ('unsupported' if hcomm_resource_descriptor_unsupported else 'no signal')} | "
        "`hcomm resource descriptor smoke` marker |")
    lines.append(
        f"| HCOMM notify-only kernel readiness? | "
        f"{'pass' if hcomm_notify_only_ok else ('unsupported' if hcomm_notify_only_unsupported else 'no signal')} | "
        "`hcomm notify-only smoke` marker |")
    lines.append(
        f"| HCOMM primitives present? | {primitives} | `hcomm_primitives` in caps |")
    lines.append(
        f"| HCOMM payload readiness? | "
        f"{'pass' if hcomm_payload_ok else ('unsupported' if hcomm_payload_unsupported else 'no signal')} | "
        "`hcomm payload smoke` marker |")
    lines.append(
        f"| HCOMM custom-op package payload-ready? | {package_status} | "
        "`hcomm-custom-op-package-preflight` log |")
    lines.append(
        f"| HCOMM custom-op package reason | {package_reason} | "
        "`reason=` from preflight log |")
    lines.append(
        f"| HCOMM custom-op package source | source={package_source}, "
        f"vendor={package_vendor}, tar={package_tar}, so={package_tar_so} | "
        "preflight package identity without absolute paths |")
    lines.append(
        f"| HCOMM primitive ABI fixture | {hcomm_abi_status} | "
        f"{hcomm_abi_evidence} |")
    lines.append(
        f"| NPU runtime ready for strict payload? | {npu_runtime_status} | "
        f"{npu_runtime_evidence} |")
    lines.append(
        f"| HCOMM payload scheduler candidate built? | "
        f"{'yes' if scheduler_candidate else 'no'} | "
        "`hcomm_payload_scheduler_candidate` in caps |")
    lines.append(
        f"| Storage to HBM path ok? | {'yes' if storage_hbm_ok else 'no'} | "
        f"`storage_hbm={storage_hbm_path}` marker |")
    lines.append(
        f"| Payload scheduler missing? | {'yes' if scheduler_missing else 'no'} | `hcomm_payload_scheduler` / scheduler detail |")
    lines.append(
        f"| Strict payload positive passed? | {'yes' if strict_positive_ok else 'no'} | "
        "`rank 0/1 passed` + `stage3b3e_payload_copy=passed` + "
        "`stage3b3e_direct_aclrt_payload_loader=passed` + "
        "`stage3b3e_payload_descriptor_handoff=passed` + "
        "`payload_kernel_status=success` + `payload_failure_step=none` + "
        "`payload_status_word=0` + "
        "`payload_kernel_hcomm_ret=0` + status schema markers + "
        "`payload_echo=passed` + `payload_trace=passed` + "
        "rank0 `payload_role=send` + "
        "rank1 `payload_role=recv` + `payload_batch_mode=on` + "
        "payload comm binding `comm-name|channel-handle` + "
        "`payload_desc_batch_tag=...` + "
        "`payload_thread_notify_order=...` + "
        "`payload_pattern=strict-v1` + checksum match + "
        "`payload_verify=passed` + "
        "`fallback=none` |")
    lines.append(
        f"| Strict payload negative expected? | {'yes' if strict_negative_expected else 'no'} | `hcomm-payload-strict-negative` log |")
    lines.append(
        f"| HCOMM payload no-batch diagnostic | {no_batch_result} | "
        f"batch={strict_batch_mode}, no-batch failure `{no_batch_failure_step}`, "
        f"hcomm ret `{no_batch_hcomm_ret}` |")
    lines.append(
        f"| HCOMM payload tagged-batch diagnostic | {tagged_result} | "
        f"tag={tagged_batch_tag}, tagged failure `{tagged_failure_step}`, "
        f"hcomm ret `{tagged_hcomm_ret}` |")
    lines.append(
        f"| HCOMM payload direct-output diagnostic | {direct_output_result} | "
        f"recv_path={direct_output_recv_path}, failure "
        f"`{direct_output_failure_step}`, hcomm ret "
        f"`{direct_output_hcomm_ret}` |")
    lines.append(
        f"| HCOMM payload no-comm-acquire diagnostic | {no_comm_result} | "
        f"comm_acquire={no_comm_acquire}, failure "
        f"`{no_comm_failure_step}`, hcomm ret `{no_comm_hcomm_ret}` |")
    if strict_log is not None:
        lines.extend([
            "",
            "| Strict Payload Stage | Result | Evidence |",
            "| --- | --- | --- |",
            f"| package preflight | {package_status} | canary + payload + ABI v4 + semantic + comm-acquire + status schema + HCOMM primitive deps + `status=PASS` |",
            f"| rank0 strict evidence | {'passed' if strict_rank0_ok else 'missing'} | rank0 line has launch/sync/kernel/status/hcomm-ret/fallback markers |",
            f"| rank1 strict evidence | {'passed' if strict_rank1_ok else 'missing'} | rank1 line has launch/sync/kernel/status/hcomm-ret/verify/fallback markers |",
            f"| rank0 kernel status | {rank_status[0]['kernel']} | `payload_kernel_status` on rank0 line, status word `{rank_status[0]['status_word']}` |",
            f"| rank1 kernel status | {rank_status[1]['kernel']} | `payload_kernel_status` on rank1 line, status word `{rank_status[1]['status_word']}` |",
            f"| rank0 kernel failure step | {rank_status[0]['failure_step']} | rank0 `payload_failure_step` |",
            f"| rank1 kernel failure step | {rank_status[1]['failure_step']} | rank1 `payload_failure_step` |",
            f"| rank0 kernel HCOMM ret | {rank_status[0]['hcomm_ret']} | rank0 `payload_kernel_hcomm_ret` |",
            f"| rank1 kernel HCOMM ret | {rank_status[1]['hcomm_ret']} | rank1 `payload_kernel_hcomm_ret` |",
            f"| rank0 primitive state | {rank_status[0]['primitive_state']} | rank0 `payload_primitive_state`; `pending` means the primitive was entered but did not return before status read |",
            f"| rank1 primitive state | {rank_status[1]['primitive_state']} | rank1 `payload_primitive_state`; `pending` means the primitive was entered but did not return before status read |",
            f"| rank0 suggested action | {rank_status[0]['action']} | stage-specific HCOMM payload diagnostic hint |",
            f"| rank1 suggested action | {rank_status[1]['action']} | stage-specific HCOMM payload diagnostic hint |",
            f"| payload resource acquisition | {strict_resource_acquire} | step={strict_resource_step}, status={strict_resource_status}; this runs before direct ACL package load/launch |",
            f"| payload loader | {strict_loader} | `stage3b3e_direct_aclrt_payload_loader` |",
            f"| descriptor handoff | {strict_handoff} | `stage3b3e_payload_descriptor_handoff` |",
            f"| direct ACL payload launch | {strict_launch} | `stage3b3e_direct_aclrt_payload_launch` |",
            f"| stream sync | {strict_sync} | `stage3b3e_payload_sync` |",
            f"| kernel status | {strict_kernel} | `payload_kernel_status`, status word `{strict_status_word}` |",
            f"| kernel failure step | {strict_failure_step} | `payload_failure_step` maps status word to a HCOMM stage |",
            f"| kernel HCOMM ret | {strict_hcomm_ret} | `payload_kernel_hcomm_ret` must be `0` on success |",
            f"| primitive state | {strict_primitive_state} | `payload_primitive_state`; `pending` points to a primitive timeout/hang |",
            f"| host descriptor fingerprint | bytes={strict_desc_bytes}, ready={strict_desc_ready_notify}, done={strict_desc_done_notify}, completion={strict_desc_completion}, thread_notify={strict_desc_thread_notify}, batch_tag={strict_desc_batch_tag}, recv_path={strict_recv_path}, local_buffer={strict_desc_local_buffer}, remote_buffer={strict_desc_remote_buffer} | `payload_desc_*` fields passed to the direct ACL kernel |",
            f"| HCOMM resource fingerprint | engine={strict_resolved_engine}, protocol={strict_resolved_protocol}, channel_desc={strict_channel_desc}, channels={strict_channel_count}, notify_num={strict_notify_num}, usable={strict_usable_buffer}, local={strict_local_buffer}, remote={strict_remote_buffer} | resource selected before direct ACL payload launch |",
            f"| payload status schema | {strict_status_schema} / {strict_status_word_count} | `payload_status_schema` and `payload_status_word_count` |",
            f"| payload descriptor echo | {strict_echo} | `payload_echo` must be `passed` so the kernel confirms role/peer/bytes |",
            f"| payload primitive trace | {strict_trace} | event={strict_trace_event}, order={strict_trace_order}, result={strict_trace_result}; trace must end at `kernel-exit` with expected HCOMM primitive order and success |",
            f"| payload role evidence | rank0={strict_rank0_role}, rank1={strict_rank1_role} | rank0 must report `payload_role=send`; rank1 must report `payload_role=recv` |",
            f"| payload batch tag | {strict_desc_batch_tag} | expected `default` or an explicit `custom` tag; `missing` or `empty` means descriptor evidence is incomplete |",
            f"| payload test pattern | {strict_pattern} | `payload_pattern=strict-v1` proves strict smoke used its dedicated source data pattern |",
            f"| payload checksum match | {strict_checksum_match} | source `{strict_source_checksum}`, received `{strict_payload_checksum}`, expected `{strict_expected_checksum}` |",
            f"| payload semantic marker | {strict_semantic} | `payload_semantic=missing` means stale package |",
            f"| payload semantic v5 marker | {strict_semantic_v5} | `payload_semantic_v5=missing` means the package predates the current recv local-buffer scheduler |",
            f"| payload semantic v6 marker | {strict_semantic_v6} | `payload_semantic_v6=missing` means the package predates the direct-output scheduler contract |",
            f"| payload semantic v7 marker | {strict_semantic_v7} | `payload_semantic_v7=missing` means the package predates the device trace contract |",
            f"| payload semantic v8 marker | {strict_semantic_v8} | `payload_semantic_v8=missing` means the package predates ordered primitive trace validation |",
            f"| payload build mode | {strict_build_mode} | `payload_build_mode=not-internal` means canary/stub package |",
            f"| runtime package identity | source={strict_runtime_package_source}, tar={strict_runtime_package_tar}, readable={strict_runtime_package_tar_readable} | package probe attached to the C++ direct ACL launcher detail |",
            f"| rank1 verify | {strict_verify} | `payload_verify` |",
            f"| fallback | {strict_fallback} | expected `none` for real HCOMM payload copy |",
        ])
    if strict_positive_ok:
        next_action = (
            "Stage 3B.3E strict payload copy passed; inspect checksum and "
            "start Stage 3B.4 storage rewiring")
    elif npu_runtime_status in (
            "driver-runtime-unavailable", "root-info-bringup-failed",
            "rank-launch-incomplete"):
        next_action = npu_runtime_next_action
    elif package and not package_payload_ready:
        next_action = PackageTextNextAction(package)
    elif (hccl_ok and p2p_ok and hcomm_channel_ok and package_payload_ready and
          (storage_hbm_ok or strict_log is not None)):
        if strict_resource_step != "missing":
            next_action = (
                "fix HCOMM payload resource acquisition before debugging the "
                f"custom-op package: step `{strict_resource_step}`, "
                f"status `{strict_resource_status}`")
        elif strict_loader != "passed":
            if strict_build_mode == "not-internal":
                next_action = (
                    "rebuild/reinstall custom-op package in payload mode; "
                    "installed package is canary/stub-only")
            elif strict_semantic_v5 == "missing":
                next_action = (
                    "rebuild/reinstall payload custom-op package with current "
                    "Flume semantic v5 kernel")
            elif strict_semantic_v6 == "missing":
                next_action = (
                    "rebuild/reinstall payload custom-op package with current "
                    "Flume semantic v6 direct-output-capable kernel")
            elif strict_semantic_v7 == "missing":
                next_action = (
                    "rebuild/reinstall payload custom-op package with current "
                    "Flume semantic v7 device-trace-capable kernel")
            elif strict_semantic_v8 == "missing":
                next_action = (
                    "rebuild/reinstall payload custom-op package with current "
                    "Flume semantic v8 ordered-trace-capable kernel")
            elif strict_semantic == "missing":
                next_action = (
                    "rebuild/reinstall payload custom-op package; semantic "
                    "marker is missing")
            else:
                next_action = "inspect payload custom-op package loading"
        elif strict_handoff != "passed":
            next_action = "inspect direct ACL payload descriptor handoff"
        elif strict_launch != "passed":
            next_action = "inspect direct ACL payload launch"
        elif tagged_ok and strict_batch_mode in ("on", "missing"):
            next_action = (
                "tagged HCOMM payload copy passed; rerun strict-positive with "
                "the same --hcomm-payload-batch-tag and then wire Stage 3B.4 "
                "storage")
        elif direct_output_effective_ok and strict_recv_path != "direct-output":
            next_action = (
                "direct-output HCOMM payload copy passed; rerun "
                "strict-positive with --hcomm-payload-recv-direct-output and "
                "use that green strict gate as Stage 3B.3E evidence")
        elif no_batch_ok and strict_batch_mode in ("on", "missing"):
            next_action = (
                "no-batch HCOMM payload copy passed; inspect "
                "HcommBatchModeStart/End submit, ordering, and selected "
                "engine batch-mode compatibility")
        elif no_comm_ok:
            next_action = (
                "no-comm-acquire HCOMM payload copy passed; inspect "
                "HcommAcquireComm/HcommReleaseComm, HCCL comm-name binding, "
                "and whether this CANN build expects ChannelHandle-only "
                "payload primitives")
        elif any(rank_status[rank]["primitive_state"] == "pending"
                 for rank in (0, 1)):
            bad_rank = next(
                rank for rank in (0, 1)
                if rank_status[rank]["primitive_state"] == "pending")
            next_action = rank_status[bad_rank]["action"]
        elif any(rank_status[rank]["failure_step"] == "output-copy"
                 for rank in (0, 1)):
            bad_rank = next(
                rank for rank in (0, 1)
                if rank_status[rank]["failure_step"] == "output-copy")
            if strict_recv_path != "direct-output":
                next_action = (
                    "rerun strict-positive with "
                    "--hcomm-payload-recv-direct-output to test the official "
                    "custom P2P-style recv path that reads directly into the "
                    "output HBM buffer")
            else:
                next_action = rank_status[bad_rank]["action"]
        elif strict_sync != "passed":
            next_action = "inspect payload stream sync or kernel hang"
        elif any(rank_status[rank]["kernel"] not in ("success", "missing")
                 for rank in (0, 1)):
            bad_rank = next(
                rank for rank in (0, 1)
                if rank_status[rank]["kernel"] not in ("success", "missing"))
            next_action = rank_status[bad_rank]["action"]
        elif any(rank_status[rank]["hcomm_ret"] not in ("0", "missing")
                 for rank in (0, 1)):
            bad_rank = next(
                rank for rank in (0, 1)
                if rank_status[rank]["hcomm_ret"] not in ("0", "missing"))
            next_action = (
                "inspect rank "
                f"{bad_rank} in-kernel HCOMM primitive return code: "
                f"{rank_status[bad_rank]['hcomm_ret']}")
        elif strict_kernel not in ("success", "missing"):
            next_action = (
                "inspect in-kernel HCOMM primitive failure: "
                f"{strict_kernel} at {strict_failure_step}")
        elif strict_hcomm_ret not in ("0", "missing"):
            next_action = (
                "inspect in-kernel HCOMM primitive return code: "
                f"{strict_hcomm_ret}")
        elif strict_verify not in ("passed", "missing"):
            next_action = "inspect rank1 payload verification mismatch"
        elif strict_checksum_match not in ("yes", "missing"):
            next_action = "inspect payload checksum mismatch"
        elif strict_fallback not in ("none", "missing"):
            next_action = "remove unexpected fallback from strict payload path"
        elif strict_desc_batch_tag in ("missing", "empty"):
            next_action = (
                "inspect HCOMM payload batch tag descriptor fill; expected "
                "the stable default tag or an explicit custom tag")
        elif hcomm_abi_status in (
                "not-collected", "missing", "call-shape-fail",
                "call-shape-unknown"):
            next_action = hcomm_abi_next_action
        else:
            next_action = "inspect hcomm-payload-strict-positive failure"
    elif (hccl_ok and p2p_ok and hcomm_channel_ok and storage_hbm_ok and
          hcomm_payload_unsupported and not package_payload_ready):
        next_action = PackageTextNextAction(package)
    else:
        next_action = "inspect first failed required matrix step"
    lines.extend(["", f"next action: {next_action}", ""])
    path = run_dir / "ASCEND_FULL_MATRIX_DECISION_TREE.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[ok] matrix decision tree -> {path}")
    return path


def FindLatestRunDir(log_root: Path) -> Optional[Path]:
    if log_root.is_dir() and log_root.name.startswith("flume-check-"):
        return log_root
    if not log_root.exists():
        return None
    candidates = sorted(
        item for item in log_root.glob("flume-check-*") if item.is_dir())
    return candidates[-1] if candidates else None


def FindStepLog(run_dir: Path, step_names: Iterable[str]) -> Optional[Path]:
    for step_name in step_names:
        matches = sorted(run_dir.glob(f"*-{step_name}.log"))
        if matches:
            return matches[-1]
    return None


def DecisionTreeStrictPositivePassed(tree_path: Path) -> bool:
    try:
        text = tree_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return "| Strict payload positive passed? | yes |" in text


def DecisionTreeHcommStoragePassed(tree_path: Path) -> bool:
    try:
        text = tree_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return (
        DecisionTreeStrictPositivePassed(tree_path) and
        "| Storage to HBM path ok? | yes | `storage_hbm=hcomm-payload-staging` marker |"
        in text)


def AnalyzeHcommPayloadStrictPositiveLogs(
        run_dir: Path) -> tuple[Path, bool, Optional[Path], Optional[Path],
                                Optional[Path]]:
    smoke_log = FindStepLog(run_dir, ["hccl-collective-smoke"])
    strict_log = FindStepLog(run_dir, ["hcomm-payload-strict-positive"])
    package_log = FindStepLog(run_dir, ["hcomm-custom-op-package-preflight"])
    tree = WriteMatrixDecisionTree(run_dir, smoke_log, strict_log, package_log)
    if not DecisionTreeStrictPositivePassed(tree):
        channel_log = FindStepLog(
            run_dir, ["hcomm-payload-channel-handle-candidate"])
        if channel_log is not None:
            channel_tree = WriteMatrixDecisionTree(
                run_dir, smoke_log, channel_log, package_log)
            if DecisionTreeStrictPositivePassed(channel_tree):
                return (channel_tree, True, smoke_log, channel_log,
                        package_log)
    return (tree, DecisionTreeStrictPositivePassed(tree), smoke_log,
            strict_log, package_log)


def AnalyzeHcommStorageStrictPositiveLogs(
        run_dir: Path) -> tuple[Path, bool, Optional[Path], Optional[Path],
                                Optional[Path]]:
    smoke_log = FindStepLog(run_dir, ["hccl-collective-smoke"])
    strict_log = FindStepLog(run_dir, ["hcomm-storage-strict-positive"])
    package_log = FindStepLog(run_dir, ["hcomm-custom-op-package-preflight"])
    tree = WriteMatrixDecisionTree(run_dir, smoke_log, strict_log, package_log)
    if not DecisionTreeHcommStoragePassed(tree):
        channel_log = FindStepLog(
            run_dir, ["hcomm-payload-channel-handle-candidate"])
        if channel_log is not None:
            channel_tree = WriteMatrixDecisionTree(
                run_dir, smoke_log, channel_log, package_log)
            if DecisionTreeHcommStoragePassed(channel_tree):
                return (channel_tree, True, smoke_log, channel_log,
                        package_log)
    return (tree, DecisionTreeHcommStoragePassed(tree), smoke_log,
            strict_log, package_log)


def RecordStrictPositiveEvidenceGate(runner: Runner, tree: Path, passed: bool,
                                     *, required: bool) -> StepResult:
    lines = [
        f"decision_tree={tree}",
        f"strict_positive_evidence={'passed' if passed else 'failed'}",
    ]
    if not passed:
        lines.append(
            "reason=missing complete Stage 3B.3E strict-positive evidence")
        lines.append(
            "required_markers=rank0/1 passed,stage3b3e_payload_copy=passed,"
            "stage3b3e_direct_aclrt_payload_loader=passed,"
            "stage3b3e_payload_descriptor_handoff=passed,"
            "stage3b3e_direct_aclrt_payload_launch=passed,"
            "stage3b3e_payload_sync=passed,payload_kernel_status=success,"
            "payload_failure_step=none,payload_status_word=0,"
            "payload_kernel_hcomm_ret=0,"
            "payload_status_schema=v2,payload_status_word_count=8,"
            "payload_echo=passed,payload_role=send/recv,"
            "payload_primitive_state=completed,payload_trace=passed,"
            "payload_trace_event=kernel-exit,payload_trace_order=passed,"
            "payload_trace_result=success,payload_desc_batch_tag=,"
            "payload_recv_path=,payload_semantic_v6=present,"
            "payload_semantic_v7=present,payload_semantic_v8=present,"
            "payload_batch_mode=on,"
            "payload_comm_acquire=default,"
            "or payload_comm_binding=channel-handle,"
            "payload_thread_notify_order=,"
            "payload_pattern=strict-v1,"
            "payload_source_checksum=,"
            "payload_checksum=,payload_expected_checksum=,"
            "payload_verify=passed,fallback=none")
    return runner.record_static("hcomm-payload-strict-evidence", lines,
                                returncode=0 if passed else 1,
                                required=required)


def RecordHcommStorageEvidenceGate(runner: Runner, tree: Path,
                                   passed: bool) -> StepResult:
    lines = [
        f"decision_tree={tree}",
        f"hcomm_storage_evidence={'passed' if passed else 'failed'}",
    ]
    if not passed:
        lines.append(
            "reason=missing Stage 3B.4 HCOMM storage evidence")
        lines.append(
            "required_markers=strict-positive passed,"
            "storage_hbm=hcomm-payload-staging,storage HBM smoke passed")
    return runner.record_static("hcomm-storage-strict-evidence", lines,
                                returncode=0 if passed else 1,
                                required=True)


def run_hcomm_payload_verify_logs(args: argparse.Namespace) -> int:
    root = (Path(args.log_dir).expanduser() if args.log_dir else
            Path(args.log_root))
    run_dir = FindLatestRunDir(root)
    if run_dir is None:
        print(f"[failed] no flume-check log directory found under {root}")
        return 2
    tree, passed, smoke_log, strict_log, package_log = (
        AnalyzeHcommPayloadStrictPositiveLogs(run_dir))
    print(f"[ok] analyzed log dir -> {run_dir}")
    print(f"[ok] strict log -> {strict_log if strict_log else '<missing>'}")
    print(f"[ok] smoke log -> {smoke_log if smoke_log else '<missing>'}")
    print(f"[ok] package log -> {package_log if package_log else '<missing>'}")
    if passed:
        print("[ok] strict-positive evidence -> passed")
        return 0
    print("[failed] strict-positive evidence -> incomplete or failed")
    print(f"[failed] inspect -> {tree}")
    return 1


def run_hcomm_storage_verify_logs(args: argparse.Namespace) -> int:
    root = (Path(args.log_dir).expanduser() if args.log_dir else
            Path(args.log_root))
    run_dir = FindLatestRunDir(root)
    if run_dir is None:
        print(f"[failed] no flume-check log directory found under {root}")
        return 2
    tree, passed, smoke_log, strict_log, package_log = (
        AnalyzeHcommStorageStrictPositiveLogs(run_dir))
    print(f"[ok] analyzed log dir -> {run_dir}")
    print(f"[ok] storage strict log -> {strict_log if strict_log else '<missing>'}")
    print(f"[ok] smoke log -> {smoke_log if smoke_log else '<missing>'}")
    print(f"[ok] package log -> {package_log if package_log else '<missing>'}")
    if passed:
        print("[ok] hcomm storage evidence -> passed")
        return 0
    print("[failed] hcomm storage evidence -> incomplete or failed")
    print(f"[failed] inspect -> {tree}")
    return 1


def run_ascend_full_matrix(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=True, timeout_seconds=args.step_timeout_sec,
               env_updates=CannRuntimeEnvUpdates(args))
    hccl_devices = ParseDeviceList(args.hccl_devices) if args.hccl_devices else []
    if len(hccl_devices) != 2:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "ascend-full-matrix requires exactly two --hccl-devices entries\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1
    if shutil.which("npu-smi"):
        runner.run("npu-smi-info-m", ["npu-smi", "info", "-m"],
                   required=False, timeout_seconds=args.step_timeout_sec)
        runner.run(
            "npu-topo-check",
            [sys.executable, "tools/flume_npu_topo_check.py",
             f"--devices={','.join(hccl_devices)}"],
            required=False,
            timeout_seconds=args.step_timeout_sec,
        )
    runtime_json_ok, runtime_json_error = ValidateRuntimeCustomOpJson(args)
    if not runtime_json_ok:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(runtime_json_error + "\n", encoding="utf-8")
        print(f"[failed] command setup -> {setup_log}")
        return 1
    package_result = runner.run(
        "hcomm-custom-op-package-preflight",
        HcommCustomOpPackageCommand(args, require_payload=True),
        required=False,
        timeout_seconds=args.step_timeout_sec,
    )
    args, package_result = MaybeAutoBuildPayloadPackage(
        runner, args, package_result)
    package_text = ""
    try:
        package_text = package_result.log_path.read_text(
            encoding="utf-8", errors="replace")
    except OSError:
        package_text = ""
    package_payload_ready = PackageTextPayloadReady(package_text)

    matrix_args = copy.copy(args)
    matrix_args.run_hccl_smoke = False
    matrix_args.run_a3_symmetric_smoke = False
    matrix_args.run_hccl_p2p_smoke = True
    matrix_args.run_hcomm_channel_probe = True
    matrix_args.run_hcomm_custom_op_launch_smoke = True
    matrix_args.run_hcomm_resource_descriptor_smoke = True
    matrix_args.run_hcomm_notify_only_smoke = True
    matrix_args.run_hcomm_payload_smoke = True
    matrix_args.run_storage_hbm_smoke = True
    matrix_args.hcomm_require_payload_copy = False
    matrix_args.hcomm_require_thread_export = False
    try:
        command_specs = build_commands(matrix_args, enable_hccl=True,
                                       run_dir=runner.run_dir)
    except RuntimeError as exc:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(str(exc) + "\n", encoding="utf-8")
        print(f"[failed] command setup -> {setup_log}")
        return 1

    WriteHcclSmokeSetupNotes(
        runner.run_dir,
        CollectHcclSmokeSetupNotes(matrix_args, ResolveHcclInitMode(matrix_args)),
    )

    smoke_result: Optional[StepResult] = None
    smoke_spec: Optional[CommandSpec] = None
    for spec in command_specs:
        timeout = (args.hccl_smoke_timeout_sec if spec.name == "hccl-collective-smoke"
                   else args.step_timeout_sec)
        result = runner.run(spec.name, spec.command, required=spec.required,
                            timeout_seconds=timeout, env_updates=spec.env_updates)
        if spec.name == "hccl-collective-smoke":
            smoke_result = result
            smoke_spec = spec
            if result.returncode != 0:
                WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)

    strict_result: Optional[StepResult] = None
    strict_tree_log: Optional[Path] = None
    if smoke_spec is not None:
        strict_command = list(smoke_spec.command)
        strict_command.append("--hcomm-require-payload-copy")
        allow_channel_candidate = (
            args.auto_run_hcomm_payload_channel_handle_candidate and
            package_payload_ready and
            not CommandUsesChannelHandleBinding(strict_command))
        strict_result = runner.run(
            "hcomm-payload-strict-positive" if package_payload_ready
            else "hcomm-payload-strict-negative",
            strict_command,
            required=package_payload_ready and not allow_channel_candidate,
            timeout_seconds=args.hccl_smoke_timeout_sec,
            env_updates=smoke_spec.env_updates,
        )
        strict_tree_log = strict_result.log_path
        if strict_result.returncode != 0:
            WriteHcclSmokeDiagnostics(runner.run_dir, strict_result.log_path)
            if allow_channel_candidate:
                channel_result = RunHcommPayloadChannelHandleCandidate(
                    runner, strict_command, smoke_spec.env_updates,
                    args.hccl_smoke_timeout_sec, strict_result.log_path)
                if StrictPayloadLogPassed(channel_result.log_path):
                    strict_tree_log = channel_result.log_path
            if (args.auto_run_hcomm_payload_nobatch_diagnostic and
                    package_payload_ready and
                    "--hcomm-payload-disable-batch" not in strict_command):
                RunHcommPayloadNoBatchDiagnostic(
                    runner, strict_command, smoke_spec.env_updates,
                    args.hccl_smoke_timeout_sec, strict_result.log_path)
            if (args.auto_run_hcomm_payload_tagged_diagnostic and
                    package_payload_ready):
                RunHcommPayloadTaggedDiagnostic(
                    runner, strict_command, smoke_spec.env_updates,
                    args.hccl_smoke_timeout_sec, strict_result.log_path,
                    args.hcomm_payload_diagnostic_batch_tag)
            if (args.auto_run_hcomm_payload_direct_output_diagnostic and
                    package_payload_ready and
                    "--hcomm-payload-recv-direct-output" not in strict_command):
                RunHcommPayloadDirectOutputDiagnostic(
                    runner, strict_command, smoke_spec.env_updates,
                    args.hccl_smoke_timeout_sec, strict_result.log_path)
            if (args.auto_run_hcomm_payload_no_comm_acquire_diagnostic and
                    package_payload_ready and
                    "--hcomm-payload-skip-comm-acquire" not in strict_command):
                RunHcommPayloadNoCommAcquireDiagnostic(
                    runner, strict_command, smoke_spec.env_updates,
                    args.hccl_smoke_timeout_sec, strict_result.log_path)

    RunCannCompatCollection(runner, args, hccl_devices)

    tree = WriteMatrixDecisionTree(
        runner.run_dir,
        smoke_result.log_path if smoke_result is not None else None,
        strict_tree_log,
        package_result.log_path,
    )
    RecordStrictPositiveEvidenceGate(
        runner, tree, DecisionTreeStrictPositivePassed(tree),
        required=package_payload_ready)
    note = runner.run_dir / "ASCEND_FULL_MATRIX_SCOPE.txt"
    note.write_text(
        "ascend-full-matrix builds once, runs local tests/sim, then runs a "
        "two-rank root-info smoke with HCCL collective, HCCL P2P fallback, "
        "HCOMM channel resource probe, and HCOMM payload readiness. It then "
        "runs --hcomm-require-payload-copy as a required positive check when "
        "the Stage 3B.3E payload package is installed, or as an optional "
        "expected negative while the package is not ready. When "
        "--auto-run-hcomm-payload-channel-handle-candidate is enabled, a "
        "failed default comm-name strict run triggers an explicit "
        "channel-handle backend candidate; that candidate can satisfy the "
        "strict evidence gate only with complete payload copy, checksum, "
        "trace, and fallback=none markers. Before "
        "the smoke, it runs hcomm-custom-op-package-preflight to record "
        "whether the installed package is canary-ready or payload-ready. The "
        "matrix also runs Stage 3A storage_hbm=hccl-p2p-staging: rank0 reads "
        "a local file slice into proxy HBM and sends it to rank1 compute HBM "
        "with HcclSend/HcclRecv. This validates storage integration plumbing, "
        "not full storage-direct DMA.\n",
        encoding="utf-8",
    )
    print(f"[ok] matrix scope -> {note}")
    return runner.write_summary()


def run_hcomm_payload_strict_positive(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    hccl_devices = ParseDeviceList(args.hccl_devices) if args.hccl_devices else []
    if len(hccl_devices) != 2:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "hcomm-payload-strict-positive requires exactly two "
            "--hccl-devices entries\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=True, timeout_seconds=args.step_timeout_sec,
               env_updates=CannRuntimeEnvUpdates(args))
    if shutil.which("npu-smi"):
        runner.run("npu-smi-info-m", ["npu-smi", "info", "-m"],
                   required=False, timeout_seconds=args.step_timeout_sec)
        runner.run(
            "npu-topo-check",
            [sys.executable, "tools/flume_npu_topo_check.py",
             f"--devices={','.join(hccl_devices)}"],
            required=False,
            timeout_seconds=args.step_timeout_sec,
        )

    runtime_json_ok, runtime_json_error = ValidateRuntimeCustomOpJson(args)
    if not runtime_json_ok:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(runtime_json_error + "\n", encoding="utf-8")
        print(f"[failed] command setup -> {setup_log}")
        return 1

    package_result = runner.run(
        "hcomm-custom-op-package-preflight",
        HcommCustomOpPackageCommand(args, require_payload=True),
        required=not args.auto_build_hcomm_payload_package,
        timeout_seconds=args.step_timeout_sec,
    )
    args, package_result = MaybeAutoBuildPayloadPackage(
        runner, args, package_result)
    if package_result.returncode != 0:
        next_steps = WritePayloadPackageBuildNextSteps(runner.run_dir, args)
        print(f"[ok] payload package next steps -> {next_steps}")
        note = runner.run_dir / "HCOMM_PAYLOAD_STRICT_POSITIVE_SCOPE.txt"
        note.write_text(
            "hcomm-payload-strict-positive stopped before launch because the "
            "installed Flume HCOMM custom-op package is not payload-ready. "
            "The package must declare and export the V4 direct ACL payload "
            "kernel and the primitive-payload build marker. See "
            "HCOMM_PAYLOAD_PACKAGE_NEXT_STEPS.txt for the build/install "
            "command to run before retrying strict-positive.\n",
            encoding="utf-8",
        )
        print(f"[ok] strict-positive scope -> {note}")
        return runner.write_summary()

    strict_args = copy.copy(args)
    strict_args.build_hcomm_custom_op = True
    strict_args.run_hccl_smoke = False
    strict_args.run_a3_symmetric_smoke = False
    strict_args.run_hccl_p2p_smoke = True
    strict_args.run_hcomm_channel_probe = True
    strict_args.run_hcomm_custom_op_launch_smoke = False
    strict_args.run_hcomm_resource_descriptor_smoke = False
    strict_args.run_hcomm_notify_only_smoke = False
    strict_args.run_hcomm_payload_smoke = True
    strict_args.run_storage_hbm_smoke = False
    strict_args.hcomm_require_payload_copy = True
    strict_args.hcomm_require_thread_export = False

    try:
        command_specs = build_commands(strict_args, enable_hccl=True,
                                       run_dir=runner.run_dir)
    except RuntimeError as exc:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(str(exc) + "\n", encoding="utf-8")
        print(f"[failed] command setup -> {setup_log}")
        return 1

    WriteHcclSmokeSetupNotes(
        runner.run_dir,
        CollectHcclSmokeSetupNotes(strict_args,
                                   ResolveHcclInitMode(strict_args)),
    )

    strict_result: Optional[StepResult] = None
    strict_tree_log: Optional[Path] = None
    for spec in command_specs:
        timeout = (args.hccl_smoke_timeout_sec if spec.name == "hccl-collective-smoke"
                   else args.step_timeout_sec)
        step_name = ("hcomm-payload-strict-positive"
                     if spec.name == "hccl-collective-smoke"
                     else spec.name)
        allow_channel_candidate = (
            step_name == "hcomm-payload-strict-positive" and
            args.auto_run_hcomm_payload_channel_handle_candidate and
            not CommandUsesChannelHandleBinding(spec.command))
        result = runner.run(step_name, spec.command,
                            required=spec.required and
                            not allow_channel_candidate,
                            timeout_seconds=timeout,
                            env_updates=spec.env_updates)
        if step_name == "hcomm-payload-strict-positive":
            strict_result = result
            strict_tree_log = result.log_path
            if result.returncode != 0:
                WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)
                if allow_channel_candidate:
                    channel_result = RunHcommPayloadChannelHandleCandidate(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path)
                    if StrictPayloadLogPassed(channel_result.log_path):
                        strict_tree_log = channel_result.log_path
                if (args.auto_run_hcomm_payload_nobatch_diagnostic and
                        not args.hcomm_payload_disable_batch and
                        "--hcomm-payload-disable-batch" not in spec.command):
                    RunHcommPayloadNoBatchDiagnostic(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path)
                if args.auto_run_hcomm_payload_tagged_diagnostic:
                    RunHcommPayloadTaggedDiagnostic(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path,
                        args.hcomm_payload_diagnostic_batch_tag)
                if (args.auto_run_hcomm_payload_direct_output_diagnostic and
                        "--hcomm-payload-recv-direct-output" not in spec.command):
                    RunHcommPayloadDirectOutputDiagnostic(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path)
                if (args.auto_run_hcomm_payload_no_comm_acquire_diagnostic and
                        not args.hcomm_payload_skip_comm_acquire and
                        "--hcomm-payload-skip-comm-acquire" not in spec.command):
                    RunHcommPayloadNoCommAcquireDiagnostic(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path)

    RunCannCompatCollection(runner, args, hccl_devices)

    tree = WriteMatrixDecisionTree(
        runner.run_dir,
        None,
        strict_tree_log,
        package_result.log_path,
    )
    RecordStrictPositiveEvidenceGate(
        runner, tree, DecisionTreeStrictPositivePassed(tree), required=True)
    note = runner.run_dir / "HCOMM_PAYLOAD_STRICT_POSITIVE_SCOPE.txt"
    note.write_text(
        "hcomm-payload-strict-positive is the focused Stage 3B.3E gate. It "
        "requires a payload-ready Flume custom-op package, configures Flume "
        "with FLUME_BUILD_HCOMM_CUSTOM_OP=ON, runs the HCCL P2P baseline, and "
        "then requires real HCOMM payload copy. Success requires both ranks "
        "to pass with stage3b3e_payload_copy=passed, direct ACL payload launch/"
        "sync passed, payload_kernel_status=success, payload_failure_step=none, "
        "payload_status_word=0, payload_kernel_hcomm_ret=0, status schema "
        "markers, payload_echo=passed, payload_primitive_state=completed, "
        "payload_trace=passed, payload_trace_event=kernel-exit, "
        "payload_trace_order=passed, "
        "payload_trace_primitive_path=send-local-copy|recv-read-*, "
        "payload_trace_result=success, "
        "payload_comm_binding=comm-name with payload_comm_acquire=default, "
        "or explicit payload_comm_binding=channel-handle, "
        "payload_desc_batch_tag=default|custom, payload_semantic_v7=present, "
        "payload_semantic_v8=present, payload_thread_notify_order=..., "
        "source/received/expected checksum match, payload_verify=passed, and "
        "fallback=none. If --auto-run-hcomm-payload-channel-handle-candidate "
        "is enabled, a failed default comm-name run may be followed by an "
        "explicit channel-handle candidate; only complete strict-positive "
        "evidence from that candidate can make the required evidence gate "
        "pass.\n",
        encoding="utf-8",
    )
    print(f"[ok] strict-positive scope -> {note}")
    return runner.write_summary()


def run_hcomm_storage_strict_positive(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    hccl_devices = ParseDeviceList(args.hccl_devices) if args.hccl_devices else []
    if len(hccl_devices) != 2:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "hcomm-storage-strict-positive requires exactly two "
            "--hccl-devices entries\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=True, timeout_seconds=args.step_timeout_sec,
               env_updates=CannRuntimeEnvUpdates(args))
    if shutil.which("npu-smi"):
        runner.run("npu-smi-info-m", ["npu-smi", "info", "-m"],
                   required=False, timeout_seconds=args.step_timeout_sec)
        runner.run(
            "npu-topo-check",
            [sys.executable, "tools/flume_npu_topo_check.py",
             f"--devices={','.join(hccl_devices)}"],
            required=False,
            timeout_seconds=args.step_timeout_sec,
        )

    runtime_json_ok, runtime_json_error = ValidateRuntimeCustomOpJson(args)
    if not runtime_json_ok:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(runtime_json_error + "\n", encoding="utf-8")
        print(f"[failed] command setup -> {setup_log}")
        return 1

    package_result = runner.run(
        "hcomm-custom-op-package-preflight",
        HcommCustomOpPackageCommand(args, require_payload=True),
        required=not args.auto_build_hcomm_payload_package,
        timeout_seconds=args.step_timeout_sec,
    )
    args, package_result = MaybeAutoBuildPayloadPackage(
        runner, args, package_result)
    if package_result.returncode != 0:
        next_steps = WritePayloadPackageBuildNextSteps(runner.run_dir, args)
        print(f"[ok] payload package next steps -> {next_steps}")
        note = runner.run_dir / "HCOMM_STORAGE_STRICT_POSITIVE_SCOPE.txt"
        note.write_text(
            "hcomm-storage-strict-positive stopped before launch because the "
            "installed Flume HCOMM custom-op package is not payload-ready. "
            "This gate requires the same Stage 3B.3E payload package as "
            "hcomm-payload-strict-positive, then wires that scheduler into "
            "the Stage 3B.4 storage proxy smoke.\n",
            encoding="utf-8",
        )
        print(f"[ok] hcomm storage scope -> {note}")
        return runner.write_summary()

    strict_args = copy.copy(args)
    strict_args.build_hcomm_custom_op = True
    strict_args.run_hccl_smoke = False
    strict_args.run_a3_symmetric_smoke = False
    strict_args.run_hccl_p2p_smoke = True
    strict_args.run_hcomm_channel_probe = True
    strict_args.run_hcomm_custom_op_launch_smoke = False
    strict_args.run_hcomm_resource_descriptor_smoke = False
    strict_args.run_hcomm_notify_only_smoke = False
    strict_args.run_hcomm_payload_smoke = True
    strict_args.run_storage_hbm_smoke = True
    strict_args.hcomm_require_payload_copy = True
    strict_args.hcomm_require_thread_export = False

    try:
        command_specs = build_commands(strict_args, enable_hccl=True,
                                       run_dir=runner.run_dir)
    except RuntimeError as exc:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(str(exc) + "\n", encoding="utf-8")
        print(f"[failed] command setup -> {setup_log}")
        return 1

    WriteHcclSmokeSetupNotes(
        runner.run_dir,
        CollectHcclSmokeSetupNotes(strict_args,
                                   ResolveHcclInitMode(strict_args)),
    )

    strict_result: Optional[StepResult] = None
    strict_tree_log: Optional[Path] = None
    for spec in command_specs:
        timeout = (args.hccl_smoke_timeout_sec if spec.name == "hccl-collective-smoke"
                   else args.step_timeout_sec)
        step_name = ("hcomm-storage-strict-positive"
                     if spec.name == "hccl-collective-smoke"
                     else spec.name)
        allow_channel_candidate = (
            step_name == "hcomm-storage-strict-positive" and
            args.auto_run_hcomm_payload_channel_handle_candidate and
            not CommandUsesChannelHandleBinding(spec.command))
        result = runner.run(step_name, spec.command,
                            required=spec.required and
                            not allow_channel_candidate,
                            timeout_seconds=timeout,
                            env_updates=spec.env_updates)
        if step_name == "hcomm-storage-strict-positive":
            strict_result = result
            strict_tree_log = result.log_path
            if result.returncode != 0:
                WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)
                if allow_channel_candidate:
                    channel_result = RunHcommPayloadChannelHandleCandidate(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path)
                    if StrictPayloadLogPassed(channel_result.log_path):
                        strict_tree_log = channel_result.log_path
                if (args.auto_run_hcomm_payload_nobatch_diagnostic and
                        not args.hcomm_payload_disable_batch and
                        "--hcomm-payload-disable-batch" not in spec.command):
                    RunHcommPayloadNoBatchDiagnostic(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path)
                if args.auto_run_hcomm_payload_tagged_diagnostic:
                    RunHcommPayloadTaggedDiagnostic(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path,
                        args.hcomm_payload_diagnostic_batch_tag)
                if (args.auto_run_hcomm_payload_direct_output_diagnostic and
                        "--hcomm-payload-recv-direct-output" not in spec.command):
                    RunHcommPayloadDirectOutputDiagnostic(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path)
                if (args.auto_run_hcomm_payload_no_comm_acquire_diagnostic and
                        not args.hcomm_payload_skip_comm_acquire and
                        "--hcomm-payload-skip-comm-acquire" not in spec.command):
                    RunHcommPayloadNoCommAcquireDiagnostic(
                        runner, spec.command, spec.env_updates, timeout,
                        result.log_path)

    RunCannCompatCollection(runner, args, hccl_devices)

    tree = WriteMatrixDecisionTree(
        runner.run_dir,
        None,
        strict_tree_log,
        package_result.log_path,
    )
    strict_passed = DecisionTreeStrictPositivePassed(tree)
    storage_passed = DecisionTreeHcommStoragePassed(tree)
    RecordStrictPositiveEvidenceGate(runner, tree, strict_passed,
                                     required=True)
    RecordHcommStorageEvidenceGate(runner, tree, storage_passed)
    note = runner.run_dir / "HCOMM_STORAGE_STRICT_POSITIVE_SCOPE.txt"
    note.write_text(
        "hcomm-storage-strict-positive is the focused Stage 3B.4 gate. It "
        "requires a payload-ready Flume custom-op package, enables "
        "FLUME_BUILD_HCOMM_CUSTOM_OP=ON, runs the HCCL P2P baseline and "
        "Stage 3B.3E strict HCOMM payload copy, then runs storage HBM smoke "
        "through the HCOMM payload scheduler. Success requires the strict "
        "payload evidence to pass and rank1 storage verification to report "
        "storage_hbm=hcomm-payload-staging. With "
        "--auto-run-hcomm-payload-channel-handle-candidate, a failed default "
        "comm-name run may be followed by an explicit channel-handle storage "
        "candidate; both strict payload evidence and storage verification "
        "must pass for the storage gate to pass. This still reads the storage file "
        "through the host into proxy HBM; it validates storage-proxy wiring "
        "onto HCOMM payload copy, not full storage-direct DMA.\n",
        encoding="utf-8",
    )
    print(f"[ok] hcomm storage scope -> {note}")
    return runner.write_summary()


def FindBuiltCustomOpArtifacts(hccl_source_root: Path,
                               vendor: str) -> tuple[Optional[Path], Optional[Path], list[Path]]:
    roots = [
        hccl_source_root / "build_out",
        hccl_source_root / "output",
        hccl_source_root / "build",
        hccl_source_root / "build_device",
    ]
    json_path: Optional[Path] = None
    tar_path: Optional[Path] = None
    run_files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        if json_path is None:
            matches = sorted(root.rglob(HCOMM_CUSTOM_OP_JSON))
            if matches:
                preferred = [
                    item for item in matches
                    if f"vendors/{vendor}/" in item.as_posix()
                ]
                json_path = (preferred or matches)[0]
        if tar_path is None:
            matches = sorted(root.rglob(HCOMM_CUSTOM_OP_TAR))
            if matches:
                preferred = [
                    item for item in matches
                    if f"vendors/{vendor}/" in item.as_posix()
                ]
                tar_path = (preferred or matches)[0]
        run_files.extend(sorted(root.rglob("*.run")))
    return json_path, tar_path, run_files


def SelectCustomOpRunPackage(run_files: list[Path]) -> Optional[Path]:
    if not run_files:
        return None
    preferred = [
        item for item in run_files
        if "custom_hcomm_payload" in item.name or
        HCOMM_CUSTOM_OP_NAME in item.name
    ]
    return sorted(preferred or run_files)[0]


def FindInstalledCustomOpRuntimeArtifacts(
        vendor: str, extra_roots: Optional[Iterable[str]] = None
) -> tuple[Optional[Path], Optional[Path]]:
    for _root, _vendor, json_path, tar_path in HcommCustomOpPackageCandidates(
            [vendor], extra_roots, "", ""):
        if (json_path is not None and tar_path is not None and
                json_path.exists() and tar_path.exists()):
            return (json_path, tar_path)
    return (None, None)


def WriteCustomOpInstallNextSteps(
        run_dir: Path,
        vendor: str,
        installed_json: Optional[Path],
        installed_tar: Optional[Path],
) -> Path:
    note = run_dir / "HCOMM_CUSTOM_OP_INSTALL_NEXT_STEPS.txt"
    json_text = str(installed_json) if installed_json else "<not-found>"
    tar_text = str(installed_tar) if installed_tar else "<not-found>"
    lines = [
        "Flume HCOMM custom-op install next steps",
        f"vendor: {vendor}",
        f"installed_json: {json_text}",
        f"installed_aicpu_tar: {tar_text}",
        "",
        "Run the focused Stage 3B.3E gate after choosing the target devices:",
        "python3 tools/flume_tool.py --build-dir build-hcomm-payload-positive \\",
        "  --hccl-devices <device-a>,<device-b> \\",
        "  --hccl-host-ifname <host-ifname> \\",
        "  --hccl-host-ip <host-ip> \\",
        "  --hccl-debug-logs \\",
    ]
    if installed_json is not None:
        lines.append(f"  --custom-op-json {installed_json} \\")
    lines.append("  hcomm-payload-strict-positive")
    lines.extend([
        "",
        "The explicit --custom-op-json path is optional when the installed "
        "vendor package is discoverable through ASCEND_HOME_PATH, "
        "ASCEND_OPP_PATH, or the default CANN layout. Passing it is useful "
        "when multiple CANN/OPP roots are present because strict-positive "
        "runtime launch treats that JSON as authoritative.",
    ])
    note.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return note


def RuntimeCustomOpArtifactPaths(root: Path, vendor: str) -> tuple[Path, Path]:
    base = root / "opp" / "vendors" / vendor
    return (
        base / "aicpu" / "config" / HCOMM_CUSTOM_OP_JSON,
        base / "aicpu" / "kernel" / HCOMM_CUSTOM_OP_TAR,
    )


def WritePayloadPackageBuildNextSteps(run_dir: Path,
                                      args: argparse.Namespace) -> Path:
    note = run_dir / "HCOMM_PAYLOAD_PACKAGE_NEXT_STEPS.txt"
    source_root = ResolveHcclSourceRoot(args)
    lines = [
        "Flume HCOMM payload package is not ready",
        "",
        "Preferred no-install path for hosts with an installed CANN toolkit:",
        "",
        "python3 tools/flume_tool.py \\",
        "  --custom-op-build-mode payload \\",
        "  --custom-op-export-root <temporary-custom-op-root> \\",
        "  hcomm-custom-op-direct-build",
        "",
        "Then rerun hcomm-payload-strict-positive with "
        "--custom-op-root <temporary-custom-op-root>.",
        "",
        "For a one-command retry, add --auto-build-hcomm-payload-package to "
        "hcomm-payload-strict-positive. It runs the same direct build/export "
        "flow into an isolated runtime root under the current log directory "
        "and does not modify the system CANN/OPP installation.",
        "",
        "If the target environment requires the HCCL source-tree packaging "
        "flow, build and install the primitive payload custom-op package, "
        "then rerun hcomm-payload-strict-positive:",
        "",
        "python3 tools/flume_tool.py \\",
        f"  --hccl-source-root {source_root} \\",
        "  --custom-op-build-mode payload \\",
        "  --install-custom-op-package \\",
        "  hcomm-custom-op-build",
        "",
        "After that command passes, follow the generated "
        "HCOMM_CUSTOM_OP_INSTALL_NEXT_STEPS.txt file. The install step is "
        "explicit because it changes the target CANN/OPP custom-op "
        "installation.",
        "",
        "If changing the system CANN/OPP installation is not desired after "
        "a source-tree build, export "
        "the preflight-passing build artifacts into an isolated runtime root:",
        "",
        "python3 tools/flume_tool.py \\",
        "  --custom-op-json <build-json> \\",
        "  --custom-op-aicpu-tar <build-aicpu-tar> \\",
        "  --custom-op-build-mode payload \\",
        "  --custom-op-export-root <temporary-custom-op-root> \\",
        "  hcomm-custom-op-export-runtime",
        "",
        "Then rerun hcomm-payload-strict-positive with "
        "--custom-op-root <temporary-custom-op-root>.",
    ]
    note.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return note


def run_hcomm_custom_op_export_runtime(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    vendor = args.custom_op_vendor.split(",")[0].strip() or "flume"
    if not args.custom_op_export_root:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "missing --custom-op-export-root for runtime package export\n"
            "Export writes a self-contained OPP runtime layout under:\n"
            "  <export-root>/opp/vendors/<vendor>/aicpu/config\n"
            "  <export-root>/opp/vendors/<vendor>/aicpu/kernel\n"
            "Pass that directory back to runtime smokes with "
            "--custom-op-root=<export-root>.\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1

    json_path: Optional[Path] = None
    tar_path: Optional[Path] = None
    if args.custom_op_json:
        json_path = Path(args.custom_op_json).expanduser().resolve()
    if args.custom_op_aicpu_tar:
        tar_path = Path(args.custom_op_aicpu_tar).expanduser().resolve()
    if json_path is None or tar_path is None:
        found_json, found_tar, _run_files = FindBuiltCustomOpArtifacts(
            ResolveHcclSourceRoot(args), vendor)
        json_path = json_path or found_json
        tar_path = tar_path or found_tar

    if json_path is None or tar_path is None:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "missing Flume custom-op JSON or AICPU tar for runtime export\n"
            "Provide --custom-op-json and --custom-op-aicpu-tar, or point "
            "--hccl-source-root at a tree that already contains custom-op "
            "build artifacts.\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1
    if not json_path.exists() or not tar_path.exists():
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "Flume custom-op export source artifact is missing\n"
            f"json: {json_path} ({'present' if json_path.exists() else 'missing'})\n"
            f"aicpu_tar: {tar_path} ({'present' if tar_path.exists() else 'missing'})\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1

    preflight_args = copy.copy(args)
    preflight_args.custom_op_json = str(json_path)
    preflight_args.custom_op_aicpu_tar = str(tar_path)
    preflight_args.custom_op_root = ""
    require_payload = args.custom_op_build_mode == "payload"
    preflight_result = runner.run(
        "hcomm-custom-op-export-preflight",
        HcommCustomOpPackageCommand(preflight_args, require_payload),
        required=True,
        timeout_seconds=args.step_timeout_sec,
    )
    if preflight_result.returncode != 0:
        setup_log = runner.run_dir / "CUSTOM_OP_EXPORT_ERROR.txt"
        setup_log.write_text(
            "refusing to export Flume HCOMM custom-op runtime package because "
            "hcomm-custom-op-export-preflight did not pass\n",
            encoding="utf-8",
        )
        print(f"[failed] custom-op export setup -> {setup_log}")
        runner.results.append(StepResult(
            "hcomm-custom-op-export-runtime", ["<preflight-failed>"], 1,
            0.0, setup_log, True))
        return runner.write_summary()

    export_root = Path(args.custom_op_export_root).expanduser().resolve()
    dest_json, dest_tar = RuntimeCustomOpArtifactPaths(export_root, vendor)
    dest_json.parent.mkdir(parents=True, exist_ok=True)
    dest_tar.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(json_path, dest_json)
    shutil.copy2(tar_path, dest_tar)

    export_note = runner.run_dir / "HCOMM_CUSTOM_OP_EXPORT_RUNTIME.txt"
    export_note.write_text(
        "Flume HCOMM custom-op runtime package exported\n"
        f"vendor: {vendor}\n"
        f"mode: {args.custom_op_build_mode}\n"
        f"export_root: {export_root}\n"
        f"json: {dest_json}\n"
        f"aicpu_tar: {dest_tar}\n"
        "\n"
        "Use this exported runtime layout without changing the system CANN "
        "installation by passing:\n"
        f"  --custom-op-root {export_root}\n",
        encoding="utf-8",
    )
    print(f"[ok] custom-op runtime export -> {export_note}")
    runner.results.append(StepResult(
        "hcomm-custom-op-export-runtime",
        ["copy", str(json_path), str(tar_path), str(export_root)],
        0, 0.0, export_note, True))

    installed_args = copy.copy(args)
    installed_args.custom_op_json = ""
    installed_args.custom_op_aicpu_tar = ""
    installed_args.custom_op_root = str(export_root)
    installed_preflight = runner.run(
        "hcomm-custom-op-exported-preflight",
        HcommCustomOpPackageCommand(installed_args, require_payload),
        required=True,
        timeout_seconds=args.step_timeout_sec,
    )
    if installed_preflight.returncode == 0:
        installed_json, installed_tar = FindInstalledCustomOpRuntimeArtifacts(
            vendor, [str(export_root)])
        next_steps = WriteCustomOpInstallNextSteps(
            runner.run_dir, vendor, installed_json, installed_tar)
        print(f"[ok] custom-op export next steps -> {next_steps}")
    return runner.write_summary()


def _DirectBuildSharedLibraryCommand(
        args: argparse.Namespace,
        cann_root: Path,
        output_so: Path,
) -> list[str]:
    cxx = os.environ.get("CXX", "c++")
    include = cann_root / "include"
    lib64 = cann_root / "lib64"
    sources = [
        HCOMM_CUSTOM_OP_PATH / "aicpu" / "direct_acl_canary_kernel.cc",
    ]
    defines: list[str] = []
    if args.custom_op_build_mode == "payload":
        sources.extend([
            HCOMM_CUSTOM_OP_PATH / "aicpu" / "notify_only_direct_acl_kernel.cc",
            HCOMM_CUSTOM_OP_PATH / "aicpu" / "payload_copy_kernel.cc",
        ])
        defines.append("FLUME_HCOMM_PAYLOAD_ENABLE_PRIMITIVE_PAYLOAD=1")
        defines.append("FLUME_HCOMM_PAYLOAD_ENABLE_INTERNAL_NOTIFY=1")
    command = [cxx, "-std=c++14"]
    if platform.system() == "Darwin":
        command.append("-dynamiclib")
    else:
        command.append("-shared")
    command.extend(["-fPIC", "-O2"])
    command.extend(f"-D{item}" for item in defines)
    command.extend([
        f"-I{HCOMM_CUSTOM_OP_PATH / 'include'}",
        f"-I{include}",
        f"-I{include / 'hccl'}",
        f"-I{include / 'hcomm'}",
        "-o",
        str(output_so),
    ])
    command.extend(str(source) for source in sources)
    if args.custom_op_build_mode == "payload":
        command.append(f"-L{lib64}")
        command.append("-lhcomm")
        for optional_lib in ("c_sec", "ascendcl"):
            if _LibraryExists(lib64, optional_lib):
                command.append(f"-l{optional_lib}")
        command.append(f"-Wl,-rpath,{lib64}")
    return command


def _WriteAicpuTar(so_path: Path, tar_path: Path) -> None:
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(so_path,
                arcname=f"aicpu_kernels_device/{HCOMM_CUSTOM_OP_KERNEL_SO}")


def run_hcomm_custom_op_direct_build(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    cann_root = ResolveCannBinaryRoot(args.cann_package_root)
    if cann_root is None:
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "missing CANN binary root for direct custom-op build\n"
            "The direct build path needs a CANN toolkit binary root "
            "containing include/ and lib64/. Set ASCEND_HOME_PATH or pass "
            "--cann-package-root=<cann-root>.\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1
    if args.custom_op_build_mode == "payload":
        hcomm_header = FindHcommPrimitivesHeader(cann_root)
        if hcomm_header is None:
            setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
            checked = "\n".join(
                f"- {item}" for item in HcommPrimitivesHeaderCandidates(
                    cann_root))
            setup_log.write_text(
                "missing hcomm_primitives.h for payload direct custom-op build\n"
                f"cann_binary_root: {cann_root}\n"
                "checked:\n"
                f"{checked}\n",
                encoding="utf-8",
            )
            print(f"[failed] command setup -> {setup_log}")
            return 1
    if args.custom_op_build_mode == "payload" and not _LibraryExists(
            cann_root / "lib64", "hcomm"):
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "missing libhcomm for payload direct custom-op build\n"
            f"checked: {cann_root / 'lib64'}\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1

    build_root = Path(args.build_dir) / "hcomm-custom-op-direct-build"
    build_root.mkdir(parents=True, exist_ok=True)
    output_so = build_root / HCOMM_CUSTOM_OP_KERNEL_SO
    output_tar = build_root / HCOMM_CUSTOM_OP_TAR
    output_json = build_root / HCOMM_CUSTOM_OP_JSON
    command = _DirectBuildSharedLibraryCommand(args, cann_root, output_so)
    build_result = runner.run(
        "hcomm-custom-op-direct-build",
        command,
        required=True,
        timeout_seconds=args.hccl_smoke_timeout_sec,
    )
    if build_result.returncode == 0:
        _WriteAicpuTar(output_so, output_tar)
        json_source = (
            HCOMM_CUSTOM_OP_PATH / "aicpu" /
            ("libflume_hcomm_payload_aicpu_kernel_payload.json"
             if args.custom_op_build_mode == "payload"
             else "libflume_hcomm_payload_aicpu_kernel_canary.json")
        )
        shutil.copy2(json_source, output_json)
        artifact_note = runner.run_dir / "HCOMM_CUSTOM_OP_DIRECT_BUILD_ARTIFACTS.txt"
        artifact_note.write_text(
            "Flume HCOMM direct custom-op build artifacts\n"
            f"cann_binary_root: {cann_root}\n"
            f"mode: {args.custom_op_build_mode}\n"
            f"json: {output_json}\n"
            f"aicpu_tar: {output_tar}\n",
            encoding="utf-8",
        )
        print(f"[ok] direct custom-op artifacts -> {artifact_note}")

        preflight_args = copy.copy(args)
        preflight_args.custom_op_json = str(output_json)
        preflight_args.custom_op_aicpu_tar = str(output_tar)
        preflight_args.custom_op_root = ""
        preflight_result = runner.run(
            "hcomm-custom-op-direct-build-preflight",
            HcommCustomOpPackageCommand(
                preflight_args,
                require_payload=args.custom_op_build_mode == "payload"),
            required=True,
            timeout_seconds=args.step_timeout_sec,
        )
        if (preflight_result.returncode == 0 and
                args.custom_op_export_root):
            vendor = args.custom_op_vendor.split(",")[0].strip() or "flume"
            export_root = Path(args.custom_op_export_root).expanduser().resolve()
            dest_json, dest_tar = RuntimeCustomOpArtifactPaths(
                export_root, vendor)
            dest_json.parent.mkdir(parents=True, exist_ok=True)
            dest_tar.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(output_json, dest_json)
            shutil.copy2(output_tar, dest_tar)
            export_note = runner.run_dir / "HCOMM_CUSTOM_OP_DIRECT_BUILD_EXPORT.txt"
            export_note.write_text(
                "Flume HCOMM direct custom-op artifacts exported\n"
                f"vendor: {vendor}\n"
                f"mode: {args.custom_op_build_mode}\n"
                f"export_root: {export_root}\n"
                f"json: {dest_json}\n"
                f"aicpu_tar: {dest_tar}\n",
                encoding="utf-8",
            )
            print(f"[ok] direct custom-op export -> {export_note}")
            installed_args = copy.copy(args)
            installed_args.custom_op_json = ""
            installed_args.custom_op_aicpu_tar = ""
            installed_args.custom_op_root = str(export_root)
            runner.run(
                "hcomm-custom-op-direct-build-exported-preflight",
                HcommCustomOpPackageCommand(
                    installed_args,
                    require_payload=args.custom_op_build_mode == "payload"),
                required=True,
                timeout_seconds=args.step_timeout_sec,
            )
    return runner.write_summary()


def run_hcomm_custom_op_build(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    hccl_source_root = ResolveHcclSourceRoot(args)
    build_sh = hccl_source_root / "build.sh"
    if not build_sh.exists():
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        toolkit_scripts = CannToolkitCustomOpTemplateBuildScripts(
            [args.custom_op_root])
        toolkit_lines = "\n".join(
            f"- {script}" for script in toolkit_scripts) or "- <not-found>"
        setup_log.write_text(
            "missing HCCL source build.sh for custom-op packaging\n"
            f"checked: {build_sh}\n"
            "Provide --hccl-source-root=<path-to-cann-hccl-source> or set "
            "FLUME_HCCL_SOURCE_ROOT. The source tree is only needed to run "
            "the CANN/HCCL custom-op packaging flow; Flume runtime tests do "
            "not depend on the local refer/ clone.\n"
            "\n"
            "Detected CANN toolkit custom-op template build scripts:\n"
            f"{toolkit_lines}\n"
            "\n"
            "These toolkit templates are useful for generating generic CANN "
            "custom-op projects, but they are not a drop-in replacement for "
            "the HCCL source build.sh flow used here because that flow accepts "
            "--vendor, --ops, and --custom_ops_path and packages the "
            "hcomm_payload layout expected by Flume. Clone/provide the CANN "
            "HCCL source tree, then rerun hcomm-custom-op-build.\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1

    vendor = args.custom_op_vendor.split(",")[0].strip() or "flume"
    env_updates: dict[str, str] = {}
    if args.custom_op_build_mode == "payload":
        env_updates["FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD"] = "ON"
        env_updates["FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY"] = "ON"
    else:
        env_updates["FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD"] = "OFF"
        env_updates["FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY"] = "OFF"
    if args.build_public_hccl_launch:
        env_updates["FLUME_HCOMM_PAYLOAD_BUILD_PUBLIC_HCCL_LAUNCH"] = "ON"

    command = [
        "bash",
        str(build_sh),
        f"--vendor={vendor}",
        f"--ops={HCOMM_CUSTOM_OP_NAME}",
        f"--custom_ops_path={HCOMM_CUSTOM_OP_PATH}",
        "--pkg-type=run",
    ]
    result = runner.run(
        "hcomm-custom-op-build",
        command,
        required=True,
        timeout_seconds=args.hccl_smoke_timeout_sec,
        env_updates=env_updates,
    )

    artifact_note = runner.run_dir / "HCOMM_CUSTOM_OP_BUILD_ARTIFACTS.txt"
    json_path, tar_path, run_files = FindBuiltCustomOpArtifacts(
        hccl_source_root, vendor)
    lines = [
        f"hccl_source_root: {hccl_source_root}",
        f"custom_ops_path: {HCOMM_CUSTOM_OP_PATH}",
        f"vendor: {vendor}",
        f"mode: {args.custom_op_build_mode}",
        f"json: {json_path if json_path else '<not-found>'}",
        f"aicpu_tar: {tar_path if tar_path else '<not-found>'}",
        "run_packages:",
    ]
    if run_files:
        lines.extend(f"- {item}" for item in run_files)
    else:
        lines.append("- <not-found>")
    artifact_note.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok] custom-op build artifacts -> {artifact_note}")

    build_preflight_result: Optional[StepResult] = None
    if result.returncode == 0 and json_path is not None and tar_path is not None:
        preflight_args = copy.copy(args)
        preflight_args.custom_op_json = str(json_path)
        preflight_args.custom_op_aicpu_tar = str(tar_path)
        preflight_args.custom_op_root = ""
        build_preflight_result = runner.run(
            "hcomm-custom-op-build-preflight",
            HcommCustomOpPackageCommand(
                preflight_args,
                require_payload=args.custom_op_build_mode == "payload"),
            required=True,
            timeout_seconds=args.step_timeout_sec,
        )
    if result.returncode == 0 and args.install_custom_op_package:
        if json_path is None or tar_path is None:
            setup_log = runner.run_dir / "CUSTOM_OP_INSTALL_ERROR.txt"
            setup_log.write_text(
                "cannot install Flume HCOMM custom-op package: build "
                "succeeded but JSON or AICPU tar artifacts were not found, "
                "so package preflight could not run\n",
                encoding="utf-8",
            )
            print(f"[failed] custom-op install setup -> {setup_log}")
            runner.results.append(StepResult(
                "hcomm-custom-op-install", ["<missing-build-artifacts>"], 1,
                0.0, setup_log, True))
            return runner.write_summary()
        if build_preflight_result is None or build_preflight_result.returncode != 0:
            setup_log = runner.run_dir / "CUSTOM_OP_INSTALL_ERROR.txt"
            setup_log.write_text(
                "refusing to install Flume HCOMM custom-op package because "
                "hcomm-custom-op-build-preflight did not pass\n",
                encoding="utf-8",
            )
            print(f"[failed] custom-op install setup -> {setup_log}")
            runner.results.append(StepResult(
                "hcomm-custom-op-install", ["<preflight-failed>"], 1, 0.0,
                setup_log, True))
            return runner.write_summary()
        run_package = SelectCustomOpRunPackage(run_files)
        if run_package is None:
            setup_log = runner.run_dir / "CUSTOM_OP_INSTALL_ERROR.txt"
            setup_log.write_text(
                "cannot install Flume HCOMM custom-op package: no .run "
                "installer was found under the HCCL source build outputs\n",
                encoding="utf-8",
            )
            print(f"[failed] custom-op install setup -> {setup_log}")
            runner.results.append(StepResult(
                "hcomm-custom-op-install", ["<missing-run-package>"], 1, 0.0,
                setup_log, True))
            return runner.write_summary()
        install_result = runner.run(
            "hcomm-custom-op-install",
            ["bash", str(run_package), "--install"],
            required=True,
            timeout_seconds=args.hccl_smoke_timeout_sec,
        )
        if install_result.returncode == 0:
            installed_args = copy.copy(args)
            installed_args.custom_op_json = ""
            installed_args.custom_op_aicpu_tar = ""
            installed_preflight = runner.run(
                "hcomm-custom-op-installed-preflight",
                HcommCustomOpPackageCommand(
                    installed_args,
                    require_payload=args.custom_op_build_mode == "payload"),
                required=True,
                timeout_seconds=args.step_timeout_sec,
            )
            if installed_preflight.returncode == 0:
                installed_json, installed_tar = FindInstalledCustomOpRuntimeArtifacts(
                    vendor, [args.custom_op_root])
                next_steps = WriteCustomOpInstallNextSteps(
                    runner.run_dir, vendor, installed_json, installed_tar)
                print(f"[ok] custom-op install next steps -> {next_steps}")
    return runner.write_summary()


def run_env(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=False, timeout_seconds=args.step_timeout_sec)
    return runner.write_summary()


def run_hcomm_custom_op_package(args: argparse.Namespace) -> int:
    vendors = [item.strip() for item in args.custom_op_vendor.split(",")
               if item.strip()]
    if not vendors:
        vendors = ["flume", "cust"]

    required_functions = ["canary_direct_aclrt"]
    if args.require_hcomm_payload_kernel:
        required_functions.append("payload_direct_aclrt")
        required_functions.append("payload_abi_v4")
        required_functions.append("payload_semantic")
        required_functions.append("payload_semantic_v5")
        required_functions.append("payload_semantic_v6")
        required_functions.append("payload_semantic_v7")
        required_functions.append("payload_semantic_v8")
        required_functions.append("payload_requires_comm_acquire")
        required_functions.append("payload_status_schema")
        required_functions.append("payload_status_word_count")
        required_functions.append("payload_primitive_deps")
        required_functions.append("build_mode_internal")

    found_any_json = False
    found_required = False
    found_legacy_payload = False
    found_canary_only_marker = False
    found_internal_payload_marker = False
    found_payload_abi_version_marker = False
    found_payload_abi_v2_marker = False
    found_payload_abi_v3_marker = False
    found_payload_semantic_marker = False
    found_payload_semantic_v5_marker = False
    found_payload_semantic_v6_marker = False
    found_payload_semantic_v7_marker = False
    found_payload_semantic_v8_marker = False
    found_payload_requires_comm_acquire_marker = False
    found_payload_status_schema_marker = False
    found_payload_status_word_count_marker = False
    found_payload_primitive_deps_marker = False
    print("HCOMM custom-op package inspection")
    print(f"json: {HCOMM_CUSTOM_OP_JSON}")
    print(f"aicpu_tar: {HCOMM_CUSTOM_OP_TAR}")
    print(f"vendors: {','.join(vendors)}")
    if args.custom_op_root:
        print(f"custom_op_root={args.custom_op_root}")
    if args.custom_op_json:
        print(f"custom_op_json={args.custom_op_json}")
    if args.custom_op_aicpu_tar:
        print(f"custom_op_aicpu_tar={args.custom_op_aicpu_tar}")
    print("")

    for root, vendor, json_path, tar_path in HcommCustomOpPackageCandidates(
            vendors, [args.custom_op_root], args.custom_op_json,
            args.custom_op_aicpu_tar):
        json_exists = json_path is not None and json_path.exists()
        tar_exists = tar_path is not None and tar_path.exists()
        if not json_exists and not tar_exists:
            continue
        print(f"root={root}")
        print(f"vendor={vendor}")
        print(f"json_path={json_path if json_path else '<unset>'}")
        print(f"aicpu_tar_path={tar_path if tar_path else '<unset>'}")
        print(f"json={'present' if json_exists else 'missing'}")
        print(f"aicpu_tar={'present' if tar_exists else 'missing'}")
        tar_state, tar_so_state, tar_members, tar_error = InspectAicpuTar(tar_path)
        print(f"aicpu_tar_readable={tar_state}")
        print(f"aicpu_tar_so.{HCOMM_CUSTOM_OP_KERNEL_SO}={tar_so_state}")
        print(f"aicpu_tar_members={tar_members}")
        if tar_error:
            print(f"aicpu_tar_error={tar_error}")
        symbol_names = list(HCOMM_CUSTOM_OP_FUNCTIONS.values()) + [
            HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT,
            HCOMM_PAYLOAD_BUILD_MODE_CANARY_ONLY,
            HCOMM_PAYLOAD_BUILD_MODE_INTERNAL,
            HCOMM_PAYLOAD_COPY_ABI_VERSION,
            HCOMM_PAYLOAD_COPY_ABI_VERSION_V2,
            HCOMM_PAYLOAD_COPY_ABI_VERSION_V3,
            HCOMM_PAYLOAD_COPY_ABI_VERSION_V4,
            HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION,
            HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5,
            HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6,
            HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7,
            HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8,
            HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE,
            HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION,
            HCOMM_PAYLOAD_STATUS_WORD_COUNT,
        ] + list(HCOMM_PAYLOAD_PRIMITIVE_SYMBOLS)
        symbol_state, symbols_present, symbol_error = InspectAicpuTarSymbols(
            tar_path, symbol_names)
        print(f"aicpu_tar_so_symbols={symbol_state}")
        if symbol_error:
            print(f"aicpu_tar_so_symbols_error={symbol_error}")

        functions_present: dict[str, bool] = {}
        primitive_deps_present = False
        legacy_payload_present = False
        if json_exists and json_path is not None:
            found_any_json = True
            try:
                payload = json.loads(json_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                print(f"json_error={exc}")
                payload = {}
            for label, function_name in HCOMM_CUSTOM_OP_FUNCTIONS.items():
                ok = JsonDeclaresFunction(
                    payload, function_name, HCOMM_CUSTOM_OP_KERNEL_SO)
                functions_present[label] = ok
                print(f"function.{label}.{function_name}="
                      f"{'present' if ok else 'missing'}")
                if symbol_state == "present":
                    print(f"function_so.{label}.{function_name}="
                          f"{'present' if symbols_present.get(function_name, False) else 'missing'}")
            found_internal_payload_marker = (
                found_internal_payload_marker or
                functions_present.get("build_mode_internal", False))
            found_payload_abi_version_marker = (
                found_payload_abi_version_marker or
                functions_present.get("payload_abi_v4", False))
            found_payload_semantic_marker = (
                found_payload_semantic_marker or
                functions_present.get("payload_semantic", False))
            found_payload_semantic_v5_marker = (
                found_payload_semantic_v5_marker or
                functions_present.get("payload_semantic_v5", False))
            found_payload_semantic_v6_marker = (
                found_payload_semantic_v6_marker or
                functions_present.get("payload_semantic_v6", False))
            found_payload_semantic_v7_marker = (
                found_payload_semantic_v7_marker or
                functions_present.get("payload_semantic_v7", False))
            found_payload_semantic_v8_marker = (
                found_payload_semantic_v8_marker or
                functions_present.get("payload_semantic_v8", False))
            found_payload_requires_comm_acquire_marker = (
                found_payload_requires_comm_acquire_marker or
                functions_present.get("payload_requires_comm_acquire", False))
            found_payload_status_schema_marker = (
                found_payload_status_schema_marker or
                functions_present.get("payload_status_schema", False))
            found_payload_status_word_count_marker = (
                found_payload_status_word_count_marker or
                functions_present.get("payload_status_word_count", False))
            legacy_payload_present = JsonDeclaresFunction(
                payload, HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT,
                HCOMM_CUSTOM_OP_KERNEL_SO)
            found_legacy_payload = (
                found_legacy_payload or legacy_payload_present)
            if symbol_state == "present":
                found_canary_only_marker = (
                    found_canary_only_marker or
                    symbols_present.get(HCOMM_PAYLOAD_BUILD_MODE_CANARY_ONLY,
                                        False))
                found_internal_payload_marker = (
                    found_internal_payload_marker or
                    symbols_present.get(HCOMM_PAYLOAD_BUILD_MODE_INTERNAL,
                                        False))
                found_payload_abi_version_marker = (
                    found_payload_abi_version_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V4,
                                        False))
                found_payload_abi_v2_marker = (
                    found_payload_abi_v2_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V2,
                                        False))
                found_payload_abi_v3_marker = (
                    found_payload_abi_v3_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V3,
                                        False))
                found_payload_semantic_marker = (
                    found_payload_semantic_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION,
                                        False))
                found_payload_semantic_v5_marker = (
                    found_payload_semantic_v5_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5,
                                        False))
                found_payload_semantic_v6_marker = (
                    found_payload_semantic_v6_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6,
                                        False))
                found_payload_semantic_v7_marker = (
                    found_payload_semantic_v7_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7,
                                        False))
                found_payload_semantic_v8_marker = (
                    found_payload_semantic_v8_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8,
                                        False))
                found_payload_requires_comm_acquire_marker = (
                    found_payload_requires_comm_acquire_marker or
                    symbols_present.get(HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE,
                                        False))
                found_payload_status_schema_marker = (
                    found_payload_status_schema_marker or
                    symbols_present.get(HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION,
                                        False))
                found_payload_status_word_count_marker = (
                    found_payload_status_word_count_marker or
                    symbols_present.get(HCOMM_PAYLOAD_STATUS_WORD_COUNT,
                                        False))
                print("function_so.payload_direct_aclrt.legacy."
                      f"{HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT}="
                      f"{'present' if symbols_present.get(HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT, False) else 'missing'}")
                print("function_so.build_mode.canary_only."
                      f"{HCOMM_PAYLOAD_BUILD_MODE_CANARY_ONLY}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_BUILD_MODE_CANARY_ONLY, False) else 'missing'}")
                print("function_so.build_mode.internal_payload."
                      f"{HCOMM_PAYLOAD_BUILD_MODE_INTERNAL}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_BUILD_MODE_INTERNAL, False) else 'missing'}")
                print("function_so.payload_abi_version."
                      f"{HCOMM_PAYLOAD_COPY_ABI_VERSION}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION, False) else 'missing'}")
                print("function_so.payload_abi_version_v2."
                      f"{HCOMM_PAYLOAD_COPY_ABI_VERSION_V2}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V2, False) else 'missing'}")
                print("function_so.payload_abi_version_v3."
                      f"{HCOMM_PAYLOAD_COPY_ABI_VERSION_V3}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V3, False) else 'missing'}")
                print("function_so.payload_abi_version_v4."
                      f"{HCOMM_PAYLOAD_COPY_ABI_VERSION_V4}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V4, False) else 'missing'}")
                print("function_so.payload_semantic_version."
                      f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION, False) else 'missing'}")
                print("function_so.payload_semantic_version_v5."
                      f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5, False) else 'missing'}")
                print("function_so.payload_semantic_version_v6."
                      f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6, False) else 'missing'}")
                print("function_so.payload_semantic_version_v7."
                      f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7, False) else 'missing'}")
                print("function_so.payload_semantic_version_v8."
                      f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8, False) else 'missing'}")
                print("function_so.payload_requires_comm_acquire."
                      f"{HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE, False) else 'missing'}")
                print("function_so.payload_status_schema."
                      f"{HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION, False) else 'missing'}")
                print("function_so.payload_status_word_count."
                      f"{HCOMM_PAYLOAD_STATUS_WORD_COUNT}="
                      f"{'present' if symbols_present.get(HCOMM_PAYLOAD_STATUS_WORD_COUNT, False) else 'missing'}")
                primitive_deps_present = all(
                    symbols_present.get(name, False)
                    for name in HCOMM_PAYLOAD_PRIMITIVE_SYMBOLS)
                found_payload_primitive_deps_marker = (
                    found_payload_primitive_deps_marker or
                    primitive_deps_present)
                for primitive_name in HCOMM_PAYLOAD_PRIMITIVE_SYMBOLS:
                    print("function_so.payload_primitive_dep."
                          f"{primitive_name}="
                          f"{'present' if symbols_present.get(primitive_name, False) else 'missing'}")
                print("payload_primitive_deps="
                      f"{'present' if primitive_deps_present else 'missing'}")
            if (args.require_hcomm_payload_kernel and
                    not functions_present.get("payload_direct_aclrt", False) and
                    legacy_payload_present):
                print("function.payload_direct_aclrt.legacy."
                      f"{HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT}=present")
                print("reason.payload_direct_aclrt="
                      "legacy-entrypoint-present")
                print("action.payload_direct_aclrt="
                      "rebuild-with-current-flume")
        else:
            for label, function_name in HCOMM_CUSTOM_OP_FUNCTIONS.items():
                functions_present[label] = False
                print(f"function.{label}.{function_name}=missing")
            if args.require_hcomm_payload_kernel:
                print("payload_primitive_deps=missing")

        required_ok = (
            tar_state == "present" and tar_so_state == "present" and
            all(functions_present.get(label, False)
                for label in required_functions
                if label != "payload_primitive_deps"))
        if symbol_state in ("unreadable", "not-checked"):
            required_ok = False
        elif symbol_state == "present":
            required_ok = required_ok and all(
                symbols_present.get(HCOMM_CUSTOM_OP_FUNCTIONS[label], False)
                for label in required_functions
                if label != "payload_primitive_deps")
            if args.require_hcomm_payload_kernel:
                required_ok = (
                    required_ok and
                    symbols_present.get(HCOMM_PAYLOAD_BUILD_MODE_INTERNAL, False) and
                    symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V4, False) and
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION, False) and
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5, False) and
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6, False) and
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7, False) and
                    symbols_present.get(HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8, False) and
                    symbols_present.get(HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE, False) and
                    symbols_present.get(HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION, False) and
                    symbols_present.get(HCOMM_PAYLOAD_STATUS_WORD_COUNT, False) and
                    primitive_deps_present)
        print(f"required={','.join(required_functions)}")
        if args.require_hcomm_payload_kernel:
            print("required_build_mode=internal_payload")
            print("required_payload_abi_version_symbol="
                  f"{HCOMM_PAYLOAD_COPY_ABI_VERSION_V4}")
            print("required_payload_semantic_symbol="
                  f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION}")
            print("required_payload_semantic_v5_symbol="
                  f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V5}")
            print("required_payload_semantic_v6_symbol="
                  f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V6}")
            print("required_payload_semantic_v7_symbol="
                  f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V7}")
            print("required_payload_semantic_v8_symbol="
                  f"{HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V8}")
            print("required_payload_comm_acquire_symbol="
                  f"{HCOMM_PAYLOAD_COPY_REQUIRES_COMM_ACQUIRE}")
            print("required_payload_status_schema_symbol="
                  f"{HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION}")
            print("required_payload_status_word_count_symbol="
                  f"{HCOMM_PAYLOAD_STATUS_WORD_COUNT}")
        print(f"status={'PASS' if required_ok else 'FAIL'}")
        print("")
        found_required = found_required or required_ok

    if not found_any_json:
        print("status=FAIL")
        print("reason=no Flume HCOMM custom-op JSON found")
        return 1
    if not found_required:
        print("status=FAIL")
        if args.require_hcomm_payload_kernel:
            if found_legacy_payload:
                print("reason=payload kernel package uses stale legacy "
                      "entrypoint")
                print("action=rebuild package with current Flume V4 payload "
                      "entrypoint")
            elif found_canary_only_marker and not found_internal_payload_marker:
                print("reason=payload kernel package is canary-only; V4 "
                      "payload entrypoint is a compatibility stub")
                print("action=rebuild package with "
                      "FLUME_HCOMM_PAYLOAD_BUILD_PRIMITIVE_PAYLOAD=ON")
            elif (found_internal_payload_marker and
                  not found_payload_abi_version_marker):
                if found_payload_abi_v2_marker:
                    if found_payload_abi_v3_marker:
                        print("reason=payload kernel package is stale ABI v3; "
                              "missing current payload ABI version marker")
                        print("action=rebuild package with current Flume V4 "
                              "headers")
                        return 1
                    print("reason=payload kernel package is stale ABI v2; "
                          "missing current payload ABI version marker")
                    print("action=rebuild package with current Flume V4 "
                          "headers")
                    return 1
                print("reason=payload kernel package is missing the payload "
                      "ABI version marker")
                print("action=rebuild package with current Flume V4 headers")
            elif (found_internal_payload_marker and
                  found_payload_abi_version_marker and
                  not found_payload_semantic_marker):
                print("reason=payload kernel package is missing the payload "
                      "semantic marker")
                print("action=rebuild package with current Flume payload "
                      "kernel")
            elif (found_internal_payload_marker and
                  found_payload_abi_version_marker and
                  found_payload_semantic_marker and
                  not found_payload_semantic_v5_marker):
                print("reason=payload kernel package has a stale payload "
                      "semantic marker")
                print("action=rebuild package with current Flume semantic "
                      "v5 payload kernel")
            elif (found_internal_payload_marker and
                  found_payload_abi_version_marker and
                  found_payload_semantic_marker and
                  found_payload_semantic_v5_marker and
                  not found_payload_semantic_v6_marker):
                print("reason=payload kernel package has a stale payload "
                      "semantic marker")
                print("action=rebuild package with current Flume semantic "
                      "v6 direct-output-capable payload kernel")
            elif (found_internal_payload_marker and
                  found_payload_abi_version_marker and
                  found_payload_semantic_marker and
                  found_payload_semantic_v5_marker and
                  found_payload_semantic_v6_marker and
                  not found_payload_semantic_v7_marker):
                print("reason=payload kernel package has a stale payload "
                      "semantic marker")
                print("action=rebuild package with current Flume semantic "
                      "v7 device-trace-capable payload kernel")
            elif (found_internal_payload_marker and
                  found_payload_abi_version_marker and
                  found_payload_semantic_marker and
                  found_payload_semantic_v5_marker and
                  found_payload_semantic_v6_marker and
                  found_payload_semantic_v7_marker and
                  not found_payload_semantic_v8_marker):
                print("reason=payload kernel package has a stale payload "
                      "semantic marker")
                print("action=rebuild package with current Flume semantic "
                      "v8 ordered-trace-capable payload kernel")
            elif (found_internal_payload_marker and
                  found_payload_abi_version_marker and
                  found_payload_semantic_marker and
                  found_payload_semantic_v5_marker and
                  found_payload_semantic_v6_marker and
                  found_payload_semantic_v7_marker and
                  found_payload_semantic_v8_marker and
                  not found_payload_requires_comm_acquire_marker):
                print("reason=payload kernel package is missing the payload "
                      "comm-acquire marker")
                print("action=rebuild package with current Flume payload "
                      "kernel")
            elif (found_internal_payload_marker and
                  found_payload_abi_version_marker and
                  found_payload_semantic_marker and
                  found_payload_semantic_v5_marker and
                  found_payload_semantic_v6_marker and
                  found_payload_semantic_v7_marker and
                  found_payload_semantic_v8_marker and
                  found_payload_requires_comm_acquire_marker and
                  (not found_payload_status_schema_marker or
                   not found_payload_status_word_count_marker)):
                print("reason=payload kernel package is missing the payload "
                      "status schema marker")
                print("action=rebuild package with current Flume payload "
                      "kernel")
            elif (found_internal_payload_marker and
                  found_payload_abi_version_marker and
                  found_payload_semantic_marker and
                  found_payload_semantic_v5_marker and
                  found_payload_semantic_v6_marker and
                  found_payload_semantic_v7_marker and
                  found_payload_semantic_v8_marker and
                  found_payload_requires_comm_acquire_marker and
                  found_payload_status_schema_marker and
                  found_payload_status_word_count_marker and
                  not found_payload_primitive_deps_marker):
                print("reason=payload kernel package is missing HCOMM "
                      "primitive dependencies")
                print("action=rebuild package with the primitive payload "
                      "kernel, not a marker-only payload stub")
            else:
                print("reason=payload kernel package is missing or incomplete")
        else:
            print("reason=canary package is missing or incomplete")
        return 1
    print("status=PASS")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flume test helper")
    parser.add_argument("--build-dir", default="build", help="CMake build directory")
    parser.add_argument("--log-root", default="logs", help="Directory for timestamped logs")
    parser.add_argument("--jobs", type=int, default=DEFAULT_BUILD_JOBS,
                        help="Parallel build jobs; defaults to min(cpu_count, 32)")
    parser.add_argument("--skip-tests", action="store_true", help="Configure without CTest targets")
    parser.add_argument("--build-hcomm-custom-op", action="store_true",
                        help=("Configure with FLUME_BUILD_HCOMM_CUSTOM_OP=ON. "
                              "This only enables the Stage 3B custom-op/AICPU "
                              "scheduler compile branch; the launcher is still "
                              "expected to report unsupported."))
    parser.add_argument("--step-timeout-sec", type=int, default=600,
                        help="Timeout for regular helper steps; 0 disables it")
    parser.add_argument("--run-hccl-smoke", action="store_true",
                        help="Run the optional real HCCL collective smoke test in ascend-probe")
    parser.add_argument("--run-a3-symmetric-smoke", action="store_true",
                        help="Run the optional Atlas A3 HCCS symmetric-memory smoke test")
    parser.add_argument("--run-hccl-p2p-smoke", action="store_true",
                        help=("Run the optional Stage 2 rank0-to-rank1 HCCL "
                              "Send/Recv HBM copy smoke test"))
    parser.add_argument("--run-hcomm-channel-probe", action="store_true",
                        help=("Run the optional Stage 2 HCOMM Channel resource "
                              "probe after the collective smoke"))
    parser.add_argument("--run-hcomm-custom-op-launch-smoke", action="store_true",
                        help=("Run the optional Stage 3B.1 HCOMM custom-op "
                              "no-op launch readiness smoke after the "
                              "channel resource path"))
    parser.add_argument("--run-hcomm-resource-descriptor-smoke", action="store_true",
                        help=("Run the optional Stage 3B.2 HCOMM resource "
                              "descriptor packaging smoke after the channel "
                              "resource path"))
    parser.add_argument("--run-hcomm-notify-only-smoke", action="store_true",
                        help=("Run the optional Stage 3B.2-complete HCOMM "
                              "notify-only kernel-consume readiness smoke"))
    parser.add_argument("--run-hcomm-payload-smoke", action="store_true",
                        help=("Run the optional Stage 2.5 HCOMM payload "
                              "readiness probe after the collective smoke"))
    parser.add_argument("--run-storage-hbm-smoke", action="store_true",
                        help=("Run the optional Stage 3A storage proxy rank "
                              "to compute rank HBM smoke. The default path "
                              "uses HCCL P2P fallback; with "
                              "--hcomm-require-payload-copy it uses the "
                              "HCOMM payload scheduler."))
    parser.add_argument("--storage-smoke-file", default="",
                        help=("Input file for --run-storage-hbm-smoke. If "
                              "empty, flume_tool generates a deterministic "
                              "file in the current log directory."))
    parser.add_argument("--storage-smoke-offset", type=int, default=0,
                        help="File offset for --run-storage-hbm-smoke")
    parser.add_argument("--storage-smoke-bytes", type=int, default=4096,
                        help=("Bytes to transfer in --run-storage-hbm-smoke; "
                              "must fit in --hccl-count * sizeof(float)"))
    parser.add_argument("--hcomm-channel-engine",
                        choices=["auto", "aicpu", "aicpu-ts", "cpu", "cpu-ts"],
                        default="auto",
                        help="HCOMM channel engine for --run-hcomm-channel-probe")
    parser.add_argument("--hcomm-channel-protocol",
                        choices=["auto", "hccs", "roce", "pcie", "sio",
                                 "hccs-only"],
                        default="hccs",
                        help="HCOMM channel protocol for --run-hcomm-channel-probe")
    parser.add_argument("--hcomm-notify-num", type=int, default=2,
                        help="HCOMM notify count for --run-hcomm-channel-probe")
    parser.add_argument("--hcomm-timeout-sec", type=int, default=60,
                        help=("Timeout for in-kernel HCOMM notify/payload "
                              "waits. Keep this below --hccl-smoke-timeout-sec "
                              "so strict smokes can report kernel status "
                              "before the process-level timeout."))
    parser.add_argument("--hcomm-require-thread-export", action="store_true",
                        help=("Require HcclThreadExportToCommEngine in the "
                              "HCOMM channel probe as an AICPU thread-export "
                              "extension check; CANN 8.5 is expected to "
                              "report unsupported for this extension"))
    parser.add_argument("--hcomm-require-payload-copy", action="store_true",
                        help=("Require real HCOMM payload copy in "
                              "--run-hcomm-payload-smoke; current Stage 2.5 "
                              "skeleton is expected to report unsupported"))
    parser.add_argument("--hcomm-payload-disable-batch", action="store_true",
                        help=("Diagnostic only: when running real HCOMM "
                              "payload copy, ask the direct ACL payload kernel "
                              "to skip HcommBatchModeStart/End so primitive "
                              "failures can be isolated from batch submit "
                              "semantics. This mode intentionally cannot "
                              "satisfy the strict-positive success gate; the "
                              "final path remains the default batch-enabled "
                              "mode."))
    parser.add_argument("--hcomm-payload-recv-direct-output",
                        action="store_true",
                        help=("Diagnostic only: ask the recv payload kernel "
                              "to HcommReadOnThread directly into the output "
                              "HBM buffer. The default remains local-buffer "
                              "staging, which reads remote HCCL Buffer into "
                              "local HCCL Buffer before the output copy."))
    parser.add_argument("--hcomm-payload-skip-comm-acquire",
                        action="store_true",
                        help=("Diagnostic only: ask the direct ACL payload "
                              "kernel to skip HcommAcquireComm/ReleaseComm and "
                              "exercise the ChannelHandle-based Notify/Read "
                              "path. This mode cannot satisfy the final "
                              "strict-positive gate, which requires "
                              "comm-name/default acquire or explicit "
                              "channel-handle binding."))
    parser.add_argument("--hcomm-payload-comm-binding",
                        choices=["comm-name", "channel-handle",
                                 "diagnostic-skip"],
                        default="",
                        help=("Select how the payload kernel binds HCOMM "
                              "communication context. `comm-name` follows the "
                              "official HcommAcquireComm/HcommReleaseComm "
                              "sample; `channel-handle` is the direct ACL "
                              "backend candidate that uses acquired "
                              "ThreadHandle/ChannelHandle resources without "
                              "in-kernel comm acquire; `diagnostic-skip` is "
                              "for isolation only and does not satisfy the "
                              "final strict-positive gate."))
    parser.add_argument("--hcomm-payload-batch-tag", default="",
                        help=("Optional HCOMM batch tag for Stage 3B.3E "
                              "experiments. Empty uses Flume's stable default "
                              "batch tag; non-empty tags test alternate CANN "
                              "tag caching compatibility without disabling "
                              "batch mode."))
    parser.add_argument("--auto-run-hcomm-payload-nobatch-diagnostic",
                        action="store_true",
                        help=("When a payload-ready package is present and "
                              "the batch-enabled strict payload gate fails, "
                              "automatically rerun the same smoke with "
                              "--hcomm-payload-disable-batch and write "
                              "HCOMM_PAYLOAD_NOBATCH_DIAGNOSTIC.md. This "
                              "collects A/B evidence only and does not turn "
                              "the strict-positive gate green."))
    parser.add_argument("--auto-run-hcomm-payload-tagged-diagnostic",
                        action="store_true",
                        help=("When a payload-ready package is present and "
                              "the default batch-enabled strict payload gate "
                              "fails, automatically rerun the same smoke with "
                              "--hcomm-payload-batch-tag set. This collects "
                              "default-batch vs tagged-batch evidence only and "
                              "does not turn the strict-positive gate green."))
    parser.add_argument("--auto-run-hcomm-payload-direct-output-diagnostic",
                        action="store_true",
                        help=("When a payload-ready package is present and "
                              "the strict payload gate fails, automatically "
                              "rerun the same smoke with "
                              "--hcomm-payload-recv-direct-output and write "
                              "HCOMM_PAYLOAD_DIRECT_OUTPUT_DIAGNOSTIC.md. "
                              "This collects local-buffer vs direct-output "
                              "recv evidence only and does not turn the "
                              "strict-positive gate green."))
    parser.add_argument("--auto-run-hcomm-payload-no-comm-acquire-diagnostic",
                        action="store_true",
                        help=("When a payload-ready package is present and "
                              "the strict payload gate fails, automatically "
                              "rerun the same smoke with "
                              "--hcomm-payload-skip-comm-acquire and write "
                              "HCOMM_PAYLOAD_NO_COMM_ACQUIRE_DIAGNOSTIC.md. "
                              "This isolates HcommAcquireComm/ReleaseComm from "
                              "the ChannelHandle Notify/Read path and does not "
                              "turn the strict-positive gate green."))
    parser.add_argument("--auto-run-hcomm-payload-channel-handle-candidate",
                        action="store_true",
                        help=("When a payload-ready package is present and "
                              "the default comm-name strict payload gate "
                              "fails, automatically rerun the same smoke with "
                              "--hcomm-payload-comm-binding=channel-handle "
                              "and write HCOMM_PAYLOAD_CHANNEL_HANDLE_CANDIDATE.md. "
                              "Unlike diagnostic-skip, a complete channel-handle "
                              "run can satisfy the strict-positive evidence gate."))
    parser.add_argument("--hcomm-payload-diagnostic-batch-tag",
                        default="flume-payload-v1",
                        help=("Batch tag used by "
                              "--auto-run-hcomm-payload-tagged-diagnostic"))
    parser.add_argument("--hccl-devices", default="",
                        help="Comma-separated device ids for the optional HCCL smoke test")
    parser.add_argument("--hccl-init-mode",
                        choices=["auto", "all", "init-all", "root-info", "rank-table"],
                        default="auto",
                        help=("HCCL communicator initialization mode for the optional "
                              "smoke; auto uses root-info when --hccl-devices is set, "
                              "rank-table is experimental diagnostics"))
    parser.add_argument("--hccl-link-mode",
                        choices=["default", "pcie", "roce"],
                        default="default",
                        help=("Server-internal HCCL link selection for smoke env; "
                              "roce sets HCCL_INTRA_PCIE_ENABLE=0 and "
                              "HCCL_INTRA_ROCE_ENABLE=1"))
    parser.add_argument("--hccl-debug-logs", action="store_true",
                        help=("Ask CANN/HCCL to print verbose logs to the smoke log "
                              "using ASCEND_GLOBAL_LOG_LEVEL=0 and "
                              "ASCEND_SLOG_PRINT_TO_STDOUT=1"))
    parser.add_argument("--hccl-host-ifname", default="",
                        help=("Host-side Linux NIC name for HCCL control-plane "
                              "sockets; sets HCCL_SOCKET_IFNAME"))
    parser.add_argument("--hccl-host-ip", default="",
                        help=("Host-side IPv4 for HCCL control-plane sockets; "
                              "sets HCCL_IF_IP and writes host_ip in generated "
                              "rank tables"))
    parser.add_argument("--hccl-rank-table", default="",
                        help=("Existing rank_table.json for --hccl-init-mode rank-table; "
                              "if omitted, the tool generates Ascend 910 v1 JSON"))
    parser.add_argument("--hccl-device-ips", default="",
                        help=("Comma-separated physical device to HCCN IP mapping, "
                              "for example <device-a>=<hccn-ip>,<device-b>=<hccn-ip>"))
    parser.add_argument("--hccl-rank-table-net",
                        choices=["auto", "none", "device"],
                        default="auto",
                        help=("Network fields for generated single-server rank table; "
                              "auto and none leave device_ip empty, device fills HCCN IPs"))
    parser.add_argument("--hccl-server-id", default="0",
                        help="server_id for generated single-server rank table")
    parser.add_argument("--no-hccl-visible-remap", dest="hccl_visible_remap",
                        action="store_false",
                        help=("Pass --hccl-devices directly instead of remapping "
                              "them through ASCEND_RT_VISIBLE_DEVICES"))
    parser.set_defaults(hccl_visible_remap=True)
    parser.add_argument("--hccl-count", type=int, default=1024,
                        help="FP32 element count per rank for the optional HCCL smoke test")
    parser.add_argument("--hccl-sym-win-gb", type=int, default=1,
                        help="Per-rank HCCL symmetric window reservation in GB")
    parser.add_argument("--hccl-smoke-timeout-sec", type=int, default=600,
                        help="Timeout for the optional real HCCL smoke step")
    parser.add_argument("--collect-cann-compat-label", default="",
                        help=("Optional label for ascend-full-matrix to collect "
                              "CANN/HCCL compatibility fixtures"))
    parser.add_argument("--custom-op-vendor", default="flume,cust",
                        help=("Comma-separated custom-op vendors to inspect in "
                              "hcomm-custom-op-package"))
    parser.add_argument("--custom-op-root", default="",
                        help=("Additional CANN root or opp root to inspect in "
                              "hcomm-custom-op-package"))
    parser.add_argument("--custom-op-json", default="",
                        help=("Explicit Flume custom-op JSON path to inspect in "
                              "hcomm-custom-op-package"))
    parser.add_argument("--custom-op-aicpu-tar", default="",
                        help=("Explicit Flume AICPU package tar path to inspect in "
                              "hcomm-custom-op-package"))
    parser.add_argument("--require-hcomm-payload-kernel", action="store_true",
                        help=("Require the Stage 3B.3E payload-copy kernel "
                              "function in hcomm-custom-op-package"))
    parser.add_argument("--hccl-source-root", default="",
                        help=("HCCL source tree used by hcomm-custom-op-build. "
                              "Defaults to FLUME_HCCL_SOURCE_ROOT or the local "
                              "ignored refer/cann-src/hccl checkout."))
    parser.add_argument("--custom-op-build-mode",
                        choices=["payload", "canary"],
                        default="payload",
                        help=("Package mode for hcomm-custom-op-build. payload "
                              "enables the HCOMM primitive payload kernel; "
                              "canary builds the no-internal-header canary."))
    parser.add_argument("--build-public-hccl-launch", action="store_true",
                        help=("Also build the legacy public HCCL-launch "
                              "notify-only entrypoint when the CANN package "
                              "exposes hccl_launch.h."))
    parser.add_argument("--install-custom-op-package", action="store_true",
                        help=("After hcomm-custom-op-build succeeds, install "
                              "the generated .run package and verify that the "
                              "installed package is visible to Flume. This is "
                              "explicit opt-in because it changes the target "
                              "CANN/OPP installation."))
    parser.add_argument("--custom-op-export-root", default="",
                        help=("Destination root for hcomm-custom-op-export-runtime. "
                              "The command writes an OPP runtime layout under "
                              "<root>/opp/vendors/<vendor> without modifying "
                              "the system CANN installation."))
    parser.add_argument("--cann-package-root", default="",
                        help=("CANN toolkit root for hcomm-custom-op-direct-build. "
                              "Defaults to ASCEND_HOME_PATH or the standard "
                              "CANN layout; the command expects an "
                              "aarch64-linux include/lib64 tree."))
    parser.add_argument("--auto-build-hcomm-payload-package",
                        action="store_true",
                        help=("For hcomm-payload-strict-positive and "
                              "hcomm-storage-strict-positive, if the payload "
                              "custom-op package preflight fails, build the "
                              "direct ACL payload package from the installed "
                              "CANN toolkit and export an isolated runtime "
                              "package under the current log directory before "
                              "retrying the strict gate. This never installs "
                              "into the system CANN/OPP tree."))

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("env", help="Only collect environment and HCCL layout information")
    subparsers.add_parser("local", help="Run no-NPU local configure/build/tests/sim demo")
    subparsers.add_parser("ascend-probe", help="Probe CANN/HCCL discovery and compile/link on Ascend host")
    subparsers.add_parser("ascend-full-matrix", help="Run the full two-rank Ascend readiness matrix")
    subparsers.add_parser(
        "hcomm-payload-strict-positive",
        help=("Run the focused Stage 3B.3E strict HCOMM payload-copy gate"))
    subparsers.add_parser(
        "hcomm-storage-strict-positive",
        help=("Run the focused Stage 3B.4 storage-over-HCOMM payload gate"))
    verify_logs_parser = subparsers.add_parser(
        "hcomm-payload-verify-logs",
        help=("Analyze an existing flume-check log directory and return "
              "success only when the Stage 3B.3E strict-positive evidence is "
              "complete"))
    verify_logs_parser.add_argument(
        "log_dir", nargs="?",
        help=("Existing flume-check-* directory to analyze. Defaults to the "
              "latest directory under --log-root."))
    storage_verify_logs_parser = subparsers.add_parser(
        "hcomm-storage-verify-logs",
        help=("Analyze an existing flume-check log directory and return "
              "success only when Stage 3B.4 storage-over-HCOMM evidence is "
              "complete"))
    storage_verify_logs_parser.add_argument(
        "log_dir", nargs="?",
        help=("Existing flume-check-* directory to analyze. Defaults to the "
              "latest directory under --log-root."))
    subparsers.add_parser(
        "hcomm-custom-op-build",
        help=("Build the Flume HCOMM custom-op package through a HCCL source "
              "tree custom-op packaging flow"))
    subparsers.add_parser(
        "hcomm-custom-op-export-runtime",
        help=("Copy a preflight-passing Flume custom-op JSON/AICPU tar into "
              "a runtime-loadable OPP layout under --custom-op-export-root"))
    subparsers.add_parser(
        "hcomm-custom-op-direct-build",
        help=("Build Flume HCOMM custom-op JSON/AICPU tar directly from the "
              "installed CANN toolkit, then preflight and optionally export "
              "the runtime layout"))
    subparsers.add_parser("hcomm-custom-op-package",
                          help=("Inspect installed Flume HCOMM custom-op JSON "
                                "and AICPU package"))

    args = parser.parse_args()
    args.hccl_host_ifname = args.hccl_host_ifname.strip()
    args.hccl_host_ip = args.hccl_host_ip.strip()
    if args.hccl_smoke_timeout_sec < 0:
        parser.error("--hccl-smoke-timeout-sec must be >= 0")
    if args.step_timeout_sec < 0:
        parser.error("--step-timeout-sec must be >= 0")
    if args.storage_smoke_offset < 0:
        parser.error("--storage-smoke-offset must be >= 0")
    if args.storage_smoke_bytes <= 0:
        parser.error("--storage-smoke-bytes must be greater than 0")
    if args.jobs <= 0:
        parser.error("--jobs must be greater than 0")
    if args.hcomm_notify_num <= 0 or args.hcomm_notify_num > 64:
        parser.error("--hcomm-notify-num must be in [1, 64]")
    if args.hcomm_timeout_sec <= 0 or args.hcomm_timeout_sec > 86400:
        parser.error("--hcomm-timeout-sec must be in [1, 86400]")
    if args.hccl_smoke_timeout_sec > 0:
        rank_timeout_sec = (args.hccl_smoke_timeout_sec - 5
                            if args.hccl_smoke_timeout_sec > 5 else
                            args.hccl_smoke_timeout_sec)
        if args.hcomm_timeout_sec >= rank_timeout_sec:
            parser.error("--hcomm-timeout-sec must be smaller than the "
                         "rank-level HCCL smoke timeout "
                         f"({rank_timeout_sec} seconds)")
    if args.hccl_server_id == "":
        parser.error("--hccl-server-id must not be empty")
    if args.run_hccl_p2p_smoke and args.run_a3_symmetric_smoke:
        parser.error("--run-hccl-p2p-smoke currently cannot be combined with "
                     "--run-a3-symmetric-smoke")
    if (args.hcomm_require_payload_copy and
            not (args.run_hcomm_payload_smoke or args.run_storage_hbm_smoke)):
        parser.error("--hcomm-require-payload-copy requires "
                     "--run-hcomm-payload-smoke or --run-storage-hbm-smoke")
    try:
        parsed_hccl_devices = ParseDeviceList(args.hccl_devices)
    except ValueError as exc:
        parser.error(str(exc))
    if ((args.run_hccl_p2p_smoke or args.run_hcomm_channel_probe or
         args.run_hcomm_custom_op_launch_smoke or
         args.run_hcomm_resource_descriptor_smoke or
         args.run_hcomm_notify_only_smoke or
         args.run_hcomm_payload_smoke or
         args.run_storage_hbm_smoke) and
            args.hccl_devices and len(parsed_hccl_devices) != 2):
        parser.error("--run-hccl-p2p-smoke, --run-hcomm-channel-probe, and "
                     "--run-hcomm-custom-op-launch-smoke, "
                     "--run-hcomm-resource-descriptor-smoke, "
                     "--run-hcomm-notify-only-smoke, "
                     "--run-hcomm-payload-smoke, --run-storage-hbm-smoke "
                     "require exactly two --hccl-devices entries")
    return args


def main() -> int:
    args = parse_args()
    if args.command == "env":
        return run_env(args)
    if args.command == "local":
        return run_local(args)
    if args.command == "ascend-probe":
        return run_ascend_probe(args)
    if args.command == "ascend-full-matrix":
        return run_ascend_full_matrix(args)
    if args.command == "hcomm-payload-strict-positive":
        return run_hcomm_payload_strict_positive(args)
    if args.command == "hcomm-storage-strict-positive":
        return run_hcomm_storage_strict_positive(args)
    if args.command == "hcomm-payload-verify-logs":
        return run_hcomm_payload_verify_logs(args)
    if args.command == "hcomm-storage-verify-logs":
        return run_hcomm_storage_verify_logs(args)
    if args.command == "hcomm-custom-op-build":
        return run_hcomm_custom_op_build(args)
    if args.command == "hcomm-custom-op-export-runtime":
        return run_hcomm_custom_op_export_runtime(args)
    if args.command == "hcomm-custom-op-direct-build":
        return run_hcomm_custom_op_direct_build(args)
    if args.command == "hcomm-custom-op-package":
        return run_hcomm_custom_op_package(args)
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
