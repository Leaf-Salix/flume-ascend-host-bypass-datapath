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
    "payload_direct_aclrt": "FlumeHcommPayloadCopyDirectAclrtKernelV2",
    "payload_abi_v2": "FlumeHcommPayloadCopyAbiVersion2",
}
HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT = "FlumeHcommPayloadCopyDirectAclrtKernel"
HCOMM_PAYLOAD_BUILD_MODE_CANARY_ONLY = "FlumeHcommPayloadBuildModeCanaryOnly"
HCOMM_PAYLOAD_BUILD_MODE_INTERNAL = "FlumeHcommPayloadBuildModeInternalPayload"
HCOMM_PAYLOAD_COPY_ABI_VERSION = "FlumeHcommPayloadCopyAbiVersion"
HCOMM_PAYLOAD_COPY_ABI_VERSION_V2 = "FlumeHcommPayloadCopyAbiVersion2"
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


def HcommCustomOpPackageCandidates(
        vendors: list[str], extra_roots: Optional[Iterable[str]] = None,
        explicit_json: str = "", explicit_tar: str = ""
) -> list[tuple[Path, str, Optional[Path], Optional[Path]]]:
    candidates: list[tuple[Path, str, Optional[Path], Optional[Path]]] = []
    if explicit_json or explicit_tar:
        candidates.append((
            Path("<explicit>"),
            "explicit",
            Path(explicit_json) if explicit_json else None,
            Path(explicit_tar) if explicit_tar else None,
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


def RuntimeAicpuTarForJson(json_path: Path) -> Optional[Path]:
    parts = json_path.parts
    if len(parts) < 3:
        return None
    if parts[-3:] != ("aicpu", "config", HCOMM_CUSTOM_OP_JSON):
        return None
    return Path(*parts[:-2]) / "kernel" / HCOMM_CUSTOM_OP_TAR


def ValidateRuntimeCustomOpJson(args: argparse.Namespace) -> tuple[bool, str]:
    if not args.custom_op_json:
        return (True, "")
    json_path = Path(args.custom_op_json).expanduser()
    runtime_tar = RuntimeAicpuTarForJson(json_path)
    if runtime_tar is not None and runtime_tar.exists():
        return (True, "")
    expected = (str(runtime_tar) if runtime_tar is not None
                else "<custom-op-root>/opp/vendors/<vendor>/aicpu/kernel/" +
                HCOMM_CUSTOM_OP_TAR)
    message = (
        "explicit --custom-op-json is not an installed/runtime-loadable "
        "Flume custom-op JSON. The preflight --custom-op-aicpu-tar checks "
        "symbols only; strict-positive runtime launches use "
        "aclrtBinaryLoadFromFile(JSON). Install the package or point "
        "--custom-op-json at the installed aicpu/config JSON with matching "
        f"AICPU tar at {expected}")
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


def ResolveHcclSourceRoot(args: argparse.Namespace) -> Path:
    if args.hccl_source_root:
        return Path(args.hccl_source_root).expanduser().resolve()
    env_root = os.environ.get("FLUME_HCCL_SOURCE_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return (REPO_ROOT / "refer" / "cann-src" / "hccl").resolve()


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
        notes.append(
            "Storage HBM smoke is Stage 3A fallback validation. Rank0 reads "
            "a file slice from local storage, copies it into proxy-rank HBM, "
            "then sends it to rank1 HBM with HcclSend/HcclRecv. This is not "
            "full storage direct; it keeps the API/test surface ready for a "
            "future HCOMM/RDMA backend."
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
        CommandSpec("cmake-configure", configure, True, {}),
        CommandSpec("cmake-build",
                    ["cmake", "--build", build_dir, "-j", str(args.jobs)],
                    True, {}),
    ]
    if not args.skip_tests:
        commands.append(CommandSpec(
            "ctest", ["ctest", "--test-dir", build_dir, "--output-on-failure"],
            True, {}))
    sim_demo = str(Path(build_dir) / "flume-sim-demo")
    commands.append(CommandSpec("sim-demo", [sim_demo], True, {}))
    sim_collective_demo = str(Path(build_dir) / "flume-sim-collective-demo")
    commands.append(CommandSpec("sim-collective-demo", [sim_collective_demo],
                                True, {}))
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
        env_updates: dict[str, str] = {}
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
                args.run_hcomm_payload_smoke):
            command.append(f"--hcomm-channel-engine={args.hcomm_channel_engine}")
            command.append(f"--hcomm-channel-protocol={args.hcomm_channel_protocol}")
            command.append(f"--hcomm-notify-num={args.hcomm_notify_num}")
            command.append(f"--hcomm-timeout-sec={args.hcomm_timeout_sec}")
            if args.hcomm_require_thread_export:
                command.append("--hcomm-require-thread-export")
            if args.hcomm_require_payload_copy:
                command.append("--hcomm-require-payload-copy")
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
        runner.run("hccl-header-syntax", syntax_command, required=False,
                   timeout_seconds=args.step_timeout_sec)
    return runner.write_summary()


def run_ascend_probe(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=True, timeout_seconds=args.step_timeout_sec)
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

    smoke = read(smoke_log)
    strict = read(strict_log)
    package = read(package_log)
    combined = smoke + "\n" + strict
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
    storage_hbm_ok = "storage HBM smoke passed" in combined
    strict_positive_ok = (
        "rank 0 hcomm payload smoke passed" in strict and
        "rank 1 hcomm payload smoke passed" in strict and
        "stage3b3e_payload_copy=passed" in strict and
        "stage3b3e_direct_aclrt_payload_launch=passed" in strict and
        "stage3b3e_payload_sync=passed" in strict and
        "payload_kernel_status=success" in strict and
        "payload_status_word=0" in strict and
        "fallback=none" in strict and
        "payload_verify=passed" in strict)
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
    package_payload_ready = ("required=canary_direct_aclrt,payload_direct_aclrt" in
                             package and "status=PASS" in package)
    package_canary_ready = ("required=canary_direct_aclrt" in package and
                            "status=PASS" in package)
    package_status = "payload-ready" if package_payload_ready else (
        "canary-ready" if package_canary_ready else (
            "not-ready" if package else "not-checked"))
    strict_loader = marker_state(strict, "stage3b3e_direct_aclrt_payload_loader")
    strict_handoff = marker_state(strict, "stage3b3e_payload_descriptor_handoff")
    strict_launch = marker_state(strict, "stage3b3e_direct_aclrt_payload_launch")
    strict_sync = marker_state(strict, "stage3b3e_payload_sync")
    strict_kernel = marker_value(strict, "payload_kernel_status")
    strict_status_word = marker_value(strict, "payload_status_word")
    strict_verify = marker_value(strict, "payload_verify")
    strict_fallback = marker_value(strict, "fallback")

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
        f"| HCOMM payload scheduler candidate built? | "
        f"{'yes' if scheduler_candidate else 'no'} | "
        "`hcomm_payload_scheduler_candidate` in caps |")
    lines.append(
        f"| Storage to HBM fallback path ok? | {'yes' if storage_hbm_ok else 'no'} | "
        "`storage HBM smoke passed` marker |")
    lines.append(
        f"| Payload scheduler missing? | {'yes' if scheduler_missing else 'no'} | `hcomm_payload_scheduler` / scheduler detail |")
    lines.append(
        f"| Strict payload positive passed? | {'yes' if strict_positive_ok else 'no'} | "
        "`rank 0/1 passed` + `stage3b3e_payload_copy=passed` + "
        "`payload_kernel_status=success` + `payload_status_word=0` + "
        "`payload_verify=passed` + `fallback=none` |")
    lines.append(
        f"| Strict payload negative expected? | {'yes' if strict_negative_expected else 'no'} | `hcomm-payload-strict-negative` log |")
    if strict_log is not None:
        lines.extend([
            "",
            "| Strict Payload Stage | Result | Evidence |",
            "| --- | --- | --- |",
            f"| package preflight | {package_status} | `required=canary_direct_aclrt,payload_direct_aclrt` + `status=PASS` |",
            f"| payload loader | {strict_loader} | `stage3b3e_direct_aclrt_payload_loader` |",
            f"| descriptor handoff | {strict_handoff} | `stage3b3e_payload_descriptor_handoff` |",
            f"| direct ACL payload launch | {strict_launch} | `stage3b3e_direct_aclrt_payload_launch` |",
            f"| stream sync | {strict_sync} | `stage3b3e_payload_sync` |",
            f"| kernel status | {strict_kernel} | `payload_kernel_status`, status word `{strict_status_word}` |",
            f"| rank1 verify | {strict_verify} | `payload_verify` |",
            f"| fallback | {strict_fallback} | expected `none` for real HCOMM payload copy |",
        ])
    if strict_positive_ok:
        next_action = (
            "Stage 3B.3E strict payload copy passed; inspect checksum and "
            "start Stage 3B.4 storage rewiring")
    elif (hccl_ok and p2p_ok and hcomm_channel_ok and package_payload_ready and
          (storage_hbm_ok or strict_log is not None)):
        if strict_loader != "passed":
            next_action = "inspect payload custom-op package loading"
        elif strict_handoff != "passed":
            next_action = "inspect direct ACL payload descriptor handoff"
        elif strict_launch != "passed":
            next_action = "inspect direct ACL payload launch"
        elif strict_sync != "passed":
            next_action = "inspect payload stream sync or kernel hang"
        elif strict_kernel not in ("success", "missing"):
            next_action = f"inspect in-kernel HCOMM primitive failure: {strict_kernel}"
        elif strict_verify not in ("passed", "missing"):
            next_action = "inspect rank1 payload verification mismatch"
        elif strict_fallback not in ("none", "missing"):
            next_action = "remove unexpected fallback from strict payload path"
        else:
            next_action = "inspect hcomm-payload-strict-positive failure"
    elif (hccl_ok and p2p_ok and hcomm_channel_ok and storage_hbm_ok and
          hcomm_payload_unsupported and not package_payload_ready):
        next_action = (
            "build/install the Stage 3B.3E internal payload custom-op package")
    else:
        next_action = "inspect first failed required matrix step"
    lines.extend(["", f"next action: {next_action}", ""])
    path = run_dir / "ASCEND_FULL_MATRIX_DECISION_TREE.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[ok] matrix decision tree -> {path}")
    return path


def run_ascend_full_matrix(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=True, timeout_seconds=args.step_timeout_sec)
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
    package_text = ""
    try:
        package_text = package_result.log_path.read_text(
            encoding="utf-8", errors="replace")
    except OSError:
        package_text = ""
    package_payload_ready = (
        "required=canary_direct_aclrt,payload_direct_aclrt" in package_text and
        "status=PASS" in package_text)

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
    if smoke_spec is not None:
        strict_command = list(smoke_spec.command)
        strict_command.append("--hcomm-require-payload-copy")
        strict_result = runner.run(
            "hcomm-payload-strict-positive" if package_payload_ready
            else "hcomm-payload-strict-negative",
            strict_command,
            required=package_payload_ready,
            timeout_seconds=args.hccl_smoke_timeout_sec,
            env_updates=smoke_spec.env_updates,
        )
        if strict_result.returncode != 0:
            WriteHcclSmokeDiagnostics(runner.run_dir, strict_result.log_path)

    if args.collect_cann_compat_label:
        runner.run(
            "collect-cann-compat",
            [sys.executable, "tools/collect_cann_compat.py",
             "--label", args.collect_cann_compat_label,
             "--flume-log-dir", str(runner.run_dir),
             "--devices", ",".join(hccl_devices)],
            required=False,
            timeout_seconds=args.step_timeout_sec,
        )

    WriteMatrixDecisionTree(
        runner.run_dir,
        smoke_result.log_path if smoke_result is not None else None,
        strict_result.log_path if strict_result is not None else None,
        package_result.log_path,
    )
    note = runner.run_dir / "ASCEND_FULL_MATRIX_SCOPE.txt"
    note.write_text(
        "ascend-full-matrix builds once, runs local tests/sim, then runs a "
        "two-rank root-info smoke with HCCL collective, HCCL P2P fallback, "
        "HCOMM channel resource probe, and HCOMM payload readiness. It then "
        "runs --hcomm-require-payload-copy as a required positive check when "
        "the Stage 3B.3E payload package is installed, or as an optional "
        "expected negative while the package is not ready. Before "
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
               required=True, timeout_seconds=args.step_timeout_sec)
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
        required=True,
        timeout_seconds=args.step_timeout_sec,
    )
    if package_result.returncode != 0:
        note = runner.run_dir / "HCOMM_PAYLOAD_STRICT_POSITIVE_SCOPE.txt"
        note.write_text(
            "hcomm-payload-strict-positive stopped before launch because the "
            "installed Flume HCOMM custom-op package is not payload-ready. "
            "The package must declare and export the V2 direct ACL payload "
            "kernel and the internal-payload build marker.\n",
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
    for spec in command_specs:
        timeout = (args.hccl_smoke_timeout_sec if spec.name == "hccl-collective-smoke"
                   else args.step_timeout_sec)
        step_name = ("hcomm-payload-strict-positive"
                     if spec.name == "hccl-collective-smoke"
                     else spec.name)
        result = runner.run(step_name, spec.command, required=spec.required,
                            timeout_seconds=timeout,
                            env_updates=spec.env_updates)
        if step_name == "hcomm-payload-strict-positive":
            strict_result = result
            if result.returncode != 0:
                WriteHcclSmokeDiagnostics(runner.run_dir, result.log_path)

    WriteMatrixDecisionTree(
        runner.run_dir,
        None,
        strict_result.log_path if strict_result is not None else None,
        package_result.log_path,
    )
    note = runner.run_dir / "HCOMM_PAYLOAD_STRICT_POSITIVE_SCOPE.txt"
    note.write_text(
        "hcomm-payload-strict-positive is the focused Stage 3B.3E gate. It "
        "requires a payload-ready Flume custom-op package, configures Flume "
        "with FLUME_BUILD_HCOMM_CUSTOM_OP=ON, runs the HCCL P2P baseline, and "
        "then requires real HCOMM payload copy. Success requires both ranks "
        "to pass with stage3b3e_payload_copy=passed, payload_kernel_status="
        "success, payload_status_word=0, payload_verify=passed, and "
        "fallback=none.\n",
        encoding="utf-8",
    )
    print(f"[ok] strict-positive scope -> {note}")
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


def run_hcomm_custom_op_build(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    hccl_source_root = ResolveHcclSourceRoot(args)
    build_sh = hccl_source_root / "build.sh"
    if not build_sh.exists():
        setup_log = runner.run_dir / "COMMAND_SETUP_ERROR.txt"
        setup_log.write_text(
            "missing HCCL source build.sh for custom-op packaging\n"
            f"checked: {build_sh}\n"
            "Provide --hccl-source-root=<path-to-cann-hccl-source> or set "
            "FLUME_HCCL_SOURCE_ROOT. The source tree is only needed to run "
            "the CANN/HCCL custom-op packaging flow; Flume runtime tests do "
            "not depend on the local refer/ clone.\n",
            encoding="utf-8",
        )
        print(f"[failed] command setup -> {setup_log}")
        return 1

    vendor = args.custom_op_vendor.split(",")[0].strip() or "flume"
    env_updates: dict[str, str] = {}
    if args.custom_op_build_mode == "payload":
        env_updates["FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY"] = "ON"
    else:
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
        required_functions.append("payload_abi_v2")

    found_any_json = False
    found_required = False
    found_legacy_payload = False
    found_canary_only_marker = False
    found_internal_payload_marker = False
    found_payload_abi_version_marker = False
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
        ]
        symbol_state, symbols_present, symbol_error = InspectAicpuTarSymbols(
            tar_path, symbol_names)
        print(f"aicpu_tar_so_symbols={symbol_state}")
        if symbol_error:
            print(f"aicpu_tar_so_symbols_error={symbol_error}")

        functions_present: dict[str, bool] = {}
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
                    symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V2,
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

        required_ok = (
            tar_state == "present" and tar_so_state == "present" and
            all(functions_present.get(label, False)
                for label in required_functions))
        if symbol_state in ("unreadable", "not-checked"):
            required_ok = False
        elif symbol_state == "present":
            required_ok = required_ok and all(
                symbols_present.get(HCOMM_CUSTOM_OP_FUNCTIONS[label], False)
                for label in required_functions)
            if args.require_hcomm_payload_kernel:
                required_ok = (
                    required_ok and
                    symbols_present.get(HCOMM_PAYLOAD_BUILD_MODE_INTERNAL, False) and
                    symbols_present.get(HCOMM_PAYLOAD_COPY_ABI_VERSION_V2, False))
        print(f"required={','.join(required_functions)}")
        if args.require_hcomm_payload_kernel:
            print("required_build_mode=internal_payload")
            print("required_payload_abi_version_symbol="
                  f"{HCOMM_PAYLOAD_COPY_ABI_VERSION_V2}")
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
                print("action=rebuild package with current Flume V2 payload "
                      "entrypoint")
            elif found_canary_only_marker and not found_internal_payload_marker:
                print("reason=payload kernel package is canary-only; V2 "
                      "payload entrypoint is a compatibility stub")
                print("action=rebuild package with "
                      "FLUME_HCOMM_PAYLOAD_BUILD_INTERNAL_NOTIFY=ON")
            elif (found_internal_payload_marker and
                  not found_payload_abi_version_marker):
                print("reason=payload kernel package is missing the payload "
                      "ABI version marker")
                print("action=rebuild package with current Flume headers")
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
                              "to compute rank HBM smoke through HCCL P2P "
                              "fallback"))
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
                              "enables the internal HCOMM primitive kernel; "
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

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("env", help="Only collect environment and HCCL layout information")
    subparsers.add_parser("local", help="Run no-NPU local configure/build/tests/sim demo")
    subparsers.add_parser("ascend-probe", help="Probe CANN/HCCL discovery and compile/link on Ascend host")
    subparsers.add_parser("ascend-full-matrix", help="Run the full two-rank Ascend readiness matrix")
    subparsers.add_parser(
        "hcomm-payload-strict-positive",
        help=("Run the focused Stage 3B.3E strict HCOMM payload-copy gate"))
    subparsers.add_parser(
        "hcomm-custom-op-build",
        help=("Build the Flume HCOMM custom-op package through a HCCL source "
              "tree custom-op packaging flow"))
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
    if args.hcomm_require_payload_copy and not args.run_hcomm_payload_smoke:
        parser.error("--hcomm-require-payload-copy requires "
                     "--run-hcomm-payload-smoke")
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
    if args.command == "hcomm-custom-op-build":
        return run_hcomm_custom_op_build(args)
    if args.command == "hcomm-custom-op-package":
        return run_hcomm_custom_op_package(args)
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
