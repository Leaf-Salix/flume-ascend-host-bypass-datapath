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
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUILD_JOBS = min(os.cpu_count() or 4, 32)


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
                r"stage3b2_kernel_consume|stage3b3a_kernel_launch|"
                r"stage3b3b_launcher_router|direct_aclrt|custom_op_package|"
                r"stage3b3c_direct_aclrt_loader|stage3b3c_descriptor_handoff|"
                r"stage3b3c_direct_aclrt_launch|"
                r"stage3b3d_no_internal_headers|direct_aclrt_canary_candidate|"
                r"stage3b3d_direct_aclrt_canary|"
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
    return runner.write_summary()


def run_ascend_probe(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=True, timeout_seconds=args.step_timeout_sec)
    requested_hccl_smoke = (args.run_hccl_smoke or args.run_a3_symmetric_smoke or
                            args.run_hccl_p2p_smoke or
                            args.run_hcomm_channel_probe or
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
        "until Flume implements the custom-op/AICPU payload scheduler. Add "
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
                            strict_log: Optional[Path]) -> Path:
    def read(path: Optional[Path]) -> str:
      if path is None:
          return ""
      try:
          return path.read_text(encoding="utf-8", errors="replace")
      except OSError:
          return ""

    smoke = read(smoke_log)
    strict = read(strict_log)
    lines = [
        "# Flume Ascend Full Matrix Decision Tree",
        "",
        "| Check | Result | Evidence |",
        "| --- | --- | --- |",
    ]
    hccl_ok = "hccl collective smoke passed" in smoke
    p2p_ok = "p2p_copy=on" in smoke and "hccl collective smoke passed" in smoke
    hcomm_channel_ok = "hcomm channel probe passed" in smoke
    hcomm_custom_op_launch_ok = "hcomm custom-op launch smoke passed" in smoke
    hcomm_custom_op_launch_unsupported = (
        "hcomm custom-op launch smoke unsupported" in smoke)
    hcomm_resource_descriptor_ok = (
        "hcomm resource descriptor smoke passed" in smoke)
    hcomm_resource_descriptor_unsupported = (
        "hcomm resource descriptor smoke unsupported" in smoke)
    hcomm_notify_only_ok = "hcomm notify-only smoke passed" in smoke
    hcomm_notify_only_unsupported = (
        "hcomm notify-only smoke unsupported" in smoke)
    hcomm_payload_ok = "hcomm payload smoke passed" in smoke
    hcomm_payload_unsupported = "hcomm payload smoke unsupported" in smoke
    storage_hbm_ok = "storage HBM smoke passed" in smoke
    strict_expected = ("HCOMM payload copy required but unavailable" in strict or
                       "unsupported" in strict)
    caps_match = re.search(r"FLUME_BACKEND_CAPS .+", smoke)
    caps = caps_match.group(0) if caps_match else "missing FLUME_BACKEND_CAPS"
    primitives = "unknown"
    if "hcomm_primitives=on" in caps:
        primitives = "present"
    elif "hcomm_primitives=off" in caps:
        primitives = "absent"
    scheduler_missing = ("hcomm_payload_scheduler=not-implemented" in caps or
                         "custom-op/AICPU scheduler" in smoke or
                         "custom-op launch" in smoke)

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
        f"| Storage to HBM fallback path ok? | {'yes' if storage_hbm_ok else 'no'} | "
        "`storage HBM smoke passed` marker |")
    lines.append(
        f"| Payload scheduler missing? | {'yes' if scheduler_missing else 'no'} | `hcomm_payload_scheduler` / scheduler detail |")
    lines.append(
        f"| Strict payload negative expected? | {'yes' if strict_expected else 'no'} | `hcomm-payload-strict-negative` log |")
    next_action = (
        "implement custom-op/AICPU HCOMM payload scheduler"
        if hccl_ok and p2p_ok and hcomm_channel_ok and hcomm_payload_unsupported
        and storage_hbm_ok
        else "inspect first failed required matrix step"
    )
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
            "hcomm-payload-strict-negative",
            strict_command,
            required=False,
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
    )
    note = runner.run_dir / "ASCEND_FULL_MATRIX_SCOPE.txt"
    note.write_text(
        "ascend-full-matrix builds once, runs local tests/sim, then runs a "
        "two-rank root-info smoke with HCCL collective, HCCL P2P fallback, "
        "HCOMM channel resource probe, and HCOMM payload readiness. It then "
        "runs --hcomm-require-payload-copy as an optional expected negative "
        "until the custom-op/AICPU payload scheduler is implemented. The "
        "matrix also runs Stage 3A storage_hbm=hccl-p2p-staging: rank0 reads "
        "a local file slice into proxy HBM and sends it to rank1 compute HBM "
        "with HcclSend/HcclRecv. This validates storage integration plumbing, "
        "not full storage-direct DMA.\n",
        encoding="utf-8",
    )
    print(f"[ok] matrix scope -> {note}")
    return runner.write_summary()


def run_env(args: argparse.Namespace) -> int:
    runner = Runner(Path(args.log_root))
    runner.write_env_report()
    runner.run("hccl-env-check", [sys.executable, "scripts/check_hccl_env.py"],
               required=False, timeout_seconds=args.step_timeout_sec)
    return runner.write_summary()


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

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("env", help="Only collect environment and HCCL layout information")
    subparsers.add_parser("local", help="Run no-NPU local configure/build/tests/sim demo")
    subparsers.add_parser("ascend-probe", help="Probe CANN/HCCL discovery and compile/link on Ascend host")
    subparsers.add_parser("ascend-full-matrix", help="Run the full two-rank Ascend readiness matrix")

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
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
