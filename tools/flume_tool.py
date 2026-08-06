#!/usr/bin/env python3
"""Convenience runner for Flume local and Ascend probes."""

from __future__ import annotations

import argparse
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
                r"HcclChannelGetHcclBuffer)",
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
                        args.run_hcomm_channel_probe):
        hccl_smoke = str(Path(build_dir) / "flume-hccl-collective-smoke")
        init_mode = ResolveHcclInitMode(args)
        command = [hccl_smoke, f"--count={args.hccl_count}",
                   f"--init={init_mode}"]
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
            command.append(f"--hcomm-channel-engine={args.hcomm_channel_engine}")
            command.append(f"--hcomm-channel-protocol={args.hcomm_channel_protocol}")
            command.append(f"--hcomm-notify-num={args.hcomm_notify_num}")
            if args.hcomm_require_thread_export:
                command.append("--hcomm-require-thread-export")
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
                            args.run_hcomm_channel_probe)
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
        "a FLUME_BACKEND_CAPS line. Add --hcomm-require-thread-export for a "
        "strict AICPU thread-export prerequisite check, which is expected to "
        "report unsupported on CANN builds without hccl_res_expt.h such as "
        "CANN 8.5.\n",
        encoding="utf-8",
    )
    print(f"[ok] scope note -> {note}")
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

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("env", help="Only collect environment and HCCL layout information")
    subparsers.add_parser("local", help="Run no-NPU local configure/build/tests/sim demo")
    subparsers.add_parser("ascend-probe", help="Probe CANN/HCCL discovery and compile/link on Ascend host")

    args = parser.parse_args()
    args.hccl_host_ifname = args.hccl_host_ifname.strip()
    args.hccl_host_ip = args.hccl_host_ip.strip()
    if args.hccl_smoke_timeout_sec < 0:
        parser.error("--hccl-smoke-timeout-sec must be >= 0")
    if args.step_timeout_sec < 0:
        parser.error("--step-timeout-sec must be >= 0")
    if args.jobs <= 0:
        parser.error("--jobs must be greater than 0")
    if args.hcomm_notify_num <= 0 or args.hcomm_notify_num > 64:
        parser.error("--hcomm-notify-num must be in [1, 64]")
    if args.hccl_server_id == "":
        parser.error("--hccl-server-id must not be empty")
    if args.run_hccl_p2p_smoke and args.run_a3_symmetric_smoke:
        parser.error("--run-hccl-p2p-smoke currently cannot be combined with "
                     "--run-a3-symmetric-smoke")
    try:
        parsed_hccl_devices = ParseDeviceList(args.hccl_devices)
    except ValueError as exc:
        parser.error(str(exc))
    if ((args.run_hccl_p2p_smoke or args.run_hcomm_channel_probe) and
            args.hccl_devices and len(parsed_hccl_devices) != 2):
        parser.error("--run-hccl-p2p-smoke and --run-hcomm-channel-probe "
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
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
