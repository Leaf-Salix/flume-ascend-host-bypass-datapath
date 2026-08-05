#!/usr/bin/env python3
"""Launch one flume-hccl-collective-smoke process per HCCL rank."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional


def parse_devices(spec: str) -> list[str]:
    devices = [item.strip() for item in spec.split(",") if item.strip()]
    if not devices:
        raise ValueError("--devices must not be empty")
    for device in devices:
        if not device.isdigit():
            raise ValueError(f"invalid device id: {device}")
    return devices


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flume HCCL multi-process launcher")
    parser.add_argument("--binary", required=True, help="flume-hccl-collective-smoke path")
    parser.add_argument("--devices", required=True,
                        help="Comma-separated per-rank local device ids")
    parser.add_argument("--count", type=int, default=1024)
    parser.add_argument("--init", choices=["root-info", "rank-table"], required=True,
                        help=("root-info mirrors the official HCCL root-info "
                              "bring-up; rank-table is an experimental diagnostic path"))
    parser.add_argument("--rank-table", default="")
    parser.add_argument("--root-info-file", default="",
                        help="Shared HcclRootInfo binary path for --init root-info")
    parser.add_argument("--rank-log-dir", default="",
                        help="Directory for per-rank child process logs")
    parser.add_argument("--a3-symmetric", action="store_true")
    parser.add_argument("--p2p-copy", action="store_true",
                        help="Run rank0->rank1 HCCL Send/Recv P2P copy smoke")
    parser.add_argument("--hcomm-channel-probe", action="store_true",
                        help="Run rank0/rank1 HCOMM Channel resource probe")
    parser.add_argument("--hcomm-channel-engine", default="aicpu",
                        choices=["auto", "aicpu", "aicpu-ts", "cpu"])
    parser.add_argument("--hcomm-channel-protocol", default="hccs",
                        choices=["auto", "hccs", "roce", "pcie", "sio"])
    parser.add_argument("--hcomm-notify-num", type=int, default=2)
    parser.add_argument("--sym-win-gb", type=int, default=1)
    parser.add_argument("--timeout-sec", type=int, default=0,
                        help="Overall timeout for all rank processes; 0 disables it")
    args = parser.parse_args()
    if args.count <= 0:
        parser.error("--count must be greater than 0")
    if args.sym_win_gb <= 0:
        parser.error("--sym-win-gb must be greater than 0")
    if args.hcomm_notify_num <= 0 or args.hcomm_notify_num > 64:
        parser.error("--hcomm-notify-num must be in [1, 64]")
    if args.timeout_sec < 0:
        parser.error("--timeout-sec must be >= 0")
    try:
        args.devices_list = parse_devices(args.devices)
    except ValueError as exc:
        parser.error(str(exc))
    if not Path(args.binary).exists():
        parser.error(f"--binary does not exist: {args.binary}")
    if args.init == "rank-table" and not args.rank_table:
        parser.error("--init rank-table requires --rank-table")
    if args.init == "rank-table" and not Path(args.rank_table).exists():
        parser.error(f"--rank-table does not exist: {args.rank_table}")
    return args


def build_rank_command(args: argparse.Namespace, rank: int, device: str,
                       rank_size: int,
                       root_info_file: Optional[Path]) -> list[str]:
    command = [
        args.binary,
        f"--count={args.count}",
        f"--init={args.init}",
        f"--devices={device}",
        f"--rank={rank}",
        f"--rank-size={rank_size}",
    ]
    if args.init == "rank-table":
        command.append(f"--rank-table={args.rank_table}")
    elif root_info_file is not None:
        if rank == 0:
            command.append(f"--root-info-out={root_info_file}")
        else:
            command.append(f"--root-info={root_info_file}")
    if args.a3_symmetric:
        command.append("--a3-symmetric")
        command.append(f"--sym-win-gb={args.sym_win_gb}")
    if args.p2p_copy:
        command.append("--p2p-copy")
    if args.hcomm_channel_probe:
        command.append("--hcomm-channel-probe")
        command.append(f"--hcomm-channel-engine={args.hcomm_channel_engine}")
        command.append(f"--hcomm-channel-protocol={args.hcomm_channel_protocol}")
        command.append(f"--hcomm-notify-num={args.hcomm_notify_num}")
    return command


def start_rank(command: list[str], rank: int,
               rank_log_dir: Path) -> tuple[subprocess.Popen[str], Path]:
    print(f"[launcher] rank {rank}: {' '.join(command)}", flush=True)
    rank_log_dir.mkdir(parents=True, exist_ok=True)
    log_path = rank_log_dir / f"rank-{rank}.log"
    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            command,
            text=True,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    return proc, log_path


def wait_for_root_info(path: Path, rank0_proc: subprocess.Popen[str],
                       timeout_sec: int) -> bool:
    deadline = time.monotonic() + (timeout_sec if timeout_sec > 0 else 60)
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size > 0:
            return True
        if rank0_proc.poll() is not None:
            return False
        time.sleep(0.1)
    return False


def main() -> int:
    args = parse_args()
    rank_size = len(args.devices_list)
    procs: list[tuple[int, list[str], subprocess.Popen[str], Path]] = []
    temp_dir = None
    rank_log_temp_dir = None
    root_info_file: Optional[Path] = None
    start = time.monotonic()
    launch_failed = False
    if args.rank_log_dir:
        rank_log_dir = Path(args.rank_log_dir)
    else:
        rank_log_temp_dir = tempfile.TemporaryDirectory(prefix="flume-rank-logs-")
        rank_log_dir = Path(rank_log_temp_dir.name)

    if args.init == "root-info":
        if args.root_info_file:
            root_info_file = Path(args.root_info_file)
            root_info_file.parent.mkdir(parents=True, exist_ok=True)
        else:
            temp_dir = tempfile.TemporaryDirectory(prefix="flume-root-info-")
            root_info_file = Path(temp_dir.name) / "root_info.bin"
        if root_info_file.exists():
            root_info_file.unlink()

    if args.init == "root-info":
        command = build_rank_command(
            args, 0, args.devices_list[0], rank_size, root_info_file)
        proc, log_path = start_rank(command, 0, rank_log_dir)
        procs.append((0, command, proc, log_path))
        assert root_info_file is not None
        if not wait_for_root_info(root_info_file, proc, args.timeout_sec):
            print(f"[launcher] root-info file was not produced: {root_info_file}")
            for _, _, child, _ in procs:
                if child.poll() is None:
                    child.terminate()
            time.sleep(2)
            for _, _, child, _ in procs:
                if child.poll() is None:
                    child.kill()
            launch_failed = True
            launch_items = []
        else:
            print(f"[launcher] root-info file ready: {root_info_file}", flush=True)
            launch_items = enumerate(args.devices_list[1:], start=1)
    else:
        launch_items = enumerate(args.devices_list)

    for rank, device in launch_items:
        command = build_rank_command(args, rank, device, rank_size,
                                     root_info_file)
        proc, log_path = start_rank(command, rank, rank_log_dir)
        procs.append((rank, command, proc, log_path))

    failed = launch_failed
    timed_out = False
    deadline = start + args.timeout_sec if args.timeout_sec > 0 else None
    for rank, command, proc, log_path in procs:
        timeout = None
        if deadline is not None:
            timeout = max(0.0, deadline - time.monotonic())
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            failed = True
            print(f"[launcher] TIMEOUT after {args.timeout_sec} seconds")
            for _, _, child, _ in procs:
                if child.poll() is None:
                    child.terminate()
            time.sleep(2)
            for _, _, child, _ in procs:
                if child.poll() is None:
                    child.kill()
            proc.wait()
        print(f"[rank {rank}] command: {' '.join(command)}")
        print(f"[rank {rank}] log: {log_path}")
        print(f"[rank {rank}] returncode: {proc.returncode}")
        output = log_path.read_text(encoding="utf-8", errors="replace")
        if output:
            for line in output.splitlines():
                print(f"[rank {rank}] {line}")
        if proc.returncode != 0:
            failed = True
        if timed_out:
            break
    print(f"[launcher] duration_seconds: {time.monotonic() - start:.3f}")
    if temp_dir is not None:
        temp_dir.cleanup()
    if rank_log_temp_dir is not None:
        rank_log_temp_dir.cleanup()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
