#!/usr/bin/env python3
"""Collect best-effort NPU topology and HCCS health evidence for Flume."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from dataclasses import dataclass


@dataclass
class CommandResult:
    name: str
    command: list[str]
    returncode: int
    output: str


def ParseDeviceList(spec: str) -> list[int]:
    devices = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        if not item.isdigit():
            raise ValueError(f"invalid device id: {item}")
        devices.append(int(item))
    return devices


def RunCommand(name: str, command: list[str]) -> CommandResult:
    try:
        proc = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=60,
        )
        return CommandResult(name, command, proc.returncode, proc.stdout or "")
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        return CommandResult(name, command, 124, output + "\nTIMEOUT after 60 seconds\n")
    except OSError as exc:
        return CommandResult(name, command, 127, f"failed to start command: {exc}\n")


def DeviceToNpuChip(device: int) -> tuple[int, int]:
    return device // 2, device % 2


def LooksUnhealthy(text: str) -> list[str]:
    findings: list[str] = []
    for line in text.splitlines():
        lower = line.lower()
        if ("health" in lower and "ok" not in lower and
                re.search(r"\b(fail|error|abnormal|down|fault|unknown)\b", lower)):
            findings.append(line.strip())
        if re.search(r"\b(error|err)\w*\b", lower):
            numbers = [int(item) for item in re.findall(r"\b\d+\b", line)]
            if numbers and any(number > 0 for number in numbers):
                findings.append(line.strip())
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect NPU topology and HCCS health")
    parser.add_argument("--devices", default="",
                        help="Comma-separated physical device ids used by the HCCL smoke")
    args = parser.parse_args()
    devices = ParseDeviceList(args.devices)

    npu_smi = shutil.which("npu-smi")
    if npu_smi is None:
        print("npu-smi: not found; skip topology collection")
        return 0

    results = [RunCommand("topo", [npu_smi, "info", "-t", "topo"])]
    seen_targets: set[tuple[int, int]] = set()
    for device in devices:
        target = DeviceToNpuChip(device)
        if target in seen_targets:
            continue
        seen_targets.add(target)
        npu_id, chip_id = target
        results.append(RunCommand(
            f"hccs-device-{device}",
            [npu_smi, "info", "-t", "hccs", "-i", str(npu_id), "-c", str(chip_id)],
        ))

    all_findings: list[str] = []
    for result in results:
        print(f"===== {result.name} =====")
        print("$ " + " ".join(result.command))
        print(f"returncode: {result.returncode}")
        print(result.output.rstrip())
        print()
        if result.returncode != 0:
            all_findings.append(f"{result.name}: command returned {result.returncode}")
        all_findings.extend(f"{result.name}: {line}" for line in LooksUnhealthy(result.output))

    if all_findings:
        print("===== potential HCCS/topology findings =====")
        for finding in all_findings:
            print(finding)
        return 2

    print("topology collection complete; no obvious HCCS health issue parsed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
