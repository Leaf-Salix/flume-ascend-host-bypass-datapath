#!/usr/bin/env python3
"""Collect a text-only CANN compatibility fixture for Flume development."""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "refer" / "cann-compat"

KEY_HEADERS = [
    "hccl/hccl.h",
    "hccl/hccl_types.h",
    "hccl/hccl_comm.h",
    "hccl/hccl_res.h",
    "hccl/hccl_res_expt.h",
    "hccl/hccl_rank_graph.h",
    "hccl/hcomm_primitives.h",
    "hcomm_primitives.h",
    "acl/acl.h",
    "acl/acl_rt.h",
    "securec.h",
]

KEY_LIBS = [
    "libhccl.so",
    "libhcomm.so",
    "libascendcl.so",
    "libc_sec.so",
]

HCOMM_PRIMITIVE_HEADERS = [
    "hccl/hcomm_primitives.h",
    "hcomm/hcomm_primitives.h",
    "hcomm_primitives.h",
]

HCOMM_PRIMITIVE_NAMES = [
    "HcommAcquireComm",
    "HcommReleaseComm",
    "HcommBatchModeStart",
    "HcommBatchModeEnd",
    "HcommLocalCopyOnThread",
    "HcommReadOnThread",
    "HcommWriteOnThread",
    "HcommChannelNotifyRecordOnThread",
    "HcommChannelNotifyWaitOnThread",
    "HcommChannelFenceOnThread",
    "HcommThreadNotifyRecordOnThread",
    "HcommThreadNotifyWaitOnThread",
]

HCOMM_OPTIONAL_PRIMITIVE_NAMES = [
    "HcommWriteWithNotifyOnThread",
    "HcommWriteWithNotifyNbiOnThread",
]

HCOMM_CALL_SHAPE_PROBE = r"""
#include <cstdint>
#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#elif __has_include(<hcomm/hcomm_primitives.h>)
#include <hcomm/hcomm_primitives.h>
#elif __has_include(<hcomm_primitives.h>)
#include <hcomm_primitives.h>
#else
#error no hcomm_primitives header
#endif

int main() {
  ThreadHandle thread = 0;
  ThreadHandle peer_thread = 0;
  ChannelHandle channel = 0;
  void *dst = nullptr;
  const void *src = nullptr;
  const char *tag = "flume-probe";
  uint64_t bytes = 64;
  uint32_t notify_idx = 0;
  uint32_t timeout_sec = 1;
  auto acquire_ret = HcommAcquireComm(tag);
  auto local_copy_ret = HcommLocalCopyOnThread(thread, dst, src, bytes);
  auto read_ret = HcommReadOnThread(thread, channel, dst, src, bytes);
  auto write_ret = HcommWriteOnThread(thread, channel, dst, src, bytes);
  auto record_ret =
      HcommChannelNotifyRecordOnThread(thread, channel, notify_idx);
  auto wait_ret =
      HcommChannelNotifyWaitOnThread(thread, channel, notify_idx, timeout_sec);
  auto fence_ret = HcommChannelFenceOnThread(thread, channel);
  auto thread_record_ret =
      HcommThreadNotifyRecordOnThread(thread, peer_thread, notify_idx);
  auto thread_wait_ret =
      HcommThreadNotifyWaitOnThread(thread, notify_idx, timeout_sec);
  auto batch_start_ret = HcommBatchModeStart(tag);
  auto batch_end_ret = HcommBatchModeEnd(tag);
  auto release_ret = HcommReleaseComm(tag);
  (void)acquire_ret;
  (void)local_copy_ret;
  (void)read_ret;
  (void)write_ret;
  (void)record_ret;
  (void)wait_ret;
  (void)fence_ret;
  (void)thread_record_ret;
  (void)thread_wait_ret;
  (void)batch_start_ret;
  (void)batch_end_ret;
  (void)release_ret;
  return 0;
}
"""

HCOMM_OPTIONAL_CALL_SHAPE_PROBE = r"""
#include <cstdint>
#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#elif __has_include(<hcomm/hcomm_primitives.h>)
#include <hcomm/hcomm_primitives.h>
#elif __has_include(<hcomm_primitives.h>)
#include <hcomm_primitives.h>
#else
#error no hcomm_primitives header
#endif

#ifndef FLUME_PROBE_NBI_WRITE_WITH_NOTIFY
#define FLUME_PROBE_NBI_WRITE_WITH_NOTIFY 0
#endif

int main() {
  ThreadHandle thread = 0;
  ChannelHandle channel = 0;
  void *dst = nullptr;
  const void *src = nullptr;
  uint64_t bytes = 64;
  uint32_t notify_idx = 0;
#if FLUME_PROBE_NBI_WRITE_WITH_NOTIFY
  auto ret =
      HcommWriteWithNotifyNbiOnThread(thread, channel, dst, src, bytes,
                                      notify_idx);
#else
  auto ret =
      HcommWriteWithNotifyOnThread(thread, channel, dst, src, bytes,
                                   notify_idx);
#endif
  (void)ret;
  return 0;
}
"""

FEATURE_LINE_RE = re.compile(
    r"(FLUME_HAVE_[A-Z0-9_]+: [01]|"
    r"hccl feature probe: .*|"
    r"FLUME_BACKEND_CAPS .*|"
    r"hcomm channel probe .*|"
    r"resolved_hcomm_engine=.*|"
    r"HCOMM .*unavailable.*|"
    r"HCOMM .*unsupported.*)",
    re.IGNORECASE,
)


def sanitize_label(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9._-]+", "-", text)
    text = text.strip("-._")
    return text or "cann-unknown"


def run_capture(command: list[str], timeout_seconds: int = 20) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_seconds,
        )
        return proc.returncode, proc.stdout or ""
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout if isinstance(exc.stdout, str) else ""
        output += f"\nTIMEOUT after {timeout_seconds} seconds\n"
        return 124, output
    except OSError as exc:
        return 127, f"failed to start {' '.join(command)}: {exc}\n"


def common_ascend_candidates() -> list[Path]:
    candidates: list[Path] = []
    for value in (
        os.environ.get("ASCEND_HOME_PATH"),
        os.environ.get("ASCEND_OPP_PATH"),
        "/usr/local/Ascend/cann",
        "/usr/local/Ascend/ascend-toolkit/latest",
        "/usr/local/Ascend/cann-8.5/cann-8.5.0",
        "/usr/local/Ascend/cann-8.5.0",
    ):
        if value:
            candidates.append(Path(value).expanduser())
    return candidates


def resolve_ascend_home(value: str) -> Path:
    if value:
        path = Path(value).expanduser()
        if path.exists():
            return path.resolve()
        raise SystemExit(f"--ascend-home does not exist: {path}")
    for candidate in common_ascend_candidates():
        if candidate.exists():
            return candidate.resolve()
    raise SystemExit(
        "Unable to find CANN. Set ASCEND_HOME_PATH or pass --ascend-home."
    )


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def iter_with_depth(root: Path, max_depth: int) -> Iterable[Path]:
    root = root.resolve()
    for current, dirs, files in os.walk(root):
        current_path = Path(current)
        try:
            depth = len(current_path.relative_to(root).parts)
        except ValueError:
            continue
        if depth >= max_depth:
            dirs[:] = []
        yield current_path
        for name in files:
            yield current_path / name


def discover_include_roots(ascend_home: Path) -> list[Path]:
    roots: list[Path] = []
    for path in iter_with_depth(ascend_home, 5):
        if not path.is_dir():
            continue
        if path.name in {"include", "pkg_inc"}:
            roots.append(path)
        elif path.name == "external" and path.parent.name == "include":
            roots.append(path)
    unique: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        resolved = root.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique.append(root)
    return unique


def discover_lib_roots(ascend_home: Path) -> list[Path]:
    roots: list[Path] = []
    for path in iter_with_depth(ascend_home, 5):
        if not path.is_dir():
            continue
        if path.name in {"lib", "lib64", "devlib"}:
            roots.append(path)
    unique: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        resolved = root.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique.append(root)
    return unique


def find_header(include_roots: list[Path], header: str) -> Optional[Path]:
    for root in include_roots:
        candidate = root / header
        if candidate.exists():
            return candidate
    return None


def find_library(lib_roots: list[Path], lib_name: str) -> Optional[Path]:
    for root in lib_roots:
        exact = root / lib_name
        if exact.exists():
            return exact
    for root in lib_roots:
        try:
            matches = sorted(root.glob(f"{lib_name}*"))
        except OSError:
            matches = []
        for match in matches:
            if match.is_file() or match.is_symlink():
                return match
    return None


def collect_env(ascend_home: Path) -> str:
    keys = [
        "ASCEND_HOME_PATH",
        "ASCEND_OPP_PATH",
        "ASCEND_AICPU_PATH",
        "LD_LIBRARY_PATH",
        "PATH",
        "PYTHONPATH",
        "HCCL_IF_IP",
        "HCCL_SOCKET_IFNAME",
        "ASCEND_RT_VISIBLE_DEVICES",
    ]
    lines = [
        f"timestamp: {_dt.datetime.now().isoformat(timespec='seconds')}",
        f"repo: {REPO_ROOT}",
        f"platform: {platform.system()} {platform.release()} {platform.machine()}",
        f"python: {sys.version.split()[0]} ({sys.executable})",
        f"ascend_home: {ascend_home}",
        "",
    ]
    for key in keys:
        lines.append(f"{key}: {os.environ.get(key, 'not set')}")
    return "\n".join(lines) + "\n"


def collect_version_files(ascend_home: Path) -> str:
    wanted = re.compile(r"(install\.info|version.*\.info|version.*\.txt)$")
    lines: list[str] = []
    for path in iter_with_depth(ascend_home, 5):
        if not path.is_file() or not wanted.search(path.name):
            continue
        try:
            rel = path.relative_to(ascend_home)
        except ValueError:
            rel = path
        lines.append(f"===== {rel} =====")
        try:
            lines.append(path.read_text(encoding="utf-8", errors="replace").strip())
        except OSError as exc:
            lines.append(f"failed to read: {exc}")
        lines.append("")
    if not lines:
        lines.append("no version/install info files found")
    return "\n".join(lines).rstrip() + "\n"


def infer_label(ascend_home: Path, version_text: str) -> str:
    version_match = re.search(r"(?:version|Version|VERSION)[^0-9]*(\d+\.\d+(?:\.\d+)?)",
                              version_text)
    arch = platform.machine() or "unknown-arch"
    if version_match:
        return sanitize_label(f"cann-{version_match.group(1)}-{arch}")
    for part in reversed(ascend_home.parts):
        if re.search(r"\d+\.\d+", part):
            return sanitize_label(f"{part}-{arch}")
    return sanitize_label(f"{ascend_home.name}-{arch}")


def collect_include_manifest(ascend_home: Path, include_roots: list[Path]) -> str:
    lines = ["# include roots"]
    for root in include_roots:
        lines.append(str(root))
    lines.append("")
    lines.append("# headers")
    for root in include_roots:
        for current, _dirs, files in os.walk(root):
            current_path = Path(current)
            for name in sorted(files):
                if not name.endswith((".h", ".hpp", ".hh", ".inc")):
                    continue
                path = current_path / name
                try:
                    rel_home = path.relative_to(ascend_home)
                except ValueError:
                    rel_home = path
                lines.append(str(rel_home))
    return "\n".join(lines) + "\n"


def collect_header_presence(include_roots: list[Path]) -> str:
    lines = ["# key header presence"]
    for header in KEY_HEADERS:
        found = find_header(include_roots, header)
        lines.append(f"{header}: {'FOUND ' + str(found) if found else 'missing'}")
    return "\n".join(lines) + "\n"


def extract_declaration_blocks(text: str, names: list[str]) -> list[str]:
    lines = text.splitlines()
    blocks: list[str] = []
    for index, line in enumerate(lines):
        if not any(name in line for name in names):
            continue
        start = index
        while start > 0 and lines[start - 1].strip() and not lines[start - 1].lstrip().startswith("#"):
            if ";" in lines[start - 1] or "{" in lines[start - 1] or "}" in lines[start - 1]:
                break
            start -= 1
        end = index
        while end + 1 < len(lines) and ";" not in lines[end] and "{" not in lines[end]:
            end += 1
        block = "\n".join(lines[start:end + 1]).strip()
        if block and block not in blocks:
            blocks.append(block)
    return blocks


def collect_hcomm_primitive_headers(include_roots: list[Path]) -> str:
    lines = [
        "# HCOMM primitive header excerpts",
        "",
        "This file captures declaration snippets that affect Flume's AICPU payload "
        "kernel call shape. It is a compatibility aid; do not treat it as a "
        "vendored copy of CANN headers.",
        "",
    ]
    found_any = False
    type_patterns = [
        re.compile(r"\b(?:typedef|using)\b.*\b(?:ThreadHandle|ChannelHandle)\b"),
        re.compile(r"\b(?:struct|class|enum)\b.*\b(?:ThreadHandle|ChannelHandle)\b"),
    ]
    for header in HCOMM_PRIMITIVE_HEADERS:
        path = find_header(include_roots, header)
        lines.append(f"## {header}: {'FOUND ' + str(path) if path else 'missing'}")
        if path is None:
            lines.append("")
            continue
        found_any = True
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            lines.append(f"failed to read: {exc}")
            lines.append("")
            continue
        type_lines = [
            raw.strip() for raw in text.splitlines()
            if any(pattern.search(raw) for pattern in type_patterns)
        ]
        if type_lines:
            lines.append("### handle type lines")
            lines.extend(type_lines)
        declarations = extract_declaration_blocks(
            text, HCOMM_PRIMITIVE_NAMES + HCOMM_OPTIONAL_PRIMITIVE_NAMES)
        if declarations:
            lines.append("### primitive declarations")
            lines.extend(declarations)
        else:
            lines.append("no matching primitive declarations found")
        lines.append("")
    if not found_any:
        lines.append("no HCOMM primitive header found")
    return "\n".join(lines).rstrip() + "\n"


def collect_hcomm_primitive_symbols(lib_roots: list[Path]) -> str:
    lib_path = find_library(lib_roots, "libhcomm.so")
    if lib_path is None:
        lib_path = find_library(lib_roots, "libhcomm.dylib")
    if lib_path is None:
        return "libhcomm.so: missing\n"
    symbols = collect_symbols(lib_path)
    lines = [
        f"# library: {lib_path}",
        "",
        "# HCOMM primitive symbol presence",
    ]
    for name in HCOMM_PRIMITIVE_NAMES:
        matches = [
            line for line in symbols.splitlines()
            if name in line
        ]
        lines.append(f"{name}: {'present' if matches else 'missing'}")
        for match in matches[:8]:
            lines.append(f"  {match}")
        if len(matches) > 8:
            lines.append(f"  ... {len(matches) - 8} more matches")
    lines.append("")
    lines.append("# Optional HCOMM primitive symbol presence")
    for name in HCOMM_OPTIONAL_PRIMITIVE_NAMES:
        matches = [
            line for line in symbols.splitlines()
            if name in line
        ]
        lines.append(f"{name}: {'present' if matches else 'missing'}")
        for match in matches[:8]:
            lines.append(f"  {match}")
        if len(matches) > 8:
            lines.append(f"  ... {len(matches) - 8} more matches")
    return "\n".join(lines) + "\n"


def compile_probe_text(include_roots: list[Path], source_text: str,
                       probe_name: str,
                       extra_flags: Optional[list[str]] = None) -> str:
    compiler_value = os.environ.get("CXX") or shutil.which("c++") or shutil.which("g++")
    if not compiler_value:
        return "status: SKIP\nreason: no C++ compiler found\n"
    compiler_command = shlex.split(compiler_value)
    with tempfile.TemporaryDirectory(prefix="flume-hcomm-abi-probe-") as tmp_text:
        tmp = Path(tmp_text)
        source = tmp / f"{probe_name}.cc"
        source.write_text(source_text, encoding="utf-8")
        command = compiler_command + [
            "-std=c++17",
            "-fsyntax-only",
            str(source),
        ]
        if extra_flags:
            command.extend(extra_flags)
        for root in include_roots:
            command.extend(["-I", str(root)])
        code, output = run_capture(command, 30)
    return "\n".join([
        f"compiler: {compiler_value}",
        "command: " + " ".join(command),
        f"returncode: {code}",
        f"status: {'PASS' if code == 0 else 'FAIL'}",
        "",
        output.rstrip() if output.strip() else "no compiler output",
    ]).rstrip() + "\n"


def collect_hcomm_primitive_compile_probe(include_roots: list[Path]) -> str:
    lines = [
        "# HCOMM primitive call-shape compile probe",
        "",
        "This probe compiles the call expressions used by Flume's direct ACL "
        "payload kernel. It does not link, load, launch, or run any NPU code.",
        "",
        "",
    ]
    lines.append(compile_probe_text(include_roots, HCOMM_CALL_SHAPE_PROBE,
                                    "hcomm_call_shape_probe"))
    lines.extend([
        "",
        "## optional HcommWriteWithNotifyOnThread",
        "",
        "This optional probe may fail on CANN builds that only expose the NBI "
        "variant or do not expose fused write-with-notify.",
        "",
    ])
    lines.append(compile_probe_text(
        include_roots, HCOMM_OPTIONAL_CALL_SHAPE_PROBE,
        "hcomm_write_with_notify_call_shape_probe"))
    lines.extend([
        "",
        "## optional HcommWriteWithNotifyNbiOnThread",
        "",
        "This optional probe may fail on CANN builds that only expose the "
        "blocking variant or do not expose fused write-with-notify.",
        "",
    ])
    lines.append(compile_probe_text(
        include_roots, HCOMM_OPTIONAL_CALL_SHAPE_PROBE,
        "hcomm_write_with_notify_nbi_call_shape_probe",
        ["-DFLUME_PROBE_NBI_WRITE_WITH_NOTIFY=1"]))
    return "\n".join(lines).rstrip() + "\n"


def collect_lib_manifest(ascend_home: Path, lib_roots: list[Path]) -> str:
    lines = ["# lib roots"]
    for root in lib_roots:
        lines.append(str(root))
    lines.append("")
    lines.append("# libraries")
    for root in lib_roots:
        for current, _dirs, files in os.walk(root):
            current_path = Path(current)
            for name in sorted(files):
                if ".so" not in name and not name.endswith((".a", ".dylib")):
                    continue
                path = current_path / name
                try:
                    rel_home = path.relative_to(ascend_home)
                except ValueError:
                    rel_home = path
                try:
                    size = path.stat().st_size
                except OSError:
                    size = -1
                lines.append(f"{rel_home}\t{size}")
    return "\n".join(lines) + "\n"


def collect_symbols(lib_path: Path) -> str:
    nm = shutil.which("nm")
    if nm:
        nm_command = ([nm, "-gU", str(lib_path)]
                      if platform.system() == "Darwin"
                      else [nm, "-D", "--defined-only", str(lib_path)])
        code, output = run_capture(nm_command, 60)
        if code == 0:
            return output
        nm_output = output
    else:
        nm_output = "nm not found\n"

    readelf = shutil.which("readelf")
    if readelf:
        code, output = run_capture([readelf, "-Ws", str(lib_path)], 60)
        if code == 0:
            return output
        return nm_output + "\nreadelf failed:\n" + output
    return nm_output


def extract_flume_log_signals(flume_log_dir: Optional[Path]) -> tuple[str, str]:
    if flume_log_dir is None:
        return "no --flume-log-dir provided\n", "no --flume-log-dir provided\n"
    if not flume_log_dir.exists():
        return f"missing flume log dir: {flume_log_dir}\n", (
            f"missing flume log dir: {flume_log_dir}\n"
        )
    feature_lines: list[str] = []
    caps_lines: list[str] = []
    for log in sorted(flume_log_dir.glob("*.log")) + sorted(flume_log_dir.glob("*.txt")):
        try:
            text = log.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            if FEATURE_LINE_RE.search(line):
                rel = log.name
                feature_lines.append(f"{rel}: {line}")
                if ("FLUME_BACKEND_CAPS" in line or
                        "hcomm channel probe" in line or
                        "hcomm payload smoke" in line):
                    caps_lines.append(f"{rel}: {line}")
    if not feature_lines:
        feature_lines.append("no Flume feature lines found")
    if not caps_lines:
        caps_lines.append("no Flume backend caps lines found")
    return "\n".join(feature_lines) + "\n", "\n".join(caps_lines) + "\n"


def collect_npu_smi() -> str:
    npu_smi = shutil.which("npu-smi")
    if not npu_smi:
        return "npu-smi not found\n"
    sections: list[str] = []
    for command in ([npu_smi, "info"], [npu_smi, "info", "-t", "topo"]):
        code, output = run_capture(command, 30)
        sections.append(f"$ {' '.join(command)}")
        sections.append(f"returncode: {code}")
        sections.append(output.rstrip())
        sections.append("")
    return "\n".join(sections).rstrip() + "\n"


def collect_hccn_ips(devices: str) -> str:
    if not devices:
        return "no --devices provided\n"
    tool = shutil.which("hccn_tool")
    if not tool:
        return "hccn_tool not found\n"
    lines: list[str] = []
    for raw in devices.split(","):
        device = raw.strip()
        if not device:
            continue
        if not device.isdigit():
            lines.append(f"device {device}: skipped, not an integer")
            continue
        command = [tool, "-i", device, "-ip", "-g"]
        code, output = run_capture(command, 15)
        lines.append(f"$ {' '.join(command)}")
        lines.append(f"returncode: {code}")
        lines.append(output.rstrip())
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_summary(
    out_dir: Path,
    ascend_home: Path,
    label: str,
    include_roots: list[Path],
    lib_roots: list[Path],
    flume_log_dir: Optional[Path],
) -> None:
    lines = [
        "# CANN Compatibility Fixture",
        "",
        f"- label: `{label}`",
        f"- ascend_home: `{ascend_home}`",
        f"- include roots: {len(include_roots)}",
        f"- lib roots: {len(lib_roots)}",
        f"- flume log dir: `{flume_log_dir}`" if flume_log_dir else "- flume log dir: not provided",
        "",
        "## Files",
        "",
        "- `VERSION.txt`: CANN install/version snippets",
        "- `env.txt`: host environment relevant to CANN/HCCL",
        "- `include-manifest.txt`: text header manifest",
        "- `include-feature-presence.txt`: key Flume/HCCL/HCOMM headers",
        "- `hcomm-primitive-headers.txt`: HCOMM primitive declaration snippets",
        "- `hcomm-primitive-call-shape-probe.txt`: compile-only probe for "
        "Flume's current HCOMM primitive call shape",
        "- `lib-manifest.txt`: text library manifest with sizes",
        "- `lib-symbols/`: `nm -D` or `readelf -Ws` output for key libraries",
        "- `hcomm-primitive-symbols.txt`: targeted HCOMM primitive symbols in "
        "`libhcomm`",
        "- `cmake-feature-probe.txt`: Flume CMake/smoke feature lines from logs",
        "- `flume-backend-caps.txt`: `FLUME_BACKEND_CAPS` and HCOMM probe lines",
        "- `npu-smi.txt`: `npu-smi info` and topology output when available",
        "- `hccn-ips.txt`: selected HCCN IPs when `--devices` is provided",
        "",
        "Do not commit machine-specific fixture directories unless they have been reviewed.",
    ]
    write_text(out_dir / "summary.md", "\n".join(lines) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect a text-only CANN compatibility fixture for Flume."
    )
    parser.add_argument("--ascend-home", default="",
                        help="CANN root; defaults to ASCEND_HOME_PATH/common paths")
    parser.add_argument("--label", default="",
                        help="Fixture label, for example cann-8.5.0-aarch64")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT),
                        help="Root directory for generated fixture")
    parser.add_argument("--flume-log-dir", default="",
                        help="Optional logs/flume-check-* directory to parse")
    parser.add_argument("--devices", default="",
                        help="Optional comma-separated physical device ids for hccn_tool")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ascend_home = resolve_ascend_home(args.ascend_home)
    output_root = Path(args.output_root).expanduser()
    version_text = collect_version_files(ascend_home)
    label = sanitize_label(args.label) if args.label else infer_label(ascend_home, version_text)
    out_dir = output_root / label
    out_dir.mkdir(parents=True, exist_ok=True)

    include_roots = discover_include_roots(ascend_home)
    lib_roots = discover_lib_roots(ascend_home)
    flume_log_dir = Path(args.flume_log_dir).expanduser() if args.flume_log_dir else None

    write_text(out_dir / "VERSION.txt", version_text)
    write_text(out_dir / "env.txt", collect_env(ascend_home))
    write_text(out_dir / "include-manifest.txt",
               collect_include_manifest(ascend_home, include_roots))
    write_text(out_dir / "include-feature-presence.txt",
               collect_header_presence(include_roots))
    write_text(out_dir / "hcomm-primitive-headers.txt",
               collect_hcomm_primitive_headers(include_roots))
    write_text(out_dir / "hcomm-primitive-call-shape-probe.txt",
               collect_hcomm_primitive_compile_probe(include_roots))
    write_text(out_dir / "lib-manifest.txt",
               collect_lib_manifest(ascend_home, lib_roots))
    write_text(out_dir / "hcomm-primitive-symbols.txt",
               collect_hcomm_primitive_symbols(lib_roots))

    symbol_dir = out_dir / "lib-symbols"
    symbol_dir.mkdir(parents=True, exist_ok=True)
    for lib_name in KEY_LIBS:
        lib_path = find_library(lib_roots, lib_name)
        if lib_path is None:
            write_text(symbol_dir / f"{lib_name}.symbols.txt", f"{lib_name}: missing\n")
            continue
        text = f"# library: {lib_path}\n\n" + collect_symbols(lib_path)
        write_text(symbol_dir / f"{lib_name}.symbols.txt", text)

    feature_text, caps_text = extract_flume_log_signals(flume_log_dir)
    write_text(out_dir / "cmake-feature-probe.txt", feature_text)
    write_text(out_dir / "flume-backend-caps.txt", caps_text)
    write_text(out_dir / "npu-smi.txt", collect_npu_smi())
    write_text(out_dir / "hccn-ips.txt", collect_hccn_ips(args.devices))
    write_summary(out_dir, ascend_home, label, include_roots, lib_roots, flume_log_dir)

    print(f"[ok] CANN compatibility fixture -> {out_dir}")
    print(f"[ok] summary -> {out_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
