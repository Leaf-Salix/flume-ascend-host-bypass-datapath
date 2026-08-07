#!/usr/bin/env python3
"""Collect a text-only CANN compatibility fixture for Flume development."""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import platform
import re
import shutil
import subprocess
import sys
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
        code, output = run_capture([nm, "-D", "--defined-only", str(lib_path)], 60)
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
        "- `lib-manifest.txt`: text library manifest with sizes",
        "- `lib-symbols/`: `nm -D` or `readelf -Ws` output for key libraries",
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
    write_text(out_dir / "lib-manifest.txt",
               collect_lib_manifest(ascend_home, lib_roots))

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
