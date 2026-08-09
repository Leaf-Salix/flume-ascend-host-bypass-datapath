#!/usr/bin/env python3
"""Reject HCCL payload API shortcuts in HCOMM payload source ranges."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path


def load_flume_tool(repo_root: Path):
    spec = importlib.util.spec_from_file_location(
        "flume_tool_source_gate_under_test",
        repo_root / "tools" / "flume_tool.py")
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load tools/flume_tool.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run_self_tests(flume_tool) -> None:
    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        write(tmp / "custom_ops" / "hcomm_payload_copy" / "kernel.cc",
              "void kernel() { HcommReadOnThread(nullptr); }\n")
        write(tmp / "src" / "hcomm_payload" / "scheduler.cc",
              "void scheduler() { HcommWriteOnThread(nullptr); }\n")
        write(tmp / "src" / "core" / "client.cc",
              "int flume_hccl_p2p_send() {\n"
              "  HcclSend(nullptr, 0, HCCL_DATA_TYPE_UINT8, 1, nullptr, nullptr);\n"
              "  return 0;\n"
              "}\n"
              "\n"
              "int flume_hcomm_payload_send_ex(\n"
              "    flume_client_t* client) {\n"
              "  HcommReadOnThread(nullptr);\n"
              "  return 0;\n"
              "}\n")
        passed, lines = flume_tool.HcommPayloadSourceGateLines(tmp)
        if not passed:
            raise AssertionError("\n".join(lines))
        joined = "\n".join(lines)
        assert "hcomm_payload_host_source_no_hccl_payload_api=passed" in joined
        assert "host_ranges_scanned=1" in joined

    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        write(tmp / "src" / "core" / "client.cc",
              "int flume_hcomm_payload_recv_ex(\n"
              "    flume_client_t* client) {\n"
              "  HcclRecv(nullptr, 0, HCCL_DATA_TYPE_UINT8, 0, nullptr, nullptr);\n"
              "  return 0;\n"
              "}\n")
        passed, lines = flume_tool.HcommPayloadSourceGateLines(tmp)
        if passed:
            raise AssertionError("expected host scheduler violation")
        joined = "\n".join(lines)
        assert "hcomm_payload_host_source_no_hccl_payload_api=failed" in joined
        assert "range=flume_hcomm_payload_recv_ex:forbidden=HcclRecv" in joined


def main() -> int:
    repo_root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else (
        Path(__file__).resolve().parents[1])
    flume_tool = load_flume_tool(repo_root)
    run_self_tests(flume_tool)
    passed, lines = flume_tool.HcommPayloadSourceGateLines(repo_root)
    if not passed:
        print("HCOMM payload source gate failed:")
        print("\n".join(lines))
        print("")
        print("HCOMM payload kernels must use HCOMM primitives such as "
              "HcommReadOnThread/HcommWriteOnThread, not HCCL payload, "
              "collective, or one-sided APIs.")
        return 1

    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
