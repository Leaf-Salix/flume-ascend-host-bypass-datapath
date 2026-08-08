#!/usr/bin/env python3
"""Synthetic tests for the strict HCOMM payload decision tree gate."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path


def load_flume_tool(repo: Path):
    spec = importlib.util.spec_from_file_location(
        "flume_tool_under_test", repo / "tools" / "flume_tool.py")
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load tools/flume_tool.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def strict_log(include_verify: bool) -> str:
    verify = " payload_verify=passed payload_checksum=1234" if include_verify else ""
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke passed: fallback=none detail=\""
        "stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_status_word=0 "
        "fallback=none\"",
        "rank 1 hcomm payload smoke passed: fallback=none" + verify +
        " detail=\"stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_status_word=0 "
        "fallback=none\"",
        "",
    ])


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: test_strict_positive_decision_tree.py <repo-root>",
              file=sys.stderr)
        return 2
    repo = Path(sys.argv[1]).resolve()
    flume_tool = load_flume_tool(repo)

    with tempfile.TemporaryDirectory(prefix="flume-strict-tree-") as tmp_text:
        tmp = Path(tmp_text)
        smoke = write(
            tmp / "smoke.log",
            "FLUME_BACKEND_CAPS hcomm_primitives=on "
            "hcomm_payload_scheduler_candidate=on\n"
            "hccl collective smoke passed p2p_copy=on\n"
            "hcomm channel probe passed\n"
            "storage HBM smoke passed\n")
        package = write(
            tmp / "package.log",
            "required=canary_direct_aclrt,payload_direct_aclrt\n"
            "status=PASS\n")

        strict_pass = write(tmp / "strict-pass.log", strict_log(True))
        pass_dir = tmp / "pass"
        pass_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            pass_dir, smoke, strict_pass, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | yes |" in text
        assert "start Stage 3B.4 storage rewiring" in text

        strict_no_verify = write(tmp / "strict-no-verify.log",
                                 strict_log(False))
        no_verify_dir = tmp / "no-verify"
        no_verify_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_verify_dir, smoke, strict_no_verify, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| rank1 verify | missing |" in text
        assert "inspect hcomm-payload-strict-positive failure" in text

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
