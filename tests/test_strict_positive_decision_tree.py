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
        "payload_kernel_hcomm_ret=0 fallback=none\"",
        "rank 1 hcomm payload smoke passed: fallback=none" + verify +
        " detail=\"stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 fallback=none\"",
        "",
    ])


def strict_log_with_cross_line_false_positive() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke passed: fallback=none detail=\""
        "stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 fallback=none payload_verify=passed\"",
        "rank 1 hcomm payload smoke passed: fallback=none "
        "payload_verify=passed detail=\"fallback=none\"",
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

        strict_cross_line = write(tmp / "strict-cross-line.log",
                                  strict_log_with_cross_line_false_positive())
        cross_line_dir = tmp / "cross-line"
        cross_line_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            cross_line_dir, smoke, strict_cross_line, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| rank0 strict evidence | passed |" in text
        assert "| rank1 strict evidence | missing |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_cross_line_false_positive())[0]

        log_dir = tmp / "flume-check-synthetic-pass"
        log_dir.mkdir()
        write(log_dir / "00-hcomm-custom-op-package-preflight.log",
              "required=canary_direct_aclrt,payload_direct_aclrt\n"
              "status=PASS\n")
        write(log_dir / "01-hccl-collective-smoke.log",
              "FLUME_BACKEND_CAPS hcomm_primitives=on "
              "hcomm_payload_scheduler_candidate=on\n"
              "hccl collective smoke passed p2p_copy=on\n"
              "hcomm channel probe passed\n"
              "storage HBM smoke passed\n")
        write(log_dir / "02-hcomm-payload-strict-positive.log",
              strict_log(True))
        tree, passed, smoke_log, found_strict_log, package_log = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(log_dir))
        assert passed
        assert smoke_log == log_dir / "01-hccl-collective-smoke.log"
        assert found_strict_log == log_dir / "02-hcomm-payload-strict-positive.log"
        assert package_log == log_dir / "00-hcomm-custom-op-package-preflight.log"
        assert tree.exists()

        fail_log_dir = tmp / "flume-check-synthetic-fail"
        fail_log_dir.mkdir()
        write(fail_log_dir / "00-hcomm-custom-op-package-preflight.log",
              "required=canary_direct_aclrt,payload_direct_aclrt\n"
              "status=PASS\n")
        write(fail_log_dir / "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        _tree, passed, _smoke_log, _strict_log, _package_log = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(fail_log_dir))
        assert not passed

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
