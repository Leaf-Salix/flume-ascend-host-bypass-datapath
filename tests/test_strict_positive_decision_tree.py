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


def strict_log_with_nonzero_hcomm_ret() -> str:
    return strict_log(True).replace(
        "payload_kernel_hcomm_ret=0", "payload_kernel_hcomm_ret=42")


def strict_log_with_missing_handoff() -> str:
    return strict_log(True).replace(
        "stage3b3e_payload_descriptor_handoff=passed ", "")


def strict_log_with_missing_semantic() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke unsupported: fallback=none detail=\""
        "stage3b3e_payload_copy=unsupported "
        "stage3b3e_direct_aclrt_payload_loader=unsupported "
        "payload_semantic=missing "
        "stage3b3e_payload_descriptor_handoff=blocked "
        "stage3b3e_direct_aclrt_payload_launch=not-attempted "
        "fallback=none\"",
        "",
    ])


def strict_log_with_canary_build_mode() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke unsupported: fallback=none detail=\""
        "stage3b3e_payload_copy=unsupported "
        "stage3b3e_direct_aclrt_payload_loader=unsupported "
        "payload_build_mode=not-internal "
        "stage3b3e_payload_descriptor_handoff=blocked "
        "stage3b3e_direct_aclrt_payload_launch=not-attempted "
        "fallback=none\"",
        "",
    ])


def payload_ready_package_log() -> str:
    return ("required=canary_direct_aclrt,payload_direct_aclrt,"
            "payload_abi_v2,payload_semantic,build_mode_internal\n"
            "status=PASS\n")


def stale_semantic_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v2,payload_semantic,build_mode_internal",
        "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=missing",
        "function_so.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=missing",
        "status=FAIL",
        "reason=payload kernel package is missing the payload semantic marker",
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
            payload_ready_package_log())

        strict_pass = write(tmp / "strict-pass.log", strict_log(True))
        pass_dir = tmp / "pass"
        pass_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            pass_dir, smoke, strict_pass, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | yes |" in text
        assert "`payload_kernel_hcomm_ret=0`" in text
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

        strict_nonzero_hcomm = write(tmp / "strict-nonzero-hcomm.log",
                                     strict_log_with_nonzero_hcomm_ret())
        nonzero_hcomm_dir = tmp / "nonzero-hcomm"
        nonzero_hcomm_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            nonzero_hcomm_dir, smoke, strict_nonzero_hcomm, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| kernel HCOMM ret | 42 |" in text
        assert "inspect in-kernel HCOMM primitive return code: 42" in text

        strict_missing_handoff = write(tmp / "strict-missing-handoff.log",
                                       strict_log_with_missing_handoff())
        missing_handoff_dir = tmp / "missing-handoff"
        missing_handoff_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            missing_handoff_dir, smoke, strict_missing_handoff, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| descriptor handoff | missing |" in text
        assert "inspect direct ACL payload descriptor handoff" in text

        stale_package = write(tmp / "package-stale-semantic.log",
                              stale_semantic_package_log())
        strict_missing_semantic = write(tmp / "strict-missing-semantic.log",
                                        strict_log_with_missing_semantic())
        stale_semantic_dir = tmp / "stale-semantic"
        stale_semantic_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            stale_semantic_dir, smoke, strict_missing_semantic, stale_package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM custom-op package payload-ready? | not-ready |" in text
        assert "| payload semantic marker | missing |" in text
        assert "installed package has stale semantics" in text

        strict_canary_mode = write(tmp / "strict-canary-mode.log",
                                   strict_log_with_canary_build_mode())
        canary_mode_dir = tmp / "canary-mode"
        canary_mode_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            canary_mode_dir, smoke, strict_canary_mode, package)
        text = tree.read_text(encoding="utf-8")
        assert "| payload build mode | not-internal |" in text
        assert "installed package is canary/stub-only" in text

        log_dir = tmp / "flume-check-synthetic-pass"
        log_dir.mkdir()
        write(log_dir / "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
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
              payload_ready_package_log())
        write(fail_log_dir / "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        _tree, passed, _smoke_log, _strict_log, _package_log = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(fail_log_dir))
        assert not passed

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
