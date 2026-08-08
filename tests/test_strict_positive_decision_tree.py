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
    desc = (" payload_desc_role=0 payload_desc_local_rank=0 "
            "payload_desc_peer_rank=1 payload_desc_rank_size=2 "
            "payload_desc_bytes=4096 payload_desc_ready_notify_idx=0 "
            "payload_desc_done_notify_idx=1 "
            "payload_desc_thread_notify_mode=0 "
            "payload_desc_completion_mode=0 "
            "payload_desc_timeout_sec=60 payload_desc_status_schema=v2 "
            "payload_desc_status_word_count=8 "
            "payload_desc_batch_tag=default "
            "payload_recv_path=local-buffer "
            "payload_desc_local_hccl_buffer_bytes=8192 "
            "payload_desc_remote_hccl_buffer_bytes=8192")
    resource = (" payload_resolved_engine=aicpu-ts "
                "payload_resolved_protocol=hccs "
                "payload_channel_desc=rank-graph "
                "payload_channel_count=1 payload_notify_num=2 "
                "payload_usable_hccl_buffer_bytes=8192 "
                "payload_local_hccl_buffer_bytes=8192 "
                "payload_remote_hccl_buffer_bytes=8192")
    package_runtime = (" package_source=explicit-json "
                       "package_aicpu_tar=present "
                       "package_aicpu_tar_readable=yes "
	                       "payload_semantic=present "
	                       "payload_semantic_v5=present "
	                       "payload_semantic_v6=present "
	                       "payload_semantic_v7=present "
	                       "payload_semantic_v8=present")
    recv_desc = desc.replace("payload_desc_role=0", "payload_desc_role=1")
    recv_desc = recv_desc.replace("payload_desc_local_rank=0",
                                  "payload_desc_local_rank=1")
    recv_desc = recv_desc.replace("payload_desc_peer_rank=1",
                                  "payload_desc_peer_rank=0")
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke passed: fallback=none "
        "payload_pattern=strict-v1 detail=\""
        "stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 payload_primitive_state=completed "
        "payload_status_schema=v2 "
        "payload_status_word_count=8 payload_echo=passed payload_role=send "
        "payload_trace=passed payload_trace_schema=v2 "
        "payload_trace_word_count=80 payload_trace_event=kernel-exit "
        "payload_trace_order=passed "
        "payload_trace_ret_order=passed "
        "payload_trace_primitive_path=send-local-copy "
        "payload_trace_result=success "
        "payload_batch_mode=on payload_comm_acquire=default "
        "payload_comm_binding=comm-name "
        "payload_thread_notify_order=not-used" + desc + resource +
        package_runtime +
        " fallback=none\" "
        "payload_source_checksum=1234",
        "rank 1 hcomm payload smoke passed: fallback=none "
        "payload_pattern=strict-v1" + verify +
        " detail=\"stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 payload_primitive_state=completed "
        "payload_status_schema=v2 "
        "payload_status_word_count=8 payload_echo=passed payload_role=recv "
        "payload_trace=passed payload_trace_schema=v2 "
        "payload_trace_word_count=80 payload_trace_event=kernel-exit "
        "payload_trace_order=passed "
        "payload_trace_ret_order=passed "
        "payload_trace_primitive_path=recv-read-local-copy "
        "payload_trace_result=success "
        "payload_batch_mode=on payload_comm_acquire=default "
        "payload_comm_binding=comm-name "
        "payload_thread_notify_order=not-used" + recv_desc +
        resource + package_runtime + " fallback=none\" "
        "payload_expected_checksum=1234",
        "",
    ])


def strict_log_with_cross_line_false_positive() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke passed: fallback=none "
        "payload_pattern=strict-v1 detail=\""
        "stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 payload_primitive_state=completed "
        "payload_status_schema=v2 "
        "payload_status_word_count=8 payload_echo=passed payload_role=send "
        "payload_trace=passed payload_trace_schema=v2 "
        "payload_trace_word_count=80 payload_trace_event=kernel-exit "
        "payload_trace_order=passed "
        "payload_trace_ret_order=passed "
        "payload_trace_primitive_path=send-local-copy "
        "payload_trace_result=success "
        "payload_batch_mode=on payload_comm_acquire=default "
        "payload_comm_binding=comm-name "
        "payload_desc_batch_tag=default "
        "payload_recv_path=local-buffer "
	        "payload_semantic_v6=present "
	        "payload_semantic_v7=present "
	        "payload_semantic_v8=present "
        "payload_thread_notify_order=not-used fallback=none "
        "payload_verify=passed\"",
        "rank 1 hcomm payload smoke passed: fallback=none "
        "payload_verify=passed detail=\"fallback=none\"",
        "",
    ])


def strict_log_with_nonzero_hcomm_ret() -> str:
    return strict_log(True).replace(
        "payload_kernel_hcomm_ret=0", "payload_kernel_hcomm_ret=42")


def strict_log_with_kernel_local_copy_failure() -> str:
    return strict_log(True).replace(
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 payload_kernel_hcomm_ret=0",
        "payload_kernel_status=local-copy-failed "
        "payload_failure_step=local-copy payload_status_word=5 "
        "payload_kernel_hcomm_ret=91")


def strict_log_with_comm_acquire_failure() -> str:
    return strict_log(True).replace(
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 payload_kernel_hcomm_ret=0",
        "payload_kernel_status=comm-acquire-failed "
        "payload_failure_step=comm-acquire payload_status_word=14 "
        "payload_kernel_hcomm_ret=77",
        1)


def strict_log_with_resource_acquire_failure() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke unsupported: fallback=hccl-p2p "
        "detail=\"stage3b3e_payload_copy=failed "
        "stage3b3e_direct_aclrt_payload_loader=not-attempted "
        "stage3b3e_payload_descriptor_handoff=blocked "
        "stage3b3e_direct_aclrt_payload_launch=not-attempted "
        "stage3b3e_payload_sync=not-attempted "
        "payload_resource_acquire=failed "
        "payload_resource_step=aicpu-ts-thread "
        "payload_resource_status=backend-error "
        "payload_role=send payload_peer_rank=1 payload_bytes=4096 "
        "fallback=hccl-p2p\"",
        "",
    ])


def strict_log_with_rank1_remote_read_failure() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke passed: fallback=none detail=\""
        "stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 payload_status_schema=v2 "
        "payload_status_word_count=8 payload_echo=passed payload_role=send "
        "fallback=none\" "
        "payload_source_checksum=1234",
        "rank 1 hcomm payload smoke unsupported: fallback=none detail=\""
        "stage3b3e_payload_copy=failed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=remote-read-failed "
        "payload_failure_step=remote-read payload_status_word=9 "
        "payload_kernel_hcomm_ret=88 payload_status_schema=v2 "
        "payload_status_word_count=8 payload_echo=observed "
        "fallback=none\"",
        "",
    ])


def strict_log_with_rank1_failed_wait_remote_read() -> str:
    return strict_log_with_rank1_remote_read_failure().replace(
        "rank 1 hcomm payload smoke unsupported",
        "rank 1 hcomm payload smoke failed")


def strict_log_with_rank1_ready_wait_failure() -> str:
    return strict_log_with_rank1_remote_read_failure().replace(
        "payload_kernel_status=remote-read-failed "
        "payload_failure_step=remote-read payload_status_word=9 "
        "payload_kernel_hcomm_ret=88",
        "payload_kernel_status=ready-notify-wait-failed "
        "payload_failure_step=ready-notify-wait payload_status_word=8 "
        "payload_kernel_hcomm_ret=66")


def strict_log_with_rank1_output_copy_failure() -> str:
    return strict_log_with_rank1_remote_read_failure().replace(
        "payload_kernel_status=remote-read-failed "
        "payload_failure_step=remote-read payload_status_word=9 "
        "payload_kernel_hcomm_ret=88",
        "payload_kernel_status=output-copy-failed "
        "payload_failure_step=output-copy payload_status_word=16 "
        "payload_kernel_hcomm_ret=91")


def strict_log_with_rank1_pending_remote_read() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke passed: fallback=none detail=\""
        "stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 payload_primitive_state=completed "
        "payload_status_schema=v2 payload_status_word_count=8 "
        "payload_echo=passed payload_role=send payload_batch_mode=on "
        "fallback=none\" "
        "payload_source_checksum=1234",
        "rank 1 hcomm payload smoke failed: fallback=none detail=\""
        "stage3b3e_payload_copy=failed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=failed "
        "payload_kernel_status=remote-read-failed "
        "payload_failure_step=remote-read payload_status_word=9 "
        "payload_kernel_hcomm_ret=4294967295 "
        "payload_primitive_state=pending payload_status_schema=v2 "
        "payload_status_word_count=8 payload_echo=observed fallback=none\"",
        "",
    ])


def strict_log_with_checksum_mismatch() -> str:
    return strict_log(True).replace(
        "payload_checksum=1234", "payload_checksum=9999")


def strict_log_with_recv_direct_output() -> str:
    text = strict_log(True)
    marker = "payload_recv_path=local-buffer"
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if first == -1 or second == -1:
        raise AssertionError("synthetic log missing rank recv path markers")
    text = text[:second] + text[second:].replace(
        marker, "payload_recv_path=direct-output", 1)
    return text.replace("payload_trace_primitive_path=recv-read-local-copy",
                        "payload_trace_primitive_path=recv-read-direct-output")


def strict_log_with_missing_handoff() -> str:
    return strict_log(True).replace(
        "stage3b3e_payload_descriptor_handoff=passed ", "")


def strict_log_with_stale_status_schema() -> str:
    return strict_log(True).replace(
        "payload_status_schema=v2", "payload_status_schema=v1")


def strict_log_with_wrong_status_word_count() -> str:
    return strict_log(True).replace(
        "payload_status_word_count=8", "payload_status_word_count=4")


def strict_log_with_missing_notify_order() -> str:
    return strict_log(True).replace(
        "payload_thread_notify_order=not-used", "payload_thread_notify_order")


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


def strict_log_with_missing_semantic_v5() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke unsupported: fallback=none detail=\""
        "stage3b3e_payload_copy=unsupported "
        "stage3b3e_direct_aclrt_payload_loader=unsupported "
        "payload_semantic_v5=missing "
        "stage3b3e_payload_descriptor_handoff=blocked "
        "stage3b3e_direct_aclrt_payload_launch=not-attempted "
        "fallback=none\"",
        "",
    ])


def strict_log_with_missing_semantic_v6() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke unsupported: fallback=none detail=\""
        "stage3b3e_payload_copy=unsupported "
        "stage3b3e_direct_aclrt_payload_loader=unsupported "
        "payload_semantic=present "
        "payload_semantic_v5=present "
        "payload_semantic_v6=missing "
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
    return ("root=<runtime-root>\n"
            "vendor=flume\n"
            "aicpu_tar=present\n"
            "aicpu_tar_so.libflume_hcomm_payload_aicpu_kernel.so=present\n"
            "required=canary_direct_aclrt,payload_direct_aclrt,"
            "payload_abi_v4,payload_semantic,payload_semantic_v5,"
            "payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,"
            "payload_requires_comm_acquire,payload_status_schema,"
            "payload_status_word_count,payload_trace_schema,"
            "payload_trace_word_count,payload_primitive_deps,"
            "build_mode_internal\n"
            "status=PASS\n")


def smoke_with_hcomm_storage_path() -> str:
    return "\n".join([
        "FLUME_BACKEND_CAPS hcomm_primitives=on "
        "hcomm_payload_scheduler_candidate=on",
        "hccl collective smoke passed p2p_copy=on",
        "hcomm channel probe passed",
        "rank 1 storage HBM smoke passed: "
        "storage_hbm=hcomm-payload-staging bytes=4096 checksum=7",
        "",
    ])


def smoke_with_rank0_only_hcomm_storage_path() -> str:
    return "\n".join([
        "FLUME_BACKEND_CAPS hcomm_primitives=on "
        "hcomm_payload_scheduler_candidate=on",
        "hccl collective smoke passed p2p_copy=on",
        "hcomm channel probe passed",
        "rank 0 storage HBM smoke sent: "
        "storage_hbm=hcomm-payload-staging bytes=4096 checksum=7",
        "",
    ])


def smoke_with_mixed_storage_path() -> str:
    return "\n".join([
        "FLUME_BACKEND_CAPS hcomm_primitives=on "
        "hcomm_payload_scheduler_candidate=on",
        "hccl collective smoke passed p2p_copy=on",
        "hcomm channel probe passed",
        "rank 0 storage HBM smoke sent: "
        "storage_hbm=hcomm-payload-staging bytes=4096 checksum=7",
        "rank 1 storage HBM smoke passed: "
        "storage_hbm=hccl-p2p-staging bytes=4096 checksum=7",
        "",
    ])


def stale_status_schema_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_requires_comm_acquire,build_mode_internal",
        "function.payload_status_schema.FlumeHcommPayloadStatusSchemaVersion=missing",
        "function.payload_status_word_count.FlumeHcommPayloadStatusWordCount=missing",
        "status=FAIL",
        "reason=payload kernel package is missing the payload status schema marker",
        "",
    ])


def old_pass_without_status_schema_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_requires_comm_acquire,build_mode_internal",
        "status=PASS",
        "",
    ])


def stale_semantic_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_requires_comm_acquire,build_mode_internal",
        "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=missing",
        "function_so.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=missing",
        "status=FAIL",
        "reason=payload kernel package is missing the payload semantic marker",
        "",
    ])


def canary_only_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_requires_comm_acquire,build_mode_internal",
        "function_so.build_mode.canary_only.FlumeHcommPayloadBuildModeCanaryOnly=present",
        "function_so.build_mode.internal_payload.FlumeHcommPayloadBuildModeInternalPayload=missing",
        "status=FAIL",
        "reason=payload kernel package is canary-only; V4 payload entrypoint is a compatibility stub",
        "",
    ])


def abi_missing_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_requires_comm_acquire,build_mode_internal",
        "function_so.payload_abi_version_v4.FlumeHcommPayloadCopyAbiVersion4=missing",
        "status=FAIL",
        "reason=payload kernel package is missing the payload ABI version marker",
        "",
    ])


def missing_aicpu_tar_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_requires_comm_acquire,payload_status_schema,payload_status_word_count,payload_trace_schema,payload_trace_word_count,payload_primitive_deps,build_mode_internal",
        "json=present",
        "aicpu_tar=missing",
        "aicpu_tar_readable=missing",
        "aicpu_tar_so.libflume_hcomm_payload_aicpu_kernel.so=missing",
        "status=FAIL",
        "reason=payload kernel package is missing or incomplete",
        "",
    ])


def multi_candidate_payload_package_log() -> str:
    return "\n".join([
        "root=/tmp/old-cann",
        "vendor=flume",
        "required=canary_direct_aclrt",
        "status=PASS",
        "",
        "root=/tmp/current-cann",
        "vendor=flume",
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_requires_comm_acquire,payload_status_schema,payload_status_word_count,payload_trace_schema,payload_trace_word_count,payload_primitive_deps,build_mode_internal",
        "status=PASS",
        "",
        "status=PASS",
        "",
    ])


def multi_candidate_canary_only_package_log() -> str:
    return "\n".join([
        "root=/tmp/stale-cann",
        "vendor=flume",
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_requires_comm_acquire,payload_status_schema,payload_status_word_count,build_mode_internal",
        "status=FAIL",
        "",
        "root=/tmp/canary-cann",
        "vendor=flume",
        "required=canary_direct_aclrt",
        "status=PASS",
        "",
        "status=FAIL",
        "reason=payload kernel package is missing or incomplete",
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

        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "flume_tool.py",
                "--build-dir", str(tmp / "build-command"),
                "--hccl-devices", "0,1",
                "--run-storage-hbm-smoke",
                "--hcomm-require-payload-copy",
                "--hcomm-payload-batch-tag=flume-payload-v1",
                "ascend-probe",
            ]
            storage_args = flume_tool.parse_args()
        finally:
            sys.argv = old_argv
        commands = flume_tool.build_commands(
            storage_args, enable_hccl=True, run_dir=tmp / "command-run")
        smoke_command = next(
            spec.command for spec in commands
            if spec.name == "hccl-collective-smoke")
        assert "--storage-hbm-smoke" in smoke_command
        assert "--hcomm-require-payload-copy" in smoke_command
        assert "--hcomm-payload-batch-tag=flume-payload-v1" in smoke_command

        strict_pass = write(tmp / "strict-pass.log", strict_log(True))
        pass_dir = tmp / "pass"
        pass_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            pass_dir, smoke, strict_pass, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | yes |" in text
        assert ("| HCOMM custom-op package source | source=<runtime-root>, "
                "vendor=flume, tar=present, so=present |") in text
        assert ("| runtime package identity | source=explicit-json, "
                "tar=present, readable=yes |") in text
        assert "`payload_kernel_hcomm_ret=0`" in text
        assert "`payload_status_schema`" in text
        assert "`payload_echo=passed`" in text
        assert "`payload_pattern=strict-v1`" in text
        assert "| kernel failure step | none |" in text
        assert "| payload checksum match | yes |" in text
        assert "| payload test pattern | strict-v1 |" in text
        assert ("| host descriptor fingerprint | bytes=4096, ready=0, "
                "done=1, completion=0, thread_notify=0, batch_tag=default, "
                "recv_path=local-buffer, local_buffer=8192, "
                "remote_buffer=8192 |") in text
        assert "| payload batch tag | default |" in text
        assert ("| HCOMM resource fingerprint | engine=aicpu-ts, "
                "protocol=hccs, channel_desc=rank-graph, channels=1, "
                "notify_num=2, usable=8192, local=8192, remote=8192 |") in text
        assert "start Stage 3B.4 storage rewiring" in text
        strict_channel_handle = strict_log(True).replace(
            "payload_comm_acquire=default payload_comm_binding=comm-name",
            "payload_comm_acquire=skipped payload_comm_binding=channel-handle")
        strict_channel_handle_direct_output = (
            strict_log_with_recv_direct_output().replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle)[0]
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle_direct_output)[0]
        channel_log = write(tmp / "strict-channel-handle.log",
                            strict_channel_handle)
        channel_note = flume_tool.WriteHcommPayloadChannelHandleCandidate(
            tmp, None, channel_log)
        channel_text = channel_note.read_text(encoding="utf-8")
        assert "channel_payload_copy_and_verify: `passed`" in channel_text
        assert "strict-positive evidence" in channel_text
        assert "payload_comm_binding=channel-handle" in channel_text
        strict_mixed_binding = strict_log(True).replace(
            "payload_comm_acquire=default payload_comm_binding=comm-name",
            "payload_comm_acquire=skipped payload_comm_binding=channel-handle",
            1)
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_mixed_binding)[0]

        strict_no_batch_tag = write(
            tmp / "strict-no-batch-tag.log",
            strict_log(True).replace(" payload_desc_batch_tag=default", ""))
        no_batch_tag_dir = tmp / "no-batch-tag"
        no_batch_tag_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_batch_tag_dir, smoke, strict_no_batch_tag, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload batch tag | missing |" in text
        assert ("inspect HCOMM payload batch tag descriptor fill" in text)

        strict_no_batch = write(
            tmp / "strict-no-batch.log",
            strict_log(True).replace("payload_batch_mode=on",
                                     "payload_batch_mode=off"))
        no_batch_dir = tmp / "no-batch"
        no_batch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_batch_dir, smoke, strict_no_batch, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_no_batch.read_text(encoding="utf-8"))[0]
        no_batch_passed, no_batch_rank0, no_batch_rank1 = (
            flume_tool.StrictPayloadNoBatchDiagnosticPassed(
                strict_no_batch.read_text(encoding="utf-8")))
        assert no_batch_passed
        assert no_batch_rank0
        assert no_batch_rank1
        default_failure = write(
            tmp / "strict-default-failure-for-nobatch.log",
            strict_log_with_rank1_remote_read_failure())
        strict_no_comm = write(
            tmp / "strict-no-comm-acquire.log",
            strict_log(True).replace("payload_comm_acquire=default",
                                     "payload_comm_acquire=skipped").replace(
                                         "payload_comm_binding=comm-name",
                                         "payload_comm_binding=diagnostic-skip"))
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_no_comm.read_text(encoding="utf-8"))[0]
        no_comm_passed, no_comm_rank0, no_comm_rank1 = (
            flume_tool.StrictPayloadNoCommAcquireDiagnosticPassed(
                strict_no_comm.read_text(encoding="utf-8")))
        assert no_comm_passed
        assert no_comm_rank0
        assert no_comm_rank1
        no_comm_note = flume_tool.WriteHcommPayloadNoCommAcquireDiagnostic(
            no_batch_dir, default_failure, strict_no_comm)
        no_comm_text = no_comm_note.read_text(encoding="utf-8")
        assert "no-comm-acquire HCOMM payload copy" in no_comm_text
        assert "`payload_comm_binding=channel-handle`" in no_comm_text
        no_batch_note = flume_tool.WriteHcommPayloadNoBatchDiagnostic(
            no_batch_dir, default_failure, strict_no_batch)
        no_batch_text = no_batch_note.read_text(encoding="utf-8")
        assert "no_batch_payload_copy_and_verify: `passed`" in no_batch_text
        assert "remaining issue is likely HcommBatchModeStart/End" in no_batch_text
        assert "default_rank1_failure_step: `remote-read`" in no_batch_text
        write(no_batch_dir / "02-hcomm-payload-nobatch-diagnostic.log",
              strict_no_batch.read_text(encoding="utf-8"))
        tree = flume_tool.WriteMatrixDecisionTree(
            no_batch_dir, smoke, default_failure, package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM payload no-batch diagnostic | passed |" in text
        assert ("no-batch HCOMM payload copy passed; inspect "
                "HcommBatchModeStart/End") in text

        tagged_dir = tmp / "tagged-batch"
        tagged_dir.mkdir()
        tagged_log = write(
            tagged_dir / "03-hcomm-payload-tagged-diagnostic.log",
            strict_log(True).replace("payload_desc_batch_tag=default",
                                     "payload_desc_batch_tag=custom"))
        tagged_note = flume_tool.WriteHcommPayloadTaggedDiagnostic(
            tagged_dir, default_failure, tagged_log, "flume-payload-v1")
        tagged_text = tagged_note.read_text(encoding="utf-8")
        assert "tagged_payload_copy_and_verify: `passed`" in tagged_text
        assert "default batch tag path is the likely compatibility problem" in tagged_text
        tree = flume_tool.WriteMatrixDecisionTree(
            tagged_dir, smoke, default_failure, package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM payload tagged-batch diagnostic | passed |" in text
        assert "tag=custom" in text
        assert "tagged HCOMM payload copy passed; rerun strict-positive" in text

        direct_output_dir = tmp / "direct-output"
        direct_output_dir.mkdir()
        output_copy_failure = write(
            tmp / "strict-output-copy-failure-for-direct-output.log",
            strict_log_with_rank1_output_copy_failure())
        direct_output_log = write(
            direct_output_dir / "04-hcomm-payload-direct-output-diagnostic.log",
            strict_log_with_recv_direct_output())
        direct_output_note = (
            flume_tool.WriteHcommPayloadDirectOutputDiagnostic(
                direct_output_dir, output_copy_failure, direct_output_log))
        direct_output_text = direct_output_note.read_text(encoding="utf-8")
        assert "direct_payload_copy_and_verify: `passed`" in direct_output_text
        assert "default local-buffer path is likely failing" in direct_output_text
        assert "direct_rank0:" in direct_output_text
        assert "payload_recv_path=local-buffer" in direct_output_text
        assert "direct_rank1:" in direct_output_text
        assert "payload_recv_path=direct-output" in direct_output_text
        assert "payload_trace_primitive_path=recv-read-direct-output" in direct_output_text
        tree = flume_tool.WriteMatrixDecisionTree(
            direct_output_dir, smoke, output_copy_failure, package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM payload direct-output diagnostic | passed |" in text
        assert "recv_path=direct-output" in text
        assert "direct-output HCOMM payload copy passed; rerun" in text

        strict_no_verify = write(tmp / "strict-no-verify.log",
                                 strict_log(False))
        no_verify_dir = tmp / "no-verify"
        no_verify_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_verify_dir, smoke, strict_no_verify, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| rank1 verify | missing |" in text
        assert "| HCOMM primitive ABI fixture | not-collected |" in text
        assert "run `tools/collect_cann_compat.py`" in text

        abi_fixture = tmp / "fixture" / "host-b-cann"
        write(abi_fixture / "hcomm-primitive-call-shape-probe.txt",
              "status: PASS\n")
        write(abi_fixture / "hcomm-primitive-symbols.txt",
              "\n".join([
                  "HcommAcquireComm: present",
                  "HcommLocalCopyOnThread: present",
                  "HcommReadOnThread: present",
                  "HcommBatchModeStart: present",
                  "HcommBatchModeEnd: present",
                  "",
              ]))
        abi_pass_dir = tmp / "abi-pass"
        abi_pass_dir.mkdir()
        write(abi_pass_dir / "99-collect-cann-compat.log",
              f"[ok] CANN compatibility fixture -> {abi_fixture}\n")
        tree = flume_tool.WriteMatrixDecisionTree(
            abi_pass_dir, smoke, strict_no_verify, package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM primitive ABI fixture | call-shape-pass |" in text
        assert "present=HcommAcquireComm,HcommLocalCopyOnThread,HcommReadOnThread,HcommBatchModeStart,HcommBatchModeEnd" in text
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
        assert "inspect rank 0 in-kernel HCOMM primitive return code: 42" in text

        strict_local_copy_fail = write(
            tmp / "strict-local-copy-fail.log",
            strict_log_with_kernel_local_copy_failure())
        local_copy_fail_dir = tmp / "local-copy-fail"
        local_copy_fail_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            local_copy_fail_dir, smoke, strict_local_copy_fail, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| kernel status | local-copy-failed |" in text
        assert "| kernel failure step | local-copy |" in text
        assert ("inspect rank 0 HcommLocalCopyOnThread input HBM to local "
                "HCCL Buffer path") in text

        strict_resource_fail = write(
            tmp / "strict-resource-fail.log",
            strict_log_with_resource_acquire_failure())
        resource_fail_dir = tmp / "resource-fail"
        resource_fail_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            resource_fail_dir, smoke, strict_resource_fail, package)
        text = tree.read_text(encoding="utf-8")
        assert "| payload resource acquisition | failed |" in text
        assert "step=aicpu-ts-thread, status=backend-error" in text
        assert ("fix HCOMM payload resource acquisition before debugging "
                "the custom-op package") in text

        strict_comm_acquire_fail = write(
            tmp / "strict-comm-acquire-fail.log",
            strict_log_with_comm_acquire_failure())
        comm_acquire_fail_dir = tmp / "comm-acquire-fail"
        comm_acquire_fail_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            comm_acquire_fail_dir, smoke, strict_comm_acquire_fail, package)
        text = tree.read_text(encoding="utf-8")
        assert "| rank0 kernel failure step | comm-acquire |" in text
        assert ("inspect rank 0 HcommAcquireComm path, HCCL comm name, and "
                "payload package libhcomm linkage") in text

        strict_rank1_remote_read_fail = write(
            tmp / "strict-rank1-remote-read-fail.log",
            strict_log_with_rank1_remote_read_failure())
        rank1_remote_read_fail_dir = tmp / "rank1-remote-read-fail"
        rank1_remote_read_fail_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            rank1_remote_read_fail_dir, smoke, strict_rank1_remote_read_fail,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| rank0 kernel status | success |" in text
        assert "| rank1 kernel status | remote-read-failed |" in text
        assert "| rank0 kernel HCOMM ret | 0 |" in text
        assert "| rank1 kernel HCOMM ret | 88 |" in text
        assert ("inspect rank 1 HcommReadOnThread remote HCCL Buffer to "
                "local HCCL Buffer path") in text

        strict_rank1_ready_wait = write(
            tmp / "strict-rank1-ready-wait.log",
            strict_log_with_rank1_ready_wait_failure())
        rank1_ready_wait_dir = tmp / "rank1-ready-wait"
        rank1_ready_wait_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            rank1_ready_wait_dir, smoke, strict_rank1_ready_wait, package)
        text = tree.read_text(encoding="utf-8")
        assert "| rank1 kernel failure step | ready-notify-wait |" in text
        assert ("inspect rank 1 HCOMM ready notify wait index, peer rank "
                "launch, and role pairing") in text

        strict_rank1_failed_wait = write(
            tmp / "strict-rank1-failed-wait.log",
            strict_log_with_rank1_failed_wait_remote_read())
        rank1_failed_wait_dir = tmp / "rank1-failed-wait"
        rank1_failed_wait_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            rank1_failed_wait_dir, smoke, strict_rank1_failed_wait, package)
        text = tree.read_text(encoding="utf-8")
        assert "| rank1 kernel status | remote-read-failed |" in text
        assert "| rank1 kernel HCOMM ret | 88 |" in text
        assert ("inspect rank 1 HcommReadOnThread remote HCCL Buffer to "
                "local HCCL Buffer path") in text

        strict_rank1_output_copy_fail = write(
            tmp / "strict-rank1-output-copy-fail.log",
            strict_log_with_rank1_output_copy_failure())
        rank1_output_copy_fail_dir = tmp / "rank1-output-copy-fail"
        rank1_output_copy_fail_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            rank1_output_copy_fail_dir, smoke,
            strict_rank1_output_copy_fail, package)
        text = tree.read_text(encoding="utf-8")
        assert "| rank1 kernel status | output-copy-failed |" in text
        assert "| rank1 kernel failure step | output-copy |" in text
        assert "| rank1 kernel HCOMM ret | 91 |" in text
        assert "--hcomm-payload-recv-direct-output" in text

        strict_rank1_pending_remote_read = write(
            tmp / "strict-rank1-pending-remote-read.log",
            strict_log_with_rank1_pending_remote_read())
        rank1_pending_remote_read_dir = tmp / "rank1-pending-remote-read"
        rank1_pending_remote_read_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            rank1_pending_remote_read_dir, smoke,
            strict_rank1_pending_remote_read, package)
        text = tree.read_text(encoding="utf-8")
        assert "| rank0 primitive state | completed |" in text
        assert "| rank1 primitive state | pending |" in text
        assert "| primitive state | completed |" in text
        assert ("inspect rank 1 pending HCOMM primitive timeout/hang at "
                "remote-read") in text

        strict_checksum_mismatch = write(
            tmp / "strict-checksum-mismatch.log",
            strict_log_with_checksum_mismatch())
        checksum_mismatch_dir = tmp / "checksum-mismatch"
        checksum_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            checksum_mismatch_dir, smoke, strict_checksum_mismatch, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload checksum match | no |" in text
        assert "inspect payload checksum mismatch" in text

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

        strict_stale_schema = write(tmp / "strict-stale-schema.log",
                                    strict_log_with_stale_status_schema())
        stale_schema_runtime_dir = tmp / "stale-schema-runtime"
        stale_schema_runtime_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            stale_schema_runtime_dir, smoke, strict_stale_schema, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload status schema | v1 / 8 |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_stale_status_schema())[0]

        strict_wrong_word_count = write(
            tmp / "strict-wrong-status-word-count.log",
            strict_log_with_wrong_status_word_count())
        wrong_word_count_runtime_dir = tmp / "wrong-word-count-runtime"
        wrong_word_count_runtime_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            wrong_word_count_runtime_dir, smoke, strict_wrong_word_count,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload status schema | v2 / 4 |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_wrong_status_word_count())[0]

        strict_missing_notify_order = write(
            tmp / "strict-missing-notify-order.log",
            strict_log_with_missing_notify_order())
        missing_notify_order_dir = tmp / "missing-notify-order"
        missing_notify_order_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            missing_notify_order_dir, smoke, strict_missing_notify_order,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_missing_notify_order())[0]

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
        assert ("| HCOMM custom-op package reason | payload kernel package is "
                "missing the payload semantic marker |") in text
        assert "| payload semantic marker | missing |" in text
        assert "installed package has stale semantics" in text

        strict_missing_semantic_v5 = write(
            tmp / "strict-missing-semantic-v5.log",
            strict_log_with_missing_semantic_v5())
        stale_semantic_v5_dir = tmp / "stale-semantic-v5-runtime"
        stale_semantic_v5_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            stale_semantic_v5_dir, smoke, strict_missing_semantic_v5, package)
        text = tree.read_text(encoding="utf-8")
        assert "| payload semantic v5 marker | missing |" in text
        assert ("rebuild/reinstall payload custom-op package with current "
                "Flume semantic v5 kernel") in text

        strict_missing_semantic_v6 = write(
            tmp / "strict-missing-semantic-v6.log",
            strict_log_with_missing_semantic_v6())
        stale_semantic_v6_dir = tmp / "stale-semantic-v6-runtime"
        stale_semantic_v6_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            stale_semantic_v6_dir, smoke, strict_missing_semantic_v6, package)
        text = tree.read_text(encoding="utf-8")
        assert "| payload semantic v6 marker | missing |" in text
        assert ("rebuild/reinstall payload custom-op package with current "
                "Flume semantic v6 direct-output-capable kernel") in text

        canary_package = write(tmp / "package-canary-only.log",
                               canary_only_package_log())
        canary_package_dir = tmp / "canary-package"
        canary_package_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            canary_package_dir, smoke, None, canary_package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM custom-op package payload-ready? | not-ready |" in text
        assert "installed package is canary/stub-only" in text

        abi_missing_package = write(tmp / "package-abi-missing.log",
                                    abi_missing_package_log())
        abi_missing_dir = tmp / "abi-missing-package"
        abi_missing_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            abi_missing_dir, smoke, None, abi_missing_package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM custom-op package payload-ready? | not-ready |" in text
        assert "current Flume V4 ABI headers" in text

        missing_tar_package = write(tmp / "package-missing-aicpu-tar.log",
                                    missing_aicpu_tar_package_log())
        missing_tar_dir = tmp / "missing-aicpu-tar-package"
        missing_tar_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            missing_tar_dir, smoke, None, missing_tar_package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM custom-op package payload-ready? | not-ready |" in text
        assert "| HCOMM custom-op package reason | custom-op AICPU tar missing |" in text
        assert "matching AICPU tar are both present" in text

        multi_payload_package = write(
            tmp / "package-multi-candidate-payload.log",
            multi_candidate_payload_package_log())
        multi_payload_dir = tmp / "multi-candidate-payload"
        multi_payload_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            multi_payload_dir, smoke, None, multi_payload_package)
        text = tree.read_text(encoding="utf-8")
        assert flume_tool.PackageTextPayloadReady(
            multi_candidate_payload_package_log())
        assert flume_tool.PackageTextCanaryReady(
            multi_candidate_payload_package_log())
        assert "| HCOMM custom-op package payload-ready? | payload-ready |" in text

        multi_canary_package = write(
            tmp / "package-multi-candidate-canary-only.log",
            multi_candidate_canary_only_package_log())
        multi_canary_dir = tmp / "multi-candidate-canary-only"
        multi_canary_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            multi_canary_dir, smoke, None, multi_canary_package)
        text = tree.read_text(encoding="utf-8")
        assert not flume_tool.PackageTextPayloadReady(
            multi_candidate_canary_only_package_log())
        assert flume_tool.PackageTextCanaryReady(
            multi_candidate_canary_only_package_log())
        assert "| HCOMM custom-op package payload-ready? | canary-ready |" in text

        hcomm_storage_smoke = write(tmp / "smoke-hcomm-storage.log",
                                    smoke_with_hcomm_storage_path())
        hcomm_storage_dir = tmp / "hcomm-storage-path"
        hcomm_storage_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            hcomm_storage_dir, hcomm_storage_smoke, None, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Storage to HBM path ok? | yes | `storage_hbm=hcomm-payload-staging` marker |" in text
        assert not flume_tool.DecisionTreeHcommStoragePassed(tree)

        rank0_only_storage = write(tmp / "smoke-rank0-only-storage.log",
                                   smoke_with_rank0_only_hcomm_storage_path())
        rank0_only_dir = tmp / "rank0-only-storage-path"
        rank0_only_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            rank0_only_dir, rank0_only_storage, strict_pass, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Storage to HBM path ok? | no | `storage_hbm=missing` marker |" in text
        assert not flume_tool.DecisionTreeHcommStoragePassed(tree)

        mixed_storage = write(tmp / "smoke-mixed-storage.log",
                              smoke_with_mixed_storage_path())
        mixed_storage_dir = tmp / "mixed-storage-path"
        mixed_storage_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            mixed_storage_dir, mixed_storage, strict_pass, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Storage to HBM path ok? | yes | `storage_hbm=hccl-p2p-staging` marker |" in text
        assert not flume_tool.DecisionTreeHcommStoragePassed(tree)

        hcomm_storage_strict_dir = tmp / "hcomm-storage-strict-path"
        hcomm_storage_strict_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            hcomm_storage_strict_dir, hcomm_storage_smoke, strict_pass,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | yes |" in text
        assert "| Storage to HBM path ok? | yes | `storage_hbm=hcomm-payload-staging` marker |" in text
        assert flume_tool.DecisionTreeHcommStoragePassed(tree)

        hcomm_storage_log_dir = tmp / "flume-check-storage-strict"
        hcomm_storage_log_dir.mkdir()
        write(hcomm_storage_log_dir / "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(hcomm_storage_log_dir / "01-hcomm-storage-strict-positive.log",
              strict_log(True) + smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                hcomm_storage_log_dir))
        assert storage_strict_log is not None
        assert storage_passed
        assert flume_tool.DecisionTreeHcommStoragePassed(analyzed_tree)

        channel_fallback_log_dir = tmp / "flume-check-channel-fallback"
        channel_fallback_log_dir.mkdir()
        write(channel_fallback_log_dir / "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(channel_fallback_log_dir / "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(
            channel_fallback_log_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_channel_handle)
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                channel_fallback_log_dir))
        assert payload_strict_log is not None
        assert payload_strict_log.name.endswith(
            "hcomm-payload-channel-handle-candidate.log")
        assert payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)

        channel_direct_fallback_dir = tmp / "flume-check-channel-direct-fallback"
        channel_direct_fallback_dir.mkdir()
        write(channel_direct_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(channel_direct_fallback_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(
            channel_direct_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        write(
            channel_direct_fallback_dir /
            "03-hcomm-payload-channel-handle-direct-output-candidate.log",
            strict_channel_handle_direct_output)
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                channel_direct_fallback_dir))
        assert payload_strict_log is not None
        assert payload_strict_log.name.endswith(
            "hcomm-payload-channel-handle-direct-output-candidate.log")
        assert payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)

        storage_channel_fallback_dir = tmp / "flume-check-storage-channel-fallback"
        storage_channel_fallback_dir.mkdir()
        write(storage_channel_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(storage_channel_fallback_dir /
              "01-hcomm-storage-strict-positive.log",
              strict_log(False))
        write(
            storage_channel_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_channel_handle + smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                storage_channel_fallback_dir))
        assert storage_strict_log is not None
        assert storage_strict_log.name.endswith(
            "hcomm-payload-channel-handle-candidate.log")
        assert storage_passed
        assert flume_tool.DecisionTreeHcommStoragePassed(analyzed_tree)

        storage_channel_direct_fallback_dir = (
            tmp / "flume-check-storage-channel-direct-fallback")
        storage_channel_direct_fallback_dir.mkdir()
        write(storage_channel_direct_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(storage_channel_direct_fallback_dir /
              "01-hcomm-storage-strict-positive.log",
              strict_log(False))
        write(
            storage_channel_direct_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        write(
            storage_channel_direct_fallback_dir /
            "03-hcomm-payload-channel-handle-direct-output-candidate.log",
            strict_channel_handle_direct_output + smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                storage_channel_direct_fallback_dir))
        assert storage_strict_log is not None
        assert storage_strict_log.name.endswith(
            "hcomm-payload-channel-handle-direct-output-candidate.log")
        assert storage_passed
        assert flume_tool.DecisionTreeHcommStoragePassed(analyzed_tree)

        stale_status_schema_package = write(
            tmp / "package-stale-status-schema.log",
            stale_status_schema_package_log())
        stale_status_schema_dir = tmp / "stale-status-schema-package"
        stale_status_schema_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            stale_status_schema_dir, smoke, None, stale_status_schema_package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM custom-op package payload-ready? | not-ready |" in text
        assert "current payload status schema" in text

        old_pass_without_schema_package = write(
            tmp / "package-old-pass-without-status-schema.log",
            old_pass_without_status_schema_package_log())
        old_pass_without_schema_dir = tmp / "old-pass-without-status-schema"
        old_pass_without_schema_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            old_pass_without_schema_dir, smoke, None,
            old_pass_without_schema_package)
        text = tree.read_text(encoding="utf-8")
        assert "| HCOMM custom-op package payload-ready? | not-ready |" in text

        strict_canary_mode = write(tmp / "strict-canary-mode.log",
                                   strict_log_with_canary_build_mode())
        canary_mode_dir = tmp / "canary-mode"
        canary_mode_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            canary_mode_dir, smoke, strict_canary_mode, package)
        text = tree.read_text(encoding="utf-8")
        assert "| payload build mode | not-internal |" in text
        assert "installed package is canary/stub-only" in text

        driver_runtime_dir = tmp / "driver-runtime-unavailable"
        driver_runtime_dir.mkdir()
        write(driver_runtime_dir / "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(driver_runtime_dir / "01-npu-smi-info-m.log",
              "returncode: 187\n"
              "DrvMngGetConsoleLogLevel failed. (ret=4)\n"
              "dcmi module initialize failed.\n")
        write(driver_runtime_dir / "hccl-rank-logs" / "rank-0.log",
              "get platform info failed, drvErr=4.\n")
        tree = flume_tool.WriteMatrixDecisionTree(
            driver_runtime_dir, smoke, None,
            driver_runtime_dir / "00-hcomm-custom-op-package-preflight.log")
        text = tree.read_text(encoding="utf-8")
        assert ("| NPU runtime ready for strict payload? | "
                "driver-runtime-unavailable |") in text
        assert "fix NPU driver/runtime visibility before rerunning strict-positive" in text

        incomplete_rank_dir = tmp / "rank-launch-incomplete"
        incomplete_rank_dir.mkdir()
        write(incomplete_rank_dir / "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(incomplete_rank_dir / "hccl-rank-logs" / "rank-0.log",
              "rank 0 started but no peer root-info arrived\n")
        tree = flume_tool.WriteMatrixDecisionTree(
            incomplete_rank_dir, smoke, None,
            incomplete_rank_dir / "00-hcomm-custom-op-package-preflight.log")
        text = tree.read_text(encoding="utf-8")
        assert ("| NPU runtime ready for strict payload? | "
                "rank-launch-incomplete |") in text
        assert "inspect multiprocess rank launch and device visibility" in text

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

        pass_runner = flume_tool.Runner(tmp / "runner-pass")
        flume_tool.RecordStrictPositiveEvidenceGate(
            pass_runner, tree, True, required=True)
        assert pass_runner.write_summary() == 0

        fail_runner = flume_tool.Runner(tmp / "runner-fail")
        flume_tool.RecordStrictPositiveEvidenceGate(
            fail_runner, tree, False, required=True)
        assert fail_runner.write_summary() == 1
        evidence_logs = sorted(fail_runner.run_dir.glob(
            "*-hcomm-payload-strict-evidence.log"))
        assert evidence_logs
        evidence_text = evidence_logs[-1].read_text(encoding="utf-8")
        assert "strict_positive_evidence=failed" in evidence_text
        assert "payload_kernel_hcomm_ret=0" in evidence_text
        assert "payload_echo=passed" in evidence_text

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
