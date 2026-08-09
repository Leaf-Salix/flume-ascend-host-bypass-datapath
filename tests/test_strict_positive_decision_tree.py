#!/usr/bin/env python3
"""Synthetic tests for the strict HCOMM payload decision tree gate."""

from __future__ import annotations

import importlib.util
import subprocess
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


def load_flume_multiproc(repo: Path):
    spec = importlib.util.spec_from_file_location(
        "flume_multiproc_under_test",
        repo / "tools" / "flume_hccl_multiproc.py")
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load tools/flume_hccl_multiproc.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _replace_trace_counts(text: str, rank0: int, rank1: int) -> str:
    marker = "payload_trace_count="
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if first == -1 or second == -1:
        raise AssertionError("synthetic log missing payload trace counts")

    def replace_at(source: str, pos: int, value: int) -> str:
        end = pos + len(marker)
        while end < len(source) and source[end].isdigit():
            end += 1
        return source[:pos] + marker + str(value) + source[end:]

    text = replace_at(text, second, rank1)
    return replace_at(text, first, rank0)


def _adjust_trace_counts(text: str, rank0_delta: int,
                         rank1_delta: int) -> str:
    marker = "payload_trace_count="
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if first == -1 or second == -1:
        raise AssertionError("synthetic log missing payload trace counts")
    first_end = first + len(marker)
    while first_end < len(text) and text[first_end].isdigit():
        first_end += 1
    second_end = second + len(marker)
    while second_end < len(text) and text[second_end].isdigit():
        second_end += 1
    rank0 = int(text[first + len(marker):first_end]) + rank0_delta
    rank1 = int(text[second + len(marker):second_end]) + rank1_delta
    return _replace_trace_counts(text, rank0, rank1)


def strict_log(include_verify: bool) -> str:
    verify = " payload_verify=passed payload_checksum=1234" if include_verify else ""
    desc = (" payload_desc_role=0 payload_desc_local_rank=0 "
            "payload_desc_peer_rank=1 payload_desc_rank_size=2 "
            "payload_desc_bytes=4096 payload_desc_ready_notify_idx=0 "
            "payload_desc_done_notify_idx=1 "
            "payload_desc_thread_notify_mode=0 "
            "payload_desc_completion_mode=0 "
            "payload_desc_timeout_sec=60 payload_desc_status_schema=v7 "
            "payload_desc_status_word_count=17 "
            "payload_desc_batch_tag=default "
            "payload_transfer_mode=read "
            "payload_layout=read-default "
            "payload_write_notify_backend=none "
            "payload_recv_path=local-buffer "
            "payload_desc_primitive_path=send-local-copy "
            "payload_desc_operand_layout=input-hbm->local-hccl-buffer "
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
                       "payload_semantic_v8=present payload_semantic_v9=present "
                       "payload_semantic_v10=present payload_semantic_v11=present "
                       "payload_semantic_v12=present "
                       "payload_semantic_v13=present "
                       "payload_semantic_v14=present "
                       "payload_semantic_v15=present payload_semantic_v16=present payload_semantic_v17=present payload_semantic_v18=present payload_semantic_v19=present "
                       "payload_official_p2p_layout=present "
                       "payload_copy_api=hcomm-direct-aclrt "
                       "payload_hccl_p2p_api=not-used "
                       "payload_no_hccl_sendrecv=passed "
                       "payload_no_hccl_payload_collective=passed")
    send_data_probe = (" payload_data_probe=observed "
                       "payload_data_user_entry_fingerprint=222 "
                       "payload_data_local_entry_fingerprint=111 "
                       "payload_data_remote_entry_fingerprint=not-sampled "
                       "payload_data_transfer_exit_fingerprint=not-sampled "
                       "payload_data_local_exit_fingerprint=222 "
                       "payload_data_user_exit_fingerprint=222 "
                       "payload_data_sample_bytes=4096 "
                       "payload_device_data_side=passed "
                       "payload_device_data_side_reason=send-read-side ")
    recv_data_probe = (" payload_data_probe=observed "
                       "payload_data_user_entry_fingerprint=111 "
                       "payload_data_local_entry_fingerprint=111 "
                       "payload_data_remote_entry_fingerprint=222 "
                       "payload_data_transfer_exit_fingerprint=222 "
                       "payload_data_local_exit_fingerprint=222 "
                       "payload_data_user_exit_fingerprint=222 "
                       "payload_data_sample_bytes=4096 "
                       "payload_device_data_side=passed "
                       "payload_device_data_side_reason=recv-read-local-buffer-side ")
    recv_desc = desc.replace("payload_desc_role=0", "payload_desc_role=1")
    recv_desc = recv_desc.replace("payload_desc_local_rank=0",
                                  "payload_desc_local_rank=1")
    recv_desc = recv_desc.replace("payload_desc_peer_rank=1",
                                  "payload_desc_peer_rank=0")
    recv_desc = recv_desc.replace("payload_desc_primitive_path=send-local-copy",
                                  "payload_desc_primitive_path=recv-read-local-copy")
    recv_desc = recv_desc.replace(
        "payload_desc_operand_layout=input-hbm->local-hccl-buffer",
        "payload_desc_operand_layout=remote-hccl-buffer->"
        "local-hccl-buffer->output-hbm")
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy",
        "rank 0 hcomm payload smoke passed: fallback=none "
        "payload_pattern=strict-v1 detail=\""
        "stage3b3e_payload_copy=passed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_launch_api=host-args "
        "payload_completion_mode=ordered-notify "
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 payload_local_buffer_prime=passed payload_local_buffer_prime_pattern=strict-sentinel-v1 payload_local_buffer_prime_source=host-sentinel-not-payload payload_local_buffer_prime_bytes=4096 payload_primitive_state=completed "
            "payload_status_schema=v7 "
            "payload_status_word_count=17 payload_echo=passed payload_descriptor_fingerprint=passed payload_role=send "
        + send_data_probe +
        "payload_trace=passed payload_trace_schema=v3 "
        "payload_trace_word_count=82 payload_trace_event=kernel-exit "
        "payload_trace_order=passed "
        "payload_trace_ret_order=passed "
        "payload_trace_primitive_counts=passed "
        "payload_trace_local_copy_count=1 "
        "payload_trace_read_count=0 "
        "payload_trace_write_count=0 "
        "payload_trace_write_notify_count=0 "
        "payload_trace_notify_record_count=1 "
        "payload_trace_notify_wait_count=1 "
        "payload_trace_channel_fence_count=0 "
        "payload_trace_count=16 "
        "payload_trace_primitive_path=send-local-copy payload_trace_operand_layout=input-hbm->local-hccl-buffer "
        "payload_trace_bytes=4096 "
        "payload_trace_batch_mode=0 "
        "payload_trace_recv_path=0 "
        "payload_trace_comm_acquire=default "
        "payload_trace_comm_binding=comm-name "
        "payload_trace_transfer_mode=read "
        "payload_trace_write_notify_backend=none "
        "payload_trace_ready_notify_idx=0 "
        "payload_trace_done_notify_idx=1 "
        "payload_trace_result=success payload_trace_status_word=0 payload_trace_hcomm_ret=0 "
        "payload_trace_first_error_event=none "
        "payload_trace_first_error_ret=0 "
        "payload_trace_first_error_index=-1 "
        "payload_trace_expected_thread_notify=off "
        "payload_batch_mode=on payload_comm_acquire=default "
        "payload_comm_binding=comm-name "
        "payload_thread_notify_order=not-used "
        "payload_host_source_fingerprint=222 "
        "payload_host_sample_bytes=4096" + desc + resource +
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
        "payload_launch_api=host-args "
        "payload_completion_mode=ordered-notify "
        "payload_kernel_status=success payload_failure_step=none "
        "payload_status_word=0 "
        "payload_kernel_hcomm_ret=0 payload_local_buffer_prime=passed payload_local_buffer_prime_pattern=strict-sentinel-v1 payload_local_buffer_prime_source=host-sentinel-not-payload payload_local_buffer_prime_bytes=4096 payload_primitive_state=completed "
            "payload_status_schema=v7 "
            "payload_status_word_count=17 payload_echo=passed payload_descriptor_fingerprint=passed payload_role=recv "
        + recv_data_probe +
        "payload_trace=passed payload_trace_schema=v3 "
        "payload_trace_word_count=82 payload_trace_event=kernel-exit "
        "payload_trace_order=passed "
        "payload_trace_ret_order=passed "
        "payload_trace_primitive_counts=passed "
        "payload_trace_local_copy_count=1 "
        "payload_trace_read_count=1 "
        "payload_trace_write_count=0 "
        "payload_trace_write_notify_count=0 "
        "payload_trace_notify_record_count=1 "
        "payload_trace_notify_wait_count=1 "
        "payload_trace_channel_fence_count=0 "
        "payload_trace_count=18 "
        "payload_trace_primitive_path=recv-read-local-copy payload_trace_operand_layout=remote-hccl-buffer->local-hccl-buffer->output-hbm "
        "payload_trace_bytes=4096 "
        "payload_trace_batch_mode=0 "
        "payload_trace_recv_path=0 "
        "payload_trace_comm_acquire=default "
        "payload_trace_comm_binding=comm-name "
        "payload_trace_transfer_mode=read "
        "payload_trace_write_notify_backend=none "
        "payload_trace_ready_notify_idx=0 "
        "payload_trace_done_notify_idx=1 "
        "payload_trace_result=success payload_trace_status_word=0 payload_trace_hcomm_ret=0 "
        "payload_trace_first_error_event=none "
        "payload_trace_first_error_ret=0 "
        "payload_trace_first_error_index=-1 "
        "payload_trace_expected_thread_notify=off "
        "payload_batch_mode=on payload_comm_acquire=default "
        "payload_comm_binding=comm-name "
        "payload_thread_notify_order=not-used "
        "payload_host_received_fingerprint=222 "
        "payload_host_expected_fingerprint=222 "
        "payload_host_sample_bytes=4096" + recv_desc +
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
        "payload_kernel_hcomm_ret=0 payload_local_buffer_prime=passed payload_local_buffer_prime_pattern=strict-sentinel-v1 payload_local_buffer_prime_source=host-sentinel-not-payload payload_local_buffer_prime_bytes=4096 payload_primitive_state=completed "
        "payload_status_schema=v7 "
        "payload_status_word_count=17 payload_echo=passed payload_descriptor_fingerprint=passed payload_role=send "
        "payload_trace=passed payload_trace_schema=v3 "
        "payload_trace_word_count=82 payload_trace_event=kernel-exit "
        "payload_trace_order=passed "
        "payload_trace_ret_order=passed "
        "payload_trace_primitive_path=send-local-copy payload_trace_operand_layout=input-hbm->local-hccl-buffer "
        "payload_trace_bytes=4096 "
        "payload_trace_batch_mode=0 "
        "payload_trace_recv_path=0 "
        "payload_trace_comm_acquire=default "
        "payload_trace_comm_binding=comm-name "
        "payload_trace_transfer_mode=read "
        "payload_trace_ready_notify_idx=0 "
        "payload_trace_done_notify_idx=1 "
        "payload_trace_result=success payload_trace_status_word=0 payload_trace_hcomm_ret=0 "
        "payload_batch_mode=on payload_comm_acquire=default "
        "payload_comm_binding=comm-name "
        "payload_desc_batch_tag=default "
        "payload_transfer_mode=read "
        "payload_layout=read-default "
        "payload_recv_path=local-buffer "
            "payload_semantic_v6=present "
            "payload_semantic_v7=present "
            "payload_semantic_v8=present payload_semantic_v9=present "
        "payload_semantic_v10=present payload_semantic_v11=present "
            "payload_semantic_v12=present payload_semantic_v13=present "
            "payload_semantic_v14=present payload_semantic_v15=present payload_semantic_v16=present payload_semantic_v17=present payload_semantic_v18=present payload_semantic_v19=present "
        "payload_thread_notify_order=not-used fallback=none "
        "payload_verify=passed\"",
        "rank 1 hcomm payload smoke passed: fallback=none "
        "payload_verify=passed detail=\"fallback=none\"",
        "",
    ])


def strict_write_path_log(include_verify: bool) -> str:
    text = strict_log(include_verify)
    text = text.replace("payload_transfer_mode=read",
                        "payload_transfer_mode=write")
    text = text.replace("payload_trace_transfer_mode=read",
                        "payload_trace_transfer_mode=write")
    text = text.replace("payload_layout=read-default",
                        "payload_layout=write")
    text = text.replace("payload_trace_primitive_path=send-local-copy",
                        "payload_trace_primitive_path=send-write")
    text = text.replace("payload_desc_primitive_path=send-local-copy",
                        "payload_desc_primitive_path=send-write")
    text = text.replace(
        "payload_trace_operand_layout=input-hbm->local-hccl-buffer",
        "payload_trace_operand_layout=input-hbm->local-hccl-buffer->"
        "remote-hccl-buffer")
    text = text.replace(
        "payload_desc_operand_layout=input-hbm->local-hccl-buffer",
        "payload_desc_operand_layout=input-hbm->local-hccl-buffer->"
        "remote-hccl-buffer")
    text = text.replace("payload_trace_primitive_path=recv-read-local-copy",
                        "payload_trace_primitive_path=recv-write-local-copy")
    text = text.replace("payload_desc_primitive_path=recv-read-local-copy",
                        "payload_desc_primitive_path=recv-write-local-copy")
    text = text.replace(
        "payload_trace_operand_layout=remote-hccl-buffer->"
        "local-hccl-buffer->output-hbm",
        "payload_trace_operand_layout=local-hccl-buffer->output-hbm")
    text = text.replace(
        "payload_desc_operand_layout=remote-hccl-buffer->"
        "local-hccl-buffer->output-hbm",
        "payload_desc_operand_layout=local-hccl-buffer->output-hbm")
    marker = "payload_data_local_entry_fingerprint=111"
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if first == -1 or second == -1:
        raise AssertionError("synthetic log missing local-entry markers")
    text = text[:second] + text[second:].replace(
        marker, "payload_data_local_entry_fingerprint=222", 1)
    return _replace_trace_counts(text, 18, 16)


def strict_write_with_notify_path_log(include_verify: bool) -> str:
    text = strict_log(include_verify)
    text = text.replace("payload_transfer_mode=read",
                        "payload_transfer_mode=write-with-notify")
    text = text.replace("payload_trace_transfer_mode=read",
                        "payload_trace_transfer_mode=write-with-notify")
    text = text.replace("payload_layout=read-default",
                        "payload_layout=write-with-notify")
    text = text.replace("payload_write_notify_backend=none",
                        "payload_write_notify_backend=blocking")
    text = text.replace("payload_trace_write_notify_backend=none",
                        "payload_trace_write_notify_backend=blocking", 1)
    text = text.replace("payload_trace_write_notify_backend=none",
                        "payload_trace_write_notify_backend=peer", 1)
    text = text.replace(
        "payload_trace_primitive_path=send-local-copy",
        "payload_trace_primitive_path=send-write-with-notify")
    text = text.replace(
        "payload_desc_primitive_path=send-local-copy",
        "payload_desc_primitive_path=send-write-with-notify")
    text = text.replace(
        "payload_trace_operand_layout=input-hbm->local-hccl-buffer",
        "payload_trace_operand_layout=input-hbm->local-hccl-buffer->"
        "remote-hccl-buffer+ready-notify")
    text = text.replace(
        "payload_desc_operand_layout=input-hbm->local-hccl-buffer",
        "payload_desc_operand_layout=input-hbm->local-hccl-buffer->"
        "remote-hccl-buffer+ready-notify")
    text = text.replace(
        "payload_trace_primitive_path=recv-read-local-copy",
        "payload_trace_primitive_path=recv-write-notify-local-copy")
    text = text.replace(
        "payload_desc_primitive_path=recv-read-local-copy",
        "payload_desc_primitive_path=recv-write-notify-local-copy")
    text = text.replace(
        "payload_trace_operand_layout=remote-hccl-buffer->"
        "local-hccl-buffer->output-hbm",
        "payload_trace_operand_layout=local-hccl-buffer->output-hbm")
    text = text.replace(
        "payload_desc_operand_layout=remote-hccl-buffer->"
        "local-hccl-buffer->output-hbm",
        "payload_desc_operand_layout=local-hccl-buffer->output-hbm")
    marker = "payload_data_local_entry_fingerprint=111"
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if first == -1 or second == -1:
        raise AssertionError("synthetic log missing local-entry markers")
    text = text[:second] + text[second:].replace(
        marker, "payload_data_local_entry_fingerprint=222", 1)
    return _replace_trace_counts(text, 16, 16)


def strict_write_with_notify_trace_mismatch_log() -> str:
    text = strict_write_with_notify_path_log(True)
    return text.replace("payload_trace_primitive_path=send-write-with-notify",
                        "payload_trace_primitive_path=send-write", 1)


def strict_write_with_notify_recv_backend_mismatch_log() -> str:
    text = strict_write_with_notify_path_log(True)
    return text.replace("payload_trace_write_notify_backend=peer",
                        "payload_trace_write_notify_backend=blocking", 1)


def strict_write_with_notify_remote_write_failure_log() -> str:
    return "\n".join([
        "$ flume-hccl-collective-smoke --hcomm-require-payload-copy "
        "--hcomm-payload-write-with-notify",
        "rank 0 hcomm payload smoke unsupported: fallback=none detail=\""
        "stage3b3e_payload_copy=failed "
        "stage3b3e_direct_aclrt_payload_loader=passed "
        "stage3b3e_payload_descriptor_handoff=passed "
        "stage3b3e_direct_aclrt_payload_launch=passed "
        "stage3b3e_payload_sync=passed "
        "payload_kernel_status=remote-write-failed "
        "payload_failure_step=remote-write payload_status_word=17 "
        "payload_kernel_hcomm_ret=62 payload_status_schema=v7 "
        "payload_status_word_count=17 payload_echo=observed "
        "payload_trace_first_error_event=send-remote-write-notify-done "
        "payload_trace_first_error_ret=62 "
        "payload_trace_first_error_index=5 "
        "payload_trace_primitive_path=send-write-with-notify payload_trace_operand_layout=input-hbm->local-hccl-buffer->remote-hccl-buffer+ready-notify "
        "payload_transfer_mode=write-with-notify "
        "payload_trace_transfer_mode=write-with-notify "
        "payload_trace_write_notify_backend=blocking "
            "payload_semantic_v12=present payload_semantic_v13=present "
            "payload_semantic_v14=present payload_semantic_v15=present payload_semantic_v16=present payload_semantic_v17=present payload_semantic_v18=present payload_semantic_v19=present fallback=none\"",
        "rank 1 hcomm payload smoke unsupported: fallback=none detail=\""
        "stage3b3e_payload_copy=failed "
        "payload_failure_step=ready-notify-wait payload_status_word=8 "
        "payload_kernel_hcomm_ret=110 payload_status_schema=v7 "
        "payload_status_word_count=17 "
        "payload_trace_first_error_event=recv-ready-wait-done "
        "payload_trace_first_error_ret=110 "
        "payload_trace_primitive_path=recv-write-notify-local-copy payload_trace_operand_layout=local-hccl-buffer->output-hbm "
        "payload_transfer_mode=write-with-notify "
        "payload_trace_transfer_mode=write-with-notify "
        "payload_trace_write_notify_backend=blocking fallback=none\"",
        "",
    ])


def strict_log_with_channel_handle_binding(text: str) -> str:
    text = text.replace(
        "payload_comm_acquire=default payload_comm_binding=comm-name",
        "payload_comm_acquire=skipped payload_comm_binding=channel-handle"
    ).replace(
        "payload_trace_comm_acquire=default payload_trace_comm_binding=comm-name",
        "payload_trace_comm_acquire=skipped "
        "payload_trace_comm_binding=channel-handle")
    return _adjust_trace_counts(text, -4, -4)


def strict_log_with_no_batch(text: str) -> str:
    text = text.replace("payload_batch_mode=on",
                        "payload_batch_mode=off").replace(
                            "payload_trace_batch_mode=0",
                            "payload_trace_batch_mode=1")
    if ("payload_comm_binding=channel-handle" in text and
            "payload_recv_path=direct-output" in text):
        text = text.replace("payload_layout=read-direct-output",
                            "payload_layout=official-p2p")
        text = text.replace("payload_resolved_engine=aicpu-ts",
                            "payload_resolved_engine=aicpu")
    return _adjust_trace_counts(text, -4, -4)


def strict_write_path_with_direct_output_log() -> str:
    text = strict_write_path_log(True)
    marker = "payload_recv_path=local-buffer"
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if first == -1 or second == -1:
        raise AssertionError("synthetic log missing rank recv path markers")
    return text[:second] + text[second:].replace(
        marker, "payload_recv_path=direct-output", 1)


def strict_log_with_nonzero_hcomm_ret() -> str:
    return strict_log(True).replace(
        "payload_kernel_hcomm_ret=0", "payload_kernel_hcomm_ret=42")


def strict_log_with_primitive_return_first_error() -> str:
    text = strict_log(True)
    text = text.replace(
        "payload_failure_step=none payload_status_word=0 "
        "payload_kernel_hcomm_ret=0",
        "payload_failure_step=primitive-return payload_status_word=0 "
        "payload_kernel_hcomm_ret=88",
        1)
    text = text.replace(
        "payload_trace_first_error_event=none "
        "payload_trace_first_error_ret=0 "
        "payload_trace_first_error_index=-1",
        "payload_trace_first_error_event=recv-remote-read-done "
        "payload_trace_first_error_ret=88 "
        "payload_trace_first_error_index=7",
        1)
    return text


def strict_log_with_trace_transfer_mismatch() -> str:
    return strict_log(True).replace(
        "payload_trace_transfer_mode=read", "payload_trace_transfer_mode=write",
        1)


def strict_log_with_trace_count_mismatch() -> str:
    return _replace_trace_counts(strict_log(True), 16, 17)


def strict_log_with_missing_layout() -> str:
    return strict_log(True).replace("payload_layout=read-default ", "")


def strict_log_with_layout_mismatch() -> str:
    return strict_log_with_recv_direct_output().replace(
        "payload_layout=read-direct-output", "payload_layout=read-default", 1)


def strict_log_with_missing_trace_first_error_markers() -> str:
    text = strict_log(True)
    for marker in (
            "payload_trace_first_error_event=none ",
            "payload_trace_first_error_ret=0 ",
            "payload_trace_first_error_index=-1 "):
        text = text.replace(marker, "")
    return text


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
        "payload_kernel_hcomm_ret=0 payload_local_buffer_prime=passed payload_local_buffer_prime_pattern=strict-sentinel-v1 payload_local_buffer_prime_source=host-sentinel-not-payload payload_local_buffer_prime_bytes=4096 payload_status_schema=v7 "
        "payload_status_word_count=17 payload_echo=passed payload_descriptor_fingerprint=passed payload_role=send "
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
        "payload_kernel_hcomm_ret=88 payload_status_schema=v7 "
        "payload_status_word_count=17 payload_echo=observed "
        "payload_trace_first_error_event=recv-remote-read-done "
        "payload_trace_first_error_ret=88 "
        "payload_trace_first_error_index=7 "
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
        "payload_kernel_hcomm_ret=66").replace(
            "payload_trace_first_error_event=recv-remote-read-done "
            "payload_trace_first_error_ret=88 "
            "payload_trace_first_error_index=7",
            "payload_trace_first_error_event=recv-ready-wait-done "
            "payload_trace_first_error_ret=66 "
            "payload_trace_first_error_index=5")


def strict_log_with_rank1_output_copy_failure() -> str:
    return strict_log_with_rank1_remote_read_failure().replace(
        "payload_kernel_status=remote-read-failed "
        "payload_failure_step=remote-read payload_status_word=9 "
        "payload_kernel_hcomm_ret=88",
        "payload_kernel_status=output-copy-failed "
        "payload_failure_step=output-copy payload_status_word=16 "
        "payload_kernel_hcomm_ret=91").replace(
            "payload_trace_first_error_event=recv-remote-read-done "
            "payload_trace_first_error_ret=88 "
            "payload_trace_first_error_index=7",
            "payload_trace_first_error_event=recv-output-copy-done "
            "payload_trace_first_error_ret=91 "
            "payload_trace_first_error_index=9")


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
        "payload_kernel_hcomm_ret=0 payload_local_buffer_prime=passed payload_local_buffer_prime_pattern=strict-sentinel-v1 payload_local_buffer_prime_source=host-sentinel-not-payload payload_local_buffer_prime_bytes=4096 payload_primitive_state=completed "
        "payload_status_schema=v7 payload_status_word_count=17 "
        "payload_echo=passed payload_descriptor_fingerprint=passed payload_role=send payload_batch_mode=on "
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
        "payload_primitive_state=pending payload_status_schema=v7 "
        "payload_status_word_count=17 payload_echo=observed fallback=none\"",
        "",
    ])


def strict_log_with_checksum_mismatch() -> str:
    return strict_log(True).replace(
        "payload_checksum=1234", "payload_checksum=9999")


def strict_log_with_data_flow_mismatch() -> str:
    text = strict_log(True)
    marker = "payload_data_user_exit_fingerprint=222"
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if first == -1 or second == -1:
        raise AssertionError("synthetic log missing payload data probe markers")
    return text[:second] + text[second:].replace(
        marker, "payload_data_user_exit_fingerprint=999", 1)


def strict_log_with_recv_local_entry_already_matched() -> str:
    text = strict_log(True)
    marker = "payload_data_local_entry_fingerprint=111"
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if first == -1 or second == -1:
        raise AssertionError("synthetic log missing local-entry markers")
    return text[:second] + text[second:].replace(
        marker, "payload_data_local_entry_fingerprint=222", 1)


def strict_log_with_recv_remote_entry_mismatch() -> str:
    text = strict_log(True)
    marker = "payload_data_remote_entry_fingerprint=222"
    pos = text.find(marker)
    if pos == -1:
        raise AssertionError("synthetic log missing remote-entry marker")
    return text[:pos] + text[pos:].replace(
        marker, "payload_data_remote_entry_fingerprint=111", 1)


def strict_log_with_recv_transfer_exit_mismatch() -> str:
    text = strict_log(True)
    marker = "payload_data_transfer_exit_fingerprint=222"
    pos = text.find(marker)
    if pos == -1:
        raise AssertionError("synthetic log missing transfer-exit marker")
    return text[:pos] + text[pos:].replace(
        marker, "payload_data_transfer_exit_fingerprint=111", 1)


def strict_write_path_with_recv_local_buffer_mismatch() -> str:
    text = strict_write_path_log(True)
    marker = "payload_data_local_entry_fingerprint=222"
    pos = text.find(marker)
    if pos == -1:
        raise AssertionError("synthetic write log missing recv local-entry")
    return text[:pos] + text[pos:].replace(
        marker, "payload_data_local_entry_fingerprint=111", 1)


def strict_write_with_notify_with_recv_local_buffer_mismatch() -> str:
    text = strict_write_with_notify_path_log(True)
    marker = "payload_data_local_entry_fingerprint=222"
    pos = text.find(marker)
    if pos == -1:
        raise AssertionError(
            "synthetic write-with-notify log missing recv local-entry")
    return text[:pos] + text[pos:].replace(
        marker, "payload_data_local_entry_fingerprint=111", 1)


def strict_log_with_host_data_mismatch() -> str:
    return strict_log(True).replace(
        "payload_host_received_fingerprint=222",
        "payload_host_received_fingerprint=999")


def strict_log_with_recv_direct_output() -> str:
    text = strict_log(True)
    marker = "payload_recv_path=local-buffer"
    if text.count(marker) < 2:
        raise AssertionError("synthetic log missing rank recv path markers")
    text = text.replace(marker, "payload_recv_path=direct-output")
    text = text.replace("payload_layout=read-default",
                        "payload_layout=read-direct-output")
    text = text.replace("payload_trace_primitive_path=recv-read-local-copy",
                        "payload_trace_primitive_path=recv-read-direct-output")
    text = text.replace("payload_desc_primitive_path=recv-read-local-copy",
                        "payload_desc_primitive_path=recv-read-direct-output")
    text = text.replace(
        "payload_trace_operand_layout=remote-hccl-buffer->"
        "local-hccl-buffer->output-hbm",
        "payload_trace_operand_layout=remote-hccl-buffer->output-hbm")
    text = text.replace(
        "payload_desc_operand_layout=remote-hccl-buffer->"
        "local-hccl-buffer->output-hbm",
        "payload_desc_operand_layout=remote-hccl-buffer->output-hbm")
    text = text.replace("payload_trace_recv_path=0",
                        "payload_trace_recv_path=1")
    return _adjust_trace_counts(text, 0, -2)


def strict_log_with_channel_fence(text: str) -> str:
    text = text.replace("payload_desc_completion_mode=0",
                        "payload_desc_completion_mode=1")
    text = text.replace("payload_completion_mode=ordered-notify",
                        "payload_completion_mode=channel-fence")
    if "payload_transfer_mode=write" in text:
        return _adjust_trace_counts(text, 2, 0)
    return _adjust_trace_counts(text, 0, 2)


def strict_write_with_notify_nbi_log(include_verify: bool) -> str:
    return strict_write_with_notify_path_log(include_verify).replace(
        "payload_trace_write_notify_backend=blocking",
        "payload_trace_write_notify_backend=nbi")


def strict_log_with_missing_handoff() -> str:
    return strict_log(True).replace(
        "stage3b3e_payload_descriptor_handoff=passed ", "")


def strict_log_with_stale_status_schema() -> str:
    return strict_log(True).replace(
        "payload_status_schema=v7", "payload_status_schema=v1")


def strict_log_with_wrong_status_word_count() -> str:
    return strict_log(True).replace(
        "payload_status_word_count=17", "payload_status_word_count=4")


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
            "payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,"
            "payload_status_schema,"
            "payload_status_word_count,payload_trace_schema,"
            "payload_trace_word_count,payload_primitive_deps,"
            "payload_no_hccl_sendrecv_deps,"
            "build_mode_internal\n"
            "payload_no_hccl_sendrecv_deps=passed\n"
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
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,build_mode_internal",
        "function.payload_status_schema.FlumeHcommPayloadStatusSchemaVersion=missing",
        "function.payload_status_word_count.FlumeHcommPayloadStatusWordCount=missing",
        "status=FAIL",
        "reason=payload kernel package is missing the payload status schema marker",
        "",
    ])


def old_pass_without_status_schema_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,build_mode_internal",
        "status=PASS",
        "",
    ])


def stale_semantic_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,build_mode_internal",
        "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=missing",
        "function_so.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=missing",
        "status=FAIL",
        "reason=payload kernel package is missing the payload semantic marker",
        "",
    ])


def canary_only_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,build_mode_internal",
        "function_so.build_mode.canary_only.FlumeHcommPayloadBuildModeCanaryOnly=present",
        "function_so.build_mode.internal_payload.FlumeHcommPayloadBuildModeInternalPayload=missing",
        "status=FAIL",
        "reason=payload kernel package is canary-only; V4 payload entrypoint is a compatibility stub",
        "",
    ])


def abi_missing_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,build_mode_internal",
        "function_so.payload_abi_version_v4.FlumeHcommPayloadCopyAbiVersion4=missing",
        "status=FAIL",
        "reason=payload kernel package is missing the payload ABI version marker",
        "",
    ])


def missing_aicpu_tar_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,payload_status_schema,payload_status_word_count,payload_trace_schema,payload_trace_word_count,payload_primitive_deps,payload_no_hccl_sendrecv_deps,build_mode_internal",
        "json=present",
        "aicpu_tar=missing",
        "aicpu_tar_readable=missing",
        "aicpu_tar_so.libflume_hcomm_payload_aicpu_kernel.so=missing",
        "status=FAIL",
        "reason=payload kernel package is missing or incomplete",
        "",
    ])


def metadata_mismatch_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,payload_status_schema,payload_status_word_count,payload_trace_schema,payload_trace_word_count,payload_primitive_deps,payload_no_hccl_sendrecv_deps,build_mode_internal",
        "function_value.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=12 expected=19 status=mismatch",
        "function_value.payload_status_word_count.FlumeHcommPayloadStatusWordCount=8 expected=17 status=mismatch",
        "payload_metadata_values=mismatch",
        "status=FAIL",
        "reason=payload kernel package is missing or incomplete",
        "",
    ])


def forbidden_hccl_p2p_package_log() -> str:
    return "\n".join([
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,payload_status_schema,payload_status_word_count,payload_trace_schema,payload_trace_word_count,payload_primitive_deps,payload_no_hccl_sendrecv_deps,build_mode_internal",
        "function_so.payload_forbidden_hccl_p2p_dep.HcclSend=present",
        "function_so.payload_forbidden_hccl_p2p_dep.HcclRecv=present",
        "payload_no_hccl_sendrecv_deps=failed",
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
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,payload_status_schema,payload_status_word_count,payload_trace_schema,payload_trace_word_count,payload_primitive_deps,payload_no_hccl_sendrecv_deps,build_mode_internal",
        "status=PASS",
        "",
        "status=PASS",
        "",
    ])


def multi_candidate_canary_only_package_log() -> str:
    return "\n".join([
        "root=/tmp/stale-cann",
        "vendor=flume",
        "required=canary_direct_aclrt,payload_direct_aclrt,payload_abi_v4,payload_semantic,payload_semantic_v5,payload_semantic_v6,payload_semantic_v7,payload_semantic_v8,payload_semantic_v9,payload_semantic_v10,payload_semantic_v11,payload_semantic_v12,payload_semantic_v13,payload_semantic_v14,payload_semantic_v15,payload_semantic_v16,payload_semantic_v17,payload_semantic_v18,payload_semantic_v19,payload_requires_comm_acquire,payload_official_p2p_layout,payload_status_schema,payload_status_word_count,build_mode_internal",
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
    flume_multiproc = load_flume_multiproc(repo)
    direct_output_write_conflict = subprocess.run(
        [
            sys.executable,
            str(repo / "tools" / "flume_tool.py"),
            "--hcomm-payload-recv-direct-output",
            "--hcomm-payload-write-path",
            "local",
        ],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert direct_output_write_conflict.returncode == 2
    assert ("--hcomm-payload-recv-direct-output only applies to the read path"
            in direct_output_write_conflict.stderr)

    official_p2p_write_conflict = subprocess.run(
        [
            sys.executable,
            str(repo / "tools" / "flume_tool.py"),
            "--hcomm-payload-official-p2p-layout",
            "--hcomm-payload-write-path",
            "local",
        ],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert official_p2p_write_conflict.returncode == 2
    assert ("--hcomm-payload-official-p2p-layout is a read-path layout"
            in official_p2p_write_conflict.stderr)

    official_p2p_engine_conflict = subprocess.run(
        [
            sys.executable,
            str(repo / "tools" / "flume_tool.py"),
            "--hcomm-payload-official-p2p-layout",
            "--hcomm-channel-engine=aicpu-ts",
            "local",
        ],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert official_p2p_engine_conflict.returncode == 2
    assert ("--hcomm-payload-official-p2p-layout follows the public custom P2P "
            "example shape" in official_p2p_engine_conflict.stderr)

    with tempfile.TemporaryDirectory(prefix="flume-strict-tree-") as tmp_text:
        tmp = Path(tmp_text)
        smoke = write(
            tmp / "smoke.log",
            "FLUME_BACKEND_CAPS hcomm_primitives=on "
            "hcomm_payload_scheduler_candidate=on\n"
            "hccl collective smoke passed p2p_copy=on\n"
            "hcomm channel probe passed\n"
            "hcomm canary smoke passed canary_launch_api=host-args\n"
            "hcomm notify-only smoke passed notify_launch_api=host-args\n"
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
        command_names = [spec.name for spec in commands]
        assert "acl-runtime-probe" in command_names
        assert command_names.index("acl-runtime-probe") < command_names.index(
            "hccl-collective-smoke")
        acl_probe_command = next(
            spec.command for spec in commands
            if spec.name == "acl-runtime-probe")
        assert acl_probe_command[-1] == "--devices=0,1"
        smoke_command = next(
            spec.command for spec in commands
            if spec.name == "hccl-collective-smoke")
        assert "--storage-hbm-smoke" in smoke_command
        assert "--hcomm-require-payload-copy" in smoke_command
        assert "--hcomm-payload-batch-tag=flume-payload-v1" in smoke_command
        storage_p2p_baseline, storage_payload_only = (
            flume_tool.SplitStrictPayloadSmokeCommands(
                smoke_command + ["--p2p-copy"]))
        assert "--p2p-copy" not in storage_payload_only
        assert "--storage-hbm-smoke" in storage_payload_only
        assert "--hcomm-require-payload-copy" in storage_payload_only
        assert "--p2p-copy" in storage_p2p_baseline
        assert "--storage-hbm-smoke" not in storage_p2p_baseline
        assert "--hcomm-require-payload-copy" not in storage_p2p_baseline
        assert not any(item.startswith("--hcomm-")
                       for item in storage_p2p_baseline)

        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "flume_tool.py",
                "--build-dir", str(tmp / "build-official-p2p"),
                "--hccl-devices", "0,1",
                "--run-hcomm-payload-smoke",
                "--hcomm-require-payload-copy",
                "--hcomm-payload-official-p2p-layout",
                "ascend-probe",
            ]
            official_args = flume_tool.parse_args()
        finally:
            sys.argv = old_argv
        assert official_args.hcomm_payload_disable_batch
        assert official_args.hcomm_payload_recv_direct_output
        assert official_args.hcomm_payload_comm_binding == "channel-handle"
        assert official_args.hcomm_channel_engine == "aicpu"
        assert not official_args.hcomm_payload_write_path
        assert not official_args.hcomm_payload_channel_fence
        official_commands = flume_tool.build_commands(
            official_args, enable_hccl=True,
            run_dir=tmp / "official-p2p-command-run")
        official_smoke_command = next(
            spec.command for spec in official_commands
            if spec.name == "hccl-collective-smoke")
        assert "--hcomm-payload-disable-batch" in official_smoke_command
        assert "--hcomm-payload-recv-direct-output" in official_smoke_command
        assert "--hcomm-payload-comm-binding=channel-handle" in official_smoke_command
        assert "--hcomm-channel-engine=aicpu" in official_smoke_command
        assert "--hcomm-payload-write-path" not in official_smoke_command
        assert "--hcomm-payload-channel-fence" not in official_smoke_command
        assert flume_tool.CommandUsesOfficialP2pLayout(official_smoke_command)
        assert not flume_tool.CommandUsesOfficialP2pLayout(
            official_smoke_command + ["--hcomm-payload-channel-fence"])
        official_p2p_baseline, official_payload_only = (
            flume_tool.SplitStrictPayloadSmokeCommands(
                official_smoke_command + ["--p2p-copy"]))
        assert "--p2p-copy" not in official_payload_only
        assert flume_tool.CommandUsesOfficialP2pLayout(official_payload_only)
        assert "--p2p-copy" in official_p2p_baseline
        assert "--hcomm-payload-smoke" not in official_p2p_baseline
        assert "--hcomm-require-payload-copy" not in official_p2p_baseline
        assert "--hcomm-channel-engine=aicpu" not in official_p2p_baseline

        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "flume_tool.py",
                "--build-dir", str(tmp / "build-official-p2p-subcommand"),
                "--hccl-devices", "0,1",
                "hcomm-payload-official-p2p-positive",
            ]
            official_subcommand_args = flume_tool.parse_args()
        finally:
            sys.argv = old_argv
        assert official_subcommand_args.hcomm_payload_disable_batch
        assert official_subcommand_args.hcomm_payload_recv_direct_output
        assert official_subcommand_args.hcomm_payload_comm_binding == "channel-handle"
        assert official_subcommand_args.hcomm_channel_engine == "aicpu"
        assert not official_subcommand_args.hcomm_payload_write_path
        assert not official_subcommand_args.hcomm_payload_channel_fence
        flume_tool.ConfigureStrictPositiveCandidateMatrix(
            official_subcommand_args)
        assert not flume_tool.HasAcceptedPayloadCandidate(
            official_subcommand_args, ["flume-hccl-collective-smoke"])
        assert not flume_tool.HasAcceptedPayloadCandidate(
            official_subcommand_args,
            ["flume-hccl-collective-smoke",
             "--hcomm-payload-write-path"])

        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "flume_tool.py",
                "--build-dir", str(tmp / "build-strict-default"),
                "--hccl-devices", "0,1",
                "hcomm-payload-strict-positive",
            ]
            strict_default_args = flume_tool.parse_args()
        finally:
            sys.argv = old_argv
        assert strict_default_args.hcomm_payload_official_p2p_layout
        assert strict_default_args.hcomm_payload_disable_batch
        assert strict_default_args.hcomm_payload_recv_direct_output
        assert strict_default_args.hcomm_payload_comm_binding == "channel-handle"
        assert strict_default_args.hcomm_channel_engine == "aicpu"
        flume_tool.ConfigureStrictPositiveCandidateMatrix(strict_default_args)
        assert not strict_default_args.auto_run_hcomm_payload_official_p2p_candidate
        assert strict_default_args.auto_run_hcomm_payload_channel_handle_candidate

        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "flume_tool.py",
                "--build-dir", str(tmp / "build-strict-write"),
                "--hccl-devices", "0,1",
                "--hcomm-payload-write-path",
                "hcomm-payload-strict-positive",
            ]
            strict_write_args = flume_tool.parse_args()
        finally:
            sys.argv = old_argv
        assert not strict_write_args.hcomm_payload_official_p2p_layout
        assert strict_write_args.hcomm_payload_write_path
        assert not strict_write_args.hcomm_payload_recv_direct_output

        normal_strict_args = type("Args", (), {
            "command": "hcomm-payload-strict-positive",
            "hcomm_payload_write_with_notify_available": False,
        })()
        flume_tool.ConfigureStrictPositiveCandidateMatrix(normal_strict_args)
        assert flume_tool.HasAcceptedPayloadCandidate(
            normal_strict_args, ["flume-hccl-collective-smoke"])
        assert flume_tool.HasAcceptedPayloadCandidate(
            normal_strict_args,
            ["flume-hccl-collective-smoke",
             "--hcomm-payload-comm-binding=channel-handle",
             "--hcomm-payload-disable-batch",
             "--hcomm-payload-recv-direct-output"])

        fake_binary = write(tmp / "flume-hccl-collective-smoke", "")
        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "flume_hccl_multiproc.py",
                "--binary", str(fake_binary),
                "--devices", "0,1",
                "--init", "root-info",
                "--hcomm-payload-smoke",
                "--hcomm-require-payload-copy",
                "--hcomm-payload-official-p2p-layout",
            ]
            multiproc_args = flume_multiproc.parse_args()
        finally:
            sys.argv = old_argv
        assert multiproc_args.hcomm_payload_disable_batch
        assert multiproc_args.hcomm_payload_recv_direct_output
        assert multiproc_args.hcomm_payload_comm_binding == "channel-handle"
        assert multiproc_args.hcomm_channel_engine == "aicpu"
        multiproc_rank_command = flume_multiproc.build_rank_command(
            multiproc_args, 1, "1", 2, tmp / "root-info.bin")
        assert "--hcomm-payload-disable-batch" in multiproc_rank_command
        assert "--hcomm-payload-recv-direct-output" in multiproc_rank_command
        assert "--hcomm-payload-comm-binding=channel-handle" in multiproc_rank_command
        assert "--hcomm-channel-engine=aicpu" in multiproc_rank_command
        assert "--hcomm-payload-write-path" not in multiproc_rank_command
        assert "--hcomm-payload-channel-fence" not in multiproc_rank_command

        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "flume_tool.py",
                "--hccl-devices", "0,1",
                "--auto-run-hcomm-payload-candidate-matrix",
                "hcomm-payload-strict-positive",
            ]
            matrix_args = flume_tool.parse_args()
        finally:
            sys.argv = old_argv
        assert matrix_args.auto_run_hcomm_payload_channel_handle_candidate
        assert matrix_args.auto_run_hcomm_payload_official_p2p_candidate
        assert matrix_args.auto_run_hcomm_payload_write_path_candidate
        assert matrix_args.auto_run_hcomm_payload_write_with_notify_candidate
        assert matrix_args.auto_run_hcomm_payload_channel_fence_diagnostic
        assert matrix_args.auto_run_hcomm_payload_nobatch_diagnostic
        assert matrix_args.auto_run_hcomm_payload_tagged_diagnostic
        assert matrix_args.auto_run_hcomm_payload_direct_output_diagnostic
        assert matrix_args.auto_run_hcomm_payload_no_comm_acquire_diagnostic
        assert flume_tool.HasAcceptedPayloadCandidate(
            matrix_args, ["flume-hccl-collective-smoke"])
        helper_args = type("Args", (), {
            "auto_run_hcomm_payload_official_p2p_candidate": False,
            "auto_run_hcomm_payload_channel_handle_candidate": False,
            "auto_run_hcomm_payload_write_path_candidate": False,
            "auto_run_hcomm_payload_write_with_notify_candidate": False,
            "auto_run_hcomm_payload_channel_fence_diagnostic": False,
            "auto_run_hcomm_payload_nobatch_diagnostic": False,
            "auto_run_hcomm_payload_tagged_diagnostic": False,
            "auto_run_hcomm_payload_direct_output_diagnostic": False,
            "auto_run_hcomm_payload_no_comm_acquire_diagnostic": False,
        })()
        flume_tool.EnableHcommPayloadCandidateMatrix(helper_args)
        assert helper_args.auto_run_hcomm_payload_official_p2p_candidate
        assert helper_args.auto_run_hcomm_payload_channel_handle_candidate
        assert helper_args.auto_run_hcomm_payload_write_path_candidate
        assert helper_args.auto_run_hcomm_payload_write_with_notify_candidate
        assert helper_args.auto_run_hcomm_payload_channel_fence_diagnostic
        assert helper_args.auto_run_hcomm_payload_nobatch_diagnostic
        assert helper_args.auto_run_hcomm_payload_tagged_diagnostic
        assert helper_args.auto_run_hcomm_payload_direct_output_diagnostic
        assert helper_args.auto_run_hcomm_payload_no_comm_acquire_diagnostic
        write_only_args = type("Args", (), {
            "auto_run_hcomm_payload_official_p2p_candidate": False,
            "auto_run_hcomm_payload_channel_handle_candidate": False,
            "auto_run_hcomm_payload_write_path_candidate": True,
            "auto_run_hcomm_payload_write_with_notify_candidate": False,
            "auto_run_hcomm_payload_channel_fence_diagnostic": False,
            "auto_run_hcomm_payload_nobatch_diagnostic": False,
            "auto_run_hcomm_payload_tagged_diagnostic": False,
            "auto_run_hcomm_payload_direct_output_diagnostic": False,
        })()
        assert flume_tool.HasAcceptedPayloadCandidate(
            write_only_args, ["flume-hccl-collective-smoke"])
        assert not flume_tool.HasAcceptedPayloadCandidate(
            write_only_args,
            ["flume-hccl-collective-smoke", "--hcomm-payload-write-path"])
        write_notify_unavailable_args = type("Args", (), {
            "auto_run_hcomm_payload_official_p2p_candidate": False,
            "auto_run_hcomm_payload_channel_handle_candidate": False,
            "auto_run_hcomm_payload_write_path_candidate": False,
            "auto_run_hcomm_payload_write_with_notify_candidate": True,
            "auto_run_hcomm_payload_channel_fence_diagnostic": False,
            "auto_run_hcomm_payload_nobatch_diagnostic": False,
            "auto_run_hcomm_payload_tagged_diagnostic": False,
            "auto_run_hcomm_payload_direct_output_diagnostic": False,
            "hcomm_payload_write_with_notify_available": False,
        })()
        assert not flume_tool.HasAcceptedPayloadCandidate(
            write_notify_unavailable_args, ["flume-hccl-collective-smoke"])
        no_comm_only_args = type("Args", (), {
            "auto_run_hcomm_payload_official_p2p_candidate": False,
            "auto_run_hcomm_payload_channel_handle_candidate": False,
            "auto_run_hcomm_payload_write_path_candidate": False,
            "auto_run_hcomm_payload_write_with_notify_candidate": False,
            "auto_run_hcomm_payload_channel_fence_diagnostic": False,
            "auto_run_hcomm_payload_nobatch_diagnostic": False,
            "auto_run_hcomm_payload_tagged_diagnostic": False,
            "auto_run_hcomm_payload_direct_output_diagnostic": False,
            "auto_run_hcomm_payload_no_comm_acquire_diagnostic": True,
        })()
        assert not flume_tool.HasAcceptedPayloadCandidate(
            no_comm_only_args, ["flume-hccl-collective-smoke"])

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
        assert "`payload_descriptor_fingerprint=passed`" in text
        assert "`payload_pattern=strict-v1`" in text
        assert "| payload data flow | passed |" in text
        assert "| payload host data | passed |" in text
        assert "| no HCCL payload/collective evidence | passed |" in text
        assert "| payload official-p2p shape match | not-applicable |" in text
        assert ("| Direct ACL custom-op launch ABI observed? | "
                "canary=host-args, notify=host-args, payload=host-args |"
                in text)
        assert "| direct ACL payload launch ABI | host-args |" in text
        assert "| kernel failure step | none |" in text
        assert ("| local HCCL Buffer prime | passed | "
                "pattern=strict-sentinel-v1, "
                "source=host-sentinel-not-payload, bytes=4096;") in text
        assert "| payload checksum match | yes |" in text
        assert "| payload test pattern | strict-v1 |" in text
        assert "| payload official-p2p layout marker | present |" in text
        assert ("| host descriptor fingerprint | bytes=4096, ready=0, "
                "done=1, completion=0/ordered-notify, thread_notify=0, transfer=read, "
                "layout=rank0:read-default/rank1:read-default, "
                "primitive=rank0:send-local-copy/rank1:recv-read-local-copy, "
                "operand=rank0:input-hbm->local-hccl-buffer/rank1:"
                "remote-hccl-buffer->local-hccl-buffer->output-hbm, "
                "write_notify_backend=rank0:none/rank1:none, "
                "batch_tag=default, recv_path=local-buffer, local_buffer=8192, "
                "remote_buffer=8192 |") in text
        assert "| payload trace descriptor match | passed |" in text
        assert "| payload batch tag | default |" in text
        assert ("| HCOMM resource fingerprint | engine=aicpu-ts, "
                "protocol=hccs, channel_desc=rank-graph, channels=1, "
                "notify_num=2, usable=8192, local=8192, remote=8192 |") in text
        assert ("rank1 user-entry/local-entry/remote-entry/transfer-exit/"
                "local-exit/user-exit=111/111/222/222/222/222") in text
        assert "start Stage 3B.4 storage rewiring" in text
        strict_no_official_p2p = write(
            tmp / "strict-no-official-p2p.log",
            strict_log(True).replace(
                " payload_official_p2p_layout=present", ""))
        no_official_p2p_dir = tmp / "no-official-p2p"
        no_official_p2p_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_official_p2p_dir, smoke, strict_no_official_p2p, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload official-p2p layout marker | missing |" in text
        assert "official-p2p-layout-capable kernel" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_no_official_p2p.read_text(encoding="utf-8"))[0]
        strict_data_mismatch = write(tmp / "strict-data-mismatch.log",
                                     strict_log_with_data_flow_mismatch())
        data_mismatch_dir = tmp / "data-mismatch"
        data_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            data_mismatch_dir, smoke, strict_data_mismatch, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload data flow | recv-output-copy-mismatch |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_data_flow_mismatch())[0]
        strict_device_side_lines = flume_tool.ExtractStrictPayloadRankLines(
            strict_log(True))
        assert flume_tool.StrictPayloadDeviceDataSidePassed(
            strict_device_side_lines) == (True, "passed")
        strict_device_side_fail = write(
            tmp / "strict-device-side-fail.log",
            strict_log(True).replace(
                "payload_device_data_side=passed "
                "payload_device_data_side_reason="
                "recv-read-local-buffer-side",
                "payload_device_data_side=failed "
                "payload_device_data_side_reason=recv-user-exit-mismatch"))
        device_side_fail_dir = tmp / "device-side-fail"
        device_side_fail_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            device_side_fail_dir, smoke, strict_device_side_fail, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert ("| payload device-side self-check | "
                "rank0=passed/send-read-side, "
                "rank1=failed/recv-user-exit-mismatch |") in text
        device_side_fail_lines = flume_tool.ExtractStrictPayloadRankLines(
            strict_device_side_fail.read_text(encoding="utf-8"))
        assert flume_tool.StrictPayloadDeviceDataSidePassed(
            device_side_fail_lines) == (
                False, "rank1-failed:recv-user-exit-mismatch")
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_device_side_fail.read_text(encoding="utf-8"))[0]
        strict_local_entry_match = write(
            tmp / "strict-local-entry-match.log",
            strict_log_with_recv_local_entry_already_matched())
        local_entry_match_dir = tmp / "local-entry-match"
        local_entry_match_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            local_entry_match_dir, smoke, strict_local_entry_match, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload data flow | recv-local-entry-already-matched |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_recv_local_entry_already_matched())[0]
        strict_write_entry_match = strict_write_path_log(True)
        write_entry_match_lines = flume_tool.ExtractStrictPayloadRankLines(
            strict_write_entry_match)
        assert flume_tool.StrictPayloadDataFlowPassed(
            write_entry_match_lines) == (True, "passed")
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_write_entry_match)[0]
        strict_write_local_mismatch_text = (
            strict_write_path_with_recv_local_buffer_mismatch())
        strict_write_local_mismatch_lines = (
            flume_tool.ExtractStrictPayloadRankLines(
                strict_write_local_mismatch_text))
        assert flume_tool.StrictPayloadDataFlowPassed(
            strict_write_local_mismatch_lines) == (
                False, "recv-local-buffer-mismatch")
        write_local_action = flume_tool._PayloadCandidateNextAction(
            strict_write_local_mismatch_text)
        assert "HcommWriteOnThread" in write_local_action
        assert "direct-output" not in write_local_action
        strict_write_notify_local_mismatch_text = (
            strict_write_with_notify_with_recv_local_buffer_mismatch())
        strict_write_notify_local_mismatch_lines = (
            flume_tool.ExtractStrictPayloadRankLines(
                strict_write_notify_local_mismatch_text))
        assert flume_tool.StrictPayloadDataFlowPassed(
            strict_write_notify_local_mismatch_lines) == (
                False, "recv-local-buffer-mismatch")
        write_notify_local_action = flume_tool._PayloadCandidateNextAction(
            strict_write_notify_local_mismatch_text)
        assert "HcommWriteWithNotifyOnThread" in write_notify_local_action
        assert "direct-output" not in write_notify_local_action
        strict_remote_entry_mismatch = write(
            tmp / "strict-remote-entry-mismatch.log",
            strict_log_with_recv_remote_entry_mismatch())
        remote_entry_mismatch_dir = tmp / "remote-entry-mismatch"
        remote_entry_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            remote_entry_mismatch_dir, smoke, strict_remote_entry_mismatch,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload data flow | recv-remote-entry-mismatch |" in text
        assert ("next action: recv rank did not observe source data in the "
                "remote HCCL Buffer before the primitive") in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_recv_remote_entry_mismatch())[0]
        strict_transfer_exit_mismatch = write(
            tmp / "strict-transfer-exit-mismatch.log",
            strict_log_with_recv_transfer_exit_mismatch())
        transfer_exit_mismatch_dir = tmp / "transfer-exit-mismatch"
        transfer_exit_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            transfer_exit_mismatch_dir, smoke, strict_transfer_exit_mismatch,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload data flow | recv-transfer-exit-mismatch |" in text
        assert ("next action: primitive returned success but recv-side "
                "transfer-exit fingerprint did not match") in text
        assert "--auto-run-hcomm-payload-candidate-matrix" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_recv_transfer_exit_mismatch())[0]
        strict_host_mismatch = write(tmp / "strict-host-mismatch.log",
                                     strict_log_with_host_data_mismatch())
        host_mismatch_dir = tmp / "host-mismatch"
        host_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            host_mismatch_dir, smoke, strict_host_mismatch, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload host data | host-received-expected-mismatch |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_host_data_mismatch())[0]
        strict_trace_transfer_mismatch = write(
            tmp / "strict-trace-transfer-mismatch.log",
            strict_log_with_trace_transfer_mismatch())
        trace_transfer_mismatch_dir = tmp / "trace-transfer-mismatch"
        trace_transfer_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            trace_transfer_mismatch_dir, smoke, strict_trace_transfer_mismatch,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "transfer=rank0:write/rank1:read" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_trace_transfer_mismatch())[0]
        strict_trace_count_mismatch = write(
            tmp / "strict-trace-count-mismatch.log",
            strict_log_with_trace_count_mismatch())
        trace_count_mismatch_dir = tmp / "trace-count-mismatch"
        trace_count_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            trace_count_mismatch_dir, smoke, strict_trace_count_mismatch,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert ("| payload trace count match | "
                "rank1-trace-count-mismatch:observed=17:expected=18 |"
                in text)
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_trace_count_mismatch())[0]
        strict_missing_layout = write(
            tmp / "strict-missing-layout.log",
            strict_log_with_missing_layout())
        missing_layout_dir = tmp / "missing-layout"
        missing_layout_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            missing_layout_dir, smoke, strict_missing_layout, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload trace descriptor match | rank0-missing-trace-descriptor-field |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_missing_layout())[0]
        strict_layout_mismatch = write(
            tmp / "strict-layout-mismatch.log",
            strict_log_with_layout_mismatch())
        layout_mismatch_dir = tmp / "layout-mismatch"
        layout_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            layout_mismatch_dir, smoke, strict_layout_mismatch, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| payload trace descriptor match | rank0-payload-layout-mismatch |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_layout_mismatch())[0]
        strict_channel_handle = strict_log_with_channel_handle_binding(
            strict_log(True))
        strict_channel_handle_no_batch = strict_log_with_no_batch(
            strict_channel_handle)
        strict_channel_handle_direct_output = (
            strict_log_with_channel_handle_binding(
                strict_log_with_recv_direct_output()))
        strict_channel_handle_no_batch_direct_output = (
            strict_log_with_no_batch(strict_channel_handle_direct_output))
        strict_channel_handle_fence = strict_log_with_channel_fence(
            strict_channel_handle)
        strict_channel_handle_direct_output_fence = strict_log_with_channel_fence(
            strict_channel_handle_direct_output)
        strict_channel_handle_no_batch_fence = strict_log_with_channel_fence(
            strict_channel_handle_no_batch)
        strict_channel_handle_no_batch_direct_output_fence = (
            strict_log_with_channel_fence(
                strict_channel_handle_no_batch_direct_output))
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle)[0]
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle_no_batch)[0]
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle_direct_output)[0]
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle_no_batch_direct_output)[0]
        assert "payload_layout=read-direct-output" in (
            strict_channel_handle_direct_output)
        assert "payload_layout=official-p2p" in (
            strict_channel_handle_no_batch_direct_output)
        assert "payload_resolved_engine=aicpu" in (
            strict_channel_handle_no_batch_direct_output)
        assert "payload_layout=read-default" not in (
            strict_channel_handle_no_batch_direct_output)
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle_fence)[0]
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle_direct_output_fence)[0]
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle_no_batch_fence)[0]
        assert flume_tool.StrictPayloadOfficialP2pShapePassed(
            flume_tool.ExtractStrictPayloadRankLines(
                strict_channel_handle_no_batch_direct_output)) == (
                    True, "passed")
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_channel_handle_no_batch_direct_output_fence)[0]
        assert flume_tool.StrictPayloadOfficialP2pShapePassed(
            flume_tool.ExtractStrictPayloadRankLines(
                strict_channel_handle_no_batch_direct_output_fence)) == (
                    False,
                    "rank0-official-p2p-payload_completion_mode-mismatch:"
                    "observed=channel-fence:expected=ordered-notify")
        official_shape_mismatch_log = write(
            tmp / "strict-official-p2p-shape-mismatch.log",
            strict_channel_handle_no_batch_direct_output_fence)
        official_shape_mismatch_dir = tmp / "official-p2p-shape-mismatch"
        official_shape_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            official_shape_mismatch_dir, smoke,
            official_shape_mismatch_log, package)
        shape_mismatch_text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in shape_mismatch_text
        assert ("| payload official-p2p shape match | "
                "rank0-official-p2p-payload_completion_mode-mismatch:"
                "observed=channel-fence:expected=ordered-notify |"
                in shape_mismatch_text)
        official_engine_mismatch = (
            strict_channel_handle_no_batch_direct_output.replace(
                "payload_resolved_engine=aicpu",
                "payload_resolved_engine=aicpu-ts"))
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            official_engine_mismatch)[0]
        mismatch_lines = flume_tool.ExtractStrictPayloadRankLines(
            official_engine_mismatch)
        assert flume_tool.StrictPayloadResourceLayoutPassed(
            mismatch_lines) == (False, "official-p2p-engine-mismatch")
        official_engine_mismatch_log = write(
            tmp / "strict-official-p2p-engine-mismatch.log",
            official_engine_mismatch)
        official_engine_mismatch_dir = tmp / "official-p2p-engine-mismatch"
        official_engine_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            official_engine_mismatch_dir, smoke,
            official_engine_mismatch_log, package)
        mismatch_text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in mismatch_text
        assert ("| payload resource layout match | "
                "official-p2p-engine-mismatch |") in mismatch_text
        assert "--hcomm-channel-engine=aicpu" in mismatch_text
        channel_log = write(tmp / "strict-channel-handle.log",
                            strict_channel_handle)
        channel_note = flume_tool.WriteHcommPayloadChannelHandleCandidate(
            tmp, None, channel_log)
        channel_text = channel_note.read_text(encoding="utf-8")
        assert "channel_payload_copy_and_verify: `passed`" in channel_text
        assert "strict-positive evidence" in channel_text
        assert "payload_comm_binding=channel-handle" in channel_text

        class FakeCandidateRunner:
            def __init__(self, run_dir: Path) -> None:
                self.run_dir = run_dir
                self.run_dir.mkdir()
                self.calls: list[tuple[str, list[str]]] = []

            def run(self, name, command, *, required=False,
                    timeout_seconds=0, env_updates=None):
                del timeout_seconds, env_updates
                self.calls.append((name, list(command)))
                log_path = self.run_dir / f"{len(self.calls):02d}-{name}.log"
                if name == ("hcomm-payload-channel-handle-nobatch-"
                             "direct-output-candidate"):
                    text = strict_channel_handle_no_batch_direct_output
                    returncode = 0
                else:
                    text = strict_log(False)
                    returncode = 1
                log_path.write_text(text, encoding="utf-8")
                return flume_tool.StepResult(
                    name=name,
                    command=list(command),
                    returncode=returncode,
                    seconds=0.0,
                    log_path=log_path,
                    required=required)

        candidate_args = type("Args", (), {
            "auto_run_hcomm_payload_channel_handle_candidate": True,
            "auto_run_hcomm_payload_channel_fence_diagnostic": True,
            "auto_run_hcomm_payload_direct_output_diagnostic": True,
            "auto_run_hcomm_payload_nobatch_diagnostic": True,
        })()
        official_runner = FakeCandidateRunner(tmp / "official-p2p-candidate")
        official_selected = flume_tool.RunHcommPayloadOfficialP2pCandidate(
            official_runner,
            ["flume-hccl-collective-smoke",
             "--hcomm-require-payload-copy",
             "--hcomm-payload-channel-fence",
             "--hcomm-payload-write-path"],
            None,
            10,
            channel_log)
        assert official_selected is None
        assert official_runner.calls[0][0] == "hcomm-payload-official-p2p-candidate"
        official_command = official_runner.calls[0][1]
        assert "--hcomm-channel-engine=aicpu" in official_command
        assert "--hcomm-channel-engine=auto" not in official_command
        assert "--hcomm-payload-comm-binding=channel-handle" in official_command
        assert "--hcomm-payload-disable-batch" in official_command
        assert "--hcomm-payload-recv-direct-output" in official_command
        assert "--hcomm-payload-channel-fence" not in official_command
        assert "--hcomm-payload-write-path" not in official_command
        assert flume_tool.CommandUsesOfficialP2pLayout(official_command)
        official_note = (
            official_runner.run_dir / "HCOMM_PAYLOAD_OFFICIAL_P2P_CANDIDATE.md")
        assert official_note.exists()
        fake_runner = FakeCandidateRunner(tmp / "candidate-sequence")
        selected_candidate = (
            flume_tool.RunHcommPayloadChannelHandleFallbackCandidates(
                fake_runner,
                ["flume-hccl-collective-smoke",
                 "--hcomm-require-payload-copy"],
                None,
                10,
                channel_log,
                candidate_args))
        assert selected_candidate is not None
        assert selected_candidate.name.endswith(
            "hcomm-payload-channel-handle-nobatch-direct-output-"
            "candidate.log")
        assert fake_runner.calls[-1][0] == (
            "hcomm-payload-channel-handle-nobatch-direct-output-"
            "candidate")
        final_command = fake_runner.calls[-1][1]
        assert "--hcomm-payload-comm-binding=channel-handle" in final_command
        assert "--hcomm-payload-disable-batch" in final_command
        assert "--hcomm-payload-recv-direct-output" in final_command
        assert "--hcomm-payload-channel-fence" not in final_command
        candidate_matrix = (
            fake_runner.run_dir /
            "HCOMM_PAYLOAD_CHANNEL_HANDLE_CANDIDATE_MATRIX.md")
        assert candidate_matrix.exists()
        candidate_matrix_text = candidate_matrix.read_text(encoding="utf-8")
        assert "candidates_run: `7`" in candidate_matrix_text
        assert ("best_candidate: `hcomm-payload-channel-handle-nobatch-"
                "direct-output-candidate`"
                in candidate_matrix_text)
        assert "best_candidate_score: `" in candidate_matrix_text
        assert "best_candidate_next_action: `" in candidate_matrix_text
        assert "| engine | resource_layout |" in candidate_matrix_text
        assert ("hcomm-payload-channel-handle-nobatch-direct-output-"
                "candidate | 0 | yes | passed"
                in candidate_matrix_text)
        assert "hcomm-payload-channel-handle-direct-output-channel-fence-candidate" in candidate_matrix_text
        assert "selected_candidate_command: `" in candidate_matrix_text
        assert "--hcomm-payload-comm-binding=channel-handle" in candidate_matrix_text
        helper_runner = FakeCandidateRunner(tmp / "strict-followup-helper")
        helper_selected = flume_tool.RunHcommPayloadStrictFailureFollowups(
            helper_runner,
            candidate_args,
            ["flume-hccl-collective-smoke",
             "--hcomm-require-payload-copy"],
            None,
            10,
            channel_log,
            package_payload_ready=True)
        assert helper_selected is not None
        assert any(
            call[0].startswith("hcomm-payload-channel-handle")
            for call in helper_runner.calls)
        helper_blocked_runner = FakeCandidateRunner(
            tmp / "strict-followup-helper-blocked")
        helper_blocked = flume_tool.RunHcommPayloadStrictFailureFollowups(
            helper_blocked_runner,
            candidate_args,
            ["flume-hccl-collective-smoke",
             "--hcomm-require-payload-copy"],
            None,
            10,
            channel_log,
            package_payload_ready=False)
        assert helper_blocked is None
        assert helper_blocked_runner.calls == []
        helper_write_runner = FakeCandidateRunner(
            tmp / "strict-followup-helper-write")
        flume_tool.RunHcommPayloadStrictFailureFollowups(
            helper_write_runner,
            candidate_args,
            ["flume-hccl-collective-smoke",
             "--hcomm-require-payload-copy",
             "--hcomm-payload-write-path"],
            None,
            10,
            channel_log,
            package_payload_ready=True)
        assert all(
            "--hcomm-payload-recv-direct-output" not in call[1]
            for call in helper_write_runner.calls)
        helper_write_notify_runner = FakeCandidateRunner(
            tmp / "strict-followup-helper-write-notify")
        flume_tool.RunHcommPayloadStrictFailureFollowups(
            helper_write_notify_runner,
            candidate_args,
            ["flume-hccl-collective-smoke",
             "--hcomm-require-payload-copy",
             "--hcomm-payload-write-with-notify"],
            None,
            10,
            channel_log,
            package_payload_ready=True)
        assert all(
            "--hcomm-payload-recv-direct-output" not in call[1]
            for call in helper_write_notify_runner.calls)

        strict_write_channel_handle = strict_log_with_channel_handle_binding(
            strict_write_path_log(True))
        strict_write_channel_handle_no_batch_fence = (
            strict_log_with_channel_fence(
                strict_log_with_no_batch(strict_write_channel_handle)))
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_write_channel_handle_no_batch_fence)[0]

        class FakeWriteCandidateRunner:
            def __init__(self, run_dir: Path) -> None:
                self.run_dir = run_dir
                self.run_dir.mkdir()
                self.calls: list[tuple[str, list[str]]] = []

            def run(self, name, command, *, required=False,
                    timeout_seconds=0, env_updates=None):
                del timeout_seconds, env_updates
                self.calls.append((name, list(command)))
                log_path = self.run_dir / f"{len(self.calls):02d}-{name}.log"
                if name == ("hcomm-payload-write-path-channel-handle-"
                             "nobatch-channel-fence-candidate"):
                    text = strict_write_channel_handle_no_batch_fence
                    returncode = 0
                else:
                    text = strict_log(False).replace(
                        "payload_failure_step=none",
                        "payload_failure_step=remote-read")
                    returncode = 1
                log_path.write_text(text, encoding="utf-8")
                return flume_tool.StepResult(
                    name=name,
                    command=list(command),
                    returncode=returncode,
                    seconds=0.0,
                    log_path=log_path,
                    required=required)

        fake_write_runner = FakeWriteCandidateRunner(tmp / "write-sequence")
        selected_write_candidate = (
            flume_tool.RunHcommPayloadWritePathFallbackCandidates(
                fake_write_runner,
                ["flume-hccl-collective-smoke",
                 "--hcomm-require-payload-copy",
                 "--hcomm-payload-recv-direct-output"],
                None,
                10,
                channel_log,
                candidate_args))
        assert selected_write_candidate is not None
        assert selected_write_candidate.name.endswith(
            "hcomm-payload-write-path-channel-handle-nobatch-"
            "channel-fence-candidate.log")
        assert fake_write_runner.calls[-1][0] == (
            "hcomm-payload-write-path-channel-handle-nobatch-"
            "channel-fence-candidate")
        final_write_command = fake_write_runner.calls[-1][1]
        assert "--hcomm-payload-write-path" in final_write_command
        assert "--hcomm-payload-comm-binding=channel-handle" in final_write_command
        assert "--hcomm-payload-disable-batch" in final_write_command
        assert "--hcomm-payload-channel-fence" in final_write_command
        assert "--hcomm-payload-recv-direct-output" not in final_write_command
        write_candidate_matrix = (
            fake_write_runner.run_dir /
            "HCOMM_PAYLOAD_WRITE_PATH_CANDIDATE_MATRIX.md")
        assert write_candidate_matrix.exists()
        write_candidate_matrix_text = write_candidate_matrix.read_text(
            encoding="utf-8")
        assert "candidates_run: `8`" in write_candidate_matrix_text
        assert ("best_candidate: `hcomm-payload-write-path-channel-handle-"
                "nobatch-channel-fence-candidate`"
                in write_candidate_matrix_text)
        assert "best_candidate_score: `" in write_candidate_matrix_text
        assert "best_candidate_next_action: `" in write_candidate_matrix_text
        assert "| engine | resource_layout |" in write_candidate_matrix_text
        assert ("hcomm-payload-write-path-nobatch-channel-fence-candidate"
                in write_candidate_matrix_text)
        assert ("hcomm-payload-write-path-channel-handle-nobatch-"
                "channel-fence-candidate | 0 | yes | passed"
                in write_candidate_matrix_text)
        assert "| write | recv-write-local-copy |" in write_candidate_matrix_text
        assert "selected_candidate_command: `" in write_candidate_matrix_text
        assert "--hcomm-payload-write-path" in write_candidate_matrix_text

        strict_write_with_notify = strict_write_with_notify_path_log(True)
        strict_write_with_notify_path = write(
            tmp / "strict-write-with-notify.log", strict_write_with_notify)
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_write_with_notify)[0]
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_write_with_notify_trace_mismatch_log())[0]
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_write_with_notify_recv_backend_mismatch_log())[0]
        strict_write_with_notify_nbi = strict_write_with_notify_nbi_log(True)
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_write_with_notify_nbi)[0]
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_channel_fence(strict_write_with_notify_nbi))[0]
        write_with_notify_note = (
            flume_tool.WriteHcommPayloadWriteWithNotifyCandidate(
                tmp, None, strict_write_with_notify_path))
        write_with_notify_text = write_with_notify_note.read_text(
            encoding="utf-8")
        assert "payload_copy_and_verify: `passed`" in write_with_notify_text
        assert "transfer_mode: `write-with-notify`" in write_with_notify_text
        assert "trace_path: `send-write-with-notify`" in write_with_notify_text
        write_with_notify_failure_path = write(
            tmp / "strict-write-with-notify-remote-write-failure.log",
            strict_write_with_notify_remote_write_failure_log())
        write_with_notify_failure_note = (
            flume_tool.WriteHcommPayloadWriteWithNotifyCandidate(
                tmp, None, write_with_notify_failure_path))
        write_with_notify_failure_text = (
            write_with_notify_failure_note.read_text(encoding="utf-8"))
        assert ("first_error_event: `send-remote-write-notify-done`"
                in write_with_notify_failure_text)
        assert ("HcommWriteWithNotifyOnThread fused remote write + ready "
                "notify path" in write_with_notify_failure_text)
        assert "--hcomm-payload-write-path" in write_with_notify_failure_text
        write_with_notify_select_dir = tmp / "write-with-notify-select"
        write_with_notify_select_dir.mkdir()
        write_with_notify_candidate_log = write(
            write_with_notify_select_dir /
            "01-hcomm-payload-write-with-notify-candidate.log",
            strict_write_with_notify)
        assert flume_tool.SelectHcommPayloadEvidenceLog(
            write_with_notify_select_dir,
            None,
            require_storage=False) == write_with_notify_candidate_log
        strict_write_notify_channel_handle = (
            strict_log_with_channel_handle_binding(strict_write_with_notify))
        strict_write_notify_channel_handle_no_batch_fence = (
            strict_log_with_channel_fence(
                strict_log_with_no_batch(strict_write_notify_channel_handle)))
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_write_notify_channel_handle_no_batch_fence)[0]

        class FakeWriteNotifyCandidateRunner:
            def __init__(self, run_dir: Path) -> None:
                self.run_dir = run_dir
                self.run_dir.mkdir()
                self.calls: list[tuple[str, list[str]]] = []

            def run(self, name, command, *, required=False,
                    timeout_seconds=0, env_updates=None):
                del timeout_seconds, env_updates
                self.calls.append((name, list(command)))
                log_path = self.run_dir / f"{len(self.calls):02d}-{name}.log"
                if name == ("hcomm-payload-write-with-notify-channel-handle-"
                             "nobatch-channel-fence-candidate"):
                    text = strict_write_notify_channel_handle_no_batch_fence
                    returncode = 0
                else:
                    text = strict_log(False).replace(
                        "payload_failure_step=none",
                        "payload_failure_step=remote-read")
                    returncode = 1
                log_path.write_text(text, encoding="utf-8")
                return flume_tool.StepResult(
                    name=name,
                    command=list(command),
                    returncode=returncode,
                    seconds=0.0,
                    log_path=log_path,
                    required=required)

        fake_write_notify_runner = FakeWriteNotifyCandidateRunner(
            tmp / "write-with-notify-sequence")
        selected_write_notify_candidate = (
            flume_tool.RunHcommPayloadWriteWithNotifyFallbackCandidates(
                fake_write_notify_runner,
                ["flume-hccl-collective-smoke",
                 "--hcomm-require-payload-copy",
                 "--hcomm-payload-recv-direct-output"],
                None,
                10,
                strict_write_with_notify_path,
                candidate_args))
        assert selected_write_notify_candidate is not None
        assert selected_write_notify_candidate.name.endswith(
            "hcomm-payload-write-with-notify-channel-handle-nobatch-"
            "channel-fence-candidate.log")
        final_write_notify_command = fake_write_notify_runner.calls[-1][1]
        assert "--hcomm-payload-write-with-notify" in final_write_notify_command
        assert "--hcomm-payload-comm-binding=channel-handle" in final_write_notify_command
        assert "--hcomm-payload-disable-batch" in final_write_notify_command
        assert "--hcomm-payload-channel-fence" in final_write_notify_command
        assert "--hcomm-payload-recv-direct-output" not in final_write_notify_command
        write_notify_matrix = (
            fake_write_notify_runner.run_dir /
            "HCOMM_PAYLOAD_WRITE_WITH_NOTIFY_CANDIDATE_MATRIX.md")
        assert write_notify_matrix.exists()
        write_notify_matrix_text = write_notify_matrix.read_text(
            encoding="utf-8")
        assert "candidates_run: `8`" in write_notify_matrix_text
        assert ("best_candidate: `hcomm-payload-write-with-notify-"
                "channel-handle-nobatch-channel-fence-candidate`"
                in write_notify_matrix_text)
        assert "best_candidate_score: `" in write_notify_matrix_text
        assert "best_candidate_next_action: `" in write_notify_matrix_text
        assert "| engine | resource_layout |" in write_notify_matrix_text
        assert ("hcomm-payload-write-with-notify-nobatch-"
                "channel-fence-candidate" in write_notify_matrix_text)
        assert ("hcomm-payload-write-with-notify-channel-handle-nobatch-"
                "channel-fence-candidate | 0 | yes | passed"
                in write_notify_matrix_text)
        assert "| write-with-notify | recv-write-notify-local-copy |" in write_notify_matrix_text
        assert "selected_candidate_command: `" in write_notify_matrix_text
        assert "--hcomm-payload-write-with-notify" in write_notify_matrix_text

        skip_write_notify_args = type("Args", (), {
            "auto_run_hcomm_payload_channel_handle_candidate": True,
            "auto_run_hcomm_payload_channel_fence_diagnostic": True,
            "auto_run_hcomm_payload_direct_output_diagnostic": True,
            "auto_run_hcomm_payload_nobatch_diagnostic": True,
            "hcomm_payload_write_with_notify_available": False,
        })()
        skip_write_notify_runner = FakeWriteNotifyCandidateRunner(
            tmp / "write-with-notify-skipped")
        skipped_write_notify_candidate = (
            flume_tool.RunHcommPayloadWriteWithNotifyFallbackCandidates(
                skip_write_notify_runner,
                ["flume-hccl-collective-smoke",
                 "--hcomm-require-payload-copy"],
                None,
                10,
                strict_write_with_notify_path,
                skip_write_notify_args))
        assert skipped_write_notify_candidate is None
        assert skip_write_notify_runner.calls == []
        skipped_write_notify_matrix = (
            skip_write_notify_runner.run_dir /
            "HCOMM_PAYLOAD_WRITE_WITH_NOTIFY_CANDIDATE_MATRIX.md")
        skipped_write_notify_text = skipped_write_notify_matrix.read_text(
            encoding="utf-8")
        assert "candidates_run: `0`" in skipped_write_notify_text
        assert "payload_optional_write_with_notify=missing" in skipped_write_notify_text

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
            strict_log_with_no_batch(strict_log(True)))
        no_batch_dir = tmp / "no-batch"
        no_batch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_batch_dir, smoke, strict_no_batch, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | yes |" in text
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_no_batch.read_text(encoding="utf-8"))[0]
        no_batch_passed, no_batch_rank0, no_batch_rank1 = (
            flume_tool.StrictPayloadNoBatchDiagnosticPassed(
                strict_no_batch.read_text(encoding="utf-8")))
        assert no_batch_passed
        assert no_batch_rank0
        assert no_batch_rank1

        strict_mixed_batch = strict_log(True).replace(
            "payload_batch_mode=on", "payload_batch_mode=off", 1)
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_mixed_batch)[0]
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
        assert "payload_recv_path=direct-output" in direct_output_text
        assert "direct_rank1:" in direct_output_text
        assert "payload_recv_path=direct-output" in direct_output_text
        assert "payload_trace_primitive_path=recv-read-direct-output" in direct_output_text
        assert flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_recv_direct_output())[0]
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_write_path_with_direct_output_log())[0]
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
        assert "| rank0 strict evidence | missing |" in text
        assert "| rank1 strict evidence | missing |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_cross_line_false_positive())[0]

        strict_with_hccl_payload_api = write(
            tmp / "strict-with-hccl-p2p-payload-api.log",
            strict_log(True).replace("payload_hccl_p2p_api=not-used",
                                     "payload_hccl_p2p_api=used"))
        hccl_payload_api_dir = tmp / "hccl-p2p-payload-api"
        hccl_payload_api_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            hccl_payload_api_dir, smoke, strict_with_hccl_payload_api, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| HCCL P2P API in payload path | used |" in text
        assert "remove HcclSend/HcclRecv fallback from strict payload path" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_with_hccl_payload_api.read_text(encoding="utf-8"))[0]

        strict_without_no_hccl_marker = write(
            tmp / "strict-without-no-hccl-sendrecv-marker.log",
            strict_log(True).replace("payload_no_hccl_sendrecv=passed", ""))
        no_hccl_marker_dir = tmp / "no-hccl-marker"
        no_hccl_marker_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_hccl_marker_dir, smoke, strict_without_no_hccl_marker, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| no HCCL Send/Recv evidence | missing |" in text
        assert "stale evidence cannot prove true HCOMM payload copy" in text

        strict_without_no_hccl_payload_collective = write(
            tmp / "strict-without-no-hccl-payload-collective-marker.log",
            strict_log(True).replace(
                "payload_no_hccl_payload_collective=passed", ""))
        no_hccl_payload_collective_dir = tmp / "no-hccl-payload-collective-marker"
        no_hccl_payload_collective_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_hccl_payload_collective_dir, smoke,
            strict_without_no_hccl_payload_collective, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| no HCCL payload/collective evidence | missing |" in text
        assert "stale evidence cannot prove the payload path avoids hidden HCCL fallback APIs" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_without_no_hccl_payload_collective.read_text(
                encoding="utf-8"))[0]
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_without_no_hccl_marker.read_text(encoding="utf-8"))[0]

        strict_without_prime_source = write(
            tmp / "strict-without-prime-source.log",
            strict_log(True).replace(
                "payload_local_buffer_prime_source=host-sentinel-not-payload ",
                ""))
        no_prime_source_dir = tmp / "no-prime-source"
        no_prime_source_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_prime_source_dir, smoke, strict_without_prime_source, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "source=missing" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_without_prime_source.read_text(encoding="utf-8"))[0]

        strict_without_operand_layout = write(
            tmp / "strict-without-operand-layout.log",
            strict_log(True).replace(
                "payload_trace_operand_layout=input-hbm->local-hccl-buffer ",
                "", 1))
        no_operand_layout_dir = tmp / "no-operand-layout"
        no_operand_layout_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            no_operand_layout_dir, smoke, strict_without_operand_layout,
            package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_without_operand_layout.read_text(encoding="utf-8"))[0]

        strict_missing_trace_first_error = write(
            tmp / "strict-missing-trace-first-error.log",
            strict_log_with_missing_trace_first_error_markers())
        missing_trace_first_error_dir = tmp / "missing-trace-first-error"
        missing_trace_first_error_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            missing_trace_first_error_dir, smoke,
            strict_missing_trace_first_error, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| rank0 strict evidence | missing |" in text
        assert "| rank1 strict evidence | missing |" in text
        assert not flume_tool.StrictPayloadRankEvidencePassed(
            strict_log_with_missing_trace_first_error_markers())[0]

        strict_nonzero_hcomm = write(tmp / "strict-nonzero-hcomm.log",
                                     strict_log_with_nonzero_hcomm_ret())
        nonzero_hcomm_dir = tmp / "nonzero-hcomm"
        nonzero_hcomm_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            nonzero_hcomm_dir, smoke, strict_nonzero_hcomm, package)
        text = tree.read_text(encoding="utf-8")
        assert "| Strict payload positive passed? | no |" in text
        assert "| kernel HCOMM ret | 42 |" in text
        assert ("| rank0 suggested action | inspect rank 0 in-kernel HCOMM "
                "primitive return code: 42 |") in text
        assert "inspect rank 0 in-kernel HCOMM primitive return code: 42" in text

        strict_primitive_return_first_error = write(
            tmp / "strict-primitive-return-first-error.log",
            strict_log_with_primitive_return_first_error())
        primitive_return_first_error_dir = tmp / "primitive-return-first-error"
        primitive_return_first_error_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            primitive_return_first_error_dir, smoke,
            strict_primitive_return_first_error, package)
        text = tree.read_text(encoding="utf-8")
        assert "| rank0 first trace error | recv-remote-read-done / 88 |" in text
        assert ("inspect rank 0 HcommReadOnThread remote HCCL Buffer to "
                "local HCCL Buffer path (first_error_event="
                "recv-remote-read-done, hcomm_ret=88)") in text

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
        assert "| rank1 first trace error | recv-remote-read-done / 88 |" in text
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
        assert "| payload status schema | v1 / 17 |" in text
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
        assert "| payload status schema | v7 / 4 |" in text
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

        metadata_mismatch_package = write(
            tmp / "package-metadata-mismatch.log",
            metadata_mismatch_package_log())
        metadata_mismatch_dir = tmp / "metadata-mismatch-package"
        metadata_mismatch_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            metadata_mismatch_dir, smoke, None, metadata_mismatch_package)
        text = tree.read_text(encoding="utf-8")
        assert not flume_tool.PackageTextPayloadReady(
            metadata_mismatch_package_log())
        assert ("| HCOMM custom-op package reason | payload kernel package "
                "metadata function returned unexpected value |") in text
        assert "exports stale ABI, semantic, status, or trace metadata values" in text

        forbidden_p2p_package = write(
            tmp / "package-forbidden-hccl-p2p.log",
            forbidden_hccl_p2p_package_log())
        forbidden_p2p_dir = tmp / "forbidden-hccl-p2p-package"
        forbidden_p2p_dir.mkdir()
        tree = flume_tool.WriteMatrixDecisionTree(
            forbidden_p2p_dir, smoke, None, forbidden_p2p_package)
        text = tree.read_text(encoding="utf-8")
        assert not flume_tool.PackageTextPayloadReady(
            forbidden_hccl_p2p_package_log())
        assert ("| HCOMM custom-op package reason | payload kernel package "
                "references forbidden HCCL payload or collective symbols |") in text
        assert "must not reference HCCL payload or collective APIs" in text

        forbidden_with_strict_dir = tmp / "forbidden-package-with-strict-pass"
        forbidden_with_strict_dir.mkdir()
        write(forbidden_with_strict_dir /
              "00-hcomm-custom-op-package-preflight.log",
              forbidden_hccl_p2p_package_log())
        write(forbidden_with_strict_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(True))
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                forbidden_with_strict_dir))
        assert payload_strict_log is not None
        assert not payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)
        assert not flume_tool.DecisionTreeStrictPositiveEvidencePassed(
            analyzed_tree)
        text = analyzed_tree.read_text(encoding="utf-8")
        assert "| HCOMM custom-op package payload-ready? | not-ready |" in text
        assert "| Strict payload positive passed? | yes |" in text

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
        storage_runner = flume_tool.Runner(tmp / "storage-runner-pass")
        flume_tool.RecordHcommStorageEvidenceGate(
            storage_runner, analyzed_tree, storage_passed,
            evidence_log=storage_strict_log)
        assert storage_runner.write_summary() == 0
        storage_evidence_logs = sorted(storage_runner.run_dir.glob(
            "*-hcomm-storage-strict-evidence.log"))
        assert storage_evidence_logs
        storage_evidence_text = storage_evidence_logs[-1].read_text(
            encoding="utf-8")
        assert "hcomm_storage_evidence=passed" in storage_evidence_text
        assert "storage_hbm=hcomm-payload-staging" in storage_evidence_text
        assert "storage_hbm_fallback=none" in storage_evidence_text
        assert ("selected_storage_evidence_log="
                "01-hcomm-storage-strict-positive.log"
                in storage_evidence_text)
        assert ("selected_storage_command=flume-hccl-collective-smoke "
                "--hcomm-require-payload-copy" in storage_evidence_text)
        assert "selected_storage_focus_flags=<default-read-path>" in storage_evidence_text
        assert "selected_storage_rank1_path=hcomm-payload-staging" in storage_evidence_text
        assert "selected_storage_rank1_bytes=4096" in storage_evidence_text
        assert "selected_storage_rank1_checksum=7" in storage_evidence_text
        assert "selected_storage_hcomm_path=passed" in storage_evidence_text
        assert ("selected_storage_payload_rank0_copy_api=hcomm-direct-aclrt"
                in storage_evidence_text)
        assert ("selected_storage_payload_rank1_copy_api=hcomm-direct-aclrt"
                in storage_evidence_text)
        assert ("selected_storage_payload_rank0_hccl_p2p_api=not-used"
                in storage_evidence_text)
        assert ("selected_storage_payload_rank1_hccl_p2p_api=not-used"
                in storage_evidence_text)
        assert ("selected_storage_payload_rank0_no_hccl_sendrecv=passed"
                in storage_evidence_text)
        assert ("selected_storage_payload_rank1_no_hccl_sendrecv=passed"
                in storage_evidence_text)
        assert ("selected_storage_payload_rank0_trace_primitive_counts=passed"
                in storage_evidence_text)
        assert ("selected_storage_payload_rank1_trace_primitive_counts=passed"
                in storage_evidence_text)
        assert ("selected_storage_payload_rank1_trace_read_count=1"
                in storage_evidence_text)

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

        channel_nobatch_fallback_dir = tmp / "flume-check-channel-nobatch-fallback"
        channel_nobatch_fallback_dir.mkdir()
        write(channel_nobatch_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(channel_nobatch_fallback_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(
            channel_nobatch_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        write(
            channel_nobatch_fallback_dir /
            "03-hcomm-payload-channel-handle-nobatch-candidate.log",
            strict_channel_handle_no_batch)
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                channel_nobatch_fallback_dir))
        assert payload_strict_log is not None
        assert payload_strict_log.name.endswith(
            "hcomm-payload-channel-handle-nobatch-candidate.log")
        assert payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)

        comm_name_nobatch_diagnostic_dir = (
            tmp / "flume-check-comm-name-nobatch-diagnostic")
        comm_name_nobatch_diagnostic_dir.mkdir()
        write(comm_name_nobatch_diagnostic_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(comm_name_nobatch_diagnostic_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(comm_name_nobatch_diagnostic_dir /
              "02-hcomm-payload-nobatch-diagnostic.log",
              strict_log_with_no_batch(strict_log(True)))
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                comm_name_nobatch_diagnostic_dir))
        assert payload_strict_log is not None
        assert payload_strict_log.name.endswith(
            "hcomm-payload-nobatch-diagnostic.log")
        assert payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)

        comm_name_channel_fence_diagnostic_dir = (
            tmp / "flume-check-comm-name-channel-fence-diagnostic")
        comm_name_channel_fence_diagnostic_dir.mkdir()
        write(comm_name_channel_fence_diagnostic_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(comm_name_channel_fence_diagnostic_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(comm_name_channel_fence_diagnostic_dir /
              "02-hcomm-payload-channel-fence-diagnostic.log",
              strict_log(True).replace("payload_desc_completion_mode=0",
                                       "payload_desc_completion_mode=1"))
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                comm_name_channel_fence_diagnostic_dir))
        assert payload_strict_log is not None
        assert payload_strict_log.name.endswith(
            "hcomm-payload-channel-fence-diagnostic.log")
        assert payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)

        channel_handle_fence_fallback_dir = (
            tmp / "flume-check-channel-handle-fence-fallback")
        channel_handle_fence_fallback_dir.mkdir()
        write(channel_handle_fence_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(channel_handle_fence_fallback_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(
            channel_handle_fence_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        write(
            channel_handle_fence_fallback_dir /
            "03-hcomm-payload-channel-handle-channel-fence-candidate.log",
            strict_channel_handle.replace("payload_desc_completion_mode=0",
                                          "payload_desc_completion_mode=1"))
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                channel_handle_fence_fallback_dir))
        assert payload_strict_log is not None
        assert payload_strict_log.name.endswith(
            "hcomm-payload-channel-handle-channel-fence-candidate.log")
        assert payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)

        channel_handle_direct_fence_fallback_dir = (
            tmp / "flume-check-channel-handle-direct-fence-fallback")
        channel_handle_direct_fence_fallback_dir.mkdir()
        write(channel_handle_direct_fence_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(channel_handle_direct_fence_fallback_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(
            channel_handle_direct_fence_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        write(
            channel_handle_direct_fence_fallback_dir /
            "03-hcomm-payload-channel-handle-direct-output-channel-fence-candidate.log",
            strict_channel_handle_direct_output_fence)
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                channel_handle_direct_fence_fallback_dir))
        assert payload_strict_log is not None
        assert payload_strict_log.name.endswith(
            "hcomm-payload-channel-handle-direct-output-channel-fence-candidate.log")
        assert payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)

        channel_handle_nobatch_fence_fallback_dir = (
            tmp / "flume-check-channel-handle-nobatch-fence-fallback")
        channel_handle_nobatch_fence_fallback_dir.mkdir()
        write(channel_handle_nobatch_fence_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(channel_handle_nobatch_fence_fallback_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(
            channel_handle_nobatch_fence_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        write(
            channel_handle_nobatch_fence_fallback_dir /
            "03-hcomm-payload-channel-handle-nobatch-channel-fence-candidate.log",
            strict_channel_handle_no_batch_fence)
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                channel_handle_nobatch_fence_fallback_dir))
        assert payload_strict_log is not None
        assert payload_strict_log.name.endswith(
            "hcomm-payload-channel-handle-nobatch-channel-fence-candidate.log")
        assert payload_passed
        assert flume_tool.DecisionTreeStrictPositivePassed(analyzed_tree)

        channel_skip_failed_candidates_dir = (
            tmp / "flume-check-channel-skip-failed-candidates")
        channel_skip_failed_candidates_dir.mkdir()
        write(channel_skip_failed_candidates_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(channel_skip_failed_candidates_dir /
              "01-hcomm-payload-strict-positive.log",
              strict_log(False))
        write(
            channel_skip_failed_candidates_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        write(
            channel_skip_failed_candidates_dir /
            "03-hcomm-payload-channel-handle-nobatch-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"
            ).replace("payload_batch_mode=on", "payload_batch_mode=off"))
        write(
            channel_skip_failed_candidates_dir /
            "04-hcomm-payload-channel-handle-direct-output-candidate.log",
            strict_channel_handle_direct_output)
        analyzed_tree, payload_passed, _, payload_strict_log, _ = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                channel_skip_failed_candidates_dir))
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

        storage_channel_nobatch_fallback_dir = (
            tmp / "flume-check-storage-channel-nobatch-fallback")
        storage_channel_nobatch_fallback_dir.mkdir()
        write(storage_channel_nobatch_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(storage_channel_nobatch_fallback_dir /
              "01-hcomm-storage-strict-positive.log",
              strict_log(False))
        write(
            storage_channel_nobatch_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"))
        write(
            storage_channel_nobatch_fallback_dir /
            "03-hcomm-payload-channel-handle-nobatch-candidate.log",
            strict_channel_handle_no_batch + smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                storage_channel_nobatch_fallback_dir))
        assert storage_strict_log is not None
        assert storage_strict_log.name.endswith(
            "hcomm-payload-channel-handle-nobatch-candidate.log")
        assert storage_passed
        assert flume_tool.DecisionTreeHcommStoragePassed(analyzed_tree)

        storage_comm_name_nobatch_diagnostic_dir = (
            tmp / "flume-check-storage-comm-name-nobatch-diagnostic")
        storage_comm_name_nobatch_diagnostic_dir.mkdir()
        write(storage_comm_name_nobatch_diagnostic_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(storage_comm_name_nobatch_diagnostic_dir /
              "01-hcomm-storage-strict-positive.log",
              strict_log(False))
        write(storage_comm_name_nobatch_diagnostic_dir /
              "02-hcomm-payload-nobatch-diagnostic.log",
              strict_log_with_no_batch(strict_log(True)) +
              smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                storage_comm_name_nobatch_diagnostic_dir))
        assert storage_strict_log is not None
        assert storage_strict_log.name.endswith(
            "hcomm-payload-nobatch-diagnostic.log")
        assert storage_passed
        assert flume_tool.DecisionTreeHcommStoragePassed(analyzed_tree)

        storage_comm_name_channel_fence_diagnostic_dir = (
            tmp / "flume-check-storage-comm-name-channel-fence-diagnostic")
        storage_comm_name_channel_fence_diagnostic_dir.mkdir()
        write(storage_comm_name_channel_fence_diagnostic_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(storage_comm_name_channel_fence_diagnostic_dir /
              "01-hcomm-storage-strict-positive.log",
              strict_log(False))
        write(storage_comm_name_channel_fence_diagnostic_dir /
              "02-hcomm-payload-channel-fence-diagnostic.log",
              strict_log(True).replace("payload_desc_completion_mode=0",
                                       "payload_desc_completion_mode=1") +
              smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                storage_comm_name_channel_fence_diagnostic_dir))
        assert storage_strict_log is not None
        assert storage_strict_log.name.endswith(
            "hcomm-payload-channel-fence-diagnostic.log")
        assert storage_passed
        assert flume_tool.DecisionTreeHcommStoragePassed(analyzed_tree)

        storage_channel_handle_fence_fallback_dir = (
            tmp / "flume-check-storage-channel-handle-fence-fallback")
        storage_channel_handle_fence_fallback_dir.mkdir()
        write(storage_channel_handle_fence_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(storage_channel_handle_fence_fallback_dir /
              "01-hcomm-storage-strict-positive.log",
              strict_log(False))
        write(
            storage_channel_handle_fence_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"
            ) + smoke_with_hcomm_storage_path())
        write(
            storage_channel_handle_fence_fallback_dir /
            "03-hcomm-payload-channel-handle-channel-fence-candidate.log",
            strict_channel_handle.replace(
                "payload_desc_completion_mode=0",
                "payload_desc_completion_mode=1") +
            smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                storage_channel_handle_fence_fallback_dir))
        assert storage_strict_log is not None
        assert storage_strict_log.name.endswith(
            "hcomm-payload-channel-handle-channel-fence-candidate.log")
        assert storage_passed
        assert flume_tool.DecisionTreeHcommStoragePassed(analyzed_tree)

        storage_channel_handle_combo_fallback_dir = (
            tmp / "flume-check-storage-channel-handle-combo-fallback")
        storage_channel_handle_combo_fallback_dir.mkdir()
        write(storage_channel_handle_combo_fallback_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(storage_channel_handle_combo_fallback_dir /
              "01-hcomm-storage-strict-positive.log",
              strict_log(False))
        write(
            storage_channel_handle_combo_fallback_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"
            ) + smoke_with_hcomm_storage_path())
        write(
            storage_channel_handle_combo_fallback_dir /
            "03-hcomm-payload-channel-handle-nobatch-direct-output-candidate.log",
            strict_channel_handle_no_batch_direct_output +
            smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                storage_channel_handle_combo_fallback_dir))
        assert storage_strict_log is not None
        assert storage_strict_log.name.endswith(
            "hcomm-payload-channel-handle-nobatch-direct-output-candidate.log")
        assert storage_passed
        assert flume_tool.DecisionTreeHcommStoragePassed(analyzed_tree)

        storage_skip_failed_candidates_dir = (
            tmp / "flume-check-storage-skip-failed-candidates")
        storage_skip_failed_candidates_dir.mkdir()
        write(storage_skip_failed_candidates_dir /
              "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(storage_skip_failed_candidates_dir /
              "01-hcomm-storage-strict-positive.log",
              strict_log(False))
        write(
            storage_skip_failed_candidates_dir /
            "02-hcomm-payload-channel-handle-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"
            ) + smoke_with_hcomm_storage_path())
        write(
            storage_skip_failed_candidates_dir /
            "03-hcomm-payload-channel-handle-nobatch-candidate.log",
            strict_log(False).replace(
                "payload_comm_acquire=default payload_comm_binding=comm-name",
                "payload_comm_acquire=skipped payload_comm_binding=channel-handle"
            ).replace("payload_batch_mode=on", "payload_batch_mode=off") +
            smoke_with_hcomm_storage_path())
        write(
            storage_skip_failed_candidates_dir /
            "04-hcomm-payload-channel-handle-direct-output-candidate.log",
            strict_channel_handle_direct_output + smoke_with_hcomm_storage_path())
        analyzed_tree, storage_passed, _, storage_strict_log, _ = (
            flume_tool.AnalyzeHcommStorageStrictPositiveLogs(
                storage_skip_failed_candidates_dir))
        assert storage_strict_log is not None
        assert storage_strict_log.name.endswith(
            "hcomm-payload-channel-handle-direct-output-candidate.log")
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

        acl_runtime_dir = tmp / "acl-runtime-unavailable"
        acl_runtime_dir.mkdir()
        write(acl_runtime_dir / "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        write(acl_runtime_dir / "01-acl-runtime-probe.log",
              "acl_runtime_probe=failed step=aclInit ret=500000\n"
              "DrvMngGetConsoleLogLevel failed. (ret=4)\n")
        tree = flume_tool.WriteMatrixDecisionTree(
            acl_runtime_dir, None, acl_runtime_dir /
            "01-acl-runtime-probe.log",
            acl_runtime_dir / "00-hcomm-custom-op-package-preflight.log")
        text = tree.read_text(encoding="utf-8")
        assert ("| NPU runtime ready for strict payload? | "
                "driver-runtime-unavailable |") in text
        assert "strict payload markers" not in text

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
        strict_pass_tree = tree
        strict_pass_log = found_strict_log

        official_log_dir = tmp / "flume-check-synthetic-official-pass"
        official_log_dir.mkdir()
        write(official_log_dir / "00-hcomm-custom-op-package-preflight.log",
              payload_ready_package_log())
        official_strict = write(
            official_log_dir / "01-hcomm-payload-official-p2p-positive.log",
            strict_channel_handle_no_batch_direct_output)
        tree, passed, _smoke_log, found_strict_log, package_log = (
            flume_tool.AnalyzeHcommPayloadStrictPositiveLogs(
                official_log_dir))
        assert passed
        assert found_strict_log == official_strict
        assert package_log == (
            official_log_dir / "00-hcomm-custom-op-package-preflight.log")
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
            pass_runner, strict_pass_tree, True, required=True,
            evidence_log=strict_pass_log)
        assert pass_runner.write_summary() == 0
        pass_evidence_logs = sorted(pass_runner.run_dir.glob(
            "*-hcomm-payload-strict-evidence.log"))
        assert pass_evidence_logs
        pass_evidence_text = pass_evidence_logs[-1].read_text(
            encoding="utf-8")
        assert "strict_positive_evidence=passed" in pass_evidence_text
        assert "payload_data_flow=passed" in pass_evidence_text
        assert "payload_host_data=passed" in pass_evidence_text
        assert "payload_trace_order=passed" in pass_evidence_text
        assert "payload_trace_ret_order=passed" in pass_evidence_text
        assert "payload_official_p2p_layout=present" in pass_evidence_text
        assert "payload_checksum_match=passed" in pass_evidence_text
        assert "fallback=none" in pass_evidence_text
        pass_tree_text = strict_pass_tree.read_text(encoding="utf-8")
        assert "| payload device-side self-check | rank0=passed/send-read-side, rank1=passed/recv-read-local-buffer-side |" in pass_tree_text
        assert "selected_evidence_log=02-hcomm-payload-strict-positive.log" in pass_evidence_text
        assert ("selected_payload_command=flume-hccl-collective-smoke "
                "--hcomm-require-payload-copy") in pass_evidence_text
        assert "selected_payload_focus_flags=<default-read-path>" in pass_evidence_text
        assert "selected_payload_rank0_trace_path=send-local-copy" in pass_evidence_text
        assert "selected_payload_rank1_trace_path=recv-read-local-copy" in pass_evidence_text
        assert "selected_payload_rank0_trace_operand_layout=input-hbm->local-hccl-buffer" in pass_evidence_text
        assert "selected_payload_rank1_trace_operand_layout=remote-hccl-buffer->local-hccl-buffer->output-hbm" in pass_evidence_text
        assert "selected_payload_rank0_transfer_mode=read" in pass_evidence_text
        assert "selected_payload_rank1_transfer_mode=read" in pass_evidence_text
        assert "selected_payload_recv_path=local-buffer" in pass_evidence_text
        assert "selected_payload_rank0_kernel_status=success" in pass_evidence_text
        assert "selected_payload_rank1_kernel_status=success" in pass_evidence_text
        assert "selected_payload_rank0_failure_step=none" in pass_evidence_text
        assert "selected_payload_rank1_failure_step=none" in pass_evidence_text
        assert "selected_payload_rank0_status_word=0" in pass_evidence_text
        assert "selected_payload_rank1_status_word=0" in pass_evidence_text
        assert "selected_payload_rank0_kernel_hcomm_ret=0" in pass_evidence_text
        assert "selected_payload_rank1_kernel_hcomm_ret=0" in pass_evidence_text
        assert "selected_payload_rank0_trace_schema=v3" in pass_evidence_text
        assert "selected_payload_rank1_trace_schema=v3" in pass_evidence_text
        assert "selected_payload_rank0_trace_word_count=82" in pass_evidence_text
        assert "selected_payload_rank1_trace_word_count=82" in pass_evidence_text
        assert "selected_payload_rank0_trace_count=16" in pass_evidence_text
        assert "selected_payload_rank1_trace_count=18" in pass_evidence_text
        assert "selected_payload_rank0_trace_status_word=0" in pass_evidence_text
        assert "selected_payload_rank1_trace_status_word=0" in pass_evidence_text
        assert "selected_payload_rank0_trace_hcomm_ret=0" in pass_evidence_text
        assert "selected_payload_rank1_trace_hcomm_ret=0" in pass_evidence_text
        assert "selected_payload_rank0_trace_result=success" in pass_evidence_text
        assert "selected_payload_rank1_trace_result=success" in pass_evidence_text
        assert "selected_payload_rank0_fallback=none" in pass_evidence_text
        assert "selected_payload_rank1_fallback=none" in pass_evidence_text

        summary_dir = tmp / "candidate-summary"
        summary_dir.mkdir()
        default_candidate = write(summary_dir / "01-hcomm-payload-strict-positive.log",
                                  strict_log(False))
        write_candidate = write(summary_dir / "02-hcomm-payload-write-path-candidate.log",
                                strict_write_path_log(True))
        summary = flume_tool.WriteHcommPayloadStrictCandidateSummary(
            summary_dir,
            [
                flume_tool.StepResult(
                    "hcomm-payload-strict-positive",
                    ["flume-hccl-collective-smoke"],
                    1,
                    1.0,
                    default_candidate,
                    True),
                flume_tool.StepResult(
                    "hcomm-payload-write-path-candidate",
                    ["flume-hccl-collective-smoke",
                     "--hcomm-payload-write-path"],
                    0,
                    1.0,
                    write_candidate,
                    False),
            ],
            default_candidate,
            write_candidate)
        assert summary is not None
        summary_text = summary.read_text(encoding="utf-8")
        assert "best_candidate: `hcomm-payload-write-path-candidate`" in summary_text
        assert ("best_candidate_command: `flume-hccl-collective-smoke "
                "--hcomm-payload-write-path`" in summary_text)
        assert "best_candidate_focus_flags: `--hcomm-payload-write-path`" in summary_text
        assert "selected_evidence_log: `" in summary_text
        assert "transfer | trace_transfer" in summary_text
        assert "binding | engine | resource_layout" in summary_text
        assert "write | write" in summary_text
        assert "candidate passed; rerun strict-positive focused on `payload_transfer_mode=write`" in summary_text

        offline_dir = tmp / "flume-check-offline-candidates"
        offline_default = write(
            offline_dir / "01-hcomm-payload-strict-positive.log",
            "$ flume-hccl-collective-smoke --hcomm-require-payload-copy\n"
            "returncode: 1\n\n" + strict_log(False))
        offline_write = write(
            offline_dir / "02-hcomm-payload-write-path-candidate.log",
            "$ flume-hccl-collective-smoke --hcomm-require-payload-copy "
            "--hcomm-payload-write-path\n"
            "returncode: 0\n\n" + strict_write_path_log(True))
        write(
            offline_dir / "03-hcomm-payload-strict-evidence.log",
            "$ internal evidence gate\nreturncode: 0\n\n"
            "strict_positive_evidence=passed\n")
        offline_results = (
            flume_tool.HcommPayloadCandidateResultsFromRunDir(offline_dir))
        assert [result.name for result in offline_results] == [
            "hcomm-payload-strict-positive",
            "hcomm-payload-write-path-candidate",
        ]
        offline_summary = flume_tool.WriteHcommPayloadStrictCandidateSummary(
            offline_dir, offline_results, offline_default, offline_write)
        assert offline_summary is not None
        offline_text = offline_summary.read_text(encoding="utf-8")
        assert "best_candidate: `hcomm-payload-write-path-candidate`" in offline_text
        assert "best_candidate_focus_flags: `--hcomm-payload-write-path`" in offline_text
        assert "binding | engine | resource_layout" in offline_text

        evidence_select_dir = tmp / "candidate-evidence-select"
        evidence_select_dir.mkdir()
        write_path_evidence = write(
            evidence_select_dir /
            "01-hcomm-payload-write-path-candidate.log",
            "$ flume-hccl-collective-smoke --hcomm-require-payload-copy "
            "--hcomm-payload-write-path\nreturncode: 0\n\n" +
            strict_write_path_log(True))
        official_p2p_evidence = write(
            evidence_select_dir /
            "02-hcomm-payload-official-p2p-candidate.log",
            "$ flume-hccl-collective-smoke --hcomm-require-payload-copy "
            "--hcomm-payload-comm-binding=channel-handle "
            "--hcomm-payload-disable-batch "
            "--hcomm-payload-recv-direct-output\nreturncode: 0\n\n" +
            strict_channel_handle_no_batch_direct_output)
        assert flume_tool.FindPassingHcommPayloadCandidateLog(
            evidence_select_dir, require_storage=False) == official_p2p_evidence
        assert official_p2p_evidence != write_path_evidence

        progress_dir = tmp / "candidate-progress-summary"
        progress_dir.mkdir()
        early_candidate = write(
            progress_dir / "01-hcomm-payload-strict-positive.log",
            "$ flume-hccl-collective-smoke --hcomm-require-payload-copy\n"
            "returncode: 1\n\n"
            "stage3b3e_direct_aclrt_payload_loader=passed\n"
            "stage3b3e_payload_descriptor_handoff=passed\n"
            "stage3b3e_direct_aclrt_payload_launch=not-attempted\n")
        transfer_candidate = write(
            progress_dir / "02-hcomm-payload-channel-fence-diagnostic.log",
            "$ flume-hccl-collective-smoke --hcomm-require-payload-copy "
            "--hcomm-payload-channel-fence\n"
            "returncode: 1\n\n" +
            strict_log_with_recv_transfer_exit_mismatch())
        progress_summary = flume_tool.WriteHcommPayloadStrictCandidateSummary(
            progress_dir,
            [
                flume_tool.StepResult(
                    "hcomm-payload-strict-positive",
                    ["flume-hccl-collective-smoke"],
                    1,
                    1.0,
                    early_candidate,
                    True),
                flume_tool.StepResult(
                    "hcomm-payload-channel-fence-diagnostic",
                    ["flume-hccl-collective-smoke",
                     "--hcomm-payload-channel-fence"],
                    1,
                    1.0,
                    transfer_candidate,
                    False),
            ],
            early_candidate,
            None)
        assert progress_summary is not None
        progress_text = progress_summary.read_text(encoding="utf-8")
        assert "best_candidate: `hcomm-payload-channel-fence-diagnostic`" in progress_text
        assert "best_candidate_focus_flags: `--hcomm-payload-channel-fence`" in progress_text
        assert "recv-transfer-exit-mismatch" in progress_text
        assert ("HCOMM primitive returned success but recv transfer-exit "
                "fingerprint did not change") in progress_text

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
        assert "payload_descriptor_fingerprint=passed" in evidence_text
        assert "payload_trace_operand_layout=," in evidence_text

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
