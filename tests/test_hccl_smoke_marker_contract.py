#!/usr/bin/env python3
"""Keep the HCCL smoke app's strict payload markers aligned with the tool gate."""

from __future__ import annotations

import sys
from pathlib import Path


REQUIRED_MARKERS = (
    "payload_status_schema=v7",
    "payload_status_word_count=17",
    "payload_local_buffer_prime=passed",
    "payload_local_buffer_prime_source=host-sentinel-not-payload",
    "payload_echo=passed",
    "payload_descriptor_fingerprint=passed",
    "payload_host_descriptor_validation=passed",
    "payload_host_validation_reason=ok",
    "payload_host_validation_reason_code=0",
    "payload_data_probe=observed",
    "payload_data_user_entry_fingerprint=",
    "payload_data_local_entry_fingerprint=",
    "payload_data_remote_entry_fingerprint=",
    "payload_data_transfer_exit_fingerprint=",
    "payload_data_local_exit_fingerprint=",
    "payload_data_user_exit_fingerprint=",
    "payload_data_sample_bytes=",
    "payload_device_data_side=passed",
    "payload_device_data_side_reason=",
    "payload_host_source_fingerprint=",
    "payload_host_received_fingerprint=",
    "payload_host_expected_fingerprint=",
    "payload_host_sample_bytes=",
    "payload_primitive_state=completed",
    "payload_trace=passed",
    "payload_trace_header=passed",
    "payload_trace_schema=v3",
    "payload_trace_word_count=82",
    "payload_trace_event=kernel-exit",
    "payload_trace_order=passed",
    "payload_trace_ret_order=passed",
    "payload_trace_primitive_counts=passed",
    "payload_trace_local_copy_count=",
    "payload_trace_read_count=",
    "payload_trace_write_count=",
    "payload_trace_write_notify_count=",
    "payload_trace_notify_record_count=",
    "payload_trace_notify_wait_count=",
    "payload_trace_channel_fence_count=",
    "payload_trace_bytes=",
    'DetailValueMarker("payload_trace_operand_layout"',
    'DetailValueMarker("payload_desc_primitive_path"',
    'DetailValueMarker("payload_desc_operand_layout"',
    "payload_trace_batch_mode=",
    'DetailValueMarker("payload_trace_recv_path"',
    "payload_trace_comm_acquire=",
    'DetailValueMarker("payload_trace_comm_binding"',
    'DetailValueMarker("payload_trace_transfer_mode"',
    "recv-read-local-copy",
    "recv-read-direct-output",
    "payload_trace_ready_notify_idx=",
    "payload_trace_done_notify_idx=",
    "payload_trace_role=",
    "payload_trace_local_rank=",
    "payload_trace_peer_rank=",
    "payload_trace_status_word=0",
    "payload_trace_hcomm_ret=0",
    "payload_trace_first_error_event=none",
    "payload_trace_first_error_ret=0",
    "payload_trace_first_error_index=-1",
    'DetailValueMarker("payload_layout"',
    "payload_semantic_v10=present",
    "payload_semantic_v11=present",
    "payload_semantic_v12=present",
    "payload_semantic_v13=present",
    "payload_semantic_v14=present",
    "payload_semantic_v15=present",
    "payload_semantic_v16=present",
    "payload_semantic_v17=present",
    "payload_semantic_v18=present",
    "payload_semantic_v19=present",
    "payload_official_p2p_layout=present",
    "payload_copy_api=hcomm-direct-aclrt",
    "payload_hccl_p2p_api=not-used",
    "payload_no_hccl_sendrecv=passed",
    "payload_no_hccl_payload_collective=passed",
)

STALE_MARKERS = (
    "payload_status_schema=v3",
    "payload_status_word_count=10",
    "HCOMM payload no-batch diagnostic completed but cannot",
)

RUNTIME_PACKAGE_READY_MARKERS = (
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V9_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V10_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V11_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V12_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V13_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V14_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V15_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V16_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V17_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V18_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V19_FUNC",
    "FLUME_HCOMM_PAYLOAD_COPY_SUPPORTS_OFFICIAL_P2P_LAYOUT_FUNC",
)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: test_hccl_smoke_marker_contract.py <repo-root>",
              file=sys.stderr)
        return 2
    repo = Path(sys.argv[1]).resolve()
    source = repo / "apps" / "flume-hccl-collective-smoke.cc"
    text = source.read_text(encoding="utf-8")
    missing = [marker for marker in REQUIRED_MARKERS if marker not in text]
    if missing:
        print("missing strict smoke marker(s):", file=sys.stderr)
        for marker in missing:
            print(f"  {marker}", file=sys.stderr)
        return 1
    stale = [marker for marker in STALE_MARKERS if marker in text]
    if stale:
        print("stale strict smoke marker(s):", file=sys.stderr)
        for marker in stale:
            print(f"  {marker}", file=sys.stderr)
        return 1
    client = repo / "src" / "core" / "client.cc"
    client_text = client.read_text(encoding="utf-8")
    if "constexpr bool HcommWriteWithNotifyUsesNbiBackend()" not in client_text:
        print("missing NBI write-with-notify backend helper",
              file=sys.stderr)
        return 1
    if client_text.count("HcommWriteWithNotifyUsesNbiBackend()") < 3:
        print("NBI write-with-notify helper must drive both plan and desc "
              "completion-mode selection", file=sys.stderr)
        return 1
    if "constexpr const char* HcommWriteWithNotifyBackendName()" not in client_text:
        print("missing host write-with-notify backend marker helper",
              file=sys.stderr)
        return 1
    if "payload_write_notify_backend=" not in client_text:
        print("missing host write-with-notify backend marker",
              file=sys.stderr)
        return 1
    desc_start = client_text.find("void FillFlumePayloadCopyDesc(")
    desc_end = client_text.find("bool IsUnsupportedHcclLaunchResult(",
                                desc_start)
    if desc_start == -1 or desc_end == -1:
        print("could not find HCOMM payload descriptor fill block",
              file=sys.stderr)
        return 1
    desc_block = client_text[desc_start:desc_end]
    desc_markers = (
        "payload_batch_tag.empty() ?",
        "kDefaultHcommPayloadBatchTag",
        "memcpy(desc->batch_tag",
        "desc->batch_tag[tag_len] = '\\0'",
    )
    missing_desc_markers = [
        marker for marker in desc_markers if marker not in desc_block
    ]
    if missing_desc_markers:
        print("missing payload descriptor batch-tag fill marker(s):",
              file=sys.stderr)
        for marker in missing_desc_markers:
            print(f"  {marker}", file=sys.stderr)
        return 1
    start = client_text.find("bool JsonLooksPayloadReady(")
    end = client_text.find("if (json_text.empty())", start)
    if start == -1 or end == -1:
        print("could not find JsonLooksPayloadReady required marker block",
              file=sys.stderr)
        return 1
    package_ready_block = client_text[start:end]
    missing_runtime = [
        marker for marker in RUNTIME_PACKAGE_READY_MARKERS
        if marker not in package_ready_block
    ]
    if missing_runtime:
        print("runtime package-ready marker(s) missing:", file=sys.stderr)
        for marker in missing_runtime:
            print(f"  {marker}", file=sys.stderr)
        return 1
    runtime_detail_start = client_text.find("std::string HcommPayloadRuntimeDetail(")
    runtime_detail_end = client_text.find("#if FLUME_BUILD_HCOMM_CUSTOM_OP",
                                          runtime_detail_start)
    if runtime_detail_start == -1 or runtime_detail_end == -1:
        print("could not find HcommPayloadRuntimeDetail block",
              file=sys.stderr)
        return 1
    runtime_detail_block = client_text[runtime_detail_start:runtime_detail_end]
    if "payload_semantic_v19=present" not in runtime_detail_block:
        print("runtime strict payload detail is missing semantic v19 marker",
              file=sys.stderr)
        return 1
    payload_lookup_start = client_text.find("const PayloadAclrtRequiredFunction required_functions[]")
    payload_lookup_end = client_text.find("void* kernel_trace_dev = nullptr;",
                                          payload_lookup_start)
    if payload_lookup_start == -1 or payload_lookup_end == -1:
        print("could not find payload direct ACL required function block",
              file=sys.stderr)
        return 1
    payload_lookup_block = client_text[payload_lookup_start:payload_lookup_end]
    if "FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION_V19_FUNC" not in payload_lookup_block:
        print("payload direct ACL loader does not require semantic v19",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
