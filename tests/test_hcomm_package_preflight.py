#!/usr/bin/env python3
"""Synthetic tests for Flume HCOMM custom-op package preflight."""

from __future__ import annotations

import json
import importlib.util
import platform
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from types import SimpleNamespace


KERNEL_SO = "libflume_hcomm_payload_aicpu_kernel.so"
KERNEL_JSON = "libflume_hcomm_payload_aicpu_kernel.json"
AICPU_TAR = "aicpu_flume_hcomm_payload.tar.gz"


def load_flume_tool(repo: Path):
    spec = importlib.util.spec_from_file_location(
        "flume_tool_under_test", repo / "tools" / "flume_tool.py")
    if spec is None or spec.loader is None:
        raise AssertionError("failed to load tools/flume_tool.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def compile_kernel(tmp: Path, mode: str) -> Path:
    source = tmp / f"kernel_{mode}.c"
    lines = [
        "unsigned int FlumeHcommCanaryDirectAclrtKernel(void *p) "
        "{ (void)p; return 0; }",
        "unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV2(void *p) "
        "{ (void)p; return 0; }",
        "unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV3(void *p) "
        "{ (void)p; return 0; }",
    ]
    if mode in ("v4", "v4_nbi_write_with_notify",
                "v4_no_write_with_notify", "wrong_values"):
        include_write_with_notify = mode != "v4_no_write_with_notify"
        include_write_with_notify_nbi = mode == "v4_nbi_write_with_notify"
        lines.extend([
            "typedef unsigned long long ThreadHandle;",
            "typedef unsigned long long ChannelHandle;",
            "int HcommAcquireComm(const char* a) { (void)a; return 0; }",
            "int HcommReleaseComm(const char* a) { (void)a; return 0; }",
            "int HcommBatchModeStart(const char* a) { (void)a; return 0; }",
            "int HcommBatchModeEnd(const char* a) { (void)a; return 0; }",
            "int HcommLocalCopyOnThread(ThreadHandle a, void* b, const void* c, unsigned long long d) { (void)a; (void)b; (void)c; (void)d; return 0; }",
            "int HcommReadOnThread(ThreadHandle a, ChannelHandle b, void* c, const void* d, unsigned long long e) { (void)a; (void)b; (void)c; (void)d; (void)e; return 0; }",
            "int HcommWriteOnThread(ThreadHandle a, ChannelHandle b, void* c, const void* d, unsigned long long e) { (void)a; (void)b; (void)c; (void)d; (void)e; return 0; }",
            "int HcommChannelNotifyRecordOnThread(ThreadHandle a, ChannelHandle b, unsigned int c) { (void)a; (void)b; (void)c; return 0; }",
            "int HcommChannelNotifyWaitOnThread(ThreadHandle a, ChannelHandle b, unsigned int c, unsigned int d) { (void)a; (void)b; (void)c; (void)d; return 0; }",
            "int HcommChannelFenceOnThread(ThreadHandle a, ChannelHandle b) { (void)a; (void)b; return 0; }",
            "int HcommThreadNotifyRecordOnThread(ThreadHandle a, ThreadHandle b, unsigned int c) { (void)a; (void)b; (void)c; return 0; }",
            "int HcommThreadNotifyWaitOnThread(ThreadHandle a, unsigned int b, unsigned int c) { (void)a; (void)b; (void)c; return 0; }",
            "unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV4(void *p) {",
            "  char a[8] = {0};",
            "  char b[8] = {0};",
            "  volatile int r = 0;",
            "  r += HcommAcquireComm(\"flume_unit_comm\");",
            "  r += HcommBatchModeStart(\"flume_unit_batch\");",
            "  r += HcommLocalCopyOnThread(1, b, a, 8);",
            "  r += HcommChannelNotifyRecordOnThread(1, 2, 0);",
            "  r += HcommChannelNotifyWaitOnThread(1, 2, 1, 60);",
            "  r += HcommReadOnThread(1, 2, a, b, 8);",
            "  r += HcommWriteOnThread(1, 2, b, a, 8);",
            "  r += HcommChannelFenceOnThread(1, 2);",
            "  r += HcommThreadNotifyWaitOnThread(1, 0, 60);",
            "  r += HcommThreadNotifyRecordOnThread(1, 3, 0);",
            "  r += HcommBatchModeEnd(\"flume_unit_batch\");",
            "  r += HcommReleaseComm(\"flume_unit_comm\");",
            "  (void)p;",
            "  return (unsigned int)(r & 0);",
            "}",
        ])
        if include_write_with_notify:
            kernel_index = lines.index(
                "unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV4(void *p) {")
            write_symbol = (
                "HcommWriteWithNotifyNbiOnThread"
                if include_write_with_notify_nbi
                else "HcommWriteWithNotifyOnThread")
            lines.insert(
                kernel_index,
                f"int {write_symbol}(ThreadHandle a, ChannelHandle b, void* c, const void* d, unsigned long long e, unsigned int f) {{ (void)a; (void)b; (void)c; (void)d; (void)e; (void)f; return 0; }}")
            write_index = lines.index(
                "  r += HcommChannelFenceOnThread(1, 2);")
            lines.insert(
                write_index,
                f"  r += {write_symbol}(1, 2, b, a, 8, 0);")
    else:
        lines.append(
            "unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV4(void *p) "
            "{ (void)p; return 0; }")
    lines.extend([
        "unsigned int FlumeHcommPayloadCopyDirectAclrtKernel(void *p) "
        "{ (void)p; return FlumeHcommPayloadCopyDirectAclrtKernelV4(p); }",
        "unsigned int FlumeHcommPayloadCopyAbiVersion(void) { return 4; }",
    ])
    if mode == "canary":
        lines.append(
            "unsigned int FlumeHcommPayloadBuildModeCanaryOnly(void) "
            "{ return 1; }"
        )
    else:
        lines.append(
            "unsigned int FlumeHcommPayloadBuildModeInternalPayload(void) "
            "{ return 1; }"
        )
    if mode != "legacy":
        lines.append(
            "unsigned int FlumeHcommPayloadCopyAbiVersion2(void) { return 1; }"
        )
    if mode not in ("legacy", "stale_v2"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopyAbiVersion3(void) { return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_v3"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopyAbiVersion4(void) { return 1; }"
        )
    semantic_value = "11" if mode == "wrong_values" else "12"
    status_schema_value = "3" if mode == "wrong_values" else "4"
    status_word_count_value = "8" if mode == "wrong_values" else "14"
    if mode not in ("legacy", "stale_v2"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion(void) "
            f"{{ return {semantic_value}; }}"
        )
    if mode not in ("legacy", "stale_v2", "stale_semantic"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion5(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion6(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion7(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion8(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7", "stale_semantic_v8"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion9(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7", "stale_semantic_v8",
                    "stale_semantic_v9"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion10(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7", "stale_semantic_v8",
                    "stale_semantic_v9", "stale_semantic_v10"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion11(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7", "stale_semantic_v8",
                    "stale_semantic_v9", "stale_semantic_v10",
                    "stale_semantic_v11"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopySemanticVersion12(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_v3",
                    "stale_v4_no_comm_acquire",
                    "canary"):
        lines.append(
            "unsigned int FlumeHcommPayloadCopyRequiresCommAcquire(void) "
            "{ return 1; }"
        )
    if mode not in ("legacy", "stale_v2", "stale_v3",
                    "stale_v4_no_status_schema"):
        lines.append(
            "unsigned int FlumeHcommPayloadStatusSchemaVersion(void) "
            f"{{ return {status_schema_value}; }}"
        )
        lines.append(
            "unsigned int FlumeHcommPayloadStatusWordCount(void) "
            f"{{ return {status_word_count_value}; }}"
        )
    if mode not in ("legacy", "stale_v2", "stale_v3",
                    "stale_v8_no_trace_schema"):
        lines.append(
            "unsigned int FlumeHcommPayloadTraceSchemaVersion(void) "
            "{ return 2; }"
        )
        lines.append(
            "unsigned int FlumeHcommPayloadTraceWordCount(void) "
            "{ return 80; }"
        )
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output = tmp / f"kernel_{mode}.so"
    if platform.system() == "Darwin":
        command = ["cc", "-dynamiclib", "-undefined", "dynamic_lookup",
                   "-o", str(output), str(source)]
    else:
        command = ["cc", "-shared", "-fPIC", "-o", str(output), str(source)]
    subprocess.run(command, check=True)
    return output


def write_package(tmp: Path, mode: str) -> tuple[Path, Path]:
    so_path = compile_kernel(tmp, mode)
    tar_path = tmp / f"pkg_{mode}.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(so_path, arcname=f"aicpu_kernels_device/{KERNEL_SO}")

    kernel_so = (
        "libwrong_flume_hcomm_payload_aicpu_kernel.so"
        if mode == "wrong_so" else KERNEL_SO
    )
    payload = {
        "FlumeHcommCanaryDirectAclrtKernel": {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommCanaryDirectAclrtKernel",
            }
        },
        "FlumeHcommPayloadCopyDirectAclrtKernelV2": {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyDirectAclrtKernelV2",
            }
        },
        "FlumeHcommPayloadCopyDirectAclrtKernelV3": {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyDirectAclrtKernelV3",
            }
        },
        "FlumeHcommPayloadCopyDirectAclrtKernelV4": {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyDirectAclrtKernelV4",
            }
        },
    }
    if mode != "legacy":
        payload["FlumeHcommPayloadCopyAbiVersion2"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyAbiVersion2",
            }
        }
    if mode not in ("legacy", "stale_v2"):
        payload["FlumeHcommPayloadCopyAbiVersion3"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyAbiVersion3",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_v3"):
        payload["FlumeHcommPayloadCopyAbiVersion4"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyAbiVersion4",
            }
        }
    if mode not in ("legacy", "stale_v2"):
        payload["FlumeHcommPayloadCopySemanticVersion"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_semantic"):
        payload["FlumeHcommPayloadCopySemanticVersion5"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion5",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5"):
        payload["FlumeHcommPayloadCopySemanticVersion6"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion6",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6"):
        payload["FlumeHcommPayloadCopySemanticVersion7"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion7",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7"):
        payload["FlumeHcommPayloadCopySemanticVersion8"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion8",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7", "stale_semantic_v8"):
        payload["FlumeHcommPayloadCopySemanticVersion9"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion9",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7", "stale_semantic_v8",
                    "stale_semantic_v9"):
        payload["FlumeHcommPayloadCopySemanticVersion10"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion10",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7", "stale_semantic_v8",
                    "stale_semantic_v9", "stale_semantic_v10"):
        payload["FlumeHcommPayloadCopySemanticVersion11"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion11",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_semantic",
                    "stale_semantic_v5", "stale_semantic_v6",
                    "stale_semantic_v7", "stale_semantic_v8",
                    "stale_semantic_v9", "stale_semantic_v10",
                    "stale_semantic_v11"):
        payload["FlumeHcommPayloadCopySemanticVersion12"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopySemanticVersion12",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_v3",
                    "stale_v4_no_comm_acquire",
                    "canary"):
        payload["FlumeHcommPayloadCopyRequiresCommAcquire"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadCopyRequiresCommAcquire",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_v3",
                    "stale_v4_no_status_schema"):
        payload["FlumeHcommPayloadStatusSchemaVersion"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadStatusSchemaVersion",
            }
        }
        payload["FlumeHcommPayloadStatusWordCount"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadStatusWordCount",
            }
        }
    if mode not in ("legacy", "stale_v2", "stale_v3",
                    "stale_v8_no_trace_schema"):
        payload["FlumeHcommPayloadTraceSchemaVersion"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadTraceSchemaVersion",
            }
        }
        payload["FlumeHcommPayloadTraceWordCount"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadTraceWordCount",
            }
        }
    if mode != "canary":
        payload["FlumeHcommPayloadBuildModeInternalPayload"] = {
            "opInfo": {
                "opKernelLib": "AICPUKernel",
                "kernelSo": kernel_so,
                "functionName": "FlumeHcommPayloadBuildModeInternalPayload",
            }
        }
    json_path = tmp / f"pkg_{mode}.json"
    json_path.write_text(json.dumps(payload), encoding="utf-8")
    return json_path, tar_path


def write_fake_cann_root(tmp: Path, name: str = "fake-cann",
                         *, hccl_header: bool = True,
                         hcomm_header: bool = True,
                         write_with_notify_lib: bool = True,
                         write_with_notify_nbi_lib: bool = True) -> Path:
    root = tmp / name
    include_hccl = root / "aarch64-linux" / "include" / "hccl"
    include_hcomm = root / "aarch64-linux" / "include" / "hcomm"
    lib64 = root / "aarch64-linux" / "lib64"
    include_hccl.mkdir(parents=True)
    include_hcomm.mkdir(parents=True)
    lib64.mkdir(parents=True)
    header = r"""
#ifndef HCOMM_PRIMITIVES_H_
#define HCOMM_PRIMITIVES_H_
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
typedef uint64_t ChannelHandle;
typedef uint64_t ThreadHandle;
int32_t HcommLocalCopyOnThread(ThreadHandle, void*, const void*, uint64_t);
int32_t HcommReadOnThread(ThreadHandle, ChannelHandle, void*, const void*, uint64_t);
int32_t HcommWriteOnThread(ThreadHandle, ChannelHandle, void*, const void*, uint64_t);
int32_t HcommWriteWithNotifyOnThread(ThreadHandle, ChannelHandle, void*, const void*, uint64_t, uint32_t);
int32_t HcommWriteWithNotifyNbiOnThread(ThreadHandle, ChannelHandle, void*, const void*, uint64_t, uint32_t);
int32_t HcommChannelNotifyRecordOnThread(ThreadHandle, ChannelHandle, uint32_t);
int32_t HcommChannelNotifyWaitOnThread(ThreadHandle, ChannelHandle, uint32_t, uint32_t);
int32_t HcommChannelFenceOnThread(ThreadHandle, ChannelHandle);
int32_t HcommAcquireComm(const char*);
int32_t HcommReleaseComm(const char*);
int32_t HcommBatchModeStart(const char*);
int32_t HcommBatchModeEnd(const char*);
int32_t HcommThreadNotifyRecordOnThread(ThreadHandle, ThreadHandle, uint32_t);
int32_t HcommThreadNotifyWaitOnThread(ThreadHandle, uint32_t, uint32_t);
#ifdef __cplusplus
}
#endif
#endif
"""
    if hccl_header:
        (include_hccl / "hcomm_primitives.h").write_text(
            header, encoding="utf-8")
    if hcomm_header:
        (include_hcomm / "hcomm_primitives.h").write_text(
            header, encoding="utf-8")
    source = tmp / "fake_hcomm.c"
    source_lines = [
        """
#include <stdint.h>
typedef uint64_t ChannelHandle;
typedef uint64_t ThreadHandle;
int32_t HcommLocalCopyOnThread(ThreadHandle a, void* b, const void* c, uint64_t d) { (void)a; (void)b; (void)c; (void)d; return 0; }
int32_t HcommReadOnThread(ThreadHandle a, ChannelHandle b, void* c, const void* d, uint64_t e) { (void)a; (void)b; (void)c; (void)d; (void)e; return 0; }
int32_t HcommWriteOnThread(ThreadHandle a, ChannelHandle b, void* c, const void* d, uint64_t e) { (void)a; (void)b; (void)c; (void)d; (void)e; return 0; }
int32_t HcommChannelNotifyRecordOnThread(ThreadHandle a, ChannelHandle b, uint32_t c) { (void)a; (void)b; (void)c; return 0; }
int32_t HcommChannelNotifyWaitOnThread(ThreadHandle a, ChannelHandle b, uint32_t c, uint32_t d) { (void)a; (void)b; (void)c; (void)d; return 0; }
int32_t HcommChannelFenceOnThread(ThreadHandle a, ChannelHandle b) { (void)a; (void)b; return 0; }
int32_t HcommAcquireComm(const char* a) { (void)a; return 0; }
int32_t HcommReleaseComm(const char* a) { (void)a; return 0; }
int32_t HcommBatchModeStart(const char* a) { (void)a; return 0; }
int32_t HcommBatchModeEnd(const char* a) { (void)a; return 0; }
int32_t HcommThreadNotifyRecordOnThread(ThreadHandle a, ThreadHandle b, uint32_t c) { (void)a; (void)b; (void)c; return 0; }
int32_t HcommThreadNotifyWaitOnThread(ThreadHandle a, uint32_t b, uint32_t c) { (void)a; (void)b; (void)c; return 0; }
"""
    ]
    if write_with_notify_lib:
        source_lines.append(
            "int32_t HcommWriteWithNotifyOnThread(ThreadHandle a, "
            "ChannelHandle b, void* c, const void* d, uint64_t e, "
            "uint32_t f) { (void)a; (void)b; (void)c; (void)d; "
            "(void)e; (void)f; return 0; }\n")
    if write_with_notify_nbi_lib:
        source_lines.append(
            "int32_t HcommWriteWithNotifyNbiOnThread(ThreadHandle a, "
            "ChannelHandle b, void* c, const void* d, uint64_t e, "
            "uint32_t f) { (void)a; (void)b; (void)c; (void)d; "
            "(void)e; (void)f; return 0; }\n")
    source.write_text("".join(source_lines), encoding="utf-8")
    output = lib64 / ("libhcomm.dylib" if platform.system() == "Darwin"
                      else "libhcomm.so")
    command = (["cc", "-dynamiclib", "-o", str(output), str(source)]
               if platform.system() == "Darwin"
               else ["cc", "-shared", "-fPIC", "-o", str(output), str(source)])
    subprocess.run(command, check=True)
    return root


def run_preflight(repo: Path, json_path: Path, tar_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(repo / "tools" / "flume_tool.py"),
            f"--custom-op-json={json_path}",
            f"--custom-op-aicpu-tar={tar_path}",
            "--require-hcomm-payload-kernel",
            "hcomm-custom-op-package",
        ],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: test_hcomm_package_preflight.py <repo-root>", file=sys.stderr)
        return 2
    repo = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="flume-package-preflight-") as tmp_text:
        tmp = Path(tmp_text)
        flume_tool = load_flume_tool(repo)
        local_hcomm_refer = (
            repo / "refer" / "cann-src" / "hcomm" / "include")
        if (local_hcomm_refer / "hcomm_primitives.h").exists():
            missing_toolkit_root = tmp / "missing-hcomm-header-toolkit"
            (missing_toolkit_root / "include").mkdir(parents=True)
            (missing_toolkit_root / "lib64").mkdir()
            candidates = flume_tool.HcommPrimitivesHeaderCandidates(
                missing_toolkit_root)
            assert local_hcomm_refer / "hcomm_primitives.h" in candidates
            assert (flume_tool.FindHcommPrimitivesHeader(
                missing_toolkit_root) ==
                    local_hcomm_refer / "hcomm_primitives.h")
            assert (flume_tool.HcommPrimitiveHeaderSourceLabel(
                local_hcomm_refer / "hcomm_primitives.h",
                missing_toolkit_root) == "local-refer")
            include_flags = flume_tool.HcommPrimitiveIncludeFlags(
                missing_toolkit_root)
            assert f"-I{local_hcomm_refer}" in include_flags
            assert any(
                flag.endswith("refer/cann-src/hcomm/test/stub/depends/include")
                for flag in include_flags)
            explicit_missing = tmp / "explicit-missing-hcomm-header"
            explicit_missing.mkdir()
            explicit_candidates = flume_tool.HcommPrimitivesHeaderCandidates(
                missing_toolkit_root, str(explicit_missing))
            assert local_hcomm_refer / "hcomm_primitives.h" not in explicit_candidates
            explicit_header_root = tmp / "explicit-hcomm-header"
            explicit_header_root.mkdir()
            explicit_header = explicit_header_root / "hcomm_primitives.h"
            explicit_header.write_text("// explicit test header\n",
                                       encoding="utf-8")
            assert (flume_tool.FindHcommPrimitivesHeader(
                missing_toolkit_root, str(explicit_header_root)) ==
                    explicit_header)
            assert (flume_tool.HcommPrimitiveHeaderSourceLabel(
                explicit_header, missing_toolkit_root) == "override")
            toolkit_header_root = tmp / "toolkit-with-hcomm-header"
            (toolkit_header_root / "include").mkdir(parents=True)
            toolkit_header = toolkit_header_root / "include" / "hcomm_primitives.h"
            toolkit_header.write_text("// toolkit test header\n",
                                      encoding="utf-8")
            assert (flume_tool.FindHcommPrimitivesHeader(
                toolkit_header_root) == toolkit_header)
            assert (flume_tool.HcommPrimitiveHeaderSourceLabel(
                toolkit_header, toolkit_header_root) == "toolkit")
        static_json_path = (
            repo / "custom_ops" / "hcomm_payload_copy" / "aicpu" /
            KERNEL_JSON
        )
        static_payload = json.loads(static_json_path.read_text(encoding="utf-8"))
        for label, function_name in flume_tool.HCOMM_CUSTOM_OP_FUNCTIONS.items():
            assert flume_tool.JsonDeclaresFunction(
                static_payload, function_name, KERNEL_SO), label
        assert flume_tool.JsonDeclaresFunction(
            static_payload, flume_tool.HCOMM_LEGACY_PAYLOAD_DIRECT_ACLRT,
            KERNEL_SO)
        static_payload_json_path = (
            repo / "custom_ops" / "hcomm_payload_copy" / "aicpu" /
            "libflume_hcomm_payload_aicpu_kernel_payload.json"
        )
        assert static_payload_json_path.read_text(
            encoding="utf-8") == static_json_path.read_text(encoding="utf-8")
        static_canary_json_path = (
            repo / "custom_ops" / "hcomm_payload_copy" / "aicpu" /
            "libflume_hcomm_payload_aicpu_kernel_canary.json"
        )
        static_canary = json.loads(static_canary_json_path.read_text(
            encoding="utf-8"))
        for label in ("canary_direct_aclrt", "payload_direct_aclrt",
                      "payload_abi_v2", "payload_abi_v3", "payload_abi_v4",
                      "payload_semantic", "payload_semantic_v5",
                      "payload_semantic_v6", "payload_semantic_v7",
                      "payload_semantic_v8",
                      "payload_status_schema",
                      "payload_status_word_count",
                      "payload_trace_schema",
                      "payload_trace_word_count"):
            assert flume_tool.JsonDeclaresFunction(
                static_canary, flume_tool.HCOMM_CUSTOM_OP_FUNCTIONS[label],
                KERNEL_SO), label
        assert not flume_tool.JsonDeclaresFunction(
            static_canary,
            flume_tool.HCOMM_CUSTOM_OP_FUNCTIONS["build_mode_internal"],
            KERNEL_SO)

        legacy_json, legacy_tar = write_package(tmp, mode="legacy")
        legacy = run_preflight(repo, legacy_json, legacy_tar)
        if legacy.returncode == 0:
            print(legacy.stdout)
            print(legacy.stderr, file=sys.stderr)
            raise AssertionError("legacy package without ABI v2 marker passed")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=missing" in legacy.stdout
        assert "function_so.payload_abi_version_v2.FlumeHcommPayloadCopyAbiVersion2=missing" in legacy.stdout
        assert "function.payload_abi_v3.FlumeHcommPayloadCopyAbiVersion3=missing" in legacy.stdout
        assert "function_so.payload_abi_version_v3.FlumeHcommPayloadCopyAbiVersion3=missing" in legacy.stdout
        assert "function.payload_abi_v4.FlumeHcommPayloadCopyAbiVersion4=missing" in legacy.stdout
        assert "function_so.payload_abi_version_v4.FlumeHcommPayloadCopyAbiVersion4=missing" in legacy.stdout
        assert "reason=payload kernel package is missing the payload ABI version marker" in legacy.stdout

        canary_json, canary_tar = write_package(tmp, mode="canary")
        canary = run_preflight(repo, canary_json, canary_tar)
        if canary.returncode == 0:
            print(canary.stdout)
            print(canary.stderr, file=sys.stderr)
            raise AssertionError("canary-only package passed as payload-ready")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=present" in canary.stdout
        assert "function_so.payload_abi_version_v2.FlumeHcommPayloadCopyAbiVersion2=present" in canary.stdout
        assert "function.payload_abi_v3.FlumeHcommPayloadCopyAbiVersion3=present" in canary.stdout
        assert "function_so.payload_abi_version_v3.FlumeHcommPayloadCopyAbiVersion3=present" in canary.stdout
        assert "function.payload_abi_v4.FlumeHcommPayloadCopyAbiVersion4=present" in canary.stdout
        assert "function_so.payload_abi_version_v4.FlumeHcommPayloadCopyAbiVersion4=present" in canary.stdout
        assert "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=present" in canary.stdout
        assert "function.payload_semantic_v6.FlumeHcommPayloadCopySemanticVersion6=present" in canary.stdout
        assert "function.payload_semantic_v7.FlumeHcommPayloadCopySemanticVersion7=present" in canary.stdout
        assert "function.payload_semantic_v8.FlumeHcommPayloadCopySemanticVersion8=present" in canary.stdout
        assert "function.payload_requires_comm_acquire.FlumeHcommPayloadCopyRequiresCommAcquire=missing" in canary.stdout
        assert "function.payload_status_schema.FlumeHcommPayloadStatusSchemaVersion=present" in canary.stdout
        assert "function.payload_status_word_count.FlumeHcommPayloadStatusWordCount=present" in canary.stdout
        assert "function.payload_trace_schema.FlumeHcommPayloadTraceSchemaVersion=present" in canary.stdout
        assert "function.payload_trace_word_count.FlumeHcommPayloadTraceWordCount=present" in canary.stdout
        assert "function.build_mode_internal.FlumeHcommPayloadBuildModeInternalPayload=missing" in canary.stdout
        assert "function_so.build_mode.canary_only.FlumeHcommPayloadBuildModeCanaryOnly=present" in canary.stdout
        assert "function_so.build_mode.internal_payload.FlumeHcommPayloadBuildModeInternalPayload=missing" in canary.stdout
        assert "reason=payload kernel package is canary-only" in canary.stdout

        wrong_so_json, wrong_so_tar = write_package(tmp, mode="wrong_so")
        wrong_so = run_preflight(repo, wrong_so_json, wrong_so_tar)
        if wrong_so.returncode == 0:
            print(wrong_so.stdout)
            print(wrong_so.stderr, file=sys.stderr)
            raise AssertionError("package with mismatched JSON kernelSo passed")
        assert "aicpu_tar_so.libflume_hcomm_payload_aicpu_kernel.so=present" in wrong_so.stdout
        assert "function.canary_direct_aclrt.FlumeHcommCanaryDirectAclrtKernel=missing" in wrong_so.stdout
        assert (
            "function.payload_direct_aclrt."
            "FlumeHcommPayloadCopyDirectAclrtKernelV4=missing"
        ) in wrong_so.stdout
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=missing" in wrong_so.stdout
        assert "function.payload_abi_v3.FlumeHcommPayloadCopyAbiVersion3=missing" in wrong_so.stdout
        assert "function.payload_abi_v4.FlumeHcommPayloadCopyAbiVersion4=missing" in wrong_so.stdout
        assert "status=FAIL" in wrong_so.stdout

        stale_v2_json, stale_v2_tar = write_package(tmp, mode="stale_v2")
        stale_v2 = run_preflight(repo, stale_v2_json, stale_v2_tar)
        if stale_v2.returncode == 0:
            print(stale_v2.stdout)
            print(stale_v2.stderr, file=sys.stderr)
            raise AssertionError("stale ABI v2 package without semantic marker passed")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=present" in stale_v2.stdout
        assert "function.payload_abi_v3.FlumeHcommPayloadCopyAbiVersion3=missing" in stale_v2.stdout
        assert "function.payload_abi_v4.FlumeHcommPayloadCopyAbiVersion4=missing" in stale_v2.stdout
        assert "function.build_mode_internal.FlumeHcommPayloadBuildModeInternalPayload=present" in stale_v2.stdout
        assert "function_so.payload_abi_version_v4.FlumeHcommPayloadCopyAbiVersion4=missing" in stale_v2.stdout
        assert "reason=payload kernel package is stale ABI v2" in stale_v2.stdout

        stale_v3_json, stale_v3_tar = write_package(tmp, mode="stale_v3")
        stale_v3 = run_preflight(repo, stale_v3_json, stale_v3_tar)
        if stale_v3.returncode == 0:
            print(stale_v3.stdout)
            print(stale_v3.stderr, file=sys.stderr)
            raise AssertionError("stale ABI v3 package passed")
        assert "function.payload_abi_v3.FlumeHcommPayloadCopyAbiVersion3=present" in stale_v3.stdout
        assert "function.payload_abi_v4.FlumeHcommPayloadCopyAbiVersion4=missing" in stale_v3.stdout
        assert "reason=payload kernel package is stale ABI v3" in stale_v3.stdout

        stale_v4_json, stale_v4_tar = write_package(
            tmp, mode="stale_v4_no_comm_acquire")
        stale_v4 = run_preflight(repo, stale_v4_json, stale_v4_tar)
        if stale_v4.returncode == 0:
            print(stale_v4.stdout)
            print(stale_v4.stderr, file=sys.stderr)
            raise AssertionError("stale V4 package without comm-acquire marker passed")
        assert "function.payload_abi_v4.FlumeHcommPayloadCopyAbiVersion4=present" in stale_v4.stdout
        assert "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=present" in stale_v4.stdout
        assert "function.payload_semantic_v6.FlumeHcommPayloadCopySemanticVersion6=present" in stale_v4.stdout
        assert "function.payload_semantic_v7.FlumeHcommPayloadCopySemanticVersion7=present" in stale_v4.stdout
        assert "function.payload_semantic_v8.FlumeHcommPayloadCopySemanticVersion8=present" in stale_v4.stdout
        assert "function.payload_requires_comm_acquire.FlumeHcommPayloadCopyRequiresCommAcquire=missing" in stale_v4.stdout
        assert "function_so.payload_requires_comm_acquire.FlumeHcommPayloadCopyRequiresCommAcquire=missing" in stale_v4.stdout
        assert "reason=payload kernel package is missing the payload comm-acquire marker" in stale_v4.stdout

        stale_semantic_json, stale_semantic_tar = write_package(
            tmp, mode="stale_semantic")
        stale_semantic = run_preflight(
            repo, stale_semantic_json, stale_semantic_tar)
        if stale_semantic.returncode == 0:
            print(stale_semantic.stdout)
            print(stale_semantic.stderr, file=sys.stderr)
            raise AssertionError("stale semantic package passed")
        assert "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=present" in stale_semantic.stdout
        assert "function.payload_semantic_v5.FlumeHcommPayloadCopySemanticVersion5=missing" in stale_semantic.stdout
        assert "function_so.payload_semantic_version_v5.FlumeHcommPayloadCopySemanticVersion5=missing" in stale_semantic.stdout
        assert "reason=payload kernel package has a stale payload semantic marker" in stale_semantic.stdout

        stale_semantic_v5_json, stale_semantic_v5_tar = write_package(
            tmp, mode="stale_semantic_v5")
        stale_semantic_v5 = run_preflight(
            repo, stale_semantic_v5_json, stale_semantic_v5_tar)
        if stale_semantic_v5.returncode == 0:
            print(stale_semantic_v5.stdout)
            print(stale_semantic_v5.stderr, file=sys.stderr)
            raise AssertionError("stale semantic v5 package passed")
        assert "function.payload_semantic_v5.FlumeHcommPayloadCopySemanticVersion5=present" in stale_semantic_v5.stdout
        assert "function.payload_semantic_v6.FlumeHcommPayloadCopySemanticVersion6=missing" in stale_semantic_v5.stdout
        assert "function_so.payload_semantic_version_v6.FlumeHcommPayloadCopySemanticVersion6=missing" in stale_semantic_v5.stdout
        assert "reason=payload kernel package has a stale payload semantic marker" in stale_semantic_v5.stdout
        assert "current Flume semantic v6 direct-output-capable payload kernel" in stale_semantic_v5.stdout

        stale_semantic_v6_json, stale_semantic_v6_tar = write_package(
            tmp, mode="stale_semantic_v6")
        stale_semantic_v6 = run_preflight(
            repo, stale_semantic_v6_json, stale_semantic_v6_tar)
        if stale_semantic_v6.returncode == 0:
            print(stale_semantic_v6.stdout)
            print(stale_semantic_v6.stderr, file=sys.stderr)
            raise AssertionError("stale semantic v6 package passed")
        assert "function.payload_semantic_v6.FlumeHcommPayloadCopySemanticVersion6=present" in stale_semantic_v6.stdout
        assert "function.payload_semantic_v7.FlumeHcommPayloadCopySemanticVersion7=missing" in stale_semantic_v6.stdout
        assert "function_so.payload_semantic_version_v7.FlumeHcommPayloadCopySemanticVersion7=missing" in stale_semantic_v6.stdout
        assert "reason=payload kernel package has a stale payload semantic marker" in stale_semantic_v6.stdout
        assert "current Flume semantic v7 device-trace-capable payload kernel" in stale_semantic_v6.stdout

        stale_semantic_v7_json, stale_semantic_v7_tar = write_package(
            tmp, mode="stale_semantic_v7")
        stale_semantic_v7 = run_preflight(
            repo, stale_semantic_v7_json, stale_semantic_v7_tar)
        if stale_semantic_v7.returncode == 0:
            print(stale_semantic_v7.stdout)
            print(stale_semantic_v7.stderr, file=sys.stderr)
            raise AssertionError("stale semantic v7 package passed")
        assert "function.payload_semantic_v7.FlumeHcommPayloadCopySemanticVersion7=present" in stale_semantic_v7.stdout
        assert "function.payload_semantic_v8.FlumeHcommPayloadCopySemanticVersion8=missing" in stale_semantic_v7.stdout
        assert "function_so.payload_semantic_version_v8.FlumeHcommPayloadCopySemanticVersion8=missing" in stale_semantic_v7.stdout
        assert "reason=payload kernel package has a stale payload semantic marker" in stale_semantic_v7.stdout
        assert "current Flume semantic v8 ordered-trace-capable payload kernel" in stale_semantic_v7.stdout

        stale_semantic_v8_json, stale_semantic_v8_tar = write_package(
            tmp, mode="stale_semantic_v8")
        stale_semantic_v8 = run_preflight(
            repo, stale_semantic_v8_json, stale_semantic_v8_tar)
        if stale_semantic_v8.returncode == 0:
            print(stale_semantic_v8.stdout)
            print(stale_semantic_v8.stderr, file=sys.stderr)
            raise AssertionError("stale semantic v8 package passed")
        assert "function.payload_semantic_v8.FlumeHcommPayloadCopySemanticVersion8=present" in stale_semantic_v8.stdout
        assert "function.payload_semantic_v9.FlumeHcommPayloadCopySemanticVersion9=missing" in stale_semantic_v8.stdout
        assert "function_so.payload_semantic_version_v9.FlumeHcommPayloadCopySemanticVersion9=missing" in stale_semantic_v8.stdout
        assert "reason=payload kernel package has a stale payload semantic marker" in stale_semantic_v8.stdout
        assert "current Flume semantic v9 descriptor-fingerprint-capable payload kernel" in stale_semantic_v8.stdout

        stale_semantic_v9_json, stale_semantic_v9_tar = write_package(
            tmp, mode="stale_semantic_v9")
        stale_semantic_v9 = run_preflight(
            repo, stale_semantic_v9_json, stale_semantic_v9_tar)
        if stale_semantic_v9.returncode == 0:
            print(stale_semantic_v9.stdout)
            print(stale_semantic_v9.stderr, file=sys.stderr)
            raise AssertionError("stale semantic v9 package passed")
        assert "function.payload_semantic_v9.FlumeHcommPayloadCopySemanticVersion9=present" in stale_semantic_v9.stdout
        assert "function.payload_semantic_v10.FlumeHcommPayloadCopySemanticVersion10=missing" in stale_semantic_v9.stdout
        assert "function_so.payload_semantic_version_v10.FlumeHcommPayloadCopySemanticVersion10=missing" in stale_semantic_v9.stdout
        assert "reason=payload kernel package has a stale payload semantic marker" in stale_semantic_v9.stdout
        assert "current Flume semantic v10 write-path-capable payload kernel" in stale_semantic_v9.stdout

        stale_semantic_v10_json, stale_semantic_v10_tar = write_package(
            tmp, mode="stale_semantic_v10")
        stale_semantic_v10 = run_preflight(
            repo, stale_semantic_v10_json, stale_semantic_v10_tar)
        if stale_semantic_v10.returncode == 0:
            print(stale_semantic_v10.stdout)
            print(stale_semantic_v10.stderr, file=sys.stderr)
            raise AssertionError("stale semantic v10 package passed")
        assert "function.payload_semantic_v10.FlumeHcommPayloadCopySemanticVersion10=present" in stale_semantic_v10.stdout
        assert "function.payload_semantic_v11.FlumeHcommPayloadCopySemanticVersion11=missing" in stale_semantic_v10.stdout
        assert "function_so.payload_semantic_version_v11.FlumeHcommPayloadCopySemanticVersion11=missing" in stale_semantic_v10.stdout
        assert "reason=payload kernel package has a stale payload semantic marker" in stale_semantic_v10.stdout
        assert "current Flume semantic v11 data-probe-capable payload kernel" in stale_semantic_v10.stdout

        stale_schema_json, stale_schema_tar = write_package(
            tmp, mode="stale_v4_no_status_schema")
        stale_schema = run_preflight(repo, stale_schema_json, stale_schema_tar)
        if stale_schema.returncode == 0:
            print(stale_schema.stdout)
            print(stale_schema.stderr, file=sys.stderr)
            raise AssertionError("stale V4 package without status schema marker passed")
        assert "function.payload_status_schema.FlumeHcommPayloadStatusSchemaVersion=missing" in stale_schema.stdout
        assert "function.payload_status_word_count.FlumeHcommPayloadStatusWordCount=missing" in stale_schema.stdout
        assert "reason=payload kernel package is missing the payload status schema marker" in stale_schema.stdout

        stale_trace_json, stale_trace_tar = write_package(
            tmp, mode="stale_v8_no_trace_schema")
        stale_trace = run_preflight(repo, stale_trace_json, stale_trace_tar)
        if stale_trace.returncode == 0:
            print(stale_trace.stdout)
            print(stale_trace.stderr, file=sys.stderr)
            raise AssertionError(
                "stale V8 package without trace schema marker passed")
        assert "function.payload_trace_schema.FlumeHcommPayloadTraceSchemaVersion=missing" in stale_trace.stdout
        assert "function.payload_trace_word_count.FlumeHcommPayloadTraceWordCount=missing" in stale_trace.stdout
        assert "reason=payload kernel package is missing the payload trace schema marker" in stale_trace.stdout

        marker_only_json, marker_only_tar = write_package(
            tmp, mode="v4_marker_only")
        marker_only = run_preflight(repo, marker_only_json, marker_only_tar)
        if marker_only.returncode == 0:
            print(marker_only.stdout)
            print(marker_only.stderr, file=sys.stderr)
            raise AssertionError("marker-only V4 package passed")
        assert "function.payload_abi_v4.FlumeHcommPayloadCopyAbiVersion4=present" in marker_only.stdout
        assert "function.payload_semantic_v5.FlumeHcommPayloadCopySemanticVersion5=present" in marker_only.stdout
        assert "function.payload_semantic_v6.FlumeHcommPayloadCopySemanticVersion6=present" in marker_only.stdout
        assert "function.payload_semantic_v7.FlumeHcommPayloadCopySemanticVersion7=present" in marker_only.stdout
        assert "function.payload_semantic_v8.FlumeHcommPayloadCopySemanticVersion8=present" in marker_only.stdout
        assert "function.payload_semantic_v9.FlumeHcommPayloadCopySemanticVersion9=present" in marker_only.stdout
        assert "function.payload_semantic_v10.FlumeHcommPayloadCopySemanticVersion10=present" in marker_only.stdout
        assert "function.payload_semantic_v11.FlumeHcommPayloadCopySemanticVersion11=present" in marker_only.stdout
        assert "function.payload_semantic_v12.FlumeHcommPayloadCopySemanticVersion12=present" in marker_only.stdout
        assert "payload_primitive_deps=missing" in marker_only.stdout
        assert "function_so.payload_primitive_dep.HcommLocalCopyOnThread=missing" in marker_only.stdout
        assert "function_so.payload_primitive_dep.HcommReadOnThread=missing" in marker_only.stdout
        assert "function_so.payload_primitive_dep.HcommWriteOnThread=missing" in marker_only.stdout
        assert "payload_optional_write_with_notify=missing" in marker_only.stdout
        assert "reason=payload kernel package is missing HCOMM primitive dependencies" in marker_only.stdout

        v4_json, v4_tar = write_package(tmp, mode="v4")
        v4 = run_preflight(repo, v4_json, v4_tar)
        if v4.returncode != 0:
            print(v4.stdout)
            print(v4.stderr, file=sys.stderr)
            raise AssertionError("ABI v4 package did not pass")
        assert "function.payload_abi_v2.FlumeHcommPayloadCopyAbiVersion2=present" in v4.stdout
        assert "function_so.payload_abi_version_v2.FlumeHcommPayloadCopyAbiVersion2=present" in v4.stdout
        assert "function.payload_abi_v3.FlumeHcommPayloadCopyAbiVersion3=present" in v4.stdout
        assert "function_so.payload_abi_version_v3.FlumeHcommPayloadCopyAbiVersion3=present" in v4.stdout
        assert "function.payload_abi_v4.FlumeHcommPayloadCopyAbiVersion4=present" in v4.stdout
        assert "function_so.payload_abi_version_v4.FlumeHcommPayloadCopyAbiVersion4=present" in v4.stdout
        assert "function.payload_semantic.FlumeHcommPayloadCopySemanticVersion=present" in v4.stdout
        assert "function_so.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=present" in v4.stdout
        assert "function.payload_semantic_v5.FlumeHcommPayloadCopySemanticVersion5=present" in v4.stdout
        assert "function_so.payload_semantic_version_v5.FlumeHcommPayloadCopySemanticVersion5=present" in v4.stdout
        assert "function.payload_semantic_v6.FlumeHcommPayloadCopySemanticVersion6=present" in v4.stdout
        assert "function_so.payload_semantic_version_v6.FlumeHcommPayloadCopySemanticVersion6=present" in v4.stdout
        assert "function.payload_semantic_v7.FlumeHcommPayloadCopySemanticVersion7=present" in v4.stdout
        assert "function_so.payload_semantic_version_v7.FlumeHcommPayloadCopySemanticVersion7=present" in v4.stdout
        assert "function.payload_semantic_v8.FlumeHcommPayloadCopySemanticVersion8=present" in v4.stdout
        assert "function_so.payload_semantic_version_v8.FlumeHcommPayloadCopySemanticVersion8=present" in v4.stdout
        assert "function.payload_semantic_v9.FlumeHcommPayloadCopySemanticVersion9=present" in v4.stdout
        assert "function_so.payload_semantic_version_v9.FlumeHcommPayloadCopySemanticVersion9=present" in v4.stdout
        assert "function.payload_semantic_v10.FlumeHcommPayloadCopySemanticVersion10=present" in v4.stdout
        assert "function_so.payload_semantic_version_v10.FlumeHcommPayloadCopySemanticVersion10=present" in v4.stdout
        assert "function.payload_semantic_v11.FlumeHcommPayloadCopySemanticVersion11=present" in v4.stdout
        assert "function_so.payload_semantic_version_v11.FlumeHcommPayloadCopySemanticVersion11=present" in v4.stdout
        assert "function.payload_semantic_v12.FlumeHcommPayloadCopySemanticVersion12=present" in v4.stdout
        assert "function_so.payload_semantic_version_v12.FlumeHcommPayloadCopySemanticVersion12=present" in v4.stdout
        assert "function.payload_requires_comm_acquire.FlumeHcommPayloadCopyRequiresCommAcquire=present" in v4.stdout
        assert "function_so.payload_requires_comm_acquire.FlumeHcommPayloadCopyRequiresCommAcquire=present" in v4.stdout
        assert "function.payload_status_schema.FlumeHcommPayloadStatusSchemaVersion=present" in v4.stdout
        assert "function_so.payload_status_schema.FlumeHcommPayloadStatusSchemaVersion=present" in v4.stdout
        assert "function.payload_status_word_count.FlumeHcommPayloadStatusWordCount=present" in v4.stdout
        assert "function_so.payload_status_word_count.FlumeHcommPayloadStatusWordCount=present" in v4.stdout
        assert "function.payload_trace_schema.FlumeHcommPayloadTraceSchemaVersion=present" in v4.stdout
        assert "function_so.payload_trace_schema.FlumeHcommPayloadTraceSchemaVersion=present" in v4.stdout
        assert "function.payload_trace_word_count.FlumeHcommPayloadTraceWordCount=present" in v4.stdout
        assert "function_so.payload_trace_word_count.FlumeHcommPayloadTraceWordCount=present" in v4.stdout
        assert "function_value.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=12 expected=12 status=match" in v4.stdout
        assert "function_value.payload_status_schema.FlumeHcommPayloadStatusSchemaVersion=4 expected=4 status=match" in v4.stdout
        assert "function_value.payload_status_word_count.FlumeHcommPayloadStatusWordCount=14 expected=14 status=match" in v4.stdout
        assert "payload_metadata_values=match" in v4.stdout
        assert "function.build_mode_internal.FlumeHcommPayloadBuildModeInternalPayload=present" in v4.stdout
        assert "payload_primitive_deps=present" in v4.stdout
        assert "function_so.payload_primitive_dep.HcommLocalCopyOnThread=present" in v4.stdout
        assert "function_so.payload_primitive_dep.HcommReadOnThread=present" in v4.stdout
        assert "function_so.payload_primitive_dep.HcommWriteOnThread=present" in v4.stdout
        assert "function_so.payload_optional_primitive_dep.HcommWriteWithNotifyOnThread=present" in v4.stdout
        assert "payload_optional_write_with_notify=present" in v4.stdout
        assert flume_tool.PackageTextWriteWithNotifyReady(v4.stdout)
        assert "function_so.payload_primitive_dep.HcommChannelNotifyRecordOnThread=present" in v4.stdout
        assert "function_so.payload_primitive_dep.HcommChannelNotifyWaitOnThread=present" in v4.stdout
        assert "status=PASS" in v4.stdout

        nbi_write_notify_json, nbi_write_notify_tar = write_package(
            tmp, mode="v4_nbi_write_with_notify")
        nbi_write_notify = run_preflight(
            repo, nbi_write_notify_json, nbi_write_notify_tar)
        if nbi_write_notify.returncode != 0:
            print(nbi_write_notify.stdout)
            print(nbi_write_notify.stderr, file=sys.stderr)
            raise AssertionError(
                "ABI v4 package with NBI write-with-notify did not pass")
        assert "payload_primitive_deps=present" in nbi_write_notify.stdout
        assert "function_so.payload_optional_primitive_dep.HcommWriteWithNotifyOnThread=missing" in nbi_write_notify.stdout
        assert "function_so.payload_optional_primitive_dep.HcommWriteWithNotifyNbiOnThread=present" in nbi_write_notify.stdout
        assert "payload_optional_write_with_notify=present" in nbi_write_notify.stdout
        assert flume_tool.PackageTextWriteWithNotifyReady(
            nbi_write_notify.stdout)
        assert "status=PASS" in nbi_write_notify.stdout

        no_write_notify_json, no_write_notify_tar = write_package(
            tmp, mode="v4_no_write_with_notify")
        no_write_notify = run_preflight(
            repo, no_write_notify_json, no_write_notify_tar)
        if no_write_notify.returncode != 0:
            print(no_write_notify.stdout)
            print(no_write_notify.stderr, file=sys.stderr)
            raise AssertionError(
                "ABI v4 package without optional write-with-notify did not pass")
        assert "payload_primitive_deps=present" in no_write_notify.stdout
        assert "function_so.payload_primitive_dep.HcommLocalCopyOnThread=present" in no_write_notify.stdout
        assert "function_so.payload_primitive_dep.HcommReadOnThread=present" in no_write_notify.stdout
        assert "function_so.payload_primitive_dep.HcommWriteOnThread=present" in no_write_notify.stdout
        assert "function_so.payload_optional_primitive_dep.HcommWriteWithNotifyOnThread=missing" in no_write_notify.stdout
        assert "function_so.payload_optional_primitive_dep.HcommWriteWithNotifyNbiOnThread=missing" in no_write_notify.stdout
        assert "payload_optional_write_with_notify=missing" in no_write_notify.stdout
        assert not flume_tool.PackageTextWriteWithNotifyReady(
            no_write_notify.stdout)
        assert "status=PASS" in no_write_notify.stdout

        wrong_values_json, wrong_values_tar = write_package(
            tmp, mode="wrong_values")
        wrong_values = run_preflight(repo, wrong_values_json, wrong_values_tar)
        if wrong_values.returncode == 0:
            print(wrong_values.stdout)
            print(wrong_values.stderr, file=sys.stderr)
            raise AssertionError("package with wrong metadata values passed")
        assert "function.payload_semantic_v11.FlumeHcommPayloadCopySemanticVersion11=present" in wrong_values.stdout
        assert "function_so.payload_semantic_version_v11.FlumeHcommPayloadCopySemanticVersion11=present" in wrong_values.stdout
        assert "function.payload_semantic_v12.FlumeHcommPayloadCopySemanticVersion12=present" in wrong_values.stdout
        assert "function_so.payload_semantic_version_v12.FlumeHcommPayloadCopySemanticVersion12=present" in wrong_values.stdout
        assert "function_value.payload_semantic_version.FlumeHcommPayloadCopySemanticVersion=11 expected=12 status=mismatch" in wrong_values.stdout
        assert "function_value.payload_status_schema.FlumeHcommPayloadStatusSchemaVersion=3 expected=4 status=mismatch" in wrong_values.stdout
        assert "function_value.payload_status_word_count.FlumeHcommPayloadStatusWordCount=8 expected=14 status=mismatch" in wrong_values.stdout
        assert "payload_metadata_values=mismatch" in wrong_values.stdout
        assert "reason=payload kernel package metadata function returned unexpected value" in wrong_values.stdout

        installed_json = (
            tmp / "runtime" / "opp" / "vendors" / "flume" / "aicpu" /
            "config" / KERNEL_JSON
        )
        installed_tar = installed_json.parents[1] / "kernel" / AICPU_TAR
        installed_json.parent.mkdir(parents=True)
        installed_tar.parent.mkdir(parents=True)
        installed_json.write_text(v4_json.read_text(encoding="utf-8"),
                                  encoding="utf-8")
        installed_tar.write_bytes(v4_tar.read_bytes())
        ok, message = flume_tool.ValidateRuntimeCustomOpJson(
            SimpleNamespace(custom_op_json=str(installed_json),
                            custom_op_aicpu_tar=""))
        assert ok, message
        ok, message = flume_tool.ValidateRuntimeCustomOpJson(
            SimpleNamespace(custom_op_json=str(v4_json),
                            custom_op_aicpu_tar=str(v4_tar)))
        assert ok, message
        export_runner = flume_tool.Runner(tmp / "explicit-runtime-export-run")
        exported_args, export_result = flume_tool.MaybeExportExplicitCustomOpRuntime(
            export_runner,
            SimpleNamespace(custom_op_vendor="flume,cust",
                            custom_op_root="",
                            custom_op_json=str(v4_json),
                            custom_op_aicpu_tar=str(v4_tar)))
        assert export_result is not None
        assert export_result.returncode == 0
        assert exported_args.custom_op_json == ""
        assert exported_args.custom_op_aicpu_tar == ""
        exported_root = Path(exported_args.custom_op_root)
        exported_json = (exported_root / "opp" / "vendors" / "flume" /
                         "aicpu" / "config" / KERNEL_JSON)
        exported_tar = (exported_root / "opp" / "vendors" / "flume" /
                        "aicpu" / "kernel" / AICPU_TAR)
        assert exported_json.exists()
        assert exported_tar.exists()
        assert flume_tool.ValidateRuntimeCustomOpJson(
            SimpleNamespace(custom_op_json=str(exported_json),
                            custom_op_aicpu_tar=""))[0]
        found_json, found_tar = flume_tool.FindInstalledCustomOpRuntimeArtifacts(
            "flume", [str(tmp / "runtime")])
        assert found_json == installed_json
        assert found_tar == installed_tar
        next_steps = flume_tool.WriteCustomOpInstallNextSteps(
            tmp, "flume", found_json, found_tar)
        next_steps_text = next_steps.read_text(encoding="utf-8")
        assert str(installed_json) in next_steps_text
        assert "hcomm-payload-strict-positive" in next_steps_text
        build_steps = flume_tool.WritePayloadPackageBuildNextSteps(
            tmp, SimpleNamespace(hccl_source_root=str(repo / "refer" /
                                                      "cann-src" / "hccl")))
        build_steps_text = build_steps.read_text(encoding="utf-8")
        assert "hcomm-custom-op-direct-build" in build_steps_text
        assert "--auto-build-hcomm-payload-package" in build_steps_text
        assert "--custom-op-build-mode payload" in build_steps_text
        assert "--install-custom-op-package" in build_steps_text
        assert "hcomm-custom-op-build" in build_steps_text
        assert "--custom-op-export-root <temporary-custom-op-root>" in build_steps_text
        assert "hcomm-custom-op-export-runtime" in build_steps_text
        auto_cann = tmp / "auto-cann"
        auto_command = flume_tool.HcommPayloadAutoDirectBuildCommand(
            SimpleNamespace(build_dir=str(tmp / "strict-build"),
                            jobs=3,
                            custom_op_vendor="flume",
                            cann_package_root=str(auto_cann),
                            hcomm_primitives_include_root=str(
                                tmp / "hcomm-include"),
                            hcomm_primitives_lib_root=str(tmp / "hcomm-lib")),
            SimpleNamespace(run_dir=tmp / "strict-logs"),
            tmp / "strict-logs" / "hcomm-payload-auto-runtime")
        assert "hcomm-custom-op-direct-build" == auto_command[-1]
        assert "--custom-op-build-mode=payload" in auto_command
        assert f"--cann-package-root={auto_cann}" in auto_command
        assert f"--hcomm-primitives-include-root={tmp / 'hcomm-include'}" in auto_command
        assert f"--hcomm-primitives-lib-root={tmp / 'hcomm-lib'}" in auto_command
        assert any(item.startswith("--custom-op-export-root=")
                   for item in auto_command)
        assert any(item.startswith("--log-root=") for item in auto_command)

        class FakeAutoBuildRunner:
            def __init__(self, run_dir: Path):
                self.run_dir = run_dir
                self.calls: list[str] = []
                self.commands: list[list[str]] = []

            def run(self, name, command, required, timeout_seconds,
                    env_updates=None):
                del required, timeout_seconds, env_updates
                self.calls.append(name)
                self.commands.append(list(command))
                log_path = self.run_dir / f"{len(self.calls):02d}-{name}.log"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                if name == "hcomm-custom-op-package-preflight-autobuilt":
                    log_path.write_text("status=PASS\n", encoding="utf-8")
                else:
                    log_path.write_text("$ " + " ".join(command) + "\n",
                                        encoding="utf-8")
                return flume_tool.StepResult(name, list(command), 0, 0.0,
                                             log_path, True)

        auto_runner = FakeAutoBuildRunner(tmp / "auto-build-run")
        failed_package = flume_tool.StepResult(
            "hcomm-custom-op-package-preflight", [], 1, 0.0,
            auto_runner.run_dir / "00-package.log", False)
        auto_args, auto_package = flume_tool.MaybeAutoBuildPayloadPackage(
            auto_runner,
            SimpleNamespace(build_dir=str(tmp / "strict-build"),
                            jobs=3,
                            custom_op_vendor="flume",
                            cann_package_root=str(auto_cann),
                            custom_op_root="",
                            custom_op_json="",
                            custom_op_aicpu_tar="",
                            hcomm_primitives_include_root="",
                            hcomm_primitives_lib_root="",
                            auto_build_hcomm_payload_package=True,
                            hccl_smoke_timeout_sec=30,
                            step_timeout_sec=30),
            failed_package)
        assert auto_package.returncode == 0
        assert auto_runner.calls == [
            "hcomm-payload-auto-direct-build",
            "hcomm-custom-op-package-preflight-autobuilt",
        ]
        assert str(auto_runner.run_dir / "hcomm-payload-auto-runtime") == (
            auto_args.custom_op_root)
        auto_note = auto_runner.run_dir / "HCOMM_PAYLOAD_AUTO_PACKAGE.txt"
        auto_note_text = auto_note.read_text(encoding="utf-8")
        assert "Focused rerun command:" in auto_note_text
        assert "Full-matrix rerun command:" in auto_note_text
        assert "hcomm-payload-strict-positive" in auto_note_text
        assert "ascend-full-matrix" in auto_note_text
        assert "--auto-run-hcomm-payload-candidate-matrix" in auto_note_text
        assert any(
            f"--custom-op-root={auto_args.custom_op_root}" in command
            for command in auto_runner.commands)

        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "flume_tool.py",
                "--build-dir", str(tmp / "auto-strict-build"),
                "--hccl-devices", "0,1",
                "--run-hcomm-payload-smoke",
                "--hcomm-require-payload-copy",
                "--build-hcomm-custom-op",
                "--custom-op-root", auto_args.custom_op_root,
                "ascend-probe",
            ]
            auto_smoke_args = flume_tool.parse_args()
        finally:
            sys.argv = old_argv
        auto_specs = flume_tool.build_commands(
            auto_smoke_args, enable_hccl=True,
            run_dir=auto_runner.run_dir / "auto-smoke-run")
        auto_smoke = next(spec for spec in auto_specs
                          if spec.name == "hccl-collective-smoke")
        assert auto_smoke.env_updates["FLUME_HCOMM_CUSTOM_OP_ROOT"] == (
            auto_args.custom_op_root)
        assert "--hcomm-require-payload-copy" in auto_smoke.command

        inferred_tar_preflight = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--custom-op-json={installed_json}",
                "--require-hcomm-payload-kernel",
                "hcomm-custom-op-package",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if inferred_tar_preflight.returncode != 0:
            print(inferred_tar_preflight.stdout)
            print(inferred_tar_preflight.stderr, file=sys.stderr)
            raise AssertionError("installed JSON did not infer matching AICPU tar")
        assert f"aicpu_tar_path={installed_tar}" in inferred_tar_preflight.stdout
        assert "status=PASS" in inferred_tar_preflight.stdout

        export_root = tmp / "exported-runtime"
        export_runtime = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--custom-op-json={v4_json}",
                f"--custom-op-aicpu-tar={v4_tar}",
                f"--custom-op-export-root={export_root}",
                "hcomm-custom-op-export-runtime",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if export_runtime.returncode != 0:
            print(export_runtime.stdout)
            print(export_runtime.stderr, file=sys.stderr)
            raise AssertionError("runtime export did not pass")
        exported_json = (
            export_root / "opp" / "vendors" / "flume" / "aicpu" /
            "config" / KERNEL_JSON
        )
        exported_tar = (
            export_root / "opp" / "vendors" / "flume" / "aicpu" /
            "kernel" / AICPU_TAR
        )
        assert exported_json.exists()
        assert exported_tar.exists()
        assert "custom-op runtime export" in export_runtime.stdout
        exported_preflight = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--custom-op-root={export_root}",
                "--require-hcomm-payload-kernel",
                "hcomm-custom-op-package",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if exported_preflight.returncode != 0:
            print(exported_preflight.stdout)
            print(exported_preflight.stderr, file=sys.stderr)
            raise AssertionError("exported runtime package did not pass")
        assert f"json_path={exported_json}" in exported_preflight.stdout
        assert f"aicpu_tar_path={exported_tar}" in exported_preflight.stdout
        assert "status=PASS" in exported_preflight.stdout

        fake_cann = write_fake_cann_root(tmp)
        compat = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "collect_cann_compat.py"),
                f"--ascend-home={fake_cann}",
                f"--output-root={tmp / 'cann-compat'}",
                "--label=fake-hcomm",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if compat.returncode != 0:
            print(compat.stdout)
            print(compat.stderr, file=sys.stderr)
            raise AssertionError("CANN compat collection did not pass")
        fixture = tmp / "cann-compat" / "fake-hcomm"
        primitive_headers = (
            fixture / "hcomm-primitive-headers.txt").read_text(
                encoding="utf-8")
        assert "HcommReadOnThread" in primitive_headers
        assert "HcommWriteOnThread" in primitive_headers
        assert "HcommWriteWithNotifyOnThread" in primitive_headers
        assert "HcommWriteWithNotifyNbiOnThread" in primitive_headers
        assert "ThreadHandle" in primitive_headers
        primitive_symbols = (
            fixture / "hcomm-primitive-symbols.txt").read_text(
                encoding="utf-8")
        assert "HcommReadOnThread: present" in primitive_symbols
        assert "HcommWriteOnThread: present" in primitive_symbols
        assert "HcommWriteWithNotifyOnThread: present" in primitive_symbols
        assert "HcommWriteWithNotifyNbiOnThread: present" in primitive_symbols
        call_shape = (
            fixture / "hcomm-primitive-call-shape-probe.txt").read_text(
                encoding="utf-8")
        assert "status: PASS" in call_shape
        assert "## optional HcommWriteWithNotifyOnThread" in call_shape
        assert "## optional HcommWriteWithNotifyNbiOnThread" in call_shape

        cann_pair = flume_tool.ResolveCannRootPair(str(fake_cann))
        assert cann_pair == (fake_cann, fake_cann / "aarch64-linux")
        assert flume_tool.ResolveCannBinaryRoot(str(fake_cann)) == (
            fake_cann / "aarch64-linux")
        primitive_args = SimpleNamespace(
            hcomm_primitives_include_root="",
            hcomm_primitives_lib_root="")
        assert flume_tool.HcommWriteWithNotifySupported(
            primitive_args, fake_cann / "aarch64-linux")
        assert flume_tool.HcommWriteWithNotifyNbiSupported(
            primitive_args, fake_cann / "aarch64-linux")
        assert flume_tool.HcommAnyWriteWithNotifySupported(
            primitive_args, fake_cann / "aarch64-linux")
        runtime_env = flume_tool.CannRuntimeEnvUpdates(
            SimpleNamespace(cann_package_root=str(fake_cann)))
        assert runtime_env["ASCEND_HOME_PATH"] == str(fake_cann)
        assert str(fake_cann / "aarch64-linux" / "lib64") in (
            runtime_env["LD_LIBRARY_PATH"].split(":"))

        fake_cann_binary = fake_cann / "aarch64-linux"
        binary_runtime_env = flume_tool.CannRuntimeEnvUpdates(
            SimpleNamespace(cann_package_root=str(fake_cann_binary)))
        assert binary_runtime_env["ASCEND_HOME_PATH"] == str(fake_cann)

        direct_export_root = tmp / "direct-exported-runtime"
        direct_build = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--cann-package-root={fake_cann}",
                f"--build-dir={tmp / 'direct-build'}",
                f"--custom-op-export-root={direct_export_root}",
                "--custom-op-build-mode=payload",
                "hcomm-custom-op-direct-build",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if direct_build.returncode != 0:
            print(direct_build.stdout)
            print(direct_build.stderr, file=sys.stderr)
            raise AssertionError("direct custom-op build did not pass")
        direct_json = (
            direct_export_root / "opp" / "vendors" / "flume" / "aicpu" /
            "config" / KERNEL_JSON
        )
        direct_tar = (
            direct_export_root / "opp" / "vendors" / "flume" / "aicpu" /
            "kernel" / AICPU_TAR
        )
        assert direct_json.exists()
        assert direct_tar.exists()
        assert "hcomm-custom-op-direct-build-preflight" in direct_build.stdout
        assert "hcomm-custom-op-direct-build-exported-preflight" in direct_build.stdout
        direct_preflight = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--custom-op-root={direct_export_root}",
                "--require-hcomm-payload-kernel",
                "hcomm-custom-op-package",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if direct_preflight.returncode != 0:
            print(direct_preflight.stdout)
            print(direct_preflight.stderr, file=sys.stderr)
            raise AssertionError("direct-build runtime package did not pass")
        assert f"json_path={direct_json}" in direct_preflight.stdout
        assert f"aicpu_tar_path={direct_tar}" in direct_preflight.stdout
        assert "status=PASS" in direct_preflight.stdout

        fake_cann_canary = tmp / "fake-cann-canary"
        (fake_cann_canary / "aarch64-linux" / "include").mkdir(parents=True)
        (fake_cann_canary / "aarch64-linux" / "lib64").mkdir(parents=True)
        direct_canary = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--cann-package-root={fake_cann_canary}",
                f"--build-dir={tmp / 'direct-build-canary'}",
                "--custom-op-build-mode=canary",
                "hcomm-custom-op-direct-build",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if direct_canary.returncode != 0:
            print(direct_canary.stdout)
            print(direct_canary.stderr, file=sys.stderr)
            raise AssertionError(
                "canary direct custom-op build without HCOMM did not pass")
        assert "hcomm-custom-op-direct-build-preflight" in direct_canary.stdout

        fake_cann_hcomm_only = write_fake_cann_root(
            tmp, "fake-cann-hcomm-only", hccl_header=False,
            hcomm_header=True)
        direct_hcomm_only = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--cann-package-root={fake_cann_hcomm_only}",
                f"--build-dir={tmp / 'direct-build-hcomm-only'}",
                "--custom-op-build-mode=payload",
                "hcomm-custom-op-direct-build",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if direct_hcomm_only.returncode != 0:
            print(direct_hcomm_only.stdout)
            print(direct_hcomm_only.stderr, file=sys.stderr)
            raise AssertionError(
                "direct custom-op build with include/hcomm header did not pass")
        assert "hcomm-custom-op-direct-build-preflight" in direct_hcomm_only.stdout

        fake_cann_no_write_notify = write_fake_cann_root(
            tmp, "fake-cann-no-write-notify", hccl_header=False,
            hcomm_header=True, write_with_notify_lib=False,
            write_with_notify_nbi_lib=False)
        assert not flume_tool.HcommWriteWithNotifySupported(
            primitive_args, fake_cann_no_write_notify / "aarch64-linux")
        assert not flume_tool.HcommWriteWithNotifyNbiSupported(
            primitive_args, fake_cann_no_write_notify / "aarch64-linux")
        assert not flume_tool.HcommAnyWriteWithNotifySupported(
            primitive_args, fake_cann_no_write_notify / "aarch64-linux")
        no_write_notify_log_root = tmp / "no-write-notify-direct-build-logs"
        no_write_notify_build = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--cann-package-root={fake_cann_no_write_notify}",
                f"--build-dir={tmp / 'direct-build-no-write-notify'}",
                f"--log-root={no_write_notify_log_root}",
                "--custom-op-build-mode=payload",
                "hcomm-custom-op-direct-build",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if no_write_notify_build.returncode != 0:
            print(no_write_notify_build.stdout)
            print(no_write_notify_build.stderr, file=sys.stderr)
            raise AssertionError(
                "direct custom-op build without write-with-notify symbol did not pass")
        no_write_notify_artifacts = next(
            no_write_notify_log_root.glob(
                "flume-check-*/HCOMM_CUSTOM_OP_DIRECT_BUILD_ARTIFACTS.txt"))
        no_write_notify_artifact_text = (
            no_write_notify_artifacts.read_text(encoding="utf-8"))
        assert "hcomm_write_with_notify_enabled: 0" in no_write_notify_artifact_text
        assert "hcomm-custom-op-direct-build-preflight" in no_write_notify_build.stdout

        fake_cann_nbi_write_notify = write_fake_cann_root(
            tmp, "fake-cann-nbi-write-notify", hccl_header=False,
            hcomm_header=True, write_with_notify_lib=False,
            write_with_notify_nbi_lib=True)
        assert not flume_tool.HcommWriteWithNotifySupported(
            primitive_args, fake_cann_nbi_write_notify / "aarch64-linux")
        assert flume_tool.HcommWriteWithNotifyNbiSupported(
            primitive_args, fake_cann_nbi_write_notify / "aarch64-linux")
        assert flume_tool.HcommAnyWriteWithNotifySupported(
            primitive_args, fake_cann_nbi_write_notify / "aarch64-linux")
        nbi_write_notify_log_root = tmp / "nbi-write-notify-direct-build-logs"
        nbi_write_notify_build = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--cann-package-root={fake_cann_nbi_write_notify}",
                f"--build-dir={tmp / 'direct-build-nbi-write-notify'}",
                f"--log-root={nbi_write_notify_log_root}",
                "--custom-op-build-mode=payload",
                "hcomm-custom-op-direct-build",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if nbi_write_notify_build.returncode != 0:
            print(nbi_write_notify_build.stdout)
            print(nbi_write_notify_build.stderr, file=sys.stderr)
            raise AssertionError(
                "direct custom-op build with NBI write-with-notify did not pass")
        nbi_write_notify_artifacts = next(
            nbi_write_notify_log_root.glob(
                "flume-check-*/HCOMM_CUSTOM_OP_DIRECT_BUILD_ARTIFACTS.txt"))
        nbi_write_notify_artifact_text = (
            nbi_write_notify_artifacts.read_text(encoding="utf-8"))
        assert "hcomm_write_with_notify_enabled: 1" in nbi_write_notify_artifact_text
        assert "hcomm_write_with_notify_blocking_enabled: 0" in nbi_write_notify_artifact_text
        assert "hcomm_write_with_notify_nbi_enabled: 1" in nbi_write_notify_artifact_text
        nbi_write_notify_preflight = next(
            nbi_write_notify_log_root.glob(
                "flume-check-*/02-hcomm-custom-op-direct-build-preflight.log"))
        assert "payload_optional_write_with_notify=present" in (
            nbi_write_notify_preflight.read_text(encoding="utf-8"))
        assert "hcomm-custom-op-direct-build-preflight" in nbi_write_notify_build.stdout

        fake_cann_no_hcomm_header = write_fake_cann_root(
            tmp, "fake-cann-no-hcomm-header", hccl_header=False,
            hcomm_header=False)
        local_refer_log_root = tmp / "local-refer-direct-build-logs"
        local_refer_build = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--cann-package-root={fake_cann_no_hcomm_header}",
                f"--build-dir={tmp / 'direct-build-local-refer-hcomm'}",
                f"--log-root={local_refer_log_root}",
                "--custom-op-build-mode=payload",
                "hcomm-custom-op-direct-build",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if local_refer_build.returncode != 0:
            print(local_refer_build.stdout)
            print(local_refer_build.stderr, file=sys.stderr)
            raise AssertionError(
                "direct custom-op build with local-refer HCOMM header did not pass")
        local_refer_artifacts = next(
            local_refer_log_root.glob(
                "flume-check-*/HCOMM_CUSTOM_OP_DIRECT_BUILD_ARTIFACTS.txt"))
        local_refer_artifact_text = local_refer_artifacts.read_text(
            encoding="utf-8")
        assert "hcomm_primitives_header_source: local-refer" in local_refer_artifact_text
        assert "hcomm_write_with_notify_enabled: 1" in local_refer_artifact_text
        assert "hcomm-custom-op-direct-build-preflight" in local_refer_build.stdout

        fake_cann_minimal = tmp / "fake-cann-minimal"
        (fake_cann_minimal / "aarch64-linux" / "include").mkdir(parents=True)
        (fake_cann_minimal / "aarch64-linux" / "lib64").mkdir(parents=True)
        fake_hcomm_external = write_fake_cann_root(
            tmp, "fake-hcomm-external", hccl_header=False,
            hcomm_header=True)
        external_build = subprocess.run(
            [
                sys.executable,
                str(repo / "tools" / "flume_tool.py"),
                f"--cann-package-root={fake_cann_minimal}",
                "--hcomm-primitives-include-root="
                f"{fake_hcomm_external / 'aarch64-linux' / 'include'}",
                "--hcomm-primitives-lib-root="
                f"{fake_hcomm_external / 'aarch64-linux' / 'lib64'}",
                f"--build-dir={tmp / 'direct-build-external-hcomm'}",
                "--custom-op-build-mode=payload",
                "hcomm-custom-op-direct-build",
            ],
            cwd=repo,
            text=True,
            capture_output=True,
            check=False,
        )
        if external_build.returncode != 0:
            print(external_build.stdout)
            print(external_build.stderr, file=sys.stderr)
            raise AssertionError(
                "direct custom-op build with external HCOMM root did not pass")
        assert "hcomm-custom-op-direct-build-preflight" in external_build.stdout

        ok, message = flume_tool.ValidateRuntimeCustomOpJson(
            SimpleNamespace(custom_op_json=str(v4_json),
                            custom_op_aicpu_tar=""))
        assert not ok
        assert "aclrtBinaryLoadFromFile(JSON)" in message
        assert "--custom-op-aicpu-tar supplies" in message

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
