#include "flume_hcomm_notify_only_abi.h"

#if __has_include(<hccl/hcomm_primitives.h>)
#include <hccl/hcomm_primitives.h>
#else
#include <hcomm_primitives.h>
#endif

#ifndef FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY
#define FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY 0
#endif
#ifndef FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI
#define FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI 0
#endif

namespace {

constexpr unsigned int kFlumePayloadSuccess =
    FLUME_HCOMM_PAYLOAD_STATUS_SUCCESS;
constexpr unsigned int kFlumePayloadInvalidArgument =
    FLUME_HCOMM_PAYLOAD_STATUS_INVALID_ARGUMENT;
constexpr unsigned int kFlumePayloadHcommError =
    FLUME_HCOMM_PAYLOAD_STATUS_HCOMM_ERROR;

bool HasPayloadDescHeader(const flume_hcomm_payload_copy_desc_v1& desc) {
  return desc.magic == FLUME_HCOMM_PAYLOAD_COPY_MAGIC &&
         desc.version == FLUME_HCOMM_PAYLOAD_COPY_VERSION &&
         desc.size == sizeof(flume_hcomm_payload_copy_desc_v1);
}

bool HasCommName(const flume_hcomm_payload_copy_desc_v1& desc) {
  for (unsigned int i = 0; i < FLUME_HCOMM_PAYLOAD_COMM_NAME_BYTES; ++i) {
    if (desc.comm_name[i] == '\0') {
      return i != 0;
    }
  }
  return false;
}

bool HasPayloadBatchTag(const flume_hcomm_payload_copy_desc_v1& desc) {
  for (unsigned int i = 0; i < FLUME_HCOMM_PAYLOAD_BATCH_TAG_BYTES; ++i) {
    if (desc.batch_tag[i] == '\0') {
      return i != 0;
    }
  }
  return false;
}

void StorePayloadStatus(const flume_hcomm_payload_copy_desc_v1& desc,
                        unsigned int status) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0 ||
      desc.status_word_count < 1U) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[0] = status;
}

void StorePayloadPrimitiveRet(const flume_hcomm_payload_copy_desc_v1& desc,
                              int32_t ret) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0 ||
      desc.status_word_count < 2U) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[1] = static_cast<unsigned int>(ret);
}

void BeginPayloadPrimitive(const flume_hcomm_payload_copy_desc_v1& desc,
                           unsigned int pending_status) {
  StorePayloadStatus(desc, pending_status);
  StorePayloadPrimitiveRet(desc, -1);
}

void StorePayloadEcho(const flume_hcomm_payload_copy_desc_v1& desc) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0 ||
      desc.status_word_count < FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[2] = desc.role;
  status_words[3] = desc.peer_rank;
  status_words[4] = static_cast<unsigned int>(desc.bytes & 0xFFFFFFFFU);
  status_words[5] = static_cast<unsigned int>(desc.bytes >> 32U);
  status_words[6] = desc.local_rank;
  status_words[7] = desc.completion_mode;
  const uint64_t fingerprint =
      flume_hcomm_payload_copy_desc_fingerprint(&desc);
  status_words[8] = static_cast<unsigned int>(fingerprint & 0xFFFFFFFFU);
  status_words[9] = static_cast<unsigned int>(fingerprint >> 32U);
}

unsigned int PayloadDataFingerprint(const void* ptr, uint64_t bytes) {
  if (ptr == nullptr || bytes == 0) {
    return 0U;
  }
  constexpr uint64_t kMaxSampleBytes = 4096U;
  const auto* data = static_cast<const uint8_t*>(ptr);
  uint32_t hash = 2166136261U;
  auto mix = [&hash](uint8_t value) {
    hash ^= value;
    hash *= 16777619U;
  };
  auto mix_u64 = [&mix](uint64_t value) {
    for (unsigned int i = 0; i < 8U; ++i) {
      mix(static_cast<uint8_t>((value >> (i * 8U)) & 0xFFU));
    }
  };
  const uint64_t first = bytes < kMaxSampleBytes ? bytes : kMaxSampleBytes;
  mix_u64(bytes);
  mix_u64(first);
  for (uint64_t i = 0; i < first; ++i) {
    mix(data[i]);
  }
  if (bytes > first) {
    const uint64_t last = first;
    mix_u64(last);
    const uint64_t start = bytes - last;
    for (uint64_t i = 0; i < last; ++i) {
      mix(data[start + i]);
    }
  }
  return hash;
}

void StorePayloadDataProbe(const flume_hcomm_payload_copy_desc_v1& desc,
                           unsigned int word_index,
                           const void* ptr) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0 ||
      desc.status_word_count < FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT ||
      word_index >= FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT) {
    return;
  }
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  status_words[word_index] = PayloadDataFingerprint(ptr, desc.bytes);
}

void StorePayloadDataProbeSampleBytes(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  if (!HasPayloadDescHeader(desc) || desc.status_word == 0 ||
      desc.status_word_count < FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT) {
    return;
  }
  constexpr uint64_t kMaxSampleBytes = 4096U;
  auto* status_words = reinterpret_cast<unsigned int*>(desc.status_word);
  const uint64_t first = desc.bytes < kMaxSampleBytes ?
      desc.bytes : kMaxSampleBytes;
  const uint64_t sampled = desc.bytes > first ? first * 2U : first;
  status_words[14] = static_cast<unsigned int>(sampled & 0xFFFFFFFFU);
}

unsigned int* PayloadTraceWords(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  if (!HasPayloadDescHeader(desc) || desc.reserved2[2] == 0) {
    return nullptr;
  }
  return reinterpret_cast<unsigned int*>(desc.reserved2[2]);
}

void InitPayloadTrace(const flume_hcomm_payload_copy_desc_v1& desc) {
  unsigned int* trace_words = PayloadTraceWords(desc);
  if (trace_words == nullptr) {
    return;
  }
  for (unsigned int i = 0; i < FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT; ++i) {
    trace_words[i] = 0xFFFFFFFFU;
  }
  trace_words[0] = FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION;
  trace_words[1] = FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT;
  trace_words[2] = FLUME_HCOMM_PAYLOAD_TRACE_EVENT_NONE;
  trace_words[3] = 0xFFFFFFFFU;
  trace_words[4] = 0U;
  trace_words[5] = desc.role;
  trace_words[6] = desc.local_rank;
  trace_words[7] = desc.peer_rank;
  trace_words[8] = static_cast<unsigned int>(desc.bytes & 0xFFFFFFFFU);
  trace_words[9] = static_cast<unsigned int>(desc.bytes >> 32U);
  trace_words[10] = static_cast<unsigned int>(desc.reserved2[0]);
  trace_words[11] = static_cast<unsigned int>(desc.reserved2[1]);
  trace_words[12] = desc.completion_mode;
  trace_words[13] = desc.ready_notify_idx;
  trace_words[14] = desc.done_notify_idx;
  trace_words[15] = 0xFFFFFFFFU;
}

void TracePayloadEvent(const flume_hcomm_payload_copy_desc_v1& desc,
                       unsigned int event,
                       int32_t ret) {
  unsigned int* trace_words = PayloadTraceWords(desc);
  if (trace_words == nullptr) {
    return;
  }
  if (trace_words[0] != FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION ||
      trace_words[1] != FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT) {
    InitPayloadTrace(desc);
  }
  const unsigned int event_count = trace_words[4];
  const unsigned int slot = event_count % FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY;
  const unsigned int event_base = FLUME_HCOMM_PAYLOAD_TRACE_HEADER_WORD_COUNT;
  const unsigned int ret_base =
      FLUME_HCOMM_PAYLOAD_TRACE_HEADER_WORD_COUNT +
      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_CAPACITY;
  trace_words[event_base + slot] = event;
  trace_words[ret_base + slot] = static_cast<unsigned int>(ret);
  trace_words[2] = event;
  trace_words[3] = static_cast<unsigned int>(ret);
  trace_words[4] = event_count + 1U;
}

void StorePayloadTraceResult(const flume_hcomm_payload_copy_desc_v1& desc,
                             unsigned int result) {
  unsigned int* trace_words = PayloadTraceWords(desc);
  if (trace_words == nullptr) {
    return;
  }
  trace_words[15] = result;
}

unsigned int FinishPayloadTrace(const flume_hcomm_payload_copy_desc_v1& desc,
                                unsigned int result) {
  StorePayloadTraceResult(desc, result);
  TracePayloadEvent(desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_EXIT, 0);
  return result;
}

bool PayloadBatchModeEnabled(const flume_hcomm_payload_copy_desc_v1& desc) {
  return desc.reserved2[0] != FLUME_HCOMM_PAYLOAD_BATCH_MODE_DISABLED;
}

bool PayloadRecvDirectOutputEnabled(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  return desc.reserved2[1] == FLUME_HCOMM_PAYLOAD_RECV_PATH_DIRECT_OUTPUT;
}

bool PayloadWritePathEnabled(const flume_hcomm_payload_copy_desc_v1& desc) {
  return (desc.completion_mode &
          FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_PATH) != 0;
}

bool PayloadWriteWithNotifyEnabled(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  return (desc.completion_mode &
          FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_WRITE_WITH_NOTIFY) != 0;
}

unsigned int PayloadCompletionMode(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  return desc.completion_mode & FLUME_HCOMM_PAYLOAD_COMPLETION_MODE_MASK;
}

bool PayloadSkipCommAcquire(const flume_hcomm_payload_copy_desc_v1& desc) {
  return (desc.completion_mode &
          (FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_SKIP_COMM_ACQUIRE |
           FLUME_HCOMM_PAYLOAD_COMPLETION_FLAG_CHANNEL_HANDLE_BINDING)) != 0;
}

bool CanRecordPayloadCompletionNotify(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  return HasPayloadDescHeader(desc) &&
         desc.thread_notify_mode ==
             FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU &&
         desc.aicpu_thread != 0 && desc.cpu_thread_on_aicpu != 0;
}

void BestEffortPayloadCompletionNotify(
    const flume_hcomm_payload_copy_desc_v1& desc) {
  if (!CanRecordPayloadCompletionNotify(desc)) {
    return;
  }
  (void)HcommThreadNotifyRecordOnThread(
      static_cast<ThreadHandle>(desc.aicpu_thread),
      static_cast<ThreadHandle>(desc.cpu_thread_on_aicpu), 0);
}

bool ValidatePayloadDesc(const flume_hcomm_payload_copy_desc_v1& desc) {
  return HasPayloadDescHeader(desc) && desc.rank_size == 2 &&
         (desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND ||
          desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV) &&
         desc.local_rank < desc.rank_size &&
         desc.peer_rank < desc.rank_size && desc.local_rank != desc.peer_rank &&
         desc.ready_notify_idx != desc.done_notify_idx && desc.bytes != 0 &&
         (desc.thread_notify_mode == FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_NONE ||
          desc.thread_notify_mode ==
              FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU) &&
         (PayloadCompletionMode(desc) ==
              FLUME_HCOMM_PAYLOAD_COMPLETION_ORDERED_NOTIFY ||
          PayloadCompletionMode(desc) ==
              FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN) &&
         desc.aicpu_thread != 0 && desc.channel_handle != 0 &&
         desc.user_buffer != 0 && desc.local_hccl_buffer != 0 &&
         desc.remote_hccl_buffer != 0 &&
         desc.bytes <= desc.local_hccl_buffer_bytes &&
         desc.bytes <= desc.remote_hccl_buffer_bytes &&
         desc.status_word != 0 &&
         desc.status_word_count >= FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT &&
         desc.status_schema_version ==
             FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION &&
         (PayloadSkipCommAcquire(desc) || HasCommName(desc)) &&
         (!PayloadBatchModeEnabled(desc) || HasPayloadBatchTag(desc)) &&
         (desc.thread_notify_mode !=
              FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU ||
          desc.cpu_thread_on_aicpu != 0);
}

unsigned int RunPayloadCopyBody(const flume_hcomm_payload_copy_desc_v1& desc) {
  ThreadHandle thread = static_cast<ThreadHandle>(desc.aicpu_thread);
  ChannelHandle channel = static_cast<ChannelHandle>(desc.channel_handle);
  void* user_buffer = reinterpret_cast<void*>(desc.user_buffer);
  void* local_hccl_buffer = reinterpret_cast<void*>(desc.local_hccl_buffer);
  void* remote_hccl_buffer = reinterpret_cast<void*>(desc.remote_hccl_buffer);

  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_SEND) {
    BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_ENTER,
                      -1);
    int32_t ret =
        HcommLocalCopyOnThread(thread, local_hccl_buffer, user_buffer,
                               desc.bytes);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_LOCAL_COPY_DONE,
                      ret);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_LOCAL_COPY_FAILED;
    }
    const bool write_with_notify = PayloadWriteWithNotifyEnabled(desc);
    if (PayloadWritePathEnabled(desc)) {
      BeginPayloadPrimitive(desc,
                            FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_WRITE_FAILED);
      if (write_with_notify) {
#if FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY
        TracePayloadEvent(
            desc,
            FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_ENTER,
            -1);
        ret = HcommWriteWithNotifyOnThread(
            thread, channel, remote_hccl_buffer, local_hccl_buffer,
            desc.bytes, desc.ready_notify_idx);
        TracePayloadEvent(
            desc,
            FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_DONE,
            ret);
#elif FLUME_HAVE_HCOMM_WRITE_WITH_NOTIFY_NBI
        TracePayloadEvent(
            desc,
            FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_ENTER,
            -1);
        ret = HcommWriteWithNotifyNbiOnThread(
            thread, channel, remote_hccl_buffer, local_hccl_buffer,
            desc.bytes, desc.ready_notify_idx);
        TracePayloadEvent(
            desc,
            FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_NBI_DONE,
            ret);
#else
        TracePayloadEvent(
            desc,
            FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_ENTER,
            -1);
        ret = -1;
        TracePayloadEvent(
            desc,
            FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_NOTIFY_DONE,
            ret);
#endif
      } else {
        TracePayloadEvent(
            desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_ENTER, -1);
        ret = HcommWriteOnThread(thread, channel, remote_hccl_buffer,
                                 local_hccl_buffer, desc.bytes);
        TracePayloadEvent(
            desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_REMOTE_WRITE_DONE, ret);
      }
      if (ret != 0) {
        StorePayloadPrimitiveRet(desc, ret);
        return FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_WRITE_FAILED;
      }
      if (PayloadCompletionMode(desc) ==
          FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN) {
        BeginPayloadPrimitive(
            desc, FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED);
        TracePayloadEvent(
            desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_ENTER,
            -1);
        ret = HcommChannelFenceOnThread(thread, channel);
        TracePayloadEvent(
            desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_CHANNEL_FENCE_DONE,
            ret);
        if (ret != 0) {
          StorePayloadPrimitiveRet(desc, ret);
          return FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED;
        }
      }
    }
    if (!write_with_notify) {
      BeginPayloadPrimitive(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED);
      TracePayloadEvent(desc,
                        FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_ENTER,
                        -1);
      ret = HcommChannelNotifyRecordOnThread(
          thread, channel, desc.ready_notify_idx);
      TracePayloadEvent(desc,
                        FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_READY_RECORD_DONE,
                        ret);
      if (ret != 0) {
        StorePayloadPrimitiveRet(desc, ret);
        return FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_RECORD_FAILED;
      }
    }
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_ENTER,
                      -1);
    ret = HcommChannelNotifyWaitOnThread(
        thread, channel, desc.done_notify_idx, desc.timeout_sec);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_SEND_DONE_WAIT_DONE,
                      ret);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_WAIT_FAILED;
    }
    return kFlumePayloadSuccess;
  }

  if (desc.role == FLUME_HCOMM_NOTIFY_ROLE_RECV) {
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_ENTER,
                      -1);
    int32_t ret = HcommChannelNotifyWaitOnThread(
        thread, channel, desc.ready_notify_idx, desc.timeout_sec);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_READY_WAIT_DONE,
                      ret);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_READY_NOTIFY_WAIT_FAILED;
    }
    if (!PayloadWritePathEnabled(desc)) {
      BeginPayloadPrimitive(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED);
      void* read_target =
          PayloadRecvDirectOutputEnabled(desc) ? user_buffer : local_hccl_buffer;
      TracePayloadEvent(desc,
                        FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_ENTER,
                        -1);
      ret = HcommReadOnThread(thread, channel, read_target,
                              remote_hccl_buffer, desc.bytes);
      TracePayloadEvent(desc,
                        FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_REMOTE_READ_DONE,
                        ret);
      if (ret != 0) {
        StorePayloadPrimitiveRet(desc, ret);
        return FLUME_HCOMM_PAYLOAD_STATUS_REMOTE_READ_FAILED;
      }
      if (PayloadCompletionMode(desc) ==
          FLUME_HCOMM_PAYLOAD_COMPLETION_CHANNEL_DRAIN) {
        BeginPayloadPrimitive(
            desc, FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED);
        TracePayloadEvent(
            desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_ENTER,
            -1);
        ret = HcommChannelFenceOnThread(thread, channel);
        TracePayloadEvent(
            desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_CHANNEL_FENCE_DONE,
            ret);
        if (ret != 0) {
          StorePayloadPrimitiveRet(desc, ret);
          return FLUME_HCOMM_PAYLOAD_STATUS_CHANNEL_DRAIN_FAILED;
        }
      }
    }
    if (PayloadWritePathEnabled(desc) || !PayloadRecvDirectOutputEnabled(desc)) {
      BeginPayloadPrimitive(desc,
                            FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED);
      TracePayloadEvent(
          desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_ENTER, -1);
      ret = HcommLocalCopyOnThread(thread, user_buffer, local_hccl_buffer,
                                   desc.bytes);
      TracePayloadEvent(
          desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_OUTPUT_COPY_DONE, ret);
      if (ret != 0) {
        StorePayloadPrimitiveRet(desc, ret);
        return FLUME_HCOMM_PAYLOAD_STATUS_OUTPUT_COPY_FAILED;
      }
    }
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_ENTER,
                      -1);
    ret = HcommChannelNotifyRecordOnThread(
        thread, channel, desc.done_notify_idx);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_RECV_DONE_RECORD_DONE,
                      ret);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      return FLUME_HCOMM_PAYLOAD_STATUS_DONE_NOTIFY_RECORD_FAILED;
    }
    return kFlumePayloadSuccess;
  }

  return kFlumePayloadInvalidArgument;
}

unsigned int RunPayloadCopy(const flume_hcomm_payload_copy_desc_v1& desc) {
  InitPayloadTrace(desc);
  TracePayloadEvent(desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_KERNEL_ENTER, -1);
  if (!ValidatePayloadDesc(desc)) {
    StorePayloadStatus(desc, kFlumePayloadInvalidArgument);
    BestEffortPayloadCompletionNotify(desc);
    return FinishPayloadTrace(desc, kFlumePayloadInvalidArgument);
  }
  StorePayloadStatus(desc, kFlumePayloadHcommError);
  StorePayloadEcho(desc);
  StorePayloadDataProbe(desc, 10U,
                        reinterpret_cast<const void*>(desc.user_buffer));
  StorePayloadDataProbe(desc, 11U,
                        reinterpret_cast<const void*>(
                            desc.local_hccl_buffer));
  StorePayloadDataProbeSampleBytes(desc);

  ThreadHandle thread = static_cast<ThreadHandle>(desc.aicpu_thread);
  const bool use_thread_notify =
      desc.thread_notify_mode ==
      FLUME_HCOMM_PAYLOAD_THREAD_NOTIFY_HOST_AICPU;
  int32_t ret = 0;
  const bool skip_comm_acquire = PayloadSkipCommAcquire(desc);
  if (!skip_comm_acquire) {
    BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_ENTER, -1);
    ret = HcommAcquireComm(desc.comm_name);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_ACQUIRE_DONE, ret);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      StorePayloadStatus(desc,
                         FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
      BestEffortPayloadCompletionNotify(desc);
      return FinishPayloadTrace(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_COMM_ACQUIRE_FAILED);
    }
  }

  unsigned int result = kFlumePayloadSuccess;
  bool batch_started = false;

  if (result == kFlumePayloadSuccess && PayloadBatchModeEnabled(desc)) {
    BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_ENTER, -1);
    ret = HcommBatchModeStart(desc.batch_tag);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_START_DONE, ret);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      result = FLUME_HCOMM_PAYLOAD_STATUS_BATCH_START_FAILED;
    } else {
      batch_started = true;
    }
  }

  if (result == kFlumePayloadSuccess && use_thread_notify) {
    BeginPayloadPrimitive(
        desc, FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED);
    TracePayloadEvent(
        desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_ENTER, -1);
    ret = HcommThreadNotifyWaitOnThread(thread, 0, desc.timeout_sec);
    TracePayloadEvent(
        desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_WAIT_DONE, ret);
    if (ret != 0) {
      StorePayloadPrimitiveRet(desc, ret);
      result = FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_WAIT_FAILED;
    }
  }

  if (result == kFlumePayloadSuccess) {
    result = RunPayloadCopyBody(desc);
  }
  StorePayloadDataProbe(desc, 12U,
                        reinterpret_cast<const void*>(desc.local_hccl_buffer));
  StorePayloadDataProbe(desc, 13U,
                        reinterpret_cast<const void*>(desc.user_buffer));
  if (result != kFlumePayloadSuccess) {
    StorePayloadStatus(desc, result);
  }
  if (use_thread_notify) {
    if (result == kFlumePayloadSuccess) {
      BeginPayloadPrimitive(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED);
    }
    TracePayloadEvent(
        desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_ENTER, -1);
    ret = HcommThreadNotifyRecordOnThread(
        thread, static_cast<ThreadHandle>(desc.cpu_thread_on_aicpu), 0);
    TracePayloadEvent(
        desc, FLUME_HCOMM_PAYLOAD_TRACE_EVENT_THREAD_NOTIFY_RECORD_DONE, ret);
    if (ret != 0 && result == kFlumePayloadSuccess) {
      StorePayloadPrimitiveRet(desc, ret);
      StorePayloadStatus(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED);
      result = FLUME_HCOMM_PAYLOAD_STATUS_THREAD_NOTIFY_RECORD_FAILED;
    }
  }
  if (batch_started) {
    if (result == kFlumePayloadSuccess) {
      BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED);
    }
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_ENTER, -1);
    ret = HcommBatchModeEnd(desc.batch_tag);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_BATCH_END_DONE, ret);
    if (ret != 0 && result == kFlumePayloadSuccess) {
      StorePayloadPrimitiveRet(desc, ret);
      result = FLUME_HCOMM_PAYLOAD_STATUS_BATCH_END_FAILED;
    }
  }
  if (result == kFlumePayloadSuccess) {
    StorePayloadStatus(desc, kFlumePayloadSuccess);
  } else {
    StorePayloadStatus(desc, result);
  }
  if (result == kFlumePayloadSuccess && !skip_comm_acquire) {
    StorePayloadPrimitiveRet(desc, 0);
    BeginPayloadPrimitive(desc, FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED);
  }
  if (!skip_comm_acquire) {
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_ENTER, -1);
    ret = HcommReleaseComm(desc.comm_name);
    TracePayloadEvent(desc,
                      FLUME_HCOMM_PAYLOAD_TRACE_EVENT_COMM_RELEASE_DONE, ret);
    if (ret != 0 && result == kFlumePayloadSuccess) {
      StorePayloadPrimitiveRet(desc, ret);
      StorePayloadStatus(desc,
                         FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED);
      return FinishPayloadTrace(
          desc, FLUME_HCOMM_PAYLOAD_STATUS_COMM_RELEASE_FAILED);
    }
  }
  if (result == kFlumePayloadSuccess) {
    StorePayloadStatus(desc, kFlumePayloadSuccess);
    StorePayloadPrimitiveRet(desc, 0);
  }
  return FinishPayloadTrace(desc, result);
}

}  // namespace

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV3(void* param) {
  if (param == nullptr) {
    return kFlumePayloadInvalidArgument;
  }
  auto* desc = static_cast<flume_hcomm_payload_copy_desc_v1*>(param);
  return RunPayloadCopy(*desc);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV4(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV3(param);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernelV2(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV4(param);
}

extern "C" unsigned int FlumeHcommPayloadCopyDirectAclrtKernel(void* param) {
  return FlumeHcommPayloadCopyDirectAclrtKernelV4(param);
}

extern "C" unsigned int FlumeHcommPayloadBuildModeInternalPayload() {
  return 1U;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion2() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION >= 2U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion3() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION >= 3U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopyAbiVersion4() {
  return FLUME_HCOMM_PAYLOAD_COPY_VERSION == 4U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion5() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 5U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion6() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 6U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion7() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 7U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion8() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 8U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion9() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 9U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion10() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 10U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion11() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 11U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion12() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION >= 12U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopySemanticVersion13() {
  return FLUME_HCOMM_PAYLOAD_COPY_SEMANTIC_VERSION == 13U ? 1U : 0U;
}

extern "C" unsigned int FlumeHcommPayloadCopyRequiresCommAcquire() {
  return 1U;
}

extern "C" unsigned int FlumeHcommPayloadStatusSchemaVersion() {
  return FLUME_HCOMM_PAYLOAD_STATUS_SCHEMA_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadStatusWordCount() {
  return FLUME_HCOMM_PAYLOAD_STATUS_WORD_COUNT;
}

extern "C" unsigned int FlumeHcommPayloadTraceSchemaVersion() {
  return FLUME_HCOMM_PAYLOAD_TRACE_SCHEMA_VERSION;
}

extern "C" unsigned int FlumeHcommPayloadTraceWordCount() {
  return FLUME_HCOMM_PAYLOAD_TRACE_WORD_COUNT;
}
