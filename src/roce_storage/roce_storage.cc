#include "roce_storage/roce_storage.h"

#include <cstring>

#ifndef FLUME_HAVE_IBVERBS
#define FLUME_HAVE_IBVERBS 0
#endif

namespace flume::roce {
namespace {

void AppendU16(uint16_t value, std::vector<uint8_t>* out) {
  out->push_back(static_cast<uint8_t>(value & 0xffU));
  out->push_back(static_cast<uint8_t>((value >> 8U) & 0xffU));
}

void AppendU32(uint32_t value, std::vector<uint8_t>* out) {
  for (unsigned shift = 0; shift != 32; shift += 8) {
    out->push_back(static_cast<uint8_t>((value >> shift) & 0xffU));
  }
}

void AppendU64(uint64_t value, std::vector<uint8_t>* out) {
  for (unsigned shift = 0; shift != 64; shift += 8) {
    out->push_back(static_cast<uint8_t>((value >> shift) & 0xffU));
  }
}

bool ReadU16(const uint8_t* wire, size_t len, size_t* offset, uint16_t* out) {
  if (*offset > len || len - *offset < 2) return false;
  *out = static_cast<uint16_t>(wire[*offset]) |
         (static_cast<uint16_t>(wire[*offset + 1]) << 8U);
  *offset += 2;
  return true;
}

bool ReadU32(const uint8_t* wire, size_t len, size_t* offset, uint32_t* out) {
  if (*offset > len || len - *offset < 4) return false;
  *out = 0;
  for (unsigned shift = 0; shift != 32; shift += 8) {
    *out |= static_cast<uint32_t>(wire[*offset + shift / 8]) << shift;
  }
  *offset += 4;
  return true;
}

bool ReadU64(const uint8_t* wire, size_t len, size_t* offset, uint64_t* out) {
  if (*offset > len || len - *offset < 8) return false;
  *out = 0;
  for (unsigned shift = 0; shift != 64; shift += 8) {
    *out |= static_cast<uint64_t>(wire[*offset + shift / 8]) << shift;
  }
  *offset += 8;
  return true;
}

}  // namespace

bool EncodeCommand(const Command& command, std::vector<uint8_t>* wire) {
  if (wire == nullptr || command.request_id == 0 || command.length == 0 ||
      (command.operation != Operation::kRead && command.operation != Operation::kWrite)) {
    return false;
  }
  wire->clear();
  wire->reserve(kCommandWireBytes);
  AppendU32(kProtocolMagic, wire);
  AppendU16(kProtocolVersion, wire);
  AppendU16(static_cast<uint16_t>(command.operation), wire);
  AppendU64(command.request_id, wire);
  AppendU64(command.object_id, wire);
  AppendU64(command.storage_offset, wire);
  AppendU64(command.length, wire);
  AppendU64(command.npu_address, wire);
  AppendU32(command.npu_rkey, wire);
  AppendU32(0, wire);
  return true;
}

bool DecodeCommand(const uint8_t* wire, size_t len, Command* command) {
  if (wire == nullptr || command == nullptr || len != kCommandWireBytes) return false;
  size_t offset = 0;
  uint32_t magic = 0;
  uint16_t version = 0;
  uint16_t operation = 0;
  uint32_t reserved = 0;
  if (!ReadU32(wire, len, &offset, &magic) || !ReadU16(wire, len, &offset, &version) ||
      !ReadU16(wire, len, &offset, &operation) ||
      !ReadU64(wire, len, &offset, &command->request_id) ||
      !ReadU64(wire, len, &offset, &command->object_id) ||
      !ReadU64(wire, len, &offset, &command->storage_offset) ||
      !ReadU64(wire, len, &offset, &command->length) ||
      !ReadU64(wire, len, &offset, &command->npu_address) ||
      !ReadU32(wire, len, &offset, &command->npu_rkey) ||
      !ReadU32(wire, len, &offset, &reserved)) {
    return false;
  }
  command->operation = static_cast<Operation>(operation);
  return magic == kProtocolMagic && version == kProtocolVersion && reserved == 0 &&
         command->request_id != 0 && command->length != 0 &&
         (command->operation == Operation::kRead || command->operation == Operation::kWrite);
}

bool EncodeCompletion(const Completion& completion, std::vector<uint8_t>* wire) {
  if (wire == nullptr || completion.request_id == 0) return false;
  wire->clear();
  wire->reserve(kCompletionWireBytes);
  AppendU64(completion.request_id, wire);
  AppendU32(completion.status, wire);
  AppendU64(completion.bytes, wire);
  AppendU32(completion.checksum, wire);
  return true;
}

bool DecodeCompletion(const uint8_t* wire, size_t len, Completion* completion) {
  if (wire == nullptr || completion == nullptr || len != kCompletionWireBytes) return false;
  size_t offset = 0;
  return ReadU64(wire, len, &offset, &completion->request_id) &&
         ReadU32(wire, len, &offset, &completion->status) &&
         ReadU64(wire, len, &offset, &completion->bytes) &&
         ReadU32(wire, len, &offset, &completion->checksum) && completion->request_id != 0;
}

bool EncodeEndpoint(const Endpoint& endpoint, std::vector<uint8_t>* wire) {
  if (wire == nullptr || endpoint.qpn == 0 || endpoint.psn > 0x00ffffffU ||
      endpoint.port == 0 || endpoint.mtu == 0) {
    return false;
  }
  wire->clear();
  wire->reserve(kEndpointWireBytes);
  wire->insert(wire->end(), endpoint.gid.begin(), endpoint.gid.end());
  AppendU32(endpoint.qpn, wire);
  AppendU32(endpoint.psn, wire);
  wire->push_back(endpoint.port);
  wire->push_back(endpoint.gid_index);
  wire->push_back(endpoint.mtu);
  wire->insert(wire->end(), 5, 0);
  return true;
}

bool DecodeEndpoint(const uint8_t* wire, size_t len, Endpoint* endpoint) {
  if (wire == nullptr || endpoint == nullptr || len != kEndpointWireBytes) return false;
  std::memcpy(endpoint->gid.data(), wire, endpoint->gid.size());
  size_t offset = endpoint->gid.size();
  uint8_t padding[5]{};
  if (!ReadU32(wire, len, &offset, &endpoint->qpn) ||
      !ReadU32(wire, len, &offset, &endpoint->psn) || offset + 8 != len) {
    return false;
  }
  endpoint->port = wire[offset++];
  endpoint->gid_index = wire[offset++];
  endpoint->mtu = wire[offset++];
  std::memcpy(padding, wire + offset, sizeof(padding));
  return endpoint->qpn != 0 && endpoint->psn <= 0x00ffffffU && endpoint->port != 0 &&
         endpoint->mtu != 0 && padding[0] == 0 && padding[1] == 0 && padding[2] == 0 &&
         padding[3] == 0 && padding[4] == 0;
}

uint32_t Checksum(const uint8_t* data, size_t len) {
  uint32_t value = 2166136261U;
  for (size_t index = 0; index < len; ++index) {
    value ^= data[index];
    value *= 16777619U;
  }
  return value;
}

bool NativeTransportCompiled() { return false; }

const char* NativeTransportReason() {
#if FLUME_HAVE_IBVERBS
  return "RoCE protocol and optional standard libibverbs storage-server path are compiled, "
         "but native CANN RA/HCCP, AICPU, and AIV posting are not enabled";
#else
  return "RoCE protocol is compiled, but libibverbs and native CANN RA/HCCP, "
         "AICPU, and AIV posting are not enabled";
#endif
}

}  // namespace flume::roce
