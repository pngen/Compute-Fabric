#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"

namespace cf::proto {

// Wire protocol constants.
inline constexpr uint32_t kFrameMagic = 0x434F4D50u;  // "COMP"
inline constexpr uint16_t kProtocolVersion = 1;
inline constexpr uint32_t kMaxFramePayload = 8u << 20;  // 8 MiB
inline constexpr uint32_t kHeaderSize = 45;

enum class SenderKind : uint8_t {
  Node = 1,
  Client = 2,
  Coordinator = 3,
};

// Network byte order framing. All integers are encoded big-endian.
// Do not serialize native structs directly over the wire.
struct FrameHeader {
  uint32_t magic = 0;
  uint16_t version = 0;
  uint16_t message_type = 0;
  uint64_t request_id = 0;
  SenderKind sender_kind = SenderKind::Node;
  Id128 sender_id;
  uint32_t payload_length = 0;
  uint32_t payload_checksum = 0;
  uint32_t header_checksum = 0;
};

struct Frame {
  FrameHeader header;
  std::vector<uint8_t> payload;
};

// Serialize a header into exactly kHeaderSize big-endian bytes.
void encode_header(const FrameHeader& h, uint8_t out[kHeaderSize]);

// Best-effort decode; returns false if the buffer is not kHeaderSize bytes
// or the header checksum fails.
Result<FrameHeader> decode_header(const uint8_t* in, size_t len);

// Build a complete frame (header + payload) with checksums populated.
Frame make_frame(uint16_t message_type, SenderKind sender_kind,
                 const Id128& sender_id, uint64_t request_id,
                 const std::vector<uint8_t>& payload);

// Validate a frame: magic, version, lengths, checksums.
Result<void> validate_frame(const Frame& frame);

// Container for a malformed/invalid frame reason (for telemetry/diagnostics).
std::string frame_error_detail(const Result<void>& r);

}  // namespace cf::proto