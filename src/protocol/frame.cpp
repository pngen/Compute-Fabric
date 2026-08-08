#include "compute_fabric/protocol/frame.h"

#include <cstring>

#include "compute_fabric/core/digest.h"

namespace cf::proto {

namespace {

void put_be_u16(uint8_t*& p, uint16_t v) {
  *p++ = static_cast<uint8_t>((v >> 8) & 0xFF);
  *p++ = static_cast<uint8_t>(v & 0xFF);
}

void put_be_u32(uint8_t*& p, uint32_t v) {
  *p++ = static_cast<uint8_t>((v >> 24) & 0xFF);
  *p++ = static_cast<uint8_t>((v >> 16) & 0xFF);
  *p++ = static_cast<uint8_t>((v >> 8) & 0xFF);
  *p++ = static_cast<uint8_t>(v & 0xFF);
}

void put_be_u64(uint8_t*& p, uint64_t v) {
  for (int i = 7; i >= 0; --i) *p++ = static_cast<uint8_t>((v >> (i * 8)) & 0xFF);
}

bool get_be_u16(const uint8_t*& p, const uint8_t* end, uint16_t& v) {
  if (end - p < 2) return false;
  v = static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
  p += 2;
  return true;
}

bool get_be_u32(const uint8_t*& p, const uint8_t* end, uint32_t& v) {
  if (end - p < 4) return false;
  v = (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
      (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
  p += 4;
  return true;
}

bool get_be_u64(const uint8_t*& p, const uint8_t* end, uint64_t& v) {
  if (end - p < 8) return false;
  v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
  p += 8;
  return true;
}

}  // namespace

void encode_header(const FrameHeader& h, uint8_t out[kHeaderSize]) {
  uint8_t* p = out;
  put_be_u32(p, h.magic);
  put_be_u16(p, h.version);
  put_be_u16(p, h.message_type);
  put_be_u64(p, h.request_id);
  *p++ = static_cast<uint8_t>(h.sender_kind);
  uint8_t id_bytes[16];
  h.sender_id.to_bytes(id_bytes);
  std::memcpy(p, id_bytes, 16);
  p += 16;
  put_be_u32(p, h.payload_length);
  put_be_u32(p, h.payload_checksum);
  put_be_u32(p, h.header_checksum);
}

Result<FrameHeader> decode_header(const uint8_t* in, size_t len) {
  if (len != kHeaderSize) {
    return Error(ErrorCode::MalformedFrame, "header size mismatch");
  }
  const uint8_t* p = in;
  const uint8_t* end = in + len;
  FrameHeader h;
  uint32_t magic = 0;
  uint16_t version = 0;
  if (!get_be_u32(p, end, magic)) return Error(ErrorCode::MalformedFrame, "truncated magic");
  if (!get_be_u16(p, end, version)) return Error(ErrorCode::MalformedFrame, "truncated version");
  if (!get_be_u16(p, end, h.message_type)) return Error(ErrorCode::MalformedFrame, "truncated message type");
  if (!get_be_u64(p, end, h.request_id)) return Error(ErrorCode::MalformedFrame, "truncated request id");
  uint8_t sk = *p++;
  // Skip the 16-byte sender id at [17..33).
  p += 16;
  if (!get_be_u32(p, end, h.payload_length)) return Error(ErrorCode::MalformedFrame, "truncated payload length");
  if (!get_be_u32(p, end, h.payload_checksum)) return Error(ErrorCode::MalformedFrame, "truncated payload checksum");
  if (!get_be_u32(p, end, h.header_checksum)) return Error(ErrorCode::MalformedFrame, "truncated header checksum");
  if (p != end) return Error(ErrorCode::MalformedFrame, "header trailing bytes");
  h.magic = magic;
  h.version = version;
  h.sender_kind = static_cast<SenderKind>(sk);
  if (magic != kFrameMagic) {
    return Error(ErrorCode::MalformedFrame, "bad magic");
  }
  if (h.payload_length > kMaxFramePayload) {
    return Error(ErrorCode::MalformedFrame, "payload exceeds maximum frame size");
  }
  // Recompute header checksum over the bytes before the checksum field.
  uint8_t tmp[kHeaderSize];
  std::memcpy(tmp, in, kHeaderSize);
  std::memset(tmp + kHeaderSize - 4, 0, 4);
  uint32_t computed = Crc32::compute(tmp, kHeaderSize);
  if (computed != h.header_checksum) {
    return Error(ErrorCode::MalformedFrame, "header checksum mismatch");
  }
  // Sender id occupies bytes [17, 33).
  uint8_t id_bytes[16];
  std::memcpy(id_bytes, in + 17, 16);
  h.sender_id = Id128::from_bytes(id_bytes);
  return h;
}

Frame make_frame(uint16_t message_type, SenderKind sender_kind,
                 const Id128& sender_id, uint64_t request_id,
                 const std::vector<uint8_t>& payload) {
  Frame f;
  f.header.magic = kFrameMagic;
  f.header.version = kProtocolVersion;
  f.header.message_type = message_type;
  f.header.request_id = request_id;
  f.header.sender_kind = sender_kind;
  f.header.sender_id = sender_id;
  f.header.payload_length = static_cast<uint32_t>(payload.size());
  f.header.payload_checksum = Crc32::compute(payload.data(), payload.size());
  uint8_t body[kHeaderSize];
  // Compute header checksum over everything except the final checksum field.
  uint8_t tmp[kHeaderSize];
  encode_header(f.header, tmp);
  std::memset(tmp + kHeaderSize - 4, 0, 4);
  f.header.header_checksum = Crc32::compute(tmp, kHeaderSize);
  encode_header(f.header, body);
  f.payload = payload;
  return f;
}

Result<void> validate_frame(const Frame& frame) {
  const auto& h = frame.header;
  if (h.magic != kFrameMagic) {
    return Error(ErrorCode::MalformedFrame, "bad magic");
  }
  if (h.version != kProtocolVersion) {
    return Error(ErrorCode::ProtocolVersionMismatch, "unsupported protocol version");
  }
  if (h.payload_length != frame.payload.size()) {
    return Error(ErrorCode::MalformedFrame, "payload length mismatch");
  }
  if (h.payload_length > kMaxFramePayload) {
    return Error(ErrorCode::MalformedFrame, "payload exceeds maximum frame size");
  }
  uint32_t computed = Crc32::compute(frame.payload.data(), frame.payload.size());
  if (computed != h.payload_checksum) {
    return Error(ErrorCode::MalformedFrame, "payload checksum mismatch");
  }
  return {};
}

std::string frame_error_detail(const Result<void>& r) {
  if (r.ok()) return "ok";
  return r.error().message();
}

}  // namespace cf::proto