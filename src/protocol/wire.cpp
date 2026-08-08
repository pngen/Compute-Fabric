#include "compute_fabric/protocol/wire.h"

#include <cstring>

namespace cf::proto {

void BinaryWriter::put_u16(uint16_t v) {
  buf_.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  buf_.push_back(static_cast<uint8_t>(v & 0xFF));
}

void BinaryWriter::put_u32(uint32_t v) {
  buf_.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  buf_.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
  buf_.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  buf_.push_back(static_cast<uint8_t>(v & 0xFF));
}

void BinaryWriter::put_u64(uint64_t v) {
  for (int i = 7; i >= 0; --i) {
    buf_.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
  }
}

void BinaryWriter::put_f64(double v) {
  uint64_t bits = 0;
  std::memcpy(&bits, &v, sizeof(bits));
  put_u64(bits);
}

void BinaryWriter::put_bytes(const void* data, size_t len) {
  const uint8_t* p = static_cast<const uint8_t*>(data);
  buf_.insert(buf_.end(), p, p + len);
}

void BinaryWriter::put_string(const std::string& s) {
  put_u32(static_cast<uint32_t>(s.size()));
  put_bytes(s.data(), s.size());
}

void BinaryWriter::put_id(const Id128& id) {
  uint8_t b[16];
  id.to_bytes(b);
  put_bytes(b, 16);
}

bool BinaryReader::read_u8(uint8_t& v) {
  if (pos_ + 1 > len_) return false;
  v = data_[pos_++];
  return true;
}

bool BinaryReader::read_u16(uint16_t& v) {
  if (pos_ + 2 > len_) return false;
  v = static_cast<uint16_t>((static_cast<uint16_t>(data_[pos_]) << 8) |
                            data_[pos_ + 1]);
  pos_ += 2;
  return true;
}

bool BinaryReader::read_i16(int16_t& v) {
  uint16_t u;
  if (!read_u16(u)) return false;
  v = static_cast<int16_t>(u);
  return true;
}

bool BinaryReader::read_u32(uint32_t& v) {
  if (pos_ + 4 > len_) return false;
  v = (static_cast<uint32_t>(data_[pos_]) << 24) |
      (static_cast<uint32_t>(data_[pos_ + 1]) << 16) |
      (static_cast<uint32_t>(data_[pos_ + 2]) << 8) |
      static_cast<uint32_t>(data_[pos_ + 3]);
  pos_ += 4;
  return true;
}

bool BinaryReader::read_i32(int32_t& v) {
  uint32_t u;
  if (!read_u32(u)) return false;
  v = static_cast<int32_t>(u);
  return true;
}

bool BinaryReader::read_u64(uint64_t& v) {
  if (pos_ + 8 > len_) return false;
  v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | data_[pos_ + i];
  pos_ += 8;
  return true;
}

bool BinaryReader::read_i64(int64_t& v) {
  uint64_t u;
  if (!read_u64(u)) return false;
  v = static_cast<int64_t>(u);
  return true;
}

bool BinaryReader::read_f64(double& v) {
  uint64_t bits;
  if (!read_u64(bits)) return false;
  std::memcpy(&v, &bits, sizeof(bits));
  return true;
}

bool BinaryReader::read_bool(bool& v) {
  uint8_t b;
  if (!read_u8(b)) return false;
  v = b != 0;
  return true;
}

bool BinaryReader::read_bytes(std::vector<uint8_t>& out, uint32_t max_len) {
  uint32_t n;
  if (!read_u32(n)) return false;
  if (n > max_len) return false;
  if (pos_ + n > len_) return false;
  out.assign(data_ + pos_, data_ + pos_ + n);
  pos_ += n;
  return true;
}

bool BinaryReader::read_raw_bytes(std::vector<uint8_t>& out, uint32_t n) {
  if (pos_ + n > len_) return false;
  out.assign(data_ + pos_, data_ + pos_ + n);
  pos_ += n;
  return true;
}

bool BinaryReader::read_string(std::string& out, uint32_t max_len) {
  uint32_t n;
  if (!read_u32(n)) return false;
  if (n > max_len) return false;
  if (pos_ + n > len_) return false;
  out.assign(reinterpret_cast<const char*>(data_ + pos_), n);
  pos_ += n;
  return true;
}

bool BinaryReader::read_id(Id128& id) {
  if (pos_ + 16 > len_) return false;
  id = Id128::from_bytes(data_ + pos_);
  pos_ += 16;
  return true;
}

bool BinaryReader::skip(size_t n) {
  if (pos_ + n > len_) return false;
  pos_ += n;
  return true;
}

}  // namespace cf::proto