#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"

namespace cf::proto {

// Big-endian binary writer. Bounded by construction; every message is subject
// to a maximum payload length enforced at the frame layer.
class BinaryWriter {
 public:
  explicit BinaryWriter(size_t reserve = 0) {
    if (reserve) buf_.reserve(reserve);
  }

  void put_u8(uint8_t v) { buf_.push_back(v); }
  void put_u16(uint16_t v);
  void put_i16(int16_t v) { put_u16(static_cast<uint16_t>(v)); }
  void put_u32(uint32_t v);
  void put_i32(int32_t v) { put_u32(static_cast<uint32_t>(v)); }
  void put_u64(uint64_t v);
  void put_i64(int64_t v) { put_u64(static_cast<uint64_t>(v)); }
  void put_f64(double v);
  void put_bool(bool v) { put_u8(v ? 1 : 0); }
  void put_bytes(const void* data, size_t len);
  void put_string(const std::string& s);
  void put_id(const Id128& id);

  const std::vector<uint8_t>& bytes() const { return buf_; }
  std::vector<uint8_t> take_bytes() { return std::move(buf_); }
  size_t size() const { return buf_.size(); }

 private:
  std::vector<uint8_t> buf_;
};

// Big-endian binary reader with strict bounds checking. Every read returns
// false (or an error) on truncation/overflow; callers must propagate.
class BinaryReader {
 public:
  BinaryReader(const uint8_t* data, size_t len) : data_(data), len_(len) {}
  explicit BinaryReader(const std::vector<uint8_t>& v)
      : data_(v.data()), len_(v.size()) {}

  bool ok() const { return pos_ <= len_; }
  size_t pos() const { return pos_; }
  size_t remaining() const { return len_ - pos_; }

  bool read_u8(uint8_t& v);
  bool read_u16(uint16_t& v);
  bool read_i16(int16_t& v);
  bool read_u32(uint32_t& v);
  bool read_i32(int32_t& v);
  bool read_u64(uint64_t& v);
  bool read_i64(int64_t& v);
  bool read_f64(double& v);
  bool read_bool(bool& v);
  // Reads a length-prefixed byte blob; returns false if out of bounds.
  bool read_bytes(std::vector<uint8_t>& out, uint32_t max_len);
  // Reads exactly n bytes (no length prefix); returns false if out of bounds.
  bool read_raw_bytes(std::vector<uint8_t>& out, uint32_t n);
  bool read_string(std::string& out, uint32_t max_len);
  bool read_id(Id128& id);
  bool skip(size_t n);

  // Convenience: returns an error describing the failure point.
  Error fail(const std::string& what) const {
    return Error(ErrorCode::OutOfBounds, what + " (reader at " +
                                             std::to_string(pos_) + "/" +
                                             std::to_string(len_) + ")");
  }

 private:
  const uint8_t* data_;
  size_t len_;
  size_t pos_ = 0;
};

}  // namespace cf::proto