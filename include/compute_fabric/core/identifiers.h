#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <random>
#include <string>

#include "compute_fabric/core/status.h"

namespace cf {

// A 128-bit stable identifier. Serializable, printable, comparable, and
// never a pointer. Used for WorkloadId, ComputeTaskId, ExecutionAttemptId,
// ReservationId, NodeId and NodeSessionId.
class Id128 {
 public:
  Id128() : hi_(0), lo_(0) {}
  Id128(uint64_t hi, uint64_t lo) : hi_(hi), lo_(lo) {}

  static Id128 zero() { return Id128(); }
  static Id128 random();  // operating-system entropy, see identifiers.cpp

  bool is_zero() const { return hi_ == 0 && lo_ == 0; }

  uint64_t hi() const { return hi_; }
  uint64_t lo() const { return lo_; }

  std::string to_hex() const;
  static Result<Id128> from_hex(const std::string& hex);

  // Big-endian byte serialization (16 bytes).
  void to_bytes(uint8_t out[16]) const;
  static Id128 from_bytes(const uint8_t in[16]);

  friend bool operator==(const Id128& a, const Id128& b) {
    return a.hi_ == b.hi_ && a.lo_ == b.lo_;
  }
  friend bool operator!=(const Id128& a, const Id128& b) { return !(a == b); }
  friend bool operator<(const Id128& a, const Id128& b) {
    if (a.hi_ != b.hi_) return a.hi_ < b.hi_;
    return a.lo_ < b.lo_;
  }

 private:
  uint64_t hi_;
  uint64_t lo_;
};

using WorkloadId = Id128;
using ComputeTaskId = Id128;
using ExecutionAttemptId = Id128;
using ReservationId = Id128;
using NodeId = Id128;
using NodeSessionId = Id128;

// Coordinator epoch: monotonically increased on every coordinator restart.
using CoordinatorEpoch = uint64_t;
// Per-node session epoch: increased when a node re-registers.
using SessionEpoch = uint64_t;

}  // namespace cf

namespace std {
template <>
struct hash<cf::Id128> {
  size_t operator()(const cf::Id128& id) const {
    uint64_t h = id.hi() ^ (id.lo() * 0x9E3779B97F4A7C15ull);
    h ^= h >> 33;
    h *= 0xFF51AFD7ED558CCDull;
    h ^= h >> 33;
    return static_cast<size_t>(h);
  }
};
}  // namespace std
