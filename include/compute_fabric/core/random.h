#pragma once

#include <cstdint>

namespace cf {

// Deterministic PRNG (SplitMix64). Same seed -> same sequence on every
// platform. Used for deterministic scheduling, task seeds and benchmarks.
class SeededRng {
 public:
  explicit SeededRng(uint64_t seed) : state_(seed) {}

  uint64_t next_u64() {
    uint64_t z = (state_ += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
  }

  uint64_t next_u64_bounded(uint64_t bound) {
    if (bound == 0) return 0;
    return next_u64() % bound;
  }

  uint32_t next_u32() { return static_cast<uint32_t>(next_u64()); }

  double next_double() { return (next_u64() >> 11) * (1.0 / 9007199254740992.0); }

  bool next_bool() { return (next_u64() & 1) != 0; }

 private:
  uint64_t state_;
};

}  // namespace cf