#pragma once

#include <cstdint>
#include <string>

namespace cf {

// CRC-32 (IEEE 802.3 / zlib polynomial). Used for frame integrity checks.
// Stable across platforms and endianness.
class Crc32 {
 public:
  static uint32_t update(uint32_t crc, const void* data, size_t len);
  static uint32_t compute(const void* data, size_t len) {
    return update(0xFFFFFFFFu, data, len) ^ 0xFFFFFFFFu;
  }
};

// Deterministic 64-bit hash (SplitMix64 finalizer over an initial seed).
uint64_t mix64(uint64_t x);

// Deterministic 64-bit hash of a byte range (FNV-1a then mix64).
uint64_t bytes_hash(const void* data, size_t len, uint64_t seed = 0);

uint64_t bytes_hash(const std::string& s, uint64_t seed = 0);

// Deterministic 64-bit hash of two 64-bit values.
uint64_t pair_hash(uint64_t a, uint64_t b);

}  // namespace cf