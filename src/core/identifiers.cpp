#include "compute_fabric/core/identifiers.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <thread>

#include "compute_fabric/core/random.h"

#if defined(_WIN32)
#include <windows.h>
#include <bcrypt.h>
#else
#include <cerrno>
#include <sys/random.h>
#include <unistd.h>
#endif

namespace cf {

namespace {

bool fill_system_random(void* destination, size_t size) {
#if defined(_WIN32)
  return BCryptGenRandom(nullptr, static_cast<PUCHAR>(destination),
                         static_cast<ULONG>(size),
                         BCRYPT_USE_SYSTEM_PREFERRED_RNG) >= 0;
#else
  auto* bytes = static_cast<uint8_t*>(destination);
  size_t offset = 0;
  while (offset < size) {
    const ssize_t received = getrandom(bytes + offset, size - offset, 0);
    if (received > 0) {
      offset += static_cast<size_t>(received);
      continue;
    }
    if (received < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
#endif
}

uint64_t current_process_id() {
#if defined(_WIN32)
  return static_cast<uint64_t>(GetCurrentProcessId());
#else
  return static_cast<uint64_t>(getpid());
#endif
}

Id128 fallback_random() {
  // The OS generator should be available on every supported platform. Keep a
  // process-aware fallback so identifier generation still makes progress if
  // the platform entropy call fails unexpectedly.
  static std::atomic<uint64_t> sequence{0};
  const uint64_t serial = sequence.fetch_add(1, std::memory_order_relaxed) + 1;
  const uint64_t wall = static_cast<uint64_t>(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  const uint64_t steady = static_cast<uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const uint64_t thread = static_cast<uint64_t>(
      std::hash<std::thread::id>{}(std::this_thread::get_id()));
  const uint64_t address =
      static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&sequence));
  SeededRng rng(wall ^ (steady << 1) ^ (current_process_id() << 32) ^
                (thread << 7) ^ address ^ serial);
  Id128 id(rng.next_u64(), rng.next_u64());
  return id.is_zero() ? Id128(current_process_id(), serial) : id;
}

}  // namespace

Id128 Id128::random() {
  uint64_t words[2]{};
  if (fill_system_random(words, sizeof(words))) {
    Id128 id(words[0], words[1]);
    if (!id.is_zero()) return id;
  }
  return fallback_random();
}

std::string Id128::to_hex() const {
  char buf[33];
  std::snprintf(buf, sizeof(buf), "%016llx%016llx",
                static_cast<unsigned long long>(hi_),
                static_cast<unsigned long long>(lo_));
  return std::string(buf);
}

Result<Id128> Id128::from_hex(const std::string& hex) {
  if (hex.size() != 32) {
    return Error(ErrorCode::InvalidArgument, "id must be 32 hex characters");
  }
  uint64_t hi = 0, lo = 0;
  auto nib = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };
  for (int i = 0; i < 16; ++i) {
    int a = nib(hex[i]);
    int b = nib(hex[i + 16]);
    if (a < 0 || b < 0) {
      return Error(ErrorCode::InvalidArgument, "invalid hex character in id");
    }
    hi = (hi << 4) | static_cast<uint64_t>(a);
    lo = (lo << 4) | static_cast<uint64_t>(b);
  }
  return Id128(hi, lo);
}

void Id128::to_bytes(uint8_t out[16]) const {
  for (int i = 0; i < 8; ++i) {
    out[i] = static_cast<uint8_t>((hi_ >> (56 - i * 8)) & 0xFF);
    out[8 + i] = static_cast<uint8_t>((lo_ >> (56 - i * 8)) & 0xFF);
  }
}

Id128 Id128::from_bytes(const uint8_t in[16]) {
  uint64_t hi = 0, lo = 0;
  for (int i = 0; i < 8; ++i) {
    hi = (hi << 8) | in[i];
    lo = (lo << 8) | in[8 + i];
  }
  return Id128(hi, lo);
}

}  // namespace cf
