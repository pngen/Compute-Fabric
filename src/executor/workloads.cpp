#include "compute_fabric/executor/workloads.h"

#include <cstdio>
#include <cstring>

#include "compute_fabric/core/digest.h"

namespace cf {

std::string hex_digest(const std::vector<uint8_t>& data) {
  char buf[65];
  for (size_t i = 0; i < data.size() && i < 32; ++i) {
    std::snprintf(buf + i * 2, 3, "%02x", data[i]);
  }
  buf[data.size() * 2] = '\0';
  return std::string(buf);
}

namespace {

uint32_t mul_mod(uint32_t a, uint32_t b, uint32_t m) {
  return static_cast<uint32_t>((static_cast<uint64_t>(a) * b) % m);
}

// Deterministic mixing step used by kernels.
inline uint32_t wmix(uint32_t x) {
  x ^= x >> 16;
  x *= 0x7FEB352Du;
  x ^= x >> 15;
  x *= 0x846CA68Bu;
  x ^= x >> 16;
  return x;
}

uint16_t hash_integer(uint32_t v) {
  return static_cast<uint16_t>(wmix(v + 0x9E3779B9u));
}

}  // namespace

bool is_supported_cpu_kernel(KernelType k) {
  switch (k) {
    case KernelType::HashInteger:
    case KernelType::VectorTransform:
    case KernelType::Checksum:
    case KernelType::SyntheticLoop:
    case KernelType::StateDependent:
    case KernelType::TensorArithmetic:
      return true;
    default:
      return false;
  }
}

WorkloadResult run_cpu_kernel(const ComputeTask& task,
                              const std::shared_ptr<CancellationToken>& token,
                              std::string& device_out) {
  WorkloadResult r;
  device_out = "cpu";

  const uint64_t n = task.kernel_param_n > 0 ? task.kernel_param_n : 1;
  const uint32_t a = static_cast<uint32_t>(task.kernel_param_a & 0xFFFFFFFFu);
  const uint32_t b = static_cast<uint32_t>(task.kernel_param_b & 0xFFFFFFFFu);
  const uint32_t seed = static_cast<uint32_t>(task.seed & 0xFFFFFFFFu);

  // Bounded work: never more than 64M elements per kernel.
  if (n > 64u * 1000u * 1000u) {
    r.success = false;
    r.notes = "kernel size exceeds bound";
    return r;
  }

  uint64_t acc_lo = 0;
  uint64_t acc_hi = 0;
  uint64_t processed = 0;

  switch (task.kernel) {
    case KernelType::HashInteger: {
      for (uint64_t i = 0; i < n; ++i) {
        if (token && token->is_cancelled()) {
          r.success = false;
          r.notes = "cancelled";
          return r;
        }
        uint32_t v = static_cast<uint32_t>(wmix(static_cast<uint32_t>(i) + seed));
        uint16_t h = hash_integer(v);
        acc_lo += h;
        acc_hi += static_cast<uint64_t>(h) * (i + 1);
        processed += 2;
      }
      break;
    }
    case KernelType::VectorTransform: {
      const uint32_t m = 0xFFFFFFFBu;  // prime near 2^32
      for (uint64_t i = 0; i < n; ++i) {
        if (token && token->is_cancelled()) {
          r.success = false;
          r.notes = "cancelled";
          return r;
        }
        uint32_t x = wmix(static_cast<uint32_t>(i) + seed);
        uint32_t y = (mul_mod(x, a, m) + b) % m;
        acc_lo += y;
        acc_hi ^= static_cast<uint64_t>(y) << 1;
        processed += 1;
      }
      break;
    }
    case KernelType::Checksum: {
      uint32_t s = seed;
      for (uint64_t i = 0; i < n; ++i) {
        if (token && token->is_cancelled()) {
          r.success = false;
          r.notes = "cancelled";
          return r;
        }
        uint32_t v = wmix(static_cast<uint32_t>(i) + s);
        s = (s * 31u) + v;
        acc_lo += s;
        acc_hi += static_cast<uint64_t>(s) * (i + 1);
        processed += 1;
      }
      break;
    }
    case KernelType::SyntheticLoop: {
      uint32_t s = seed;
      for (uint64_t i = 0; i < n && i < 50u * 1000u * 1000u; ++i) {
        if ((i & 0x3FFFu) == 0 && token && token->is_cancelled()) {
          r.success = false;
          r.notes = "cancelled";
          return r;
        }
        s = wmix(s + static_cast<uint32_t>(i));
        acc_lo += s;
        acc_hi ^= s;
        processed += 1;
      }
      break;
    }
    case KernelType::StateDependent: {
      // Incorporate the state fingerprint so output depends on state identity.
      uint64_t state_fp = 0;
      for (const auto& dep : task.state_dependencies) {
        state_fp ^= bytes_hash(dep.state_id) + bytes_hash(dep.generation);
      }
      uint32_t s = static_cast<uint32_t>(state_fp) ^ seed;
      for (uint64_t i = 0; i < n; ++i) {
        if (token && token->is_cancelled()) {
          r.success = false;
          r.notes = "cancelled";
          return r;
        }
        s = wmix(s + static_cast<uint32_t>(i));
        acc_lo += s;
        acc_hi ^= static_cast<uint64_t>(s) << 3;
        processed += 1;
      }
      break;
    }
    case KernelType::TensorArithmetic: {
      // Small deterministic matrix multiply: (k x k) x (k x k).
      uint32_t k = static_cast<uint32_t>(n);
      if (k > 256) k = 256;
      if (k == 0) k = 1;
      std::vector<uint32_t> A(k * k), B(k * k), C(k * k);
      for (uint32_t i = 0; i < k * k; ++i) {
        A[i] = wmix(static_cast<uint32_t>(i) + seed);
        B[i] = wmix(static_cast<uint32_t>(i) + seed + 0x1234u);
      }
      for (uint32_t i = 0; i < k; ++i) {
        if (token && token->is_cancelled()) {
          r.success = false;
          r.notes = "cancelled";
          return r;
        }
        for (uint32_t j = 0; j < k; ++j) {
          uint64_t s = 0;
          for (uint32_t l = 0; l < k; ++l) {
            s += static_cast<uint64_t>(A[i * k + l]) * B[l * k + j];
          }
          C[i * k + j] = static_cast<uint32_t>((s >> 32) ^ (s & 0xFFFFFFFFu));
          acc_lo += C[i * k + j];
          acc_hi ^= static_cast<uint64_t>(C[i * k + j]) << 5;
        }
      }
      processed = static_cast<uint64_t>(k) * k * k * 2;
      break;
    }
    default: {
      r.success = false;
      r.notes = "unsupported kernel: " + std::string(kernel_type_name(task.kernel));
      return r;
    }
  }

  // Final deterministic 16-byte digest.
  uint64_t buf[2] = {acc_lo, acc_hi};
  uint64_t h0 = bytes_hash(buf, sizeof(buf), seed);
  uint64_t h1 = mix64(h0 ^ static_cast<uint64_t>(n));
  r.output.resize(16);
  for (int i = 0; i < 8; ++i) {
    r.output[i] = static_cast<uint8_t>((h0 >> (56 - i * 8)) & 0xFF);
    r.output[8 + i] = static_cast<uint8_t>((h1 >> (56 - i * 8)) & 0xFF);
  }
  r.checksum_hex = hex_digest(r.output);
  r.bytes_processed = processed;
  return r;
}

}  // namespace cf