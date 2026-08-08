#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "compute_fabric/executor/executor.h"
#include "compute_fabric/task/task.h"

namespace cf {

// Deterministic built-in CPU kernels. Same task descriptor -> same output on
// every node and every platform. Output is a 16-byte digest plus metadata.
struct WorkloadResult {
  bool success = true;
  std::vector<uint8_t> output;   // 16-byte deterministic digest
  std::string checksum_hex;
  std::string notes;
  uint64_t bytes_processed = 0;
};

// Runs a kernel; returns success and a deterministic output digest.
WorkloadResult run_cpu_kernel(const ComputeTask& task,
                              const std::shared_ptr<CancellationToken>& token,
                              std::string& device_out);

// Whether the kernel kind is recognized by the CPU backend.
bool is_supported_cpu_kernel(KernelType k);

// Deterministic host-side digest of a byte buffer (32 hex chars).
std::string hex_digest(const std::vector<uint8_t>& data);

}  // namespace cf