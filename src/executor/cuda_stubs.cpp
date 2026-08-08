// Compile-time stubs for the CUDA executor API when CUDA is not available.
// Allows host code (CLI, node runtime, benchmarks, examples, tests) to be
// written against the full executor API while building CPU-only.

#include "compute_fabric/executor/cuda_executor.h"

namespace cf {

std::vector<ExecutorCapability> enumerate_cuda_devices() { return {}; }

int64_t cuda_outstanding_allocations() { return 0; }

CudaDeviceGuard::CudaDeviceGuard(int device_ordinal) {
  (void)device_ordinal;
  ok_ = false;
  error_ = "cuda not available in this build";
}

CudaDeviceGuard::~CudaDeviceGuard() {}

CudaRunResult cuda_run_vector_transform(
    int device_ordinal, const ComputeTask& task,
    const std::shared_ptr<CancellationToken>& token) {
  (void)device_ordinal;
  (void)task;
  (void)token;
  CudaRunResult r;
  r.success = false;
  r.error_message = "cuda not available in this build";
  return r;
}

CudaExecutorBackend::CudaExecutorBackend(int device_ordinal, int32_t slots,
                                         int32_t queue_limit) {
  (void)device_ordinal;
  (void)slots;
  (void)queue_limit;
  available_ = false;
}

CudaExecutorBackend::~CudaExecutorBackend() {}

ExecutorCapability CudaExecutorBackend::capability() const {
  ExecutorCapability c;
  c.type = ExecutorType::Cuda;
  c.name = "cuda";
  c.slots = 0;
  c.healthy = false;
  return c;
}

Result<bool> CudaExecutorBackend::can_execute(const ComputeTask& task) const {
  (void)task;
  return false;
}

Result<void> CudaExecutorBackend::enqueue(std::shared_ptr<AttemptWork> work) {
  (void)work;
  return Error(ErrorCode::ExecutorUnavailable, "cuda not available");
}

Result<void> CudaExecutorBackend::cancel_attempt(const ExecutionAttemptId& id) {
  (void)id;
  return Error(ErrorCode::TaskNotFound, "cuda not available");
}

bool CudaExecutorBackend::has_active(const ExecutionAttemptId& id) const {
  (void)id;
  return false;
}

ExecutorRuntimeStats CudaExecutorBackend::stats() const {
  ExecutorRuntimeStats s;
  s.max_slots = 0;
  return s;
}

void CudaExecutorBackend::shutdown(bool drain) {
  (void)drain;
  stopping_ = true;
}

}  // namespace cf