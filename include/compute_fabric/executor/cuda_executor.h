#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "compute_fabric/executor/executor.h"

namespace cf {

// Enumerates CUDA devices visible to this process. Returns one capability per
// device. Empty when no CUDA runtime / devices are present.
std::vector<ExecutorCapability> enumerate_cuda_devices();

// Host helper that runs the deterministic CUDA vector-transform kernel on the
// given device and returns the same digest as the CPU VectorTransform kernel.
// Used by CudaExecutorBackend and by tests/benchmarks directly. Returns false
// with a message on CUDA error.
struct CudaRunResult {
  bool success = false;
  std::vector<uint8_t> output;  // 16-byte deterministic digest
  std::string integrity_hex;
  std::string device_name;
  std::string error_message;
  double kernel_millis = 0.0;
  uint64_t bytes_transferred = 0;
  uint64_t elements = 0;
};

CudaRunResult cuda_run_vector_transform(int device_ordinal,
                                        const ComputeTask& task,
                                        const std::shared_ptr<CancellationToken>& token);

// RAII helper: ensures the CUDA runtime is usable and restores the current
// device on destruction. Not thread-safe to nest on different devices.
class CudaDeviceGuard {
 public:
  explicit CudaDeviceGuard(int device_ordinal);
  ~CudaDeviceGuard();
  bool ok() const { return ok_; }
  const std::string& error() const { return error_; }

 private:
  bool ok_ = false;
  std::string error_;
  int previous_ = 0;
};

// Number of live CUDA device allocations tracked by the runtime (0 after a
// clean lifecycle). Used to detect allocation leaks.
int64_t cuda_outstanding_allocations();

// CUDA executor backend: real kernel dispatch on a bounded set of streams,
// real host<->device transfers, typed CUDA errors, output integrity, slot
// accounting, allocation-leak tracking and clean shutdown.
class CudaExecutorBackend : public ExecutorBackend {
 public:
  CudaExecutorBackend(int device_ordinal, int32_t slots, int32_t queue_limit = 64);
  ~CudaExecutorBackend() override;

  std::string name() const override { return "cuda"; }
  bool available() const override { return available_; }
  ExecutorCapability capability() const override;
  Result<bool> can_execute(const ComputeTask& task) const override;
  Result<void> enqueue(std::shared_ptr<AttemptWork> work) override;
  Result<void> cancel_attempt(const ExecutionAttemptId& id) override;
  bool has_active(const ExecutionAttemptId& id) const override;
  ExecutorRuntimeStats stats() const override;
  void shutdown(bool drain) override;

 private:
  void worker_loop();
  void run_one(std::shared_ptr<AttemptWork> work);

  int device_ordinal_;
  int32_t slots_;
  int32_t queue_limit_;
  bool available_ = false;
  std::string device_name_;
  int major_ = 0, minor_ = 0;
  uint64_t device_memory_bytes_ = 0;
  std::string arch_;

  std::atomic<bool> stopping_{false};
  std::atomic<int64_t> active_{0};
  std::atomic<int64_t> total_{0};
  std::atomic<int64_t> succeeded_{0};
  std::atomic<int64_t> failed_{0};

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::shared_ptr<AttemptWork>> queue_;
  std::vector<std::thread> workers_;
  std::map<ExecutionAttemptId, std::shared_ptr<CancellationToken>> active_tokens_;
};

}  // namespace cf