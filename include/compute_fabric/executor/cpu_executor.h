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
#include "compute_fabric/executor/workloads.h"

namespace cf {

// CPU executor backend: bounded worker pool executing deterministic built-in
// kernels. Enforces slot accounting (bounded worker slots), reports timing,
// cooperative cancellation checks, failure reporting and deterministic output.
class CpuExecutorBackend : public ExecutorBackend {
 public:
  explicit CpuExecutorBackend(int32_t worker_slots);
  ~CpuExecutorBackend() override;

  std::string name() const override { return "cpu"; }
  bool available() const override { return motors_ > 0; }
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

  int32_t motors_;
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