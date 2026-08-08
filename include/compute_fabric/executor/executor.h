#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/resource/capacity.h"
#include "compute_fabric/task/attempt.h"
#include "compute_fabric/task/task.h"

namespace cf {

// Cooperative cancellation token shared by an attempt.
struct CancellationToken {
  std::atomic<bool> cancelled{false};
  void request() { cancelled.store(true, std::memory_order_release); }
  bool is_cancelled() const { return cancelled.load(std::memory_order_acquire); }
};

// Result of executing an attempt on a node.
struct AttemptOutcome {
  bool success = false;
  AttemptState state = AttemptState::Failed;
  FailureKind failure_kind = FailureKind::None;
  std::string failure_reason;
  std::vector<uint8_t> output_bytes;
  std::string output_integrity;  // hex digest of deterministic output
  double duration_ms = 0.0;
  uint64_t bytes_transferred = 0;
  std::string device;
  std::vector<std::string> notes;
};

// Work item handed to an executor backend.
struct AttemptWork {
  ComputeTask task;
  ExecutionAttemptId attempt_id;
  NodeId node_id;
  ReservationId reservation_id;
  CoordinatorEpoch coordinator_epoch = 0;
  std::shared_ptr<CancellationToken> token;
  int64_t enqueued_at_ms = 0;
  std::function<void()> on_start;  // invoked on worker thread when execution begins
  std::function<void(AttemptOutcome)> on_complete;  // invoked on worker thread
};

struct ExecutorRuntimeStats {
  uint64_t total_attempts = 0;
  uint64_t succeeded = 0;
  uint64_t failed = 0;
  uint32_t active = 0;
  uint32_t queued = 0;
  uint32_t max_slots = 0;
  uint64_t allocations_outstanding = 0;  // CUDA: live device allocations
};

// Vendor-neutral executor backend contract. CUDA-specific types never leak
// through this interface.
class ExecutorBackend {
 public:
  virtual ~ExecutorBackend() = default;

  virtual std::string name() const = 0;
  virtual bool available() const = 0;
  virtual ExecutorCapability capability() const = 0;

  // Whether this backend can execute the task (capability + capacity).
  virtual Result<bool> can_execute(const ComputeTask& task) const = 0;

  // Enqueue work. The backend enforces its own bounded queue and slot count.
  virtual Result<void> enqueue(std::shared_ptr<AttemptWork> work) = 0;

  // Cooperative cancel (best effort; running kernels may not be preempted).
  virtual Result<void> cancel_attempt(const ExecutionAttemptId& id) = 0;

  virtual bool has_active(const ExecutionAttemptId& id) const = 0;

  virtual ExecutorRuntimeStats stats() const = 0;

  // Stop accepting work, finish or cancel according to mode, join workers.
  virtual void shutdown(bool drain) = 0;
};

}  // namespace cf