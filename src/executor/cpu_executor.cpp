#include "compute_fabric/executor/cpu_executor.h"

#include <algorithm>

#include "compute_fabric/core/time_util.h"

namespace cf {

CpuExecutorBackend::CpuExecutorBackend(int32_t worker_slots)
    : motors_(worker_slots > 0 ? worker_slots : 1) {
  for (int32_t i = 0; i < motors_; ++i) {
    workers_.emplace_back([this] { worker_loop(); });
  }
}

CpuExecutorBackend::~CpuExecutorBackend() {
  shutdown(false);
}

ExecutorCapability CpuExecutorBackend::capability() const {
  ExecutorCapability c;
  c.type = ExecutorType::Cpu;
  c.name = "cpu";
  c.slots = motors_;
  c.vendor = "generic";
  c.architecture = "x64";
  c.device_name = "cpu";
  c.relative_speed = 1.0;
  c.healthy = true;
  c.device_ids = {0};
  return c;
}

Result<bool> CpuExecutorBackend::can_execute(const ComputeTask& task) const {
  if (task.required_executor != ExecutorType::Cpu &&
      task.required_executor != ExecutorType::Any_) {
    return false;
  }
  if (!is_supported_cpu_kernel(task.kernel)) {
    return false;
  }
  if (task.required_cpu_slots > static_cast<uint32_t>(motors_)) {
    return false;
  }
  return true;
}

Result<void> CpuExecutorBackend::enqueue(std::shared_ptr<AttemptWork> work) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopping_) {
      return Error(ErrorCode::ShuttingDown, "cpu executor shutting down");
    }
    if (queue_.size() >= 1024) {
      return Error(ErrorCode::NoCapacity, "cpu executor queue full");
    }
    queue_.push_back(std::move(work));
  }
  cv_.notify_one();
  return {};
}

Result<void> CpuExecutorBackend::cancel_attempt(const ExecutionAttemptId& id) {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& w : queue_) {
    if (w->attempt_id == id) {
      if (w->token) w->token->request();
      return {};
    }
  }
  auto it = active_tokens_.find(id);
  if (it != active_tokens_.end()) {
    it->second->request();
    return {};
  }
  return Error(ErrorCode::TaskNotFound, "attempt not found in cpu executor");
}

bool CpuExecutorBackend::has_active(const ExecutionAttemptId& id) const {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& w : queue_) {
    if (w->attempt_id == id) return true;
  }
  return active_tokens_.count(id) > 0;
}

ExecutorRuntimeStats CpuExecutorBackend::stats() const {
  ExecutorRuntimeStats s;
  s.total_attempts = static_cast<uint64_t>(total_.load());
  s.succeeded = static_cast<uint64_t>(succeeded_.load());
  s.failed = static_cast<uint64_t>(failed_.load());
  s.active = static_cast<uint32_t>(active_.load());
  s.max_slots = static_cast<uint32_t>(motors_);
  std::lock_guard<std::mutex> lk(mu_);
  s.queued = static_cast<uint32_t>(queue_.size());
  return s;
}

void CpuExecutorBackend::worker_loop() {
  for (;;) {
    std::shared_ptr<AttemptWork> work;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait_for(lk, std::chrono::milliseconds(100), [this] {
        return stopping_ || !queue_.empty();
      });
      if (queue_.empty()) {
        if (stopping_) return;
        continue;
      }
      work = queue_.front();
      queue_.pop_front();
      active_tokens_[work->attempt_id] = work->token;
    }
    active_.fetch_add(1);
    run_one(work);
    active_.fetch_add(-1);
    {
      std::lock_guard<std::mutex> lk(mu_);
      active_tokens_.erase(work->attempt_id);
    }
  }
}

void CpuExecutorBackend::run_one(std::shared_ptr<AttemptWork> work) {
  total_.fetch_add(1);
  int64_t t0 = now_micros();
  AttemptOutcome out;
  out.device = "cpu";
  if (work->on_start) work->on_start();
  if (work->token && work->token->is_cancelled()) {
    out.success = false;
    out.state = AttemptState::Cancelled;
    out.failure_kind = FailureKind::Cancelled;
    out.failure_reason = "cancelled before execution";
  } else {
    std::string device;
    WorkloadResult wr = run_cpu_kernel(work->task, work->token, device);
    out.device = device;
    out.duration_ms = (now_micros() - t0) / 1000.0;
    if (wr.success) {
      out.success = true;
      out.state = AttemptState::Succeeded;
      out.output_bytes = wr.output;
      out.output_integrity = wr.checksum_hex;
      out.bytes_transferred = wr.bytes_processed;
      out.notes = {wr.notes};
      succeeded_.fetch_add(1);
    } else {
      out.success = false;
      out.state = AttemptState::Failed;
      out.failure_kind = wr.notes == "cancelled" ? FailureKind::Cancelled
                                                 : FailureKind::ExecutorFailed;
      out.failure_reason = wr.notes;
      failed_.fetch_add(1);
    }
  }
  out.duration_ms = (now_micros() - t0) / 1000.0;
  if (work->on_complete) work->on_complete(std::move(out));
}

void CpuExecutorBackend::shutdown(bool drain) {
  bool expected = false;
  if (!stopping_.compare_exchange_strong(expected, true)) return;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (!drain) {
      for (auto& w : queue_) {
        if (w->token) w->token->request();
      }
    }
  }
  cv_.notify_all();
  for (auto& t : workers_) {
    if (t.joinable()) t.join();
  }
  workers_.clear();
}

}  // namespace cf