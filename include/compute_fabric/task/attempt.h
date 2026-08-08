#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/time_util.h"
#include "compute_fabric/task/task.h"

namespace cf {

enum class AttemptState : uint8_t {
  Dispatched = 0,
  Started = 1,
  Succeeded = 2,
  Failed = 3,
  Cancelled = 4,
  Lost = 5,
};

const char* attempt_state_name(AttemptState s);

enum class FailureKind : uint8_t {
  None = 0,
  ExecutorFailed = 1,
  NodeLost = 2,
  Timeout = 3,
  Cancelled = 4,
  InvalidOutput = 5,
  CudaError = 6,
  Preempted = 7,
  Internal = 8,
};

const char* failure_kind_name(FailureKind f);

// One execution attempt. Only one attempt may hold active execution authority
// for a non-speculative task at a time. Attempt tokens are epoch-bound so a
// stale node can never overwrite a newer authoritative attempt.
struct ExecutionAttempt {
  ExecutionAttemptId id;
  ComputeTaskId task_id;
  NodeId node_id;
  NodeSessionId node_session;
  CoordinatorEpoch coordinator_epoch = 0;
  ReservationId reservation_id;
  ExecutorType executor = ExecutorType::Cpu;
  uint32_t attempt_number = 0;   // 0-based within the task's lineage
  int64_t dispatched_at_ms = 0;
  int64_t started_at_ms = 0;
  int64_t completed_at_ms = 0;
  AttemptState state = AttemptState::Dispatched;
  FailureKind failure_kind = FailureKind::None;
  std::string failure_reason;
  uint64_t output_size = 0;
  std::string output_integrity;  // hex digest of deterministic output
  double cpu_millis = 0.0;
  uint64_t bytes_transferred = 0;
  std::string device;  // e.g. "cpu" or "cuda:0"
  bool committed = false;  // completion committed by coordinator
};

// Deterministic result payload produced by executors.
struct AttemptOutput {
  uint8_t result_code = 0;   // 0 = success
  std::vector<uint8_t> bytes;
  std::vector<std::string> notes;
};

}  // namespace cf