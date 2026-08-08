#pragma once

#include <cstdint>
#include <string>

#include "compute_fabric/core/status.h"

namespace cf {

enum class TaskState : uint8_t {
  Submitted = 0,
  Blocked = 1,
  Ready = 2,
  Planning = 3,
  Reserved = 4,
  Dispatching = 5,
  Running = 6,
  Succeeded = 7,
  Failed = 8,
  RetryPending = 9,
  Cancelled = 10,
  Lost = 11,
  Preempted = 12,
  Expired = 13,
};

const char* task_state_name(TaskState s);
bool parse_task_state(const std::string& s, TaskState& out);

// Terminal states.
inline bool is_terminal(TaskState s) {
  return s == TaskState::Succeeded || s == TaskState::Failed ||
         s == TaskState::Cancelled || s == TaskState::Lost ||
         s == TaskState::Preempted || s == TaskState::Expired;
}

// Active/executing states that imply outstanding execution authority.
inline bool is_active(TaskState s) {
  return s == TaskState::Planning || s == TaskState::Reserved ||
         s == TaskState::Dispatching || s == TaskState::Running;
}

// The authoritative lifecycle state machine. One implementation, used by
// every caller. Illegal transitions return typed errors.
class TaskLifecycle {
 public:
  TaskLifecycle() {}  // starts in Submitted

  TaskState state() const { return state_; }

  // Attempts a transition; returns the target state or a typed error.
  Result<TaskState> transition(TaskState next);

  // Short-hand queries.
  bool is_terminal() const { return cf::is_terminal(state_); }
  bool is_active() const { return cf::is_active(state_); }

  // Whether the given transition is legal, without applying it.
  static bool is_legal(TaskState from, TaskState to);

 private:
  TaskState state_ = TaskState::Submitted;
};

}  // namespace cf