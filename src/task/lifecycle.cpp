#include "compute_fabric/task/lifecycle.h"

namespace cf {

const char* task_state_name(TaskState s) {
  switch (s) {
    case TaskState::Submitted: return "Submitted";
    case TaskState::Blocked: return "Blocked";
    case TaskState::Ready: return "Ready";
    case TaskState::Planning: return "Planning";
    case TaskState::Reserved: return "Reserved";
    case TaskState::Dispatching: return "Dispatching";
    case TaskState::Running: return "Running";
    case TaskState::Succeeded: return "Succeeded";
    case TaskState::Failed: return "Failed";
    case TaskState::RetryPending: return "RetryPending";
    case TaskState::Cancelled: return "Cancelled";
    case TaskState::Lost: return "Lost";
    case TaskState::Preempted: return "Preempted";
    case TaskState::Expired: return "Expired";
    default: return "Unknown";
  }
}

bool parse_task_state(const std::string& s, TaskState& out) {
  for (int i = 0; i <= static_cast<int>(TaskState::Expired); ++i) {
    if (task_state_name(static_cast<TaskState>(i)) == s) {
      out = static_cast<TaskState>(i);
      return true;
    }
  }
  return false;
}

bool TaskLifecycle::is_legal(TaskState from, TaskState to) {
  if (from == to) return true;
  switch (from) {
    case TaskState::Submitted:
      return to == TaskState::Blocked || to == TaskState::Ready ||
             to == TaskState::Cancelled || to == TaskState::Expired;
    case TaskState::Blocked:
      return to == TaskState::Ready || to == TaskState::Cancelled ||
             to == TaskState::Expired;
    case TaskState::Ready:
      return to == TaskState::Planning || to == TaskState::Cancelled ||
             to == TaskState::Expired;
    case TaskState::Planning:
      return to == TaskState::Reserved || to == TaskState::Ready ||
             to == TaskState::Cancelled || to == TaskState::Lost ||
             to == TaskState::Expired;
    case TaskState::Reserved:
      return to == TaskState::Dispatching || to == TaskState::Ready ||
             to == TaskState::Cancelled || to == TaskState::Lost ||
             to == TaskState::Preempted || to == TaskState::Expired;
    case TaskState::Dispatching:
      return to == TaskState::Running || to == TaskState::Ready ||
             to == TaskState::Lost || to == TaskState::Cancelled ||
             to == TaskState::Preempted || to == TaskState::Expired;
    case TaskState::Running:
      return to == TaskState::Succeeded || to == TaskState::Failed ||
             to == TaskState::Lost || to == TaskState::Preempted ||
             to == TaskState::RetryPending || to == TaskState::Cancelled ||
             to == TaskState::Expired;
    case TaskState::Failed:
      return to == TaskState::RetryPending;
    case TaskState::Lost:
      return to == TaskState::RetryPending || to == TaskState::Failed;
    case TaskState::Preempted:
      return to == TaskState::RetryPending || to == TaskState::Ready ||
             to == TaskState::Failed;
    case TaskState::RetryPending:
      return to == TaskState::Ready || to == TaskState::Cancelled ||
             to == TaskState::Expired;
    case TaskState::Succeeded:
    case TaskState::Cancelled:
    case TaskState::Expired:
    default:
      return false;
  }
}

Result<TaskState> TaskLifecycle::transition(TaskState next) {
  if (state_ == next) return state_;
  if (!is_legal(state_, next)) {
    return Error(ErrorCode::IllegalTransition,
                 "illegal task transition " +
                     std::string(task_state_name(state_)) + " -> " +
                     std::string(task_state_name(next)));
  }
  state_ = next;
  return state_;
}

}  // namespace cf