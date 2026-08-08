#include "compute_fabric/task/attempt.h"

namespace cf {

const char* attempt_state_name(AttemptState s) {
  switch (s) {
    case AttemptState::Dispatched: return "Dispatched";
    case AttemptState::Started: return "Started";
    case AttemptState::Succeeded: return "Succeeded";
    case AttemptState::Failed: return "Failed";
    case AttemptState::Cancelled: return "Cancelled";
    case AttemptState::Lost: return "Lost";
    default: return "Unknown";
  }
}

const char* failure_kind_name(FailureKind f) {
  switch (f) {
    case FailureKind::None: return "none";
    case FailureKind::ExecutorFailed: return "executor_failed";
    case FailureKind::NodeLost: return "node_lost";
    case FailureKind::Timeout: return "timeout";
    case FailureKind::Cancelled: return "cancelled";
    case FailureKind::InvalidOutput: return "invalid_output";
    case FailureKind::CudaError: return "cuda_error";
    case FailureKind::Preempted: return "preempted";
    case FailureKind::Internal: return "internal";
    default: return "unknown";
  }
}

}  // namespace cf