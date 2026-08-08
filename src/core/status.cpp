#include "compute_fabric/core/status.h"

#include <cstdint>

namespace cf {

const char* error_code_name(ErrorCode code) {
  switch (code) {
    case ErrorCode::Ok: return "ok";
    case ErrorCode::InvalidArgument: return "invalid_argument";
    case ErrorCode::OutOfBounds: return "out_of_bounds";
    case ErrorCode::MalformedFrame: return "malformed_frame";
    case ErrorCode::ProtocolVersionMismatch: return "protocol_version_mismatch";
    case ErrorCode::Timeout: return "timeout";
    case ErrorCode::NetworkError: return "network_error";
    case ErrorCode::ConnectionClosed: return "connection_closed";
    case ErrorCode::NotConnected: return "not_connected";
    case ErrorCode::IllegalTransition: return "illegal_transition";
    case ErrorCode::TaskNotFound: return "task_not_found";
    case ErrorCode::WorkloadNotFound: return "workload_not_found";
    case ErrorCode::DependencyCycle: return "dep_cycle";
    case ErrorCode::SelfDependency: return "self_dependency";
    case ErrorCode::MissingDependency: return "missing_dependency";
    case ErrorCode::DependencyFailed: return "dependency_failed";
    case ErrorCode::DependencyCancelled: return "dependency_cancelled";
    case ErrorCode::NodeNotFound: return "node_not_found";
    case ErrorCode::NodeOffline: return "node_offline";
    case ErrorCode::NodeDraining: return "node_draining";
    case ErrorCode::StaleSession: return "stale_session";
    case ErrorCode::StaleEpoch: return "stale_epoch";
    case ErrorCode::StaleAttempt: return "stale_attempt";
    case ErrorCode::NoCapacity: return "no_capacity";
    case ErrorCode::ReservationConflict: return "reservation_conflict";
    case ErrorCode::ReservationNotFound: return "reservation_not_found";
    case ErrorCode::ExecutorUnavailable: return "executor_unavailable";
    case ErrorCode::CapabilityMismatch: return "capability_mismatch";
    case ErrorCode::NoSchedulableNode: return "no_schedulable_node";
    case ErrorCode::PersistenceError: return "persistence_error";
    case ErrorCode::CorruptionDetected: return "corruption_detected";
    case ErrorCode::IoError: return "io_error";
    case ErrorCode::CudaError: return "cuda_error";
    case ErrorCode::Unsupported: return "unsupported";
    case ErrorCode::Internal: return "internal";
    case ErrorCode::Cancelled: return "cancelled";
    case ErrorCode::DeadlineExceeded: return "deadline_exceeded";
    case ErrorCode::AlreadyExists: return "already_exists";
    case ErrorCode::InvalidState: return "invalid_state";
    case ErrorCode::DuplicateRequest: return "duplicate_request";
    case ErrorCode::ShuttingDown: return "shutting_down";
    case ErrorCode::LaunchError: return "launch_error";
    case ErrorCode::ProcessError: return "process_error";
    case ErrorCode::IdempotentReplay: return "idempotent_replay";
    default: return "unknown";
  }
}

const char* error_code_message(ErrorCode code) {
  switch (code) {
    case ErrorCode::Ok: return "no error";
    case ErrorCode::InvalidArgument: return "invalid argument";
    case ErrorCode::OutOfBounds: return "value out of bounds";
    case ErrorCode::MalformedFrame: return "malformed network frame";
    case ErrorCode::ProtocolVersionMismatch: return "protocol version mismatch";
    case ErrorCode::Timeout: return "operation timed out";
    case ErrorCode::NetworkError: return "network error";
    case ErrorCode::ConnectionClosed: return "connection closed";
    case ErrorCode::NotConnected: return "not connected";
    case ErrorCode::IllegalTransition: return "illegal lifecycle transition";
    case ErrorCode::TaskNotFound: return "task not found";
    case ErrorCode::WorkloadNotFound: return "workload not found";
    case ErrorCode::DependencyCycle: return "dependency cycle detected";
    case ErrorCode::SelfDependency: return "task depends on itself";
    case ErrorCode::MissingDependency: return "missing dependency";
    case ErrorCode::DependencyFailed: return "dependency failed";
    case ErrorCode::DependencyCancelled: return "dependency cancelled";
    case ErrorCode::NodeNotFound: return "node not found";
    case ErrorCode::NodeOffline: return "node is offline";
    case ErrorCode::NodeDraining: return "node is draining";
    case ErrorCode::StaleSession: return "stale node session";
    case ErrorCode::StaleEpoch: return "stale coordinator epoch";
    case ErrorCode::StaleAttempt: return "stale attempt token";
    case ErrorCode::NoCapacity: return "no capacity available";
    case ErrorCode::ReservationConflict: return "reservation conflict";
    case ErrorCode::ReservationNotFound: return "reservation not found";
    case ErrorCode::ExecutorUnavailable: return "executor unavailable";
    case ErrorCode::CapabilityMismatch: return "capability mismatch";
    case ErrorCode::NoSchedulableNode: return "no schedulable node";
    case ErrorCode::PersistenceError: return "persistence error";
    case ErrorCode::CorruptionDetected: return "state corruption detected";
    case ErrorCode::IoError: return "io error";
    case ErrorCode::CudaError: return "cuda error";
    case ErrorCode::Unsupported: return "unsupported operation";
    case ErrorCode::Internal: return "internal error";
    case ErrorCode::Cancelled: return "operation cancelled";
    case ErrorCode::DeadlineExceeded: return "deadline exceeded";
    case ErrorCode::AlreadyExists: return "already exists";
    case ErrorCode::InvalidState: return "invalid state";
    case ErrorCode::DuplicateRequest: return "duplicate request";
    case ErrorCode::ShuttingDown: return "runtime is shutting down";
    case ErrorCode::LaunchError: return "process launch error";
    case ErrorCode::ProcessError: return "child process error";
    case ErrorCode::IdempotentReplay: return "idempotent replay of request";
    default: return "unknown error";
  }
}

}  // namespace cf