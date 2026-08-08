#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/protocol/frame.h"
#include "compute_fabric/protocol/serialize.h"
#include "compute_fabric/protocol/wire.h"
#include "compute_fabric/resource/capacity.h"
#include "compute_fabric/scheduler/placements.h"
#include "compute_fabric/task/attempt.h"
#include "compute_fabric/task/lifecycle.h"
#include "compute_fabric/task/task.h"

namespace cf::proto {

// Control-plane message types (stable).
enum MessageType : uint16_t {
  kHello = 1,
  kNodeRegister = 2,
  kNodeRegisterAck = 3,
  kNodeHeartbeat = 4,
  kNodeHeartbeatAck = 5,
  kNodeShutdown = 6,
  kNodeShutdownAck = 7,

  kTaskSubmit = 10,
  kTaskSubmitResponse = 11,
  kTaskInspect = 12,
  kTaskInspectResponse = 13,
  kTaskList = 14,
  kTaskListResponse = 15,
  kTaskCancel = 16,
  kTaskCancelResponse = 17,
  kWorkloadInspect = 18,
  kWorkloadInspectResponse = 19,
  kFabricStatus = 20,
  kFabricStatusResponse = 21,
  kPlacementQuery = 22,
  kPlacementResponse = 23,
  kNodeList = 24,
  kNodeListResponse = 25,
  kNodeDrain = 26,
  kNodeDrainResponse = 27,
  kCoordinatorShutdown = 28,
  kCoordinatorShutdownResponse = 29,

  kDispatchAttempt = 30,
  kDispatchAck = 31,
  kAttemptStarted = 32,
  kAttemptProgress = 33,
  kAttemptComplete = 34,
  kAttemptCompleteAck = 35,
  kAttemptFailed = 36,
  kAttemptFailedAck = 37,
  kAttemptCancelled = 38,
  kAttemptCancel = 39,
  kAttemptCancelAck = 40,
  kReservationGrant = 41,
  kReservationRelease = 42,
  kReservationReleaseAck = 43,

  kErrorResponse = 999,
};

const char* message_type_name(uint16_t t);

// ---- Hello ----
struct Hello {
  uint16_t protocol_version = kProtocolVersion;
  std::string role;  // "node" | "client"
};
void serialize(BinaryWriter& w, const Hello& m);
Result<Hello> deserialize_hello(BinaryReader& r);

// ---- Node registration ----
struct NodeRegister {
  NodeId node_id;
  std::string label;
  std::string address;
  NodeCapacity capacity;
};
void serialize(BinaryWriter& w, const NodeRegister& m);
Result<NodeRegister> deserialize_node_register(BinaryReader& r);

struct NodeRegisterAck {
  NodeId node_id;
  NodeSessionId session_id;
  CoordinatorEpoch coordinator_epoch = 0;
  SessionEpoch session_epoch = 0;
  std::string error;  // empty when ok
};
void serialize(BinaryWriter& w, const NodeRegisterAck& m);
Result<NodeRegisterAck> deserialize_node_register_ack(BinaryReader& r);

// ---- Heartbeat ----
struct ExecutorRuntimeSnapshot {
  std::string name;
  uint32_t active = 0;
};

struct NodeHeartbeat {
  NodeId node_id;
  NodeSessionId session_id;
  SessionEpoch session_epoch = 0;
  CoordinatorEpoch coordinator_epoch = 0;
  // Snapshot of current utilization for scheduling hints.
  uint32_t active_cpu_slots = 0;
  uint32_t active_gpu_slots = 0;
  std::vector<ExecutorRuntimeSnapshot> executors;
};
void serialize(BinaryWriter& w, const NodeHeartbeat& m);
Result<NodeHeartbeat> deserialize_node_heartbeat(BinaryReader& r);

struct NodeHeartbeatAck {
  bool accepted = true;
  std::string error;
};
void serialize(BinaryWriter& w, const NodeHeartbeatAck& m);
Result<NodeHeartbeatAck> deserialize_node_heartbeat_ack(BinaryReader& r);

// ---- Node shutdown ----
struct NodeShutdown {
  NodeId node_id;
  NodeSessionId session_id;
};
void serialize(BinaryWriter& w, const NodeShutdown& m);
Result<NodeShutdown> deserialize_node_shutdown(BinaryReader& r);
struct NodeShutdownAck {
  bool accepted = true;
};
void serialize(BinaryWriter& w, const NodeShutdownAck& m);
Result<NodeShutdownAck> deserialize_node_shutdown_ack(BinaryReader& r);

// ---- Task submit ----
struct TaskSubmit {
  WorkloadId workload_id;
  std::string workload_name;
  ComputeTask task;
};
void serialize(BinaryWriter& w, const TaskSubmit& m);
Result<TaskSubmit> deserialize_task_submit(BinaryReader& r);

struct TaskSubmitResponse {
  bool accepted = false;
  ComputeTaskId task_id;
  std::string error;
};
void serialize(BinaryWriter& w, const TaskSubmitResponse& m);
Result<TaskSubmitResponse> deserialize_task_submit_response(BinaryReader& r);

// ---- Task inspect ----
struct TaskInspect {
  ComputeTaskId task_id;
};
void serialize(BinaryWriter& w, const TaskInspect& m);
Result<TaskInspect> deserialize_task_inspect(BinaryReader& r);

struct TaskInspectResponse {
  bool found = false;
  ComputeTaskId task_id;
  std::string name;
  TaskClass task_class = TaskClass::Generic;
  TaskState state = TaskState::Submitted;
  std::string error;
  std::vector<ExecutionAttempt> attempts;
  uint32_t retry_count = 0;
  std::string workload_id_hex;
};
void serialize(BinaryWriter& w, const TaskInspectResponse& m);
Result<TaskInspectResponse> deserialize_task_inspect_response(BinaryReader& r);

// ---- Task list ----
struct TaskList {
  std::string filter_state;  // empty = all
};
void serialize(BinaryWriter& w, const TaskList& m);
Result<TaskList> deserialize_task_list(BinaryReader& r);

struct TaskListEntry {
  ComputeTaskId task_id;
  std::string name;
  TaskState state = TaskState::Submitted;
  TaskClass task_class = TaskClass::Generic;
  Priority priority = Priority::Normal;
};
struct TaskListResponse {
  std::vector<TaskListEntry> tasks;
};
void serialize(BinaryWriter& w, const TaskListResponse& m);
Result<TaskListResponse> deserialize_task_list_response(BinaryReader& r);

// ---- Task cancel ----
struct TaskCancel {
  ComputeTaskId task_id;
};
void serialize(BinaryWriter& w, const TaskCancel& m);
Result<TaskCancel> deserialize_task_cancel(BinaryReader& r);

struct TaskCancelResponse {
  bool found = false;
  bool cancelled = false;
  std::string error;
};
void serialize(BinaryWriter& w, const TaskCancelResponse& m);
Result<TaskCancelResponse> deserialize_task_cancel_response(BinaryReader& r);

// ---- Workload inspect ----
struct WorkloadInspect {
  WorkloadId workload_id;
};
void serialize(BinaryWriter& w, const WorkloadInspect& m);
Result<WorkloadInspect> deserialize_workload_inspect(BinaryReader& r);

struct WorkloadInspectResponse {
  bool found = false;
  WorkloadId workload_id;
  std::string name;
  std::vector<TaskListEntry> tasks;
  std::string error;
};
void serialize(BinaryWriter& w, const WorkloadInspectResponse& m);
Result<WorkloadInspectResponse> deserialize_workload_inspect_response(BinaryReader& r);

// ---- Fabric status ----
struct FabricStatus {
  std::string detail;  // optional
};
void serialize(BinaryWriter& w, const FabricStatus& m);
Result<FabricStatus> deserialize_fabric_status(BinaryReader& r);

struct NodeStatusEntry {
  NodeId node_id;
  std::string label;
  std::string address;
  NodeHealth health = NodeHealth::Online;
  bool connected = false;
  uint32_t cpu_slots = 0;
  uint32_t cpu_used = 0;
  SessionEpoch session_epoch = 0;
  std::vector<std::string> executors;
};
struct FabricStatusResponse {
  CoordinatorEpoch coordinator_epoch = 0;
  std::string policy;
  uint32_t node_count = 0;
  uint32_t task_count = 0;
  uint32_t running_count = 0;
  uint32_t reserved_count = 0;
  uint32_t completed_count = 0;
  std::vector<NodeStatusEntry> nodes;
  std::string error;
};
void serialize(BinaryWriter& w, const FabricStatusResponse& m);
Result<FabricStatusResponse> deserialize_fabric_status_response(BinaryReader& r);

// ---- Placement query ----
struct PlacementQuery {
  ComputeTask task;
};
void serialize(BinaryWriter& w, const PlacementQuery& m);
Result<PlacementQuery> deserialize_placement_query(BinaryReader& r);

struct PlacementResponse {
  bool found = false;
  PlacementDecision decision;
  std::string error;
};
void serialize(BinaryWriter& w, const PlacementResponse& m);
Result<PlacementResponse> deserialize_placement_response(BinaryReader& r);

// ---- Node list ----
struct NodeList {};
void serialize(BinaryWriter& w, const NodeList& m);
Result<NodeList> deserialize_node_list(BinaryReader& r);
struct NodeListResponse {
  std::vector<NodeStatusEntry> nodes;
};
void serialize(BinaryWriter& w, const NodeListResponse& m);
Result<NodeListResponse> deserialize_node_list_response(BinaryReader& r);

// ---- Node drain ----
struct NodeDrain {
  NodeId node_id;
  bool drain = true;
};
void serialize(BinaryWriter& w, const NodeDrain& m);
Result<NodeDrain> deserialize_node_drain(BinaryReader& r);
struct NodeDrainResponse {
  bool found = false;
  bool accepted = false;
  std::string error;
};
void serialize(BinaryWriter& w, const NodeDrainResponse& m);
Result<NodeDrainResponse> deserialize_node_drain_response(BinaryReader& r);

// ---- Coordinator shutdown ----
struct CoordinatorShutdown {};
void serialize(BinaryWriter& w, const CoordinatorShutdown& m);
Result<CoordinatorShutdown> deserialize_coordinator_shutdown(BinaryReader& r);
struct CoordinatorShutdownResponse {
  bool accepted = false;
  std::string error;
};
void serialize(BinaryWriter& w, const CoordinatorShutdownResponse& m);
Result<CoordinatorShutdownResponse> deserialize_coordinator_shutdown_response(BinaryReader& r);

// ---- Dispatch (coordinator -> node) ----
struct DispatchAttempt {
  ComputeTask task;
  ExecutionAttemptId attempt_id;
  ReservationId reservation_id;
  CoordinatorEpoch coordinator_epoch = 0;
  uint32_t attempt_number = 0;
};
void serialize(BinaryWriter& w, const DispatchAttempt& m);
Result<DispatchAttempt> deserialize_dispatch_attempt(BinaryReader& r);

struct DispatchAck {
  ExecutionAttemptId attempt_id;
  bool accepted = false;
  std::string error;
};
void serialize(BinaryWriter& w, const DispatchAck& m);
Result<DispatchAck> deserialize_dispatch_ack(BinaryReader& r);

// ---- Attempt lifecycle ----
struct AttemptStarted {
  ExecutionAttemptId attempt_id;
  int64_t started_at_ms = 0;
  std::string device;
};
void serialize(BinaryWriter& w, const AttemptStarted& m);
Result<AttemptStarted> deserialize_attempt_started(BinaryReader& r);

struct AttemptProgress {
  ExecutionAttemptId attempt_id;
  uint32_t percent = 0;
};
void serialize(BinaryWriter& w, const AttemptProgress& m);
Result<AttemptProgress> deserialize_attempt_progress(BinaryReader& r);

struct AttemptComplete {
  ExecutionAttemptId attempt_id;
  ComputeTaskId task_id;
  ReservationId reservation_id;
  CoordinatorEpoch coordinator_epoch = 0;
  uint8_t result_code = 0;
  uint64_t output_size = 0;
  std::string output_integrity;
  double duration_ms = 0.0;
  double cpu_millis = 0.0;
  uint64_t bytes_transferred = 0;
  std::string device;
  std::vector<std::string> notes;
};
void serialize(BinaryWriter& w, const AttemptComplete& m);
Result<AttemptComplete> deserialize_attempt_complete(BinaryReader& r);

struct AttemptFailed {
  ExecutionAttemptId attempt_id;
  ComputeTaskId task_id;
  ReservationId reservation_id;
  CoordinatorEpoch coordinator_epoch = 0;
  uint8_t failure_kind = 0;
  std::string failure_reason;
  double duration_ms = 0.0;
};
void serialize(BinaryWriter& w, const AttemptFailed& m);
Result<AttemptFailed> deserialize_attempt_failed(BinaryReader& r);

struct AttemptCancelled {
  ExecutionAttemptId attempt_id;
  ComputeTaskId task_id;
  ReservationId reservation_id;
  CoordinatorEpoch coordinator_epoch = 0;
  std::string reason;
};
void serialize(BinaryWriter& w, const AttemptCancelled& m);
Result<AttemptCancelled> deserialize_attempt_cancelled(BinaryReader& r);

struct AttemptCancel {
  ExecutionAttemptId attempt_id;
  std::string reason;
};
void serialize(BinaryWriter& w, const AttemptCancel& m);
Result<AttemptCancel> deserialize_attempt_cancel(BinaryReader& r);
struct AttemptCancelAck {
  ExecutionAttemptId attempt_id;
  bool accepted = false;
};
void serialize(BinaryWriter& w, const AttemptCancelAck& m);
Result<AttemptCancelAck> deserialize_attempt_cancel_ack(BinaryReader& r);

// ---- Acknowledgment for attempt outcomes ----
struct AttemptAck {
  ExecutionAttemptId attempt_id;
  bool accepted = false;
  std::string error;
};
void serialize(BinaryWriter& w, const AttemptAck& m);
Result<AttemptAck> deserialize_attempt_ack(BinaryReader& r);

// ---- Reservation ----
struct ReservationGrant {
  ReservationId reservation_id;
  ComputeTaskId task_id;
  ExecutorType executor = ExecutorType::Cpu;
  uint32_t slots = 0;
};
void serialize(BinaryWriter& w, const ReservationGrant& m);
Result<ReservationGrant> deserialize_reservation_grant(BinaryReader& r);

struct ReservationRelease {
  ReservationId reservation_id;
  ComputeTaskId task_id;
  NodeId node_id;
  NodeSessionId session_id;
  CoordinatorEpoch coordinator_epoch = 0;
};
void serialize(BinaryWriter& w, const ReservationRelease& m);
Result<ReservationRelease> deserialize_reservation_release(BinaryReader& r);

struct ReservationReleaseAck {
  ReservationId reservation_id;
  bool accepted = false;
  std::string error;
};
void serialize(BinaryWriter& w, const ReservationReleaseAck& m);
Result<ReservationReleaseAck> deserialize_reservation_release_ack(BinaryReader& r);

// ---- Error ----
struct ErrorResponse {
  uint16_t for_message_type = 0;
  std::string error;
};
void serialize(BinaryWriter& w, const ErrorResponse& m);
Result<ErrorResponse> deserialize_error_response(BinaryReader& r);

}  // namespace cf::proto