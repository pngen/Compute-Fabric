#include "compute_fabric/protocol/messages.h"

namespace cf::proto {

namespace {

constexpr uint32_t kMaxStr = 1u << 20;
constexpr uint32_t kMaxList = 1u << 16;

}  // namespace

const char* message_type_name(uint16_t t) {
  switch (t) {
    case kHello: return "hello";
    case kNodeRegister: return "node_register";
    case kNodeRegisterAck: return "node_register_ack";
    case kNodeHeartbeat: return "node_heartbeat";
    case kNodeHeartbeatAck: return "node_heartbeat_ack";
    case kNodeShutdown: return "node_shutdown";
    case kNodeShutdownAck: return "node_shutdown_ack";
    case kTaskSubmit: return "task_submit";
    case kTaskSubmitResponse: return "task_submit_response";
    case kTaskInspect: return "task_inspect";
    case kTaskInspectResponse: return "task_inspect_response";
    case kTaskList: return "task_list";
    case kTaskListResponse: return "task_list_response";
    case kTaskCancel: return "task_cancel";
    case kTaskCancelResponse: return "task_cancel_response";
    case kWorkloadInspect: return "workload_inspect";
    case kWorkloadInspectResponse: return "workload_inspect_response";
    case kFabricStatus: return "fabric_status";
    case kFabricStatusResponse: return "fabric_status_response";
    case kPlacementQuery: return "placement_query";
    case kPlacementResponse: return "placement_response";
    case kNodeList: return "node_list";
    case kNodeListResponse: return "node_list_response";
    case kNodeDrain: return "node_drain";
    case kNodeDrainResponse: return "node_drain_response";
    case kCoordinatorShutdown: return "coordinator_shutdown";
    case kCoordinatorShutdownResponse: return "coordinator_shutdown_response";
    case kDispatchAttempt: return "dispatch_attempt";
    case kDispatchAck: return "dispatch_ack";
    case kAttemptStarted: return "attempt_started";
    case kAttemptProgress: return "attempt_progress";
    case kAttemptComplete: return "attempt_complete";
    case kAttemptCompleteAck: return "attempt_complete_ack";
    case kAttemptFailed: return "attempt_failed";
    case kAttemptFailedAck: return "attempt_failed_ack";
    case kAttemptCancelled: return "attempt_cancelled";
    case kAttemptCancel: return "attempt_cancel";
    case kAttemptCancelAck: return "attempt_cancel_ack";
    case kReservationGrant: return "reservation_grant";
    case kReservationRelease: return "reservation_release";
    case kReservationReleaseAck: return "reservation_release_ack";
    case kErrorResponse: return "error_response";
    default: return "unknown";
  }
}

// ---- Hello ----
void serialize(BinaryWriter& w, const Hello& m) {
  w.put_u16(m.protocol_version);
  w.put_string(m.role);
}
Result<Hello> deserialize_hello(BinaryReader& r) {
  Hello m;
  if (!r.read_u16(m.protocol_version)) return r.fail("hello version");
  if (!r.read_string(m.role, kMaxStr)) return r.fail("hello role");
  return m;
}

// ---- Node register ----
void serialize_capacity(BinaryWriter& w, const NodeCapacity& c) {
  w.put_i32(c.cpu_worker_slots);
  w.put_u64(c.host_memory_scheduling_bytes);
  w.put_u32(static_cast<uint32_t>(c.executors.size()));
  for (const auto& e : c.executors) {
    w.put_u8(static_cast<uint8_t>(e.type));
    w.put_string(e.name);
    w.put_i32(e.slots);
    w.put_i32(e.major);
    w.put_i32(e.minor);
    w.put_string(e.vendor);
    w.put_string(e.architecture);
    w.put_string(e.device_name);
    w.put_u64(e.device_memory_bytes);
    w.put_u32(static_cast<uint32_t>(e.device_ids.size()));
    for (auto id : e.device_ids) w.put_i32(id);
    w.put_f64(e.relative_speed);
    w.put_bool(e.healthy);
    w.put_bool(e.supports_preemption);
  }
  w.put_u32(static_cast<uint32_t>(c.tags.size()));
  for (const auto& [k, v] : c.tags) {
    w.put_string(k);
    w.put_string(v);
  }
  w.put_string(c.address);
  w.put_string(c.label);
}

Result<ExecutorCapability> deserialize_executor_cap(BinaryReader& r) {
  ExecutorCapability e;
  uint8_t t = 0;
  if (!r.read_u8(t)) return r.fail("executor type");
  e.type = static_cast<ExecutorType>(t);
  if (!r.read_string(e.name, kMaxStr)) return r.fail("executor name");
  if (!r.read_i32(e.slots)) return r.fail("executor slots");
  if (!r.read_i32(e.major)) return r.fail("executor major");
  if (!r.read_i32(e.minor)) return r.fail("executor minor");
  if (!r.read_string(e.vendor, kMaxStr)) return r.fail("executor vendor");
  if (!r.read_string(e.architecture, kMaxStr)) return r.fail("executor arch");
  if (!r.read_string(e.device_name, kMaxStr)) return r.fail("executor device");
  if (!r.read_u64(e.device_memory_bytes)) return r.fail("executor mem");
  uint32_t nd = 0;
  if (!r.read_u32(nd) || nd > kMaxList) return r.fail("executor device count");
  for (uint32_t i = 0; i < nd; ++i) {
    int32_t id = 0;
    if (!r.read_i32(id)) return r.fail("executor device id");
    e.device_ids.push_back(id);
  }
  if (!r.read_f64(e.relative_speed)) return r.fail("executor speed");
  if (!r.read_bool(e.healthy)) return r.fail("executor healthy");
  if (!r.read_bool(e.supports_preemption)) return r.fail("executor preempt");
  return e;
}

Result<NodeCapacity> deserialize_capacity(BinaryReader& r) {
  NodeCapacity c;
  if (!r.read_i32(c.cpu_worker_slots)) return r.fail("capacity cpu slots");
  if (!r.read_u64(c.host_memory_scheduling_bytes)) return r.fail("capacity mem");
  uint32_t ne = 0;
  if (!r.read_u32(ne) || ne > kMaxList) return r.fail("capacity executors");
  c.executors.reserve(ne);
  for (uint32_t i = 0; i < ne; ++i) {
    auto e = deserialize_executor_cap(r);
    if (e.failed()) return e.error();
    c.executors.push_back(e.value());
  }
  uint32_t nt = 0;
  if (!r.read_u32(nt) || nt > kMaxList) return r.fail("capacity tags");
  for (uint32_t i = 0; i < nt; ++i) {
    std::string k, v;
    if (!r.read_string(k, kMaxStr) || !r.read_string(v, kMaxStr))
      return r.fail("capacity tag");
    c.tags[k] = v;
  }
  if (!r.read_string(c.address, kMaxStr)) return r.fail("capacity address");
  if (!r.read_string(c.label, kMaxStr)) return r.fail("capacity label");
  return c;
}

void serialize(BinaryWriter& w, const NodeRegister& m) {
  w.put_id(m.node_id);
  w.put_string(m.label);
  w.put_string(m.address);
  serialize_capacity(w, m.capacity);
}
Result<NodeRegister> deserialize_node_register(BinaryReader& r) {
  NodeRegister m;
  if (!r.read_id(m.node_id)) return r.fail("register node id");
  if (!r.read_string(m.label, kMaxStr)) return r.fail("register label");
  if (!r.read_string(m.address, kMaxStr)) return r.fail("register address");
  auto c = deserialize_capacity(r);
  if (c.failed()) return c.error();
  m.capacity = c.value();
  return m;
}

void serialize(BinaryWriter& w, const NodeRegisterAck& m) {
  w.put_id(m.node_id);
  w.put_id(m.session_id);
  w.put_u64(m.coordinator_epoch);
  w.put_u64(m.session_epoch);
  w.put_string(m.error);
}
Result<NodeRegisterAck> deserialize_node_register_ack(BinaryReader& r) {
  NodeRegisterAck m;
  if (!r.read_id(m.node_id)) return r.fail("ack node id");
  if (!r.read_id(m.session_id)) return r.fail("ack session id");
  if (!r.read_u64(m.coordinator_epoch)) return r.fail("ack epoch");
  if (!r.read_u64(m.session_epoch)) return r.fail("ack session epoch");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("ack error");
  return m;
}

// ---- Heartbeat ----
void serialize(BinaryWriter& w, const NodeHeartbeat& m) {
  w.put_id(m.node_id);
  w.put_id(m.session_id);
  w.put_u64(m.session_epoch);
  w.put_u64(m.coordinator_epoch);
  w.put_u32(m.active_cpu_slots);
  w.put_u32(m.active_gpu_slots);
  w.put_u32(static_cast<uint32_t>(m.executors.size()));
  for (const auto& e : m.executors) {
    w.put_string(e.name);
    w.put_u32(e.active);
  }
}
Result<NodeHeartbeat> deserialize_node_heartbeat(BinaryReader& r) {
  NodeHeartbeat m;
  if (!r.read_id(m.node_id)) return r.fail("hb node id");
  if (!r.read_id(m.session_id)) return r.fail("hb session id");
  if (!r.read_u64(m.session_epoch)) return r.fail("hb session epoch");
  if (!r.read_u64(m.coordinator_epoch)) return r.fail("hb epoch");
  if (!r.read_u32(m.active_cpu_slots)) return r.fail("hb cpu");
  if (!r.read_u32(m.active_gpu_slots)) return r.fail("hb gpu");
  uint32_t ne = 0;
  if (!r.read_u32(ne) || ne > kMaxList) return r.fail("hb executors");
  for (uint32_t i = 0; i < ne; ++i) {
    ExecutorRuntimeSnapshot s;
    if (!r.read_string(s.name, kMaxStr)) return r.fail("hb exec name");
    if (!r.read_u32(s.active)) return r.fail("hb exec active");
    m.executors.push_back(s);
  }
  return m;
}

void serialize(BinaryWriter& w, const NodeHeartbeatAck& m) {
  w.put_bool(m.accepted);
  w.put_string(m.error);
}
Result<NodeHeartbeatAck> deserialize_node_heartbeat_ack(BinaryReader& r) {
  NodeHeartbeatAck m;
  if (!r.read_bool(m.accepted)) return r.fail("hb ack accepted");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("hb ack error");
  return m;
}

// ---- Node shutdown ----
void serialize(BinaryWriter& w, const NodeShutdown& m) {
  w.put_id(m.node_id);
  w.put_id(m.session_id);
}
Result<NodeShutdown> deserialize_node_shutdown(BinaryReader& r) {
  NodeShutdown m;
  if (!r.read_id(m.node_id)) return r.fail("shutdown node id");
  if (!r.read_id(m.session_id)) return r.fail("shutdown session id");
  return m;
}
void serialize(BinaryWriter& w, const NodeShutdownAck& m) {
  w.put_bool(m.accepted);
}
Result<NodeShutdownAck> deserialize_node_shutdown_ack(BinaryReader& r) {
  NodeShutdownAck m;
  if (!r.read_bool(m.accepted)) return r.fail("shutdown ack");
  return m;
}

// ---- Task submit ----
void serialize(BinaryWriter& w, const TaskSubmit& m) {
  w.put_id(m.workload_id);
  w.put_string(m.workload_name);
  serialize_task(w, m.task);
}
Result<TaskSubmit> deserialize_task_submit(BinaryReader& r) {
  TaskSubmit m;
  if (!r.read_id(m.workload_id)) return r.fail("submit workload");
  if (!r.read_string(m.workload_name, kMaxStr)) return r.fail("submit workload name");
  auto t = deserialize_task(r);
  if (t.failed()) return t.error();
  m.task = t.value();
  return m;
}
void serialize(BinaryWriter& w, const TaskSubmitResponse& m) {
  w.put_bool(m.accepted);
  w.put_id(m.task_id);
  w.put_string(m.error);
}
Result<TaskSubmitResponse> deserialize_task_submit_response(BinaryReader& r) {
  TaskSubmitResponse m;
  if (!r.read_bool(m.accepted)) return r.fail("submit resp accepted");
  if (!r.read_id(m.task_id)) return r.fail("submit resp task");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("submit resp error");
  return m;
}

// ---- Task inspect ----
void serialize(BinaryWriter& w, const TaskInspect& m) { w.put_id(m.task_id); }
Result<TaskInspect> deserialize_task_inspect(BinaryReader& r) {
  TaskInspect m;
  if (!r.read_id(m.task_id)) return r.fail("inspect task id");
  return m;
}
void serialize(BinaryWriter& w, const TaskInspectResponse& m) {
  w.put_bool(m.found);
  w.put_id(m.task_id);
  w.put_string(m.name);
  w.put_u8(static_cast<uint8_t>(m.task_class));
  w.put_u8(static_cast<uint8_t>(m.state));
  w.put_string(m.error);
  w.put_u32(static_cast<uint32_t>(m.attempts.size()));
  for (const auto& a : m.attempts) serialize_attempt(w, a);
  w.put_u32(m.retry_count);
  w.put_string(m.workload_id_hex);
}
Result<TaskInspectResponse> deserialize_task_inspect_response(BinaryReader& r) {
  TaskInspectResponse m;
  if (!r.read_bool(m.found)) return r.fail("inspect found");
  if (!r.read_id(m.task_id)) return r.fail("inspect task id");
  if (!r.read_string(m.name, kMaxStr)) return r.fail("inspect name");
  uint8_t tc = 0, st = 0;
  if (!r.read_u8(tc)) return r.fail("inspect class");
  if (!r.read_u8(st)) return r.fail("inspect state");
  m.task_class = static_cast<TaskClass>(tc);
  m.state = static_cast<TaskState>(st);
  if (!r.read_string(m.error, kMaxStr)) return r.fail("inspect error");
  uint32_t na = 0;
  if (!r.read_u32(na) || na > kMaxList) return r.fail("inspect attempts");
  for (uint32_t i = 0; i < na; ++i) {
    auto a = deserialize_attempt(r);
    if (a.failed()) return a.error();
    m.attempts.push_back(a.value());
  }
  if (!r.read_u32(m.retry_count)) return r.fail("inspect retry");
  if (!r.read_string(m.workload_id_hex, kMaxStr)) return r.fail("inspect workload");
  return m;
}

// ---- Task list ----
void serialize(BinaryWriter& w, const TaskList& m) { w.put_string(m.filter_state); }
Result<TaskList> deserialize_task_list(BinaryReader& r) {
  TaskList m;
  if (!r.read_string(m.filter_state, kMaxStr)) return r.fail("task list filter");
  return m;
}
void serialize(BinaryWriter& w, const TaskListEntry& e) {
  w.put_id(e.task_id);
  w.put_string(e.name);
  w.put_u8(static_cast<uint8_t>(e.state));
  w.put_u8(static_cast<uint8_t>(e.task_class));
  w.put_u8(static_cast<uint8_t>(e.priority));
}
Result<TaskListEntry> deserialize_task_list_entry(BinaryReader& r) {
  TaskListEntry e;
  if (!r.read_id(e.task_id)) return r.fail("list entry id");
  if (!r.read_string(e.name, kMaxStr)) return r.fail("list entry name");
  uint8_t st = 0, tc = 0, pr = 0;
  if (!r.read_u8(st) || !r.read_u8(tc) || !r.read_u8(pr))
    return r.fail("list entry state");
  e.state = static_cast<TaskState>(st);
  e.task_class = static_cast<TaskClass>(tc);
  e.priority = static_cast<Priority>(pr);
  return e;
}
void serialize(BinaryWriter& w, const TaskListResponse& m) {
  w.put_u32(static_cast<uint32_t>(m.tasks.size()));
  for (const auto& e : m.tasks) serialize(w, e);
}
Result<TaskListResponse> deserialize_task_list_response(BinaryReader& r) {
  TaskListResponse m;
  uint32_t n = 0;
  if (!r.read_u32(n) || n > kMaxList) return r.fail("list resp count");
  for (uint32_t i = 0; i < n; ++i) {
    auto e = deserialize_task_list_entry(r);
    if (e.failed()) return e.error();
    m.tasks.push_back(e.value());
  }
  return m;
}

// ---- Task cancel ----
void serialize(BinaryWriter& w, const TaskCancel& m) { w.put_id(m.task_id); }
Result<TaskCancel> deserialize_task_cancel(BinaryReader& r) {
  TaskCancel m;
  if (!r.read_id(m.task_id)) return r.fail("cancel task id");
  return m;
}
void serialize(BinaryWriter& w, const TaskCancelResponse& m) {
  w.put_bool(m.found);
  w.put_bool(m.cancelled);
  w.put_string(m.error);
}
Result<TaskCancelResponse> deserialize_task_cancel_response(BinaryReader& r) {
  TaskCancelResponse m;
  if (!r.read_bool(m.found)) return r.fail("cancel found");
  if (!r.read_bool(m.cancelled)) return r.fail("cancel flag");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("cancel error");
  return m;
}

// ---- Workload inspect ----
void serialize(BinaryWriter& w, const WorkloadInspect& m) { w.put_id(m.workload_id); }
Result<WorkloadInspect> deserialize_workload_inspect(BinaryReader& r) {
  WorkloadInspect m;
  if (!r.read_id(m.workload_id)) return r.fail("workload inspect id");
  return m;
}
void serialize(BinaryWriter& w, const WorkloadInspectResponse& m) {
  w.put_bool(m.found);
  w.put_id(m.workload_id);
  w.put_string(m.name);
  w.put_u32(static_cast<uint32_t>(m.tasks.size()));
  for (const auto& e : m.tasks) serialize(w, e);
  w.put_string(m.error);
}
Result<WorkloadInspectResponse> deserialize_workload_inspect_response(BinaryReader& r) {
  WorkloadInspectResponse m;
  if (!r.read_bool(m.found)) return r.fail("workload found");
  if (!r.read_id(m.workload_id)) return r.fail("workload id");
  if (!r.read_string(m.name, kMaxStr)) return r.fail("workload name");
  uint32_t n = 0;
  if (!r.read_u32(n) || n > kMaxList) return r.fail("workload tasks");
  for (uint32_t i = 0; i < n; ++i) {
    auto e = deserialize_task_list_entry(r);
    if (e.failed()) return e.error();
    m.tasks.push_back(e.value());
  }
  if (!r.read_string(m.error, kMaxStr)) return r.fail("workload error");
  return m;
}

// ---- Fabric status ----
void serialize(BinaryWriter& w, const FabricStatus& m) { w.put_string(m.detail); }
Result<FabricStatus> deserialize_fabric_status(BinaryReader& r) {
  FabricStatus m;
  if (!r.read_string(m.detail, kMaxStr)) return r.fail("fabric detail");
  return m;
}
void serialize(BinaryWriter& w, const NodeStatusEntry& e) {
  w.put_id(e.node_id);
  w.put_string(e.label);
  w.put_string(e.address);
  w.put_u8(static_cast<uint8_t>(e.health));
  w.put_bool(e.connected);
  w.put_u32(e.cpu_slots);
  w.put_u32(e.cpu_used);
  w.put_u64(e.session_epoch);
  w.put_u32(static_cast<uint32_t>(e.executors.size()));
  for (const auto& s : e.executors) w.put_string(s);
}
Result<NodeStatusEntry> deserialize_node_status_entry(BinaryReader& r) {
  NodeStatusEntry e;
  if (!r.read_id(e.node_id)) return r.fail("node status id");
  if (!r.read_string(e.label, kMaxStr)) return r.fail("node status label");
  if (!r.read_string(e.address, kMaxStr)) return r.fail("node status address");
  uint8_t h = 0;
  if (!r.read_u8(h)) return r.fail("node status health");
  e.health = static_cast<NodeHealth>(h);
  if (!r.read_bool(e.connected)) return r.fail("node status connected");
  if (!r.read_u32(e.cpu_slots)) return r.fail("node status cpu slots");
  if (!r.read_u32(e.cpu_used)) return r.fail("node status cpu used");
  if (!r.read_u64(e.session_epoch)) return r.fail("node status session epoch");
  uint32_t n = 0;
  if (!r.read_u32(n) || n > kMaxList) return r.fail("node status executors");
  for (uint32_t i = 0; i < n; ++i) {
    std::string s;
    if (!r.read_string(s, kMaxStr)) return r.fail("node status executor");
    e.executors.push_back(s);
  }
  return e;
}
void serialize(BinaryWriter& w, const FabricStatusResponse& m) {
  w.put_u64(m.coordinator_epoch);
  w.put_string(m.policy);
  w.put_u32(m.node_count);
  w.put_u32(m.task_count);
  w.put_u32(m.running_count);
  w.put_u32(m.reserved_count);
  w.put_u32(m.completed_count);
  w.put_u32(static_cast<uint32_t>(m.nodes.size()));
  for (const auto& e : m.nodes) serialize(w, e);
  w.put_string(m.error);
}
Result<FabricStatusResponse> deserialize_fabric_status_response(BinaryReader& r) {
  FabricStatusResponse m;
  if (!r.read_u64(m.coordinator_epoch)) return r.fail("status epoch");
  if (!r.read_string(m.policy, kMaxStr)) return r.fail("status policy");
  if (!r.read_u32(m.node_count)) return r.fail("status nodes");
  if (!r.read_u32(m.task_count)) return r.fail("status tasks");
  if (!r.read_u32(m.running_count)) return r.fail("status running");
  if (!r.read_u32(m.reserved_count)) return r.fail("status reserved");
  if (!r.read_u32(m.completed_count)) return r.fail("status completed");
  uint32_t n = 0;
  if (!r.read_u32(n) || n > kMaxList) return r.fail("status node list");
  for (uint32_t i = 0; i < n; ++i) {
    auto e = deserialize_node_status_entry(r);
    if (e.failed()) return e.error();
    m.nodes.push_back(e.value());
  }
  if (!r.read_string(m.error, kMaxStr)) return r.fail("status error");
  return m;
}

// ---- Placement ----
void serialize(BinaryWriter& w, const PlacementQuery& m) { serialize_task(w, m.task); }
Result<PlacementQuery> deserialize_placement_query(BinaryReader& r) {
  PlacementQuery m;
  auto t = deserialize_task(r);
  if (t.failed()) return t.error();
  m.task = t.value();
  return m;
}
void serialize(BinaryWriter& w, const PlacementResponse& m) {
  w.put_bool(m.found);
  serialize_placement(w, m.decision);
  w.put_string(m.error);
}
Result<PlacementResponse> deserialize_placement_response(BinaryReader& r) {
  PlacementResponse m;
  if (!r.read_bool(m.found)) return r.fail("placement found");
  auto d = deserialize_placement(r);
  if (d.failed()) return d.error();
  m.decision = d.value();
  if (!r.read_string(m.error, kMaxStr)) return r.fail("placement error");
  return m;
}

// ---- Node list ----
void serialize(BinaryWriter& w, const NodeList& m) { (void)w; (void)m; }
Result<NodeList> deserialize_node_list(BinaryReader& r) {
  (void)r;
  return NodeList{};
}
void serialize(BinaryWriter& w, const NodeListResponse& m) {
  w.put_u32(static_cast<uint32_t>(m.nodes.size()));
  for (const auto& e : m.nodes) serialize(w, e);
}
Result<NodeListResponse> deserialize_node_list_response(BinaryReader& r) {
  NodeListResponse m;
  uint32_t n = 0;
  if (!r.read_u32(n) || n > kMaxList) return r.fail("node list count");
  for (uint32_t i = 0; i < n; ++i) {
    auto e = deserialize_node_status_entry(r);
    if (e.failed()) return e.error();
    m.nodes.push_back(e.value());
  }
  return m;
}

// ---- Node drain ----
void serialize(BinaryWriter& w, const NodeDrain& m) {
  w.put_id(m.node_id);
  w.put_bool(m.drain);
}
Result<NodeDrain> deserialize_node_drain(BinaryReader& r) {
  NodeDrain m;
  if (!r.read_id(m.node_id)) return r.fail("drain node id");
  if (!r.read_bool(m.drain)) return r.fail("drain flag");
  return m;
}
void serialize(BinaryWriter& w, const NodeDrainResponse& m) {
  w.put_bool(m.found);
  w.put_bool(m.accepted);
  w.put_string(m.error);
}
Result<NodeDrainResponse> deserialize_node_drain_response(BinaryReader& r) {
  NodeDrainResponse m;
  if (!r.read_bool(m.found)) return r.fail("drain found");
  if (!r.read_bool(m.accepted)) return r.fail("drain accepted");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("drain error");
  return m;
}

// ---- Coordinator shutdown ----
void serialize(BinaryWriter& w, const CoordinatorShutdown& m) { (void)w; (void)m; }
Result<CoordinatorShutdown> deserialize_coordinator_shutdown(BinaryReader& r) {
  (void)r;
  return CoordinatorShutdown{};
}
void serialize(BinaryWriter& w, const CoordinatorShutdownResponse& m) {
  w.put_bool(m.accepted);
  w.put_string(m.error);
}
Result<CoordinatorShutdownResponse> deserialize_coordinator_shutdown_response(BinaryReader& r) {
  CoordinatorShutdownResponse m;
  if (!r.read_bool(m.accepted)) return r.fail("cood shutdown accepted");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("coor shutdown error");
  return m;
}

// ---- Dispatch ----
void serialize(BinaryWriter& w, const DispatchAttempt& m) {
  serialize_task(w, m.task);
  w.put_id(m.attempt_id);
  w.put_id(m.reservation_id);
  w.put_u64(m.coordinator_epoch);
  w.put_u32(m.attempt_number);
}
Result<DispatchAttempt> deserialize_dispatch_attempt(BinaryReader& r) {
  DispatchAttempt m;
  auto t = deserialize_task(r);
  if (t.failed()) return t.error();
  m.task = t.value();
  if (!r.read_id(m.attempt_id)) return r.fail("dispatch attempt id");
  if (!r.read_id(m.reservation_id)) return r.fail("dispatch reservation");
  if (!r.read_u64(m.coordinator_epoch)) return r.fail("dispatch epoch");
  if (!r.read_u32(m.attempt_number)) return r.fail("dispatch attempt num");
  return m;
}
void serialize(BinaryWriter& w, const DispatchAck& m) {
  w.put_id(m.attempt_id);
  w.put_bool(m.accepted);
  w.put_string(m.error);
}
Result<DispatchAck> deserialize_dispatch_ack(BinaryReader& r) {
  DispatchAck m;
  if (!r.read_id(m.attempt_id)) return r.fail("dispatch ack id");
  if (!r.read_bool(m.accepted)) return r.fail("dispatch ack flag");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("dispatch ack error");
  return m;
}

// ---- Attempt lifecycle ----
void serialize(BinaryWriter& w, const AttemptStarted& m) {
  w.put_id(m.attempt_id);
  w.put_i64(m.started_at_ms);
  w.put_string(m.device);
}
Result<AttemptStarted> deserialize_attempt_started(BinaryReader& r) {
  AttemptStarted m;
  if (!r.read_id(m.attempt_id)) return r.fail("started id");
  if (!r.read_i64(m.started_at_ms)) return r.fail("started at");
  if (!r.read_string(m.device, kMaxStr)) return r.fail("started device");
  return m;
}
void serialize(BinaryWriter& w, const AttemptProgress& m) {
  w.put_id(m.attempt_id);
  w.put_u32(m.percent);
}
Result<AttemptProgress> deserialize_attempt_progress(BinaryReader& r) {
  AttemptProgress m;
  if (!r.read_id(m.attempt_id)) return r.fail("progress id");
  if (!r.read_u32(m.percent)) return r.fail("progress pct");
  return m;
}
void serialize(BinaryWriter& w, const AttemptComplete& m) {
  w.put_id(m.attempt_id);
  w.put_id(m.task_id);
  w.put_id(m.reservation_id);
  w.put_u64(m.coordinator_epoch);
  w.put_u8(m.result_code);
  w.put_u64(m.output_size);
  w.put_string(m.output_integrity);
  w.put_f64(m.duration_ms);
  w.put_f64(m.cpu_millis);
  w.put_u64(m.bytes_transferred);
  w.put_string(m.device);
  w.put_u32(static_cast<uint32_t>(m.notes.size()));
  for (const auto& n : m.notes) w.put_string(n);
}
Result<AttemptComplete> deserialize_attempt_complete(BinaryReader& r) {
  AttemptComplete m;
  if (!r.read_id(m.attempt_id)) return r.fail("complete id");
  if (!r.read_id(m.task_id)) return r.fail("complete task");
  if (!r.read_id(m.reservation_id)) return r.fail("complete resv");
  if (!r.read_u64(m.coordinator_epoch)) return r.fail("complete epoch");
  if (!r.read_u8(m.result_code)) return r.fail("complete code");
  if (!r.read_u64(m.output_size)) return r.fail("complete size");
  if (!r.read_string(m.output_integrity, kMaxStr)) return r.fail("complete integrity");
  if (!r.read_f64(m.duration_ms)) return r.fail("complete duration");
  if (!r.read_f64(m.cpu_millis)) return r.fail("complete cpu");
  if (!r.read_u64(m.bytes_transferred)) return r.fail("complete bytes");
  if (!r.read_string(m.device, kMaxStr)) return r.fail("complete device");
  uint32_t nn = 0;
  if (!r.read_u32(nn) || nn > kMaxList) return r.fail("complete notes");
  for (uint32_t i = 0; i < nn; ++i) {
    std::string s;
    if (!r.read_string(s, kMaxStr)) return r.fail("complete note");
    m.notes.push_back(s);
  }
  return m;
}
void serialize(BinaryWriter& w, const AttemptFailed& m) {
  w.put_id(m.attempt_id);
  w.put_id(m.task_id);
  w.put_id(m.reservation_id);
  w.put_u64(m.coordinator_epoch);
  w.put_u8(m.failure_kind);
  w.put_string(m.failure_reason);
  w.put_f64(m.duration_ms);
}
Result<AttemptFailed> deserialize_attempt_failed(BinaryReader& r) {
  AttemptFailed m;
  if (!r.read_id(m.attempt_id)) return r.fail("fail id");
  if (!r.read_id(m.task_id)) return r.fail("fail task");
  if (!r.read_id(m.reservation_id)) return r.fail("fail resv");
  if (!r.read_u64(m.coordinator_epoch)) return r.fail("fail epoch");
  if (!r.read_u8(m.failure_kind)) return r.fail("fail kind");
  if (!r.read_string(m.failure_reason, kMaxStr)) return r.fail("fail reason");
  if (!r.read_f64(m.duration_ms)) return r.fail("fail duration");
  return m;
}
void serialize(BinaryWriter& w, const AttemptCancelled& m) {
  w.put_id(m.attempt_id);
  w.put_id(m.task_id);
  w.put_id(m.reservation_id);
  w.put_u64(m.coordinator_epoch);
  w.put_string(m.reason);
}
Result<AttemptCancelled> deserialize_attempt_cancelled(BinaryReader& r) {
  AttemptCancelled m;
  if (!r.read_id(m.attempt_id)) return r.fail("cancelled id");
  if (!r.read_id(m.task_id)) return r.fail("cancelled task");
  if (!r.read_id(m.reservation_id)) return r.fail("cancelled resv");
  if (!r.read_u64(m.coordinator_epoch)) return r.fail("cancelled epoch");
  if (!r.read_string(m.reason, kMaxStr)) return r.fail("cancelled reason");
  return m;
}
void serialize(BinaryWriter& w, const AttemptCancel& m) {
  w.put_id(m.attempt_id);
  w.put_string(m.reason);
}
Result<AttemptCancel> deserialize_attempt_cancel(BinaryReader& r) {
  AttemptCancel m;
  if (!r.read_id(m.attempt_id)) return r.fail("cancel attempt id");
  if (!r.read_string(m.reason, kMaxStr)) return r.fail("cancel attempt reason");
  return m;
}
void serialize(BinaryWriter& w, const AttemptCancelAck& m) {
  w.put_id(m.attempt_id);
  w.put_bool(m.accepted);
}
Result<AttemptCancelAck> deserialize_attempt_cancel_ack(BinaryReader& r) {
  AttemptCancelAck m;
  if (!r.read_id(m.attempt_id)) return r.fail("cancel ack id");
  if (!r.read_bool(m.accepted)) return r.fail("cancel ack flag");
  return m;
}
void serialize(BinaryWriter& w, const AttemptAck& m) {
  w.put_id(m.attempt_id);
  w.put_bool(m.accepted);
  w.put_string(m.error);
}
Result<AttemptAck> deserialize_attempt_ack(BinaryReader& r) {
  AttemptAck m;
  if (!r.read_id(m.attempt_id)) return r.fail("ack id");
  if (!r.read_bool(m.accepted)) return r.fail("ack flag");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("ack error");
  return m;
}

// ---- Reservation ----
void serialize(BinaryWriter& w, const ReservationGrant& m) {
  w.put_id(m.reservation_id);
  w.put_id(m.task_id);
  w.put_u8(static_cast<uint8_t>(m.executor));
  w.put_u32(m.slots);
}
Result<ReservationGrant> deserialize_reservation_grant(BinaryReader& r) {
  ReservationGrant m;
  if (!r.read_id(m.reservation_id)) return r.fail("grant resv");
  if (!r.read_id(m.task_id)) return r.fail("grant task");
  uint8_t e = 0;
  if (!r.read_u8(e)) return r.fail("grant executor");
  m.executor = static_cast<ExecutorType>(e);
  if (!r.read_u32(m.slots)) return r.fail("grant slots");
  return m;
}
void serialize(BinaryWriter& w, const ReservationRelease& m) {
  w.put_id(m.reservation_id);
  w.put_id(m.task_id);
  w.put_id(m.node_id);
  w.put_id(m.session_id);
  w.put_u64(m.coordinator_epoch);
}
Result<ReservationRelease> deserialize_reservation_release(BinaryReader& r) {
  ReservationRelease m;
  if (!r.read_id(m.reservation_id)) return r.fail("release resv");
  if (!r.read_id(m.task_id)) return r.fail("release task");
  if (!r.read_id(m.node_id)) return r.fail("release node");
  if (!r.read_id(m.session_id)) return r.fail("release session");
  if (!r.read_u64(m.coordinator_epoch)) return r.fail("release epoch");
  return m;
}
void serialize(BinaryWriter& w, const ReservationReleaseAck& m) {
  w.put_id(m.reservation_id);
  w.put_bool(m.accepted);
  w.put_string(m.error);
}
Result<ReservationReleaseAck> deserialize_reservation_release_ack(BinaryReader& r) {
  ReservationReleaseAck m;
  if (!r.read_id(m.reservation_id)) return r.fail("release ack resv");
  if (!r.read_bool(m.accepted)) return r.fail("release ack flag");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("release ack error");
  return m;
}

// ---- Error ----
void serialize(BinaryWriter& w, const ErrorResponse& m) {
  w.put_u16(m.for_message_type);
  w.put_string(m.error);
}
Result<ErrorResponse> deserialize_error_response(BinaryReader& r) {
  ErrorResponse m;
  if (!r.read_u16(m.for_message_type)) return r.fail("error for type");
  if (!r.read_string(m.error, kMaxStr)) return r.fail("error msg");
  return m;
}

}  // namespace cf::proto