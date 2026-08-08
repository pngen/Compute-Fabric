#include "compute_fabric/protocol/serialize.h"

namespace cf::proto {

namespace {

constexpr uint32_t kMaxString = 1u << 20;
constexpr uint32_t kMaxList = 1u << 16;

bool read_optional_id(BinaryReader& r, std::optional<ComputeTaskId>& out) {
  uint8_t present = 0;
  if (!r.read_u8(present)) return false;
  if (present) {
    Id128 id;
    if (!r.read_id(id)) return false;
    out = id;
  } else {
    out.reset();
  }
  return true;
}

void write_optional_id(BinaryWriter& w, const std::optional<ComputeTaskId>& v) {
  if (v) {
    w.put_u8(1);
    w.put_id(*v);
  } else {
    w.put_u8(0);
  }
}

}  // namespace

void serialize_retry_policy(BinaryWriter& w, const RetryPolicy& p) {
  w.put_u32(p.max_attempts);
  w.put_bool(p.retry_on_node_loss);
  w.put_bool(p.retry_on_executor_failure);
  w.put_bool(p.retry_on_timeout);
  w.put_u64(p.retry_backoff_ms);
}

Result<RetryPolicy> deserialize_retry_policy(BinaryReader& r) {
  RetryPolicy p;
  if (!r.read_u32(p.max_attempts)) return r.fail("retry max_attempts");
  if (!r.read_bool(p.retry_on_node_loss)) return r.fail("retry on_node_loss");
  if (!r.read_bool(p.retry_on_executor_failure)) return r.fail("retry on_executor_failure");
  if (!r.read_bool(p.retry_on_timeout)) return r.fail("retry on_timeout");
  if (!r.read_u64(p.retry_backoff_ms)) return r.fail("retry backoff");
  return p;
}

void serialize_state_dep(BinaryWriter& w, const StateDependency& d) {
  w.put_string(d.state_id);
  w.put_string(d.generation);
  w.put_u64(d.size_bytes);
  w.put_u32(static_cast<uint32_t>(d.resident_nodes.size()));
  for (const auto& n : d.resident_nodes) w.put_id(n);
  w.put_f64(d.locality_quality);
  w.put_f64(d.transfer_estimate_seconds);
  w.put_f64(d.recompute_estimate_seconds);
  w.put_bool(d.provider_backed);
  w.put_string(d.compatibility);
}

Result<StateDependency> deserialize_state_dep(BinaryReader& r) {
  StateDependency d;
  if (!r.read_string(d.state_id, kMaxString)) return r.fail("state id");
  if (!r.read_string(d.generation, kMaxString)) return r.fail("state generation");
  if (!r.read_u64(d.size_bytes)) return r.fail("state size");
  uint32_t n = 0;
  if (!r.read_u32(n) || n > kMaxList) return r.fail("state resident count");
  d.resident_nodes.reserve(n);
  for (uint32_t i = 0; i < n; ++i) {
    Id128 id;
    if (!r.read_id(id)) return r.fail("state resident node");
    d.resident_nodes.push_back(id);
  }
  if (!r.read_f64(d.locality_quality)) return r.fail("state locality");
  if (!r.read_f64(d.transfer_estimate_seconds)) return r.fail("state transfer");
  if (!r.read_f64(d.recompute_estimate_seconds)) return r.fail("state recompute");
  if (!r.read_bool(d.provider_backed)) return r.fail("state provider");
  if (!r.read_string(d.compatibility, kMaxString)) return r.fail("state compat");
  return d;
}

void serialize_task(BinaryWriter& w, const ComputeTask& t) {
  w.put_id(t.id);
  w.put_id(t.workload);
  w.put_string(t.name);
  w.put_u8(static_cast<uint8_t>(t.task_class));
  w.put_u8(static_cast<uint8_t>(t.required_executor));
  w.put_u8(static_cast<uint8_t>(t.min_compute_capability_major));
  w.put_u8(static_cast<uint8_t>(t.min_compute_capability_minor));
  w.put_u32(t.required_cpu_slots);
  w.put_u32(t.required_gpu_slots);
  w.put_u64(t.scratch_memory_bytes);
  w.put_u32(static_cast<uint32_t>(t.dependencies.size()));
  for (const auto& d : t.dependencies) w.put_id(d);
  w.put_u32(static_cast<uint32_t>(t.state_dependencies.size()));
  for (const auto& d : t.state_dependencies) serialize_state_dep(w, d);
  w.put_u64(t.expected_input_bytes);
  w.put_u64(t.expected_output_bytes);
  w.put_f64(t.estimated_duration_seconds);
  w.put_bool(t.has_deadline);
  w.put_i64(t.deadline_epoch_ms);
  w.put_u8(static_cast<uint8_t>(t.priority));
  serialize_retry_policy(w, t.retry);
  w.put_u32(static_cast<uint32_t>(t.preferred_nodes.size()));
  for (const auto& n : t.preferred_nodes) w.put_id(n);
  w.put_u32(static_cast<uint32_t>(t.forbidden_nodes.size()));
  for (const auto& n : t.forbidden_nodes) w.put_id(n);
  w.put_u32(static_cast<uint32_t>(t.required_tags.size()));
  for (const auto& s : t.required_tags) w.put_string(s);
  w.put_u32(static_cast<uint32_t>(t.preferred_tags.size()));
  for (const auto& s : t.preferred_tags) w.put_string(s);
  w.put_string(t.anti_affinity_group);
  write_optional_id(w, t.same_node_as_task);
  write_optional_id(w, t.different_node_from_task);
  w.put_f64(t.expected_transfer_gb);
  w.put_f64(t.expected_recompute_cost_seconds);
  w.put_f64(t.cost_estimate);
  w.put_bool(t.preemptible);
  w.put_u8(static_cast<uint8_t>(t.kernel));
  w.put_u64(t.kernel_param_n);
  w.put_u64(t.kernel_param_a);
  w.put_u64(t.kernel_param_b);
  w.put_u64(t.seed);
  w.put_u32(static_cast<uint32_t>(t.user_tags.size()));
  for (const auto& [k, v] : t.user_tags) {
    w.put_string(k);
    w.put_string(v);
  }
  w.put_u32(static_cast<uint32_t>(t.payload.size()));
  w.put_bytes(t.payload.data(), t.payload.size());
}

Result<ComputeTask> deserialize_task(BinaryReader& r) {
  ComputeTask t;
  if (!r.read_id(t.id)) return r.fail("task id");
  if (!r.read_id(t.workload)) return r.fail("task workload");
  if (!r.read_string(t.name, kMaxString)) return r.fail("task name");
  uint8_t tc = 0;
  if (!r.read_u8(tc)) return r.fail("task class");
  t.task_class = static_cast<TaskClass>(tc);
  uint8_t re = 0;
  if (!r.read_u8(re)) return r.fail("task executor");
  t.required_executor = static_cast<ExecutorType>(re);
  uint8_t cc_major = 0, cc_minor = 0;
  if (!r.read_u8(cc_major) || !r.read_u8(cc_minor)) return r.fail("task cap");
  t.min_compute_capability_major = cc_major;
  t.min_compute_capability_minor = cc_minor;
  if (!r.read_u32(t.required_cpu_slots)) return r.fail("task cpu slots");
  if (!r.read_u32(t.required_gpu_slots)) return r.fail("task gpu slots");
  if (!r.read_u64(t.scratch_memory_bytes)) return r.fail("task scratch");
  uint32_t nd = 0;
  if (!r.read_u32(nd) || nd > kMaxList) return r.fail("task dep count");
  t.dependencies.reserve(nd);
  for (uint32_t i = 0; i < nd; ++i) {
    Id128 d;
    if (!r.read_id(d)) return r.fail("task dep");
    t.dependencies.push_back(d);
  }
  uint32_t ns = 0;
  if (!r.read_u32(ns) || ns > kMaxList) return r.fail("task state count");
  t.state_dependencies.reserve(ns);
  for (uint32_t i = 0; i < ns; ++i) {
    auto d = deserialize_state_dep(r);
    if (d.failed()) return d.error();
    t.state_dependencies.push_back(d.value());
  }
  if (!r.read_u64(t.expected_input_bytes)) return r.fail("task input bytes");
  if (!r.read_u64(t.expected_output_bytes)) return r.fail("task output bytes");
  if (!r.read_f64(t.estimated_duration_seconds)) return r.fail("task duration");
  if (!r.read_bool(t.has_deadline)) return r.fail("task deadline flag");
  if (!r.read_i64(t.deadline_epoch_ms)) return r.fail("task deadline");
  uint8_t pr = 0;
  if (!r.read_u8(pr)) return r.fail("task priority");
  t.priority = static_cast<Priority>(pr);
  auto rp = deserialize_retry_policy(r);
  if (rp.failed()) return rp.error();
  t.retry = rp.value();
  uint32_t np = 0;
  if (!r.read_u32(np) || np > kMaxList) return r.fail("task pref count");
  t.preferred_nodes.reserve(np);
  for (uint32_t i = 0; i < np; ++i) {
    Id128 n;
    if (!r.read_id(n)) return r.fail("task pref node");
    t.preferred_nodes.push_back(n);
  }
  uint32_t nf = 0;
  if (!r.read_u32(nf) || nf > kMaxList) return r.fail("task forbid count");
  t.forbidden_nodes.reserve(nf);
  for (uint32_t i = 0; i < nf; ++i) {
    Id128 n;
    if (!r.read_id(n)) return r.fail("task forbid node");
    t.forbidden_nodes.push_back(n);
  }
  uint32_t nrt = 0;
  if (!r.read_u32(nrt) || nrt > kMaxList) return r.fail("task req tags");
  t.required_tags.reserve(nrt);
  for (uint32_t i = 0; i < nrt; ++i) {
    std::string s;
    if (!r.read_string(s, kMaxString)) return r.fail("task req tag");
    t.required_tags.push_back(s);
  }
  uint32_t npt = 0;
  if (!r.read_u32(npt) || npt > kMaxList) return r.fail("task pref tags");
  t.preferred_tags.reserve(npt);
  for (uint32_t i = 0; i < npt; ++i) {
    std::string s;
    if (!r.read_string(s, kMaxString)) return r.fail("task pref tag");
    t.preferred_tags.push_back(s);
  }
  if (!r.read_string(t.anti_affinity_group, kMaxString)) return r.fail("task anti-aff");
  std::optional<ComputeTaskId> sn, dn;
  if (!read_optional_id(r, sn)) return r.fail("task same_node");
  if (!read_optional_id(r, dn)) return r.fail("task diff_node");
  t.same_node_as_task = sn;
  t.different_node_from_task = dn;
  if (!r.read_f64(t.expected_transfer_gb)) return r.fail("task xfer gb");
  if (!r.read_f64(t.expected_recompute_cost_seconds)) return r.fail("task recompute");
  if (!r.read_f64(t.cost_estimate)) return r.fail("task cost");
  if (!r.read_bool(t.preemptible)) return r.fail("task preemptible");
  uint8_t k = 0;
  if (!r.read_u8(k)) return r.fail("task kernel");
  t.kernel = static_cast<KernelType>(k);
  if (!r.read_u64(t.kernel_param_n)) return r.fail("task kn");
  if (!r.read_u64(t.kernel_param_a)) return r.fail("task ka");
  if (!r.read_u64(t.kernel_param_b)) return r.fail("task kb");
  if (!r.read_u64(t.seed)) return r.fail("task seed");
  uint32_t ntags = 0;
  if (!r.read_u32(ntags) || ntags > kMaxList) return r.fail("task user tags");
  for (uint32_t i = 0; i < ntags; ++i) {
    std::string kk, vv;
    if (!r.read_string(kk, kMaxString) || !r.read_string(vv, kMaxString))
      return r.fail("task user tag");
    t.user_tags[kk] = vv;
  }
  uint32_t npl = 0;
  if (!r.read_u32(npl) || npl > kMaxString) return r.fail("task payload len");
  if (!r.read_raw_bytes(t.payload, npl)) return r.fail("task payload");
  return t;
}

void serialize_workload(BinaryWriter& w, const Workload& wl) {
  w.put_id(wl.id);
  w.put_string(wl.name);
  w.put_u32(static_cast<uint32_t>(wl.user_tags.size()));
  for (const auto& [k, v] : wl.user_tags) {
    w.put_string(k);
    w.put_string(v);
  }
}

Result<Workload> deserialize_workload(BinaryReader& r) {
  Workload wl;
  if (!r.read_id(wl.id)) return r.fail("workload id");
  if (!r.read_string(wl.name, kMaxString)) return r.fail("workload name");
  uint32_t n = 0;
  if (!r.read_u32(n) || n > kMaxList) return r.fail("workload tag count");
  for (uint32_t i = 0; i < n; ++i) {
    std::string k, v;
    if (!r.read_string(k, kMaxString) || !r.read_string(v, kMaxString))
      return r.fail("workload tag");
    wl.user_tags[k] = v;
  }
  return wl;
}

void serialize_attempt(BinaryWriter& w, const ExecutionAttempt& a) {
  w.put_id(a.id);
  w.put_id(a.task_id);
  w.put_id(a.node_id);
  w.put_id(a.node_session);
  w.put_u64(a.coordinator_epoch);
  w.put_id(a.reservation_id);
  w.put_u8(static_cast<uint8_t>(a.executor));
  w.put_u32(a.attempt_number);
  w.put_i64(a.dispatched_at_ms);
  w.put_i64(a.started_at_ms);
  w.put_i64(a.completed_at_ms);
  w.put_u8(static_cast<uint8_t>(a.state));
  w.put_u8(static_cast<uint8_t>(a.failure_kind));
  w.put_string(a.failure_reason);
  w.put_u64(a.output_size);
  w.put_string(a.output_integrity);
  w.put_f64(a.cpu_millis);
  w.put_u64(a.bytes_transferred);
  w.put_string(a.device);
  w.put_bool(a.committed);
}

Result<ExecutionAttempt> deserialize_attempt(BinaryReader& r) {
  ExecutionAttempt a;
  if (!r.read_id(a.id)) return r.fail("attempt id");
  if (!r.read_id(a.task_id)) return r.fail("attempt task");
  if (!r.read_id(a.node_id)) return r.fail("attempt node");
  if (!r.read_id(a.node_session)) return r.fail("attempt session");
  if (!r.read_u64(a.coordinator_epoch)) return r.fail("attempt epoch");
  if (!r.read_id(a.reservation_id)) return r.fail("attempt reservation");
  uint8_t ex = 0;
  if (!r.read_u8(ex)) return r.fail("attempt executor");
  a.executor = static_cast<ExecutorType>(ex);
  if (!r.read_u32(a.attempt_number)) return r.fail("attempt number");
  if (!r.read_i64(a.dispatched_at_ms)) return r.fail("attempt dispatched");
  if (!r.read_i64(a.started_at_ms)) return r.fail("attempt started");
  if (!r.read_i64(a.completed_at_ms)) return r.fail("attempt completed");
  uint8_t st = 0;
  if (!r.read_u8(st)) return r.fail("attempt state");
  a.state = static_cast<AttemptState>(st);
  uint8_t fk = 0;
  if (!r.read_u8(fk)) return r.fail("attempt failure kind");
  a.failure_kind = static_cast<FailureKind>(fk);
  if (!r.read_string(a.failure_reason, kMaxString)) return r.fail("attempt fail reason");
  if (!r.read_u64(a.output_size)) return r.fail("attempt out size");
  if (!r.read_string(a.output_integrity, kMaxString)) return r.fail("attempt integrity");
  if (!r.read_f64(a.cpu_millis)) return r.fail("attempt cpu ms");
  if (!r.read_u64(a.bytes_transferred)) return r.fail("attempt bytes");
  if (!r.read_string(a.device, kMaxString)) return r.fail("attempt device");
  if (!r.read_bool(a.committed)) return r.fail("attempt committed");
  return a;
}

void serialize_placement(BinaryWriter& w, const PlacementDecision& d) {
  w.put_bool(d.feasible);
  w.put_bool(d.chosen_node.has_value());
  if (d.chosen_node) w.put_id(*d.chosen_node);
  w.put_bool(d.chosen_executor.has_value());
  if (d.chosen_executor) w.put_u8(static_cast<uint8_t>(*d.chosen_executor));
  w.put_string(d.chosen_device);
  w.put_u32(static_cast<uint32_t>(d.candidates.size()));
  for (const auto& c : d.candidates) {
    w.put_id(c.node_id);
    w.put_u8(static_cast<uint8_t>(c.executor));
    w.put_string(c.device);
    w.put_f64(c.score);
    w.put_f64(c.execution_cost);
    w.put_f64(c.queue_cost);
    w.put_f64(c.transfer_cost);
    w.put_f64(c.recompute_cost);
    w.put_f64(c.pressure_cost);
    w.put_f64(c.affinity_cost);
    w.put_f64(c.capability_cost);
    w.put_f64(c.state_locality_score);
    w.put_f64(c.resource_pressure_score);
  }
  w.put_u32(static_cast<uint32_t>(d.excluded.size()));
  for (const auto& e : d.excluded) {
    w.put_id(e.node_id);
    w.put_u32(static_cast<uint32_t>(e.reasons.size()));
    for (const auto& reason : e.reasons) w.put_string(reason);
  }
  w.put_f64(d.total_cost);
  w.put_f64(d.execution_cost);
  w.put_f64(d.queue_cost);
  w.put_f64(d.transfer_cost);
  w.put_f64(d.recompute_cost);
  w.put_f64(d.pressure_cost);
  w.put_f64(d.affinity_cost);
  w.put_f64(d.capability_cost);
  w.put_f64(d.state_locality_score);
  w.put_f64(d.resource_pressure_score);
  w.put_string(d.reason);
}

Result<PlacementDecision> deserialize_placement(BinaryReader& r) {
  PlacementDecision d;
  if (!r.read_bool(d.feasible)) return r.fail("placement feasible");
  uint8_t has_node = 0;
  if (!r.read_u8(has_node)) return r.fail("placement node flag");
  if (has_node) {
    Id128 id;
    if (!r.read_id(id)) return r.fail("placement node");
    d.chosen_node = id;
  }
  uint8_t has_exec = 0;
  if (!r.read_u8(has_exec)) return r.fail("placement exec flag");
  if (has_exec) {
    uint8_t e = 0;
    if (!r.read_u8(e)) return r.fail("placement exec");
    d.chosen_executor = static_cast<ExecutorType>(e);
  }
  if (!r.read_string(d.chosen_device, kMaxString)) return r.fail("placement device");
  uint32_t nc = 0;
  if (!r.read_u32(nc) || nc > kMaxList) return r.fail("placement cand count");
  d.candidates.reserve(nc);
  for (uint32_t i = 0; i < nc; ++i) {
    CandidateScore c;
    if (!r.read_id(c.node_id)) return r.fail("placement cand node");
    uint8_t e = 0;
    if (!r.read_u8(e)) return r.fail("placement cand exec");
    c.executor = static_cast<ExecutorType>(e);
    if (!r.read_string(c.device, kMaxString)) return r.fail("placement cand device");
    if (!r.read_f64(c.score)) return r.fail("placement cand score");
    if (!r.read_f64(c.execution_cost)) return r.fail("placement cand exec cost");
    if (!r.read_f64(c.queue_cost)) return r.fail("placement cand queue cost");
    if (!r.read_f64(c.transfer_cost)) return r.fail("placement cand xfer cost");
    if (!r.read_f64(c.recompute_cost)) return r.fail("placement cand recompute");
    if (!r.read_f64(c.pressure_cost)) return r.fail("placement cand pressure");
    if (!r.read_f64(c.affinity_cost)) return r.fail("placement cand affinity");
    if (!r.read_f64(c.capability_cost)) return r.fail("placement cand cap");
    if (!r.read_f64(c.state_locality_score)) return r.fail("placement cand locality");
    if (!r.read_f64(c.resource_pressure_score)) return r.fail("placement cand pressure");
    d.candidates.push_back(c);
  }
  uint32_t ne = 0;
  if (!r.read_u32(ne) || ne > kMaxList) return r.fail("placement excl count");
  d.excluded.reserve(ne);
  for (uint32_t i = 0; i < ne; ++i) {
    Exclusion e;
    if (!r.read_id(e.node_id)) return r.fail("placement excl node");
    uint32_t nr = 0;
    if (!r.read_u32(nr) || nr > kMaxList) return r.fail("placement excl reason count");
    for (uint32_t j = 0; j < nr; ++j) {
      std::string reason;
      if (!r.read_string(reason, kMaxString)) return r.fail("placement excl reason");
      e.reasons.push_back(reason);
    }
    d.excluded.push_back(e);
  }
  if (!r.read_f64(d.total_cost)) return r.fail("placement total");
  if (!r.read_f64(d.execution_cost)) return r.fail("placement exec");
  if (!r.read_f64(d.queue_cost)) return r.fail("placement queue");
  if (!r.read_f64(d.transfer_cost)) return r.fail("placement xfer");
  if (!r.read_f64(d.recompute_cost)) return r.fail("placement recompute");
  if (!r.read_f64(d.pressure_cost)) return r.fail("placement pressure");
  if (!r.read_f64(d.affinity_cost)) return r.fail("placement affinity");
  if (!r.read_f64(d.capability_cost)) return r.fail("placement cap");
  if (!r.read_f64(d.state_locality_score)) return r.fail("placement locality");
  if (!r.read_f64(d.resource_pressure_score)) return r.fail("placement pressure");
  if (!r.read_string(d.reason, kMaxString)) return r.fail("placement reason");
  return d;
}

}  // namespace cf::proto