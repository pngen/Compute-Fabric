#include "compute_fabric/scheduler/scheduler.h"

#include <algorithm>

namespace cf {

namespace {

const ExecutorCapability* find_executor_cap(const NodeRecord& node,
                                            ExecutorType type) {
  // Any_ resolves to the CPU executor (the universal baseline).
  if (type == ExecutorType::Any_) type = ExecutorType::Cpu;
  for (const auto& e : node.capacity.executors) {
    if (e.type == type) return &e;
  }
  return nullptr;
}

}  // namespace

void SchedulerPolicyEngine::record_history(const HistoryKey& key,
                                           double duration_seconds,
                                           bool success,
                                           uint64_t bytes_transferred) {
  auto& s = history_[key];
  s.run_count += 1;
  if (success) s.success_count += 1;
  else s.failure_count += 1;
  s.recent_duration_seconds = duration_seconds;
  double total = s.mean_duration_seconds * (s.run_count - 1);
  s.mean_duration_seconds = (total + duration_seconds) / s.run_count;
  s.bytes_transferred += bytes_transferred;
}

bool SchedulerPolicyEngine::capability_compatible(
    const ComputeTask& task, const NodeRecord& node) const {
  const auto* cap = find_executor_cap(node, task.required_executor);
  if (cap == nullptr) return false;
  if (!cap->healthy) return false;
  if (cap->slots <= 0) return false;
  if (task.required_executor == ExecutorType::Cuda) {
    if (task.min_compute_capability_major > 0) {
      if (cap->major < task.min_compute_capability_major) return false;
      if (cap->major == task.min_compute_capability_major &&
          cap->minor < task.min_compute_capability_minor)
        return false;
    }
  }
  return true;
}

std::vector<std::string> SchedulerPolicyEngine::exclusion_reasons(
    const ComputeTask& task, const NodeRecord& node) const {
  std::vector<std::string> reasons;
  if (node.health == NodeHealth::Offline) reasons.push_back("node offline");
  if (node.health == NodeHealth::Draining) reasons.push_back("node draining");
  if (node.health == NodeHealth::Suspect) reasons.push_back("node suspect");
  if (!node.connected && node.health != NodeHealth::Online) {
    reasons.push_back("not connected");
  }
  if (!capability_compatible(task, node)) {
    reasons.push_back("capability mismatch or executor unavailable");
  }
  const auto* cap = find_executor_cap(node, task.required_executor);
  if (cap != nullptr) {
    if (cap->slots < static_cast<int32_t>(
                         task.required_executor == ExecutorType::Cuda
                             ? task.required_gpu_slots
                             : task.required_cpu_slots)) {
      reasons.push_back("insufficient executor slots");
    }
  }
  if (task.scratch_memory_bytes > node.capacity.host_memory_scheduling_bytes) {
    reasons.push_back("insufficient scratch budget");
  }
  for (const auto& fn : task.forbidden_nodes) {
    if (fn == node.id) reasons.push_back("node forbidden by task");
  }
  for (const auto& rt : task.required_tags) {
    if (node.capacity.tags.count(rt) == 0) {
      reasons.push_back("missing required tag: " + rt);
    }
  }
  return reasons;
}

std::vector<NodeId> SchedulerPolicyEngine::eligible_nodes(
    const ComputeTask& task,
    const std::map<NodeId, NodeRecord>& nodes) const {
  std::vector<NodeId> out;
  for (const auto& [id, node] : nodes) {
    if (exclusion_reasons(task, node).empty()) out.push_back(id);
  }
  return out;
}

bool SchedulerPolicyEngine::better(const CandidateScore& a,
                                   const CandidateScore& b) {
  if (a.score != b.score) return a.score < b.score;
  if (a.node_id != b.node_id) return a.node_id < b.node_id;
  return a.executor < b.executor;
}

PlacementDecision SchedulerPolicyEngine::place(
    const ComputeTask& task, const std::map<NodeId, NodeRecord>& nodes,
    const ReservationRegistry& reservations,
    const StateLocalityRegistry& state) const {
  PlacementDecision d;
  d.state_locality_score = 0.0;
  d.resource_pressure_score = 0.0;

  std::vector<CandidateScore> candidates;
  for (const auto& [id, node] : nodes) {
    (void)id;
    Exclusion ex;
    ex.node_id = node.id;
    ex.reasons = exclusion_reasons(task, node);
    if (!ex.reasons.empty()) {
      d.excluded.push_back(ex);
      continue;
    }

    CandidateScore c;
    c.node_id = node.id;
    const auto* cap = find_executor_cap(node, task.required_executor);
    c.executor = cap ? cap->type : task.required_executor;
    c.device = cap ? cap->name : "unknown";

    if (policy_ == SchedulerPolicy::Baseline) {
      // Deterministic baseline: capability first, then state-local node,
      // then lowest node id.
      double score = 0.0;
      score += state.transfer_seconds_for(task, node.id,
                                          cost_model_.context.bandwidth_bytes_per_sec,
                                          cost_model_.context.transfer_latency_seconds);
      score += task.estimated_duration_seconds;
      c.score = score;
      c.state_locality_score = cost_model_.state_locality_score(task, node, state);
      CostBreakdown b = cost_model_.evaluate(task, node, reservations, state);
      c.execution_cost = b.execution;
      c.transfer_cost = b.transfer;
      c.recompute_cost = b.recompute;
      c.queue_cost = b.queue;
      c.resource_pressure_score = b.resource_pressure_score;
    } else {
      double duration = task.estimated_duration_seconds;
      auto hit = history_.find({task.task_class, task.required_executor, node.id});
      if (hit != history_.end() && hit->second.run_count > 0) {
        duration = hit->second.mean_duration_seconds;
      }
      CostBreakdown b = cost_model_.evaluate(task, node, reservations, state,
                                             duration);
      c.score = b.total;
      c.execution_cost = b.execution;
      c.queue_cost = b.queue;
      c.transfer_cost = b.transfer;
      c.recompute_cost = b.recompute;
      c.pressure_cost = b.pressure;
      c.affinity_cost = b.affinity;
      c.capability_cost = b.capability;
      c.state_locality_score = b.state_locality_score;
      c.resource_pressure_score = b.resource_pressure_score;
    }
    candidates.push_back(std::move(c));
  }

  d.candidates = candidates;
  if (candidates.empty()) {
    d.feasible = false;
    d.reason = "no schedulable node";
    return d;
  }

  std::sort(candidates.begin(), candidates.end(), better);
  const CandidateScore& best = candidates.front();
  d.feasible = true;
  d.chosen_node = best.node_id;
  d.chosen_executor = best.executor;
  d.chosen_device = best.device;
  d.total_cost = best.score;
  d.execution_cost = best.execution_cost;
  d.queue_cost = best.queue_cost;
  d.transfer_cost = best.transfer_cost;
  d.recompute_cost = best.recompute_cost;
  d.pressure_cost = best.pressure_cost;
  d.affinity_cost = best.affinity_cost;
  d.capability_cost = best.capability_cost;
  d.state_locality_score = best.state_locality_score;
  d.resource_pressure_score = best.resource_pressure_score;
  d.reason = "best candidate";
  return d;
}

}  // namespace cf
