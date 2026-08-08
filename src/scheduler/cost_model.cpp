#include "compute_fabric/scheduler/cost_model.h"

#include <algorithm>

namespace cf {

const ExecutorCapability* find_executor_cap(const NodeRecord& node,
                                            ExecutorType type) {
  if (type == ExecutorType::Any_) type = ExecutorType::Cpu;
  for (const auto& e : node.capacity.executors) {
    if (e.type == type) return &e;
  }
  return nullptr;
}

double CostModel::pressure_score(const NodeRecord& node, ExecutorType executor,
                                 const ReservationRegistry& reservations) const {
  const auto* cap = find_executor_cap(node, executor);
  if (cap == nullptr || cap->slots <= 0) return 1.0;
  uint32_t used_executor = reservations.used_slots(node.id, executor);
  uint32_t used_total = used_executor;
  if (executor == ExecutorType::Cpu) {
    used_total = 0;
    for (const auto& r : reservations.all_active()) {
      if (r.node_id == node.id) used_total += r.slots;
    }
  }
  double ratio = static_cast<double>(used_total) /
                 static_cast<double>(cap->slots > 0 ? cap->slots : 1);
  return std::min(1.0, ratio);
}

double CostModel::queue_cost(const ComputeTask& task, const NodeRecord& node,
                             const ReservationRegistry& reservations) const {
  double total = 0.0;
  for (const auto& r : reservations.all_active()) {
    if (r.node_id != node.id) continue;
    // Estimate remaining duration as the task's own estimate for simplicity.
    total += std::max(0.0, task.estimated_duration_seconds);
  }
  return total;
}

double CostModel::state_locality_score(const ComputeTask& task,
                                       const NodeRecord& node,
                                       const StateLocalityRegistry& state) const {
  if (task.state_dependencies.empty()) return 1.0;
  double resident_bytes = 0.0;
  double total_bytes = 0.0;
  for (const auto& dep : task.state_dependencies) {
    uint64_t bytes = dep.size_bytes;
    auto loc = state.find(dep.state_id);
    if (loc) bytes = loc->size_bytes;
    total_bytes += static_cast<double>(bytes);
    if (state.resident_on(dep.state_id, node.id)) {
      resident_bytes += static_cast<double>(bytes);
    }
  }
  if (total_bytes <= 0.0) return 1.0;
  return resident_bytes / total_bytes;
}

CostBreakdown CostModel::evaluate(const ComputeTask& task,
                                  const NodeRecord& node,
                                  const ReservationRegistry& reservations,
                                  const StateLocalityRegistry& state,
                                  double execution_duration_override) const {
  const auto* cap = find_executor_cap(node, task.required_executor);
  CostBreakdown b;
  if (cap == nullptr) {
    b.capability = weights.capability;
    b.total = b.capability;
    return b;
  }

  double duration = execution_duration_override >= 0.0
                        ? execution_duration_override
                        : task.estimated_duration_seconds;
  b.execution = duration * weights.execution;
  b.queue = queue_cost(task, node, reservations) * weights.queue;
  b.transfer = state.transfer_seconds_for(task, node.id,
                                          context.bandwidth_bytes_per_sec,
                                          context.transfer_latency_seconds) *
               weights.transfer;
  b.recompute = state.recompute_seconds_for(task) * weights.recompute;
  double pressure = pressure_score(node, task.required_executor, reservations);
  b.resource_pressure_score = pressure;
  if (pressure > context.pressure_threshold) {
    b.pressure = (pressure - context.pressure_threshold) * context.pressure_weight *
                 weights.pressure;
  }
  // Affinity: reward preferred nodes, penalize preferred_tags misses.
  double affinity_delta = 0.0;
  for (const auto& pn : task.preferred_nodes) {
    if (pn == node.id) affinity_delta -= 0.1;
  }
  for (const auto& pt : task.preferred_tags) {
    if (node.capacity.tags.count(pt) == 0) affinity_delta += 0.1;
  }
  b.affinity = affinity_delta * weights.affinity;
  b.state_locality_score = state_locality_score(task, node, state);

  b.total = b.execution + b.queue + b.transfer + b.recompute + b.pressure +
            b.affinity + b.capability;
  return b;
}

}  // namespace cf