#include "compute_fabric/state/state_locality.h"

namespace cf {

void StateLocalityRegistry::upsert(const StateLocation& loc) {
  by_id_[loc.state_id] = loc;
}

void StateLocalityRegistry::remove(const std::string& state_id) {
  by_id_.erase(state_id);
}

std::optional<StateLocation> StateLocalityRegistry::find(
    const std::string& state_id) const {
  auto it = by_id_.find(state_id);
  if (it == by_id_.end()) return std::nullopt;
  return it->second;
}

std::vector<NodeId> StateLocalityRegistry::resident_nodes(
    const std::string& state_id) const {
  auto it = by_id_.find(state_id);
  if (it == by_id_.end()) return {};
  return it->second.resident_nodes;
}

bool StateLocalityRegistry::resident_on(const std::string& state_id,
                                        const NodeId& node) const {
  auto it = by_id_.find(state_id);
  if (it == by_id_.end()) return false;
  for (const auto& n : it->second.resident_nodes) {
    if (n == node) return true;
  }
  return false;
}

std::vector<StateLocation> StateLocalityRegistry::all() const {
  std::vector<StateLocation> out;
  for (const auto& [id, loc] : by_id_) out.push_back(loc);
  return out;
}

double StateLocalityRegistry::transfer_seconds_for(
    const ComputeTask& task, const NodeId& node,
    double bandwidth_bytes_per_sec, double transfer_latency_seconds) const {
  double total = 0.0;
  for (const auto& dep : task.state_dependencies) {
    if (resident_on(dep.state_id, node)) continue;  // already local
    if (dep.transfer_estimate_seconds >= 0.0) {
      total += dep.transfer_estimate_seconds;
      continue;
    }
    auto loc = find(dep.state_id);
    if (loc && loc->transfer_estimate_seconds >= 0.0) {
      total += loc->transfer_estimate_seconds;
      continue;
    }
    uint64_t bytes = dep.size_bytes;
    if (loc) bytes = loc->size_bytes;
    if (bandwidth_bytes_per_sec > 0) {
      total += static_cast<double>(bytes) / bandwidth_bytes_per_sec;
    }
    total += transfer_latency_seconds;
  }
  return total;
}

double StateLocalityRegistry::recompute_seconds_for(
    const ComputeTask& task) const {
  double total = 0.0;
  for (const auto& dep : task.state_dependencies) {
    if (dep.recompute_estimate_seconds >= 0.0) {
      total += dep.recompute_estimate_seconds;
      continue;
    }
    auto loc = find(dep.state_id);
    if (loc && loc->recompute_estimate_seconds >= 0.0) {
      total += loc->recompute_estimate_seconds;
      continue;
    }
  }
  return total;
}

uint64_t StateLocalityRegistry::state_bytes_for(
    const ComputeTask& task) const {
  uint64_t total = 0;
  for (const auto& dep : task.state_dependencies) {
    auto loc = find(dep.state_id);
    if (loc) {
      total += loc->size_bytes;
    } else {
      total += dep.size_bytes;
    }
  }
  return total;
}

}  // namespace cf