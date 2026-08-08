#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/task/task.h"

namespace cf {

// Where a reusable state object is believed to be held, with locality
// quality and transfer/recompute estimates. In standalone mode these are
// supplied directly via task StateDependency entries; with a Context Fabric
// adapter they may be resolved through ContextStateProvider.
struct StateLocation {
  std::string state_id;
  std::string generation;
  uint64_t size_bytes = 0;
  std::vector<NodeId> resident_nodes;
  double locality_quality = 0.0;
  double transfer_estimate_seconds = -1.0;   // -1 = not provided
  double recompute_estimate_seconds = -1.0;  // -1 = not provided
  std::string compatibility;
};

// Coordinator-side registry of state-locality facts. Deterministic and
// bounded. Populated from task submissions (standalone) or adapter queries.
class StateLocalityRegistry {
 public:
  void upsert(const StateLocation& loc);
  void remove(const std::string& state_id);

  // Best-effort lookup; returns nullopt when unknown.
  std::optional<StateLocation> find(const std::string& state_id) const;

  // Nodes believed to hold the given state (empty when unknown).
  std::vector<NodeId> resident_nodes(const std::string& state_id) const;

  // Whether any candidate node holds (a compatible copy of) the state.
  bool resident_on(const std::string& state_id, const NodeId& node) const;

  size_t size() const { return by_id_.size(); }

  // Deterministic listing by state id.
  std::vector<StateLocation> all() const;

  // Combined transfer estimate in seconds for a task's state deps if the task
  // ran on `node`; uses per-dependency transfer estimates when provided,
  // otherwise bytes / default_bandwidth.
  double transfer_seconds_for(const ComputeTask& task, const NodeId& node,
                              double bandwidth_bytes_per_sec,
                              double transfer_latency_seconds) const;

  // Combined recompute estimate in seconds for a task's state deps.
  double recompute_seconds_for(const ComputeTask& task) const;

  // Sum of state bytes for a task.
  uint64_t state_bytes_for(const ComputeTask& task) const;

 private:
  std::map<std::string, StateLocation> by_id_;
};

}  // namespace cf