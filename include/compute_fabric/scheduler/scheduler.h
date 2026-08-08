#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/resource/capacity.h"
#include "compute_fabric/resource/reservation.h"
#include "compute_fabric/scheduler/cost_model.h"
#include "compute_fabric/scheduler/placements.h"
#include "compute_fabric/state/state_locality.h"
#include "compute_fabric/task/task.h"

namespace cf {

// Per (task_class, backend, node) execution history. Simple, deterministic,
// optional estimate source for the cost-aware policy.
struct HistoryKey {
  TaskClass task_class;
  ExecutorType backend;
  NodeId node;
  friend bool operator<(const HistoryKey& a, const HistoryKey& b) {
    if (a.task_class != b.task_class) return a.task_class < b.task_class;
    if (a.backend != b.backend) return a.backend < b.backend;
    return a.node < b.node;
  }
};

struct HistoryStats {
  uint64_t run_count = 0;
  uint64_t success_count = 0;
  uint64_t failure_count = 0;
  double mean_duration_seconds = 0.0;
  double recent_duration_seconds = 0.0;  // last completed duration
  uint64_t bytes_transferred = 0;
  double queue_delay_seconds = 0.0;
};

class SchedulerPolicyEngine {
 public:
  SchedulerPolicyEngine() = default;

  void set_policy(SchedulerPolicy p) { policy_ = p; }
  SchedulerPolicy policy() const { return policy_; }

  CostWeights& weights() { return cost_model_.weights; }
  const CostWeights& weights() const { return cost_model_.weights; }
  CostContext& context() { return cost_model_.context; }
  const CostContext& context() const { return cost_model_.context; }

  // Records a completed attempt into history.
  void record_history(const HistoryKey& key, double duration_seconds,
                      bool success, uint64_t bytes_transferred);

  // Deterministic placement of a task.
  // nodes: all registered nodes (any health); offline/draining excluded here.
  PlacementDecision place(const ComputeTask& task,
                          const std::map<NodeId, NodeRecord>& nodes,
                          const ReservationRegistry& reservations,
                          const StateLocalityRegistry& state) const;

  // Candidate set for a task (deterministic node-id order).
  std::vector<NodeId> eligible_nodes(
      const ComputeTask& task,
      const std::map<NodeId, NodeRecord>& nodes) const;

  // Constraint check; returns empty when the node is eligible, otherwise the
  // exclusion reasons.
  std::vector<std::string> exclusion_reasons(
      const ComputeTask& task, const NodeRecord& node) const;

  // Stable tie-breaking order: score, then node_id, then executor/device.
  static bool better(const CandidateScore& a, const CandidateScore& b);

 private:
  SchedulerPolicy policy_ = SchedulerPolicy::CostAware;
  CostModel cost_model_;
  std::map<HistoryKey, HistoryStats> history_;

  // True when the node can at least attempt the task (executor present,
  // capability-compatible, capacity sufficient).
  bool capability_compatible(const ComputeTask& task,
                             const NodeRecord& node) const;
};

}  // namespace cf