#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/task/task.h"

namespace cf {

enum class SchedulerPolicy : uint8_t {
  Baseline = 0,
  CostAware = 1,
};

const char* scheduler_policy_name(SchedulerPolicy p);
bool parse_scheduler_policy(const std::string& s, SchedulerPolicy& out);

struct CandidateScore {
  NodeId node_id;
  ExecutorType executor;
  std::string device;  // device/executor name
  double score;
  double execution_cost = 0.0;
  double queue_cost = 0.0;
  double transfer_cost = 0.0;
  double recompute_cost = 0.0;
  double pressure_cost = 0.0;
  double affinity_cost = 0.0;
  double capability_cost = 0.0;
  double state_locality_score = 0.0;
  double resource_pressure_score = 0.0;
};

struct Exclusion {
  NodeId node_id;
  std::vector<std::string> reasons;
};

// Every scheduling decision is explainable: chosen node/executor, the full
// candidate set with scores, exclusions with reasons, and the final reason.
struct PlacementDecision {
  bool feasible = false;
  std::optional<NodeId> chosen_node;
  std::optional<ExecutorType> chosen_executor;
  std::string chosen_device;
  std::vector<CandidateScore> candidates;
  std::vector<Exclusion> excluded;
  double total_cost = 0.0;
  double execution_cost = 0.0;
  double queue_cost = 0.0;
  double transfer_cost = 0.0;
  double recompute_cost = 0.0;
  double pressure_cost = 0.0;
  double affinity_cost = 0.0;
  double capability_cost = 0.0;
  double state_locality_score = 0.0;
  double resource_pressure_score = 0.0;
  std::string reason;
};

}  // namespace cf