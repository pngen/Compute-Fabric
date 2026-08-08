#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "compute_fabric/resource/capacity.h"
#include "compute_fabric/resource/reservation.h"
#include "compute_fabric/state/state_locality.h"
#include "compute_fabric/task/task.h"

namespace cf {

// Configurable cost-model weights. All weights are configurable; defaults are
// documented in ARCHITECTURE.md.
struct CostWeights {
  double execution = 1.0;
  double queue = 1.0;
  double transfer = 1.0;
  double recompute = 1.0;
  double pressure = 1.0;
  double affinity = 1.0;
  double capability = 1e9;  // effectively an exclusion
  double state_locality = 1.0;
};

struct CostContext {
  // Default transfer bandwidth (bytes/sec) and latency (sec) when no per-dep
  // estimate is available.
  double bandwidth_bytes_per_sec = 1.0e9;   // 1 GB/s
  double transfer_latency_seconds = 0.001;  // 1 ms
  // Pressure scaling: penalty when used/total slots ratio exceeds threshold.
  double pressure_threshold = 0.8;
  double pressure_weight = 2.0;
};

// A single candidate evaluation for a task on a node/executor.
struct CostBreakdown {
  double execution = 0.0;
  double queue = 0.0;
  double transfer = 0.0;
  double recompute = 0.0;
  double pressure = 0.0;
  double affinity = 0.0;
  double capability = 0.0;
  double total = 0.0;
  double state_locality_score = 0.0;
  double resource_pressure_score = 0.0;
};

// Pure, deterministic cost model. Domain: duration-seconds. Lower is better.
class CostModel {
 public:
  CostModel() = default;
  CostWeights weights;
  CostContext context;

  // Evaluates a task on a node/executor given current reservations and state
  // locality. `execution_duration_override` is used by the scheduler to feed
  // historical estimates when available (otherwise task.estimated_duration).
  CostBreakdown evaluate(const ComputeTask& task, const NodeRecord& node,
                         const ReservationRegistry& reservations,
                         const StateLocalityRegistry& state,
                         double execution_duration_override = -1.0) const;

  // Queue cost: sum of estimated durations of active reservations ahead.
  double queue_cost(const ComputeTask& task, const NodeRecord& node,
                    const ReservationRegistry& reservations) const;

  // Resource pressure metric in [0,1] and derived penalty.
  double pressure_score(const NodeRecord& node, ExecutorType executor,
                        const ReservationRegistry& reservations) const;

  // State-locality score in [0,1]; 1.0 when all state is resident.
  double state_locality_score(const ComputeTask& task, const NodeRecord& node,
                              const StateLocalityRegistry& state) const;
};

}  // namespace cf