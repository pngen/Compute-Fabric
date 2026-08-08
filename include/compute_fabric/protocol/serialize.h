#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/protocol/wire.h"
#include "compute_fabric/scheduler/placements.h"
#include "compute_fabric/task/attempt.h"
#include "compute_fabric/task/task.h"

namespace cf::proto {

// Serialization of domain types. Used by both the wire protocol and the
// durable store. All integer encodings are big-endian; all reads are bounded.

void serialize_task(BinaryWriter& w, const ComputeTask& t);
Result<ComputeTask> deserialize_task(BinaryReader& r);

void serialize_workload(BinaryWriter& w, const Workload& wl);
Result<Workload> deserialize_workload(BinaryReader& r);

void serialize_state_dep(BinaryWriter& w, const StateDependency& d);
Result<StateDependency> deserialize_state_dep(BinaryReader& r);

void serialize_attempt(BinaryWriter& w, const ExecutionAttempt& a);
Result<ExecutionAttempt> deserialize_attempt(BinaryReader& r);

void serialize_placement(BinaryWriter& w, const PlacementDecision& d);
Result<PlacementDecision> deserialize_placement(BinaryReader& r);

void serialize_retry_policy(BinaryWriter& w, const RetryPolicy& p);
Result<RetryPolicy> deserialize_retry_policy(BinaryReader& r);

}  // namespace cf::proto