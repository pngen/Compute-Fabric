#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/scheduler/placements.h"
#include "compute_fabric/task/attempt.h"
#include "compute_fabric/task/lifecycle.h"
#include "compute_fabric/task/task.h"

namespace cf {

struct StoredPlacement {
  ComputeTaskId task_id;
  uint64_t sequence = 0;
  PlacementDecision decision;
};

// Durable coordinator state. Does not contain transient socket handles.
struct DurableState {
  CoordinatorEpoch epoch = 1;
  std::map<WorkloadId, Workload> workloads;
  std::map<ComputeTaskId, ComputeTask> tasks;
  std::map<ComputeTaskId, TaskState> task_states;
  std::map<ComputeTaskId, std::vector<ExecutionAttempt>> attempts;
  std::map<ComputeTaskId, uint32_t> retry_counts;
  std::vector<StoredPlacement> placements;
};

// Versioned, integrity-checked, atomic (staging + rename) store.
// Corruption and truncation are detected on load.
class Store {
 public:
  ~Store();

  Store(const Store&) = delete;
  Store& operator=(const Store&) = delete;

  // Opens (or creates) the store directory. On load failure returns a typed
  // error (CorruptionDetected / IoError).
  static Result<std::unique_ptr<Store>> open(const std::string& store_dir,
                                             bool create_if_missing);

  Result<DurableState> load();
  Result<void> save(const DurableState& state);

  const std::string& path() const { return path_; }

  static constexpr uint32_t kFormatVersion = 1;
  // Maximum durable file size (defensive bound).
  static constexpr uint64_t kMaxFileSize = 256u << 20;

 private:
  explicit Store(std::string path);
  Result<void> ensure_dir() const;

  std::string path_;
};

}  // namespace cf