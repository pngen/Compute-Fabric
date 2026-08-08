#pragma once

#include <map>
#include <set>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/task/lifecycle.h"
#include "compute_fabric/task/task.h"

namespace cf {

// Dependency graph over ComputeTasks. Tasks reference other tasks by id.
// The graph is a DAG; adding an edge that creates a cycle is rejected.
// The scheduler only marks a task Ready when all task dependencies have
// committed successfully (state == Succeeded).
class TaskGraph {
 public:
  // Adds a task node. Returns AlreadyExists if the id is already present.
  Result<void> add_task(const ComputeTask& task);

  // Removes a task (only if it has no dependents). Typically unused at runtime.
  Result<void> remove_task(const ComputeTaskId& id);

  bool has_task(const ComputeTaskId& id) const { return tasks_.count(id) > 0; }

  const ComputeTask* find(const ComputeTaskId& id) const {
    auto it = tasks_.find(id);
    return it == tasks_.end() ? nullptr : &it->second;
  }

  // Validates the full dependency set of a task: no self-dep, no missing deps,
  // no cycles. Must be called after the task (and all its deps) are present.
  Result<void> validate_dependencies(const ComputeTaskId& id) const;

  // Direct dependencies of a task.
  std::vector<ComputeTaskId> dependencies(const ComputeTaskId& id) const;

  // Whether all dependencies of `id` are in Succeeded state.
  bool dependencies_satisfied(const ComputeTaskId& id) const;

  // Whether any dependency is Failed/Lost/Cancelled/Expired (poisoned).
  bool dependency_poisoned(const ComputeTaskId& id) const;

  // All task ids in deterministic (id) order.
  std::vector<ComputeTaskId> task_ids() const;

  // Downstream dependents of a task (reverse edges).
  std::vector<ComputeTaskId> dependents(const ComputeTaskId& id) const;

  size_t size() const { return tasks_.size(); }

  // Sets the lifecycle state of a task (used by the coordinator driver).
  Result<void> set_state(const ComputeTaskId& id, TaskState s);

  // Restores a persisted state without lifecycle validation. Used ONLY by the
  // coordinator's restart recovery path; never for runtime transitions.
  void restore_state(const ComputeTaskId& id, TaskState s);

  TaskState state(const ComputeTaskId& id) const;

  // Deterministic topological order, or an error if a cycle exists.
  Result<std::vector<ComputeTaskId>> topological_order() const;

 private:
  std::map<ComputeTaskId, ComputeTask> tasks_;
  std::map<ComputeTaskId, TaskState> states_;
  std::map<ComputeTaskId, std::set<ComputeTaskId>> deps_;   // task -> deps
  std::map<ComputeTaskId, std::set<ComputeTaskId>> dependents_;  // task -> users
};

}  // namespace cf