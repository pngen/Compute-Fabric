#include "compute_fabric/task/dag.h"

#include <algorithm>
#include <functional>

namespace cf {

Result<void> TaskGraph::add_task(const ComputeTask& task) {
  if (tasks_.count(task.id)) {
    return Error(ErrorCode::AlreadyExists, "task already present in graph");
  }
  for (const auto& d : task.dependencies) {
    if (d == task.id) {
      return Error(ErrorCode::SelfDependency, "task depends on itself");
    }
  }
  tasks_[task.id] = task;
  states_[task.id] = TaskState::Submitted;
  deps_[task.id];  // always create the dep set (even when empty)
  std::vector<ComputeTaskId> added_dependents;
  for (const auto& d : task.dependencies) {
    deps_[task.id].insert(d);
    dependents_[d].insert(task.id);
    added_dependents.push_back(d);
  }
  auto vr = validate_dependencies(task.id);
  if (vr.failed()) {
    // Roll back the partial insertion.
    for (const auto& d : added_dependents) {
      dependents_[d].erase(task.id);
    }
    deps_.erase(task.id);
    dependents_.erase(task.id);
    states_.erase(task.id);
    tasks_.erase(task.id);
    return vr.error();
  }
  return {};
}

Result<void> TaskGraph::remove_task(const ComputeTaskId& id) {
  auto it = tasks_.find(id);
  if (it == tasks_.end()) {
    return Error(ErrorCode::TaskNotFound, "task not found");
  }
  if (!dependents_[id].empty()) {
    return Error(ErrorCode::InvalidState, "task has dependents");
  }
  for (const auto& d : deps_[id]) dependents_[d].erase(id);
  deps_.erase(id);
  dependents_.erase(id);
  states_.erase(id);
  tasks_.erase(id);
  return {};
}

std::vector<ComputeTaskId> TaskGraph::dependencies(const ComputeTaskId& id) const {
  auto it = deps_.find(id);
  if (it == deps_.end()) return {};
  return std::vector<ComputeTaskId>(it->second.begin(), it->second.end());
}

std::vector<ComputeTaskId> TaskGraph::dependents(const ComputeTaskId& id) const {
  auto it = dependents_.find(id);
  if (it == dependents_.end()) return {};
  return std::vector<ComputeTaskId>(it->second.begin(), it->second.end());
}

Result<void> TaskGraph::validate_dependencies(const ComputeTaskId& id) const {
  auto vit = tasks_.find(id);
  if (vit == tasks_.end()) {
    return Error(ErrorCode::TaskNotFound, "task not found in graph");
  }
  // Cycle detection via recursion along dependency edges starting from id.
  // Forward references (deps not yet added) are terminated safely.
  std::map<ComputeTaskId, int> color;  // 0=white 1=gray 2=black
  std::function<bool(const ComputeTaskId&)> dfs = [&](const ComputeTaskId& cur) -> bool {
    color[cur] = 1;
    auto dit = deps_.find(cur);
    if (dit != deps_.end()) {
      for (const auto& d : dit->second) {
        if (color[d] == 1) return true;       // back edge -> cycle
        if (color[d] == 0 && dfs(d)) return true;
      }
    }
    color[cur] = 2;
    return false;
  };
  if (dfs(id)) {
    return Error(ErrorCode::DependencyCycle, "dependency cycle detected");
  }
  return {};
}

bool TaskGraph::dependencies_satisfied(const ComputeTaskId& id) const {
  auto it = deps_.find(id);
  if (it == deps_.end()) return true;
  for (const auto& d : it->second) {
    auto sit = states_.find(d);
    if (sit == states_.end() || sit->second != TaskState::Succeeded) return false;
  }
  return true;
}

bool TaskGraph::dependency_poisoned(const ComputeTaskId& id) const {
  auto it = deps_.find(id);
  if (it == deps_.end()) return false;
  for (const auto& d : it->second) {
    auto sit = states_.find(d);
    if (sit == states_.end()) continue;
    if (sit->second == TaskState::Failed || sit->second == TaskState::Lost ||
        sit->second == TaskState::Cancelled || sit->second == TaskState::Expired) {
      return true;
    }
  }
  return false;
}

std::vector<ComputeTaskId> TaskGraph::task_ids() const {
  std::vector<ComputeTaskId> ids;
  ids.reserve(tasks_.size());
  for (const auto& [id, unused] : tasks_) ids.push_back(id);
  return ids;
}

Result<void> TaskGraph::set_state(const ComputeTaskId& id, TaskState s) {
  auto it = states_.find(id);
  if (it == states_.end()) {
    return Error(ErrorCode::TaskNotFound, "task not found");
  }
  if (!TaskLifecycle::is_legal(it->second, s)) {
    return Error(ErrorCode::IllegalTransition,
                 "illegal task transition " +
                     std::string(task_state_name(it->second)) + " -> " +
                     std::string(task_state_name(s)));
  }
  it->second = s;
  return {};
}

void TaskGraph::restore_state(const ComputeTaskId& id, TaskState s) {
  auto it = states_.find(id);
  if (it != states_.end()) {
    it->second = s;
  }
}

TaskState TaskGraph::state(const ComputeTaskId& id) const {
  auto it = states_.find(id);
  return it == states_.end() ? TaskState::Submitted : it->second;
}

Result<std::vector<ComputeTaskId>> TaskGraph::topological_order() const {
  std::map<ComputeTaskId, int> indegree;
  std::map<ComputeTaskId, std::vector<ComputeTaskId>> out;
  for (const auto& [id, unused] : tasks_) {
    indegree[id] = static_cast<int>(deps_.at(id).size());
    for (const auto& d : deps_.at(id)) out[d].push_back(id);
  }
  std::vector<ComputeTaskId> order;
  std::set<ComputeTaskId> ready;
  for (const auto& [id, deg] : indegree) {
    if (deg == 0) ready.insert(id);
  }
  while (!ready.empty()) {
    ComputeTaskId cur = *ready.begin();
    ready.erase(ready.begin());
    order.push_back(cur);
    for (const auto& nxt : out[cur]) {
      if (--indegree[nxt] == 0) ready.insert(nxt);
    }
  }
  if (order.size() != tasks_.size()) {
    return Error(ErrorCode::DependencyCycle, "cycle detected in task graph");
  }
  return order;
}

}  // namespace cf