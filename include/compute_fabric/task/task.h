#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"

namespace cf {

// Semantic task class. Metadata only; never a hardcoded architectural case.
enum class TaskClass : uint8_t {
  Generic = 0,
  Inference = 1,
  Prefill = 2,
  Decode = 3,
  Embedding = 4,
  Retrieval = 5,
  AgentStep = 6,
  ToolExecution = 7,
  TensorKernel = 8,
  TrainingStep = 9,
  CheckpointTransform = 10,
  ApplicationDefined = 11,
};

const char* task_class_name(TaskClass c);
bool parse_task_class(const std::string& s, TaskClass& out);

// Executor kind required/available on a node.
enum class ExecutorType : uint8_t { Cpu = 0, Cuda = 1, Any_ = 2 };

const char* executor_type_name(ExecutorType t);
bool parse_executor_type(const std::string& s, ExecutorType& out);

// Built-in deterministic workloads executed by the CPU executor.
enum class KernelType : uint8_t {
  None = 0,
  HashInteger = 1,
  VectorTransform = 2,
  Checksum = 3,
  SyntheticLoop = 4,
  StateDependent = 5,
  TensorArithmetic = 6,
};

const char* kernel_type_name(KernelType k);
bool parse_kernel_type(const std::string& s, KernelType& out);

enum class Priority : uint8_t {
  Critical = 0,
  High = 1,
  Normal = 2,
  Low = 3,
  Background = 4,
};

const char* priority_name(Priority p);
bool parse_priority(const std::string& s, Priority& out);

struct RetryPolicy {
  uint32_t max_attempts = 1;
  bool retry_on_node_loss = true;
  bool retry_on_executor_failure = true;
  bool retry_on_timeout = false;
  uint64_t retry_backoff_ms = 100;
  bool valid() const { return max_attempts >= 1; }
};

// A reusable-state dependency referenced by a task. In standalone mode these
// fields are supplied directly; with a Context Fabric adapter they may be
// resolved through ContextStateProvider instead.
struct StateDependency {
  std::string state_id;                 // Context Object identity or app id
  std::string generation;               // version/epoch
  uint64_t size_bytes = 0;
  std::vector<NodeId> resident_nodes;   // nodes believed to hold it
  double locality_quality = 0.0;        // 0..1
  double transfer_estimate_seconds = -1.0;   // -1 = not directly provided
  double recompute_estimate_seconds = -1.0;  // -1 = not directly provided
  bool provider_backed = false;
  std::string compatibility;
};

struct ComputeTask {
  ComputeTaskId id;
  WorkloadId workload;
  std::string name;
  TaskClass task_class = TaskClass::Generic;
  ExecutorType required_executor = ExecutorType::Any_;
  int min_compute_capability_major = 0;  // 0 = any
  int min_compute_capability_minor = 0;
  uint32_t required_cpu_slots = 1;
  uint32_t required_gpu_slots = 1;
  uint64_t scratch_memory_bytes = 0;
  std::vector<ComputeTaskId> dependencies;
  std::vector<StateDependency> state_dependencies;
  uint64_t expected_input_bytes = 0;
  uint64_t expected_output_bytes = 0;
  double estimated_duration_seconds = 0.1;
  bool has_deadline = false;
  int64_t deadline_epoch_ms = 0;  // wall-clock unix ms when has_deadline
  Priority priority = Priority::Normal;
  RetryPolicy retry;
  std::vector<NodeId> preferred_nodes;
  std::vector<NodeId> forbidden_nodes;
  std::vector<std::string> required_tags;
  std::vector<std::string> preferred_tags;
  std::string anti_affinity_group;
  std::optional<ComputeTaskId> same_node_as_task;
  std::optional<ComputeTaskId> different_node_from_task;
  double expected_transfer_gb = 0.0;          // input-state transfer estimate
  double expected_recompute_cost_seconds = 0.0;
  double cost_estimate = 0.0;                 // optional monetary/resource cost
  bool preemptible = false;
  KernelType kernel = KernelType::None;
  uint64_t kernel_param_n = 0;        // element/size parameter
  uint64_t kernel_param_a = 0;
  uint64_t kernel_param_b = 0;
  uint64_t seed = 0;
  std::map<std::string, std::string> user_tags;
  std::vector<uint8_t> payload;       // deterministic execution descriptor
};

// An immutable workload grouping.
struct Workload {
  WorkloadId id;
  std::string name;
  std::map<std::string, std::string> user_tags;
};

}  // namespace cf