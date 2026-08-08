#include "compute_fabric/task/task.h"

#include <cstring>

namespace cf {

const char* task_class_name(TaskClass c) {
  switch (c) {
    case TaskClass::Generic: return "Generic";
    case TaskClass::Inference: return "Inference";
    case TaskClass::Prefill: return "Prefill";
    case TaskClass::Decode: return "Decode";
    case TaskClass::Embedding: return "Embedding";
    case TaskClass::Retrieval: return "Retrieval";
    case TaskClass::AgentStep: return "AgentStep";
    case TaskClass::ToolExecution: return "ToolExecution";
    case TaskClass::TensorKernel: return "TensorKernel";
    case TaskClass::TrainingStep: return "TrainingStep";
    case TaskClass::CheckpointTransform: return "CheckpointTransform";
    case TaskClass::ApplicationDefined: return "ApplicationDefined";
    default: return "Unknown";
  }
}

bool parse_task_class(const std::string& s, TaskClass& out) {
  for (int i = 0; i <= static_cast<int>(TaskClass::ApplicationDefined); ++i) {
    if (task_class_name(static_cast<TaskClass>(i)) == s) {
      out = static_cast<TaskClass>(i);
      return true;
    }
  }
  return false;
}

const char* executor_type_name(ExecutorType t) {
  switch (t) {
    case ExecutorType::Cpu: return "cpu";
    case ExecutorType::Cuda: return "cuda";
    case ExecutorType::Any_: return "any";
    default: return "unknown";
  }
}

bool parse_executor_type(const std::string& s, ExecutorType& out) {
  if (s == "cpu") { out = ExecutorType::Cpu; return true; }
  if (s == "cuda") { out = ExecutorType::Cuda; return true; }
  if (s == "any") { out = ExecutorType::Any_; return true; }
  return false;
}

const char* kernel_type_name(KernelType k) {
  switch (k) {
    case KernelType::None: return "none";
    case KernelType::HashInteger: return "hash_integer";
    case KernelType::VectorTransform: return "vector_transform";
    case KernelType::Checksum: return "checksum";
    case KernelType::SyntheticLoop: return "synthetic_loop";
    case KernelType::StateDependent: return "state_dependent";
    case KernelType::TensorArithmetic: return "tensor_arithmetic";
    default: return "unknown";
  }
}

bool parse_kernel_type(const std::string& s, KernelType& out) {
  for (int i = 0; i <= static_cast<int>(KernelType::TensorArithmetic); ++i) {
    if (kernel_type_name(static_cast<KernelType>(i)) == s) {
      out = static_cast<KernelType>(i);
      return true;
    }
  }
  return false;
}

const char* priority_name(Priority p) {
  switch (p) {
    case Priority::Critical: return "critical";
    case Priority::High: return "high";
    case Priority::Normal: return "normal";
    case Priority::Low: return "low";
    case Priority::Background: return "background";
    default: return "unknown";
  }
}

bool parse_priority(const std::string& s, Priority& out) {
  for (int i = 0; i <= static_cast<int>(Priority::Background); ++i) {
    if (priority_name(static_cast<Priority>(i)) == s) {
      out = static_cast<Priority>(i);
      return true;
    }
  }
  return false;
}

}  // namespace cf