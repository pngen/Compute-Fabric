#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/task/task.h"

namespace cf {

// Health state of a registered node.
enum class NodeHealth : uint8_t {
  Online = 0,
  Suspect = 1,
  Draining = 2,
  Offline = 3,
};

const char* node_health_name(NodeHealth h);
bool parse_node_health(const std::string& s, NodeHealth& out);

// Capability of a single executor backend on a node.
struct ExecutorCapability {
  ExecutorType type = ExecutorType::Cpu;
  std::string name;              // "cpu", "cuda"
  int32_t slots = 0;             // max concurrent execution slots
  int32_t major = 0;             // compute capability (CUDA)
  int32_t minor = 0;
  std::string vendor;
  std::string architecture;
  std::string device_name;
  uint64_t device_memory_bytes = 0;
  std::vector<int32_t> device_ids;
  double relative_speed = 1.0;   // relative to baseline CPU
  bool healthy = true;
  bool supports_preemption = false;
};

// Schedulable capacity of a node. Compute Fabric reasons about *available
// capacity* only; it never owns physical memory residency.
struct NodeCapacity {
  int32_t cpu_worker_slots = 0;
  uint64_t host_memory_scheduling_bytes = 0;
  std::vector<ExecutorCapability> executors;
  std::map<std::string, std::string> tags;  // label/tag store
  std::string address;                      // host:port of control plane
  std::string label;
};

// A node's registered record (durable on the coordinator).
struct NodeRecord {
  NodeId id;
  NodeSessionId session;
  CoordinatorEpoch epoch = 0;
  SessionEpoch session_epoch = 0;
  NodeHealth health = NodeHealth::Online;
  NodeCapacity capacity;
  int64_t last_heartbeat_ms = 0;
  int64_t register_ms = 0;
  bool connected = false;  // currently has a live control connection
};

}  // namespace cf