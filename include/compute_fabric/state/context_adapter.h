#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/state/state_locality.h"

namespace cf {

// Narrow optional adapter boundary between Compute Fabric and Context
// Fabric. Compute Fabric builds and operates fully without Context Fabric;
// the default factory returns nullptr. When a real adapter is attached it may
// resolve state-location, generation, size, replica, transfer and recompute
// facts. request_state_move is ONLY invoked when an external adapter
// explicitly supports it.
//
// Abstraction boundaries:
//   FlashTier:        physical residency.
//   Context Fabric:   computational-state continuity.
//   Compute Fabric:   execution placement.
class ContextStateProvider {
 public:
  virtual ~ContextStateProvider() = default;

  // Resolve where the given state id currently lives.
  virtual Result<std::vector<NodeId>> resolve_state_location(
      const std::string& state_id) = 0;

  // Resolve the current generation/version of a state object.
  virtual Result<std::string> resolve_generation(const std::string& state_id) = 0;

  // Query the byte size of a state object.
  virtual Result<uint64_t> query_state_size(const std::string& state_id) = 0;

  // Query the current replica set.
  virtual Result<std::vector<NodeId>> query_replicas(const std::string& state_id) = 0;

  // Estimate transfer cost to a node in seconds.
  virtual Result<double> estimate_transfer(const std::string& state_id,
                                           const NodeId& to_node) = 0;

  // Estimate recomputation cost in seconds.
  virtual Result<double> estimate_recompute(const std::string& state_id) = 0;

  // Optionally request a state move. Default implementation returns
  // Unsupported; external adapters may override.
  virtual Result<void> request_state_move(const std::string& state_id,
                                          const NodeId& to_node) {
    (void)state_id;
    (void)to_node;
    return Error(ErrorCode::Unsupported,
                 "state move not supported by this adapter");
  }

  // Whether request_state_move is implemented.
  virtual bool supports_state_move() const { return false; }
};

// Default factory: returns nullptr (standalone mode). Attach via
// set_context_state_provider under a mutex before the scheduler runs.
class ContextAdapterManager {
 public:
  ContextStateProvider* get() const;
  void set(ContextStateProvider* provider);
  bool attached() const { return get() != nullptr; }

 private:
  ContextStateProvider* provider_ = nullptr;  // borrowed, never owned
};

}  // namespace cf