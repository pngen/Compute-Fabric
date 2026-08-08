#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/resource/capacity.h"
#include "compute_fabric/task/task.h"

namespace cf {

// A bounded resource reservation on a specific node. Reservations are
// epoch-bound and session-bound; they are invalidated on coordinator restart
// and on node session changes.
struct Reservation {
  ReservationId id;
  ComputeTaskId task_id;
  NodeId node_id;
  NodeSessionId session_id;
  CoordinatorEpoch coordinator_epoch = 0;
  SessionEpoch session_epoch = 0;
  ExecutorType executor = ExecutorType::Cpu;
  uint32_t slots = 0;
  uint64_t scratch_bytes = 0;
  int64_t created_at_ms = 0;
  bool released = false;
};

// Result of an attempted reservation.
struct ReservationResult {
  std::optional<Reservation> reservation;
  std::vector<std::string> reasons;  // why it failed, if it did
};

// Tracks per-node utilization of execution slots and scratch budget.
// Prevents overcommitting: never assigns more slots than exist, never
// double-books exclusive resources, invalidated across coordinator epochs.
class ReservationRegistry {
 public:
  ReservationRegistry() = default;

  // Attempts to reserve `slots` on the given executor of a node.
  // Returns a reservation token on success, reasons on failure.
  ReservationResult try_reserve(const NodeRecord& node,
                                CoordinatorEpoch epoch,
                                const ComputeTaskId& task_id,
                                ExecutorType executor, uint32_t slots,
                                uint64_t scratch_bytes);

  // Releases a reservation; returns false if unknown/already released.
  Result<void> release(const ReservationId& id);

  // Marks all reservations for a node as released (node dropped/lost).
  void release_all_for_node(const NodeId& node_id);

  // Invalidate every reservation held under a stale coordinator epoch.
  // Returns the number invalidated.
  size_t invalidate_before(CoordinatorEpoch current_epoch);

  // Invalidate every reservation for a stale node session.
  size_t invalidate_session(const NodeId& node_id,
                            const NodeSessionId& session);

  const Reservation* find(const ReservationId& id) const;

  // Active (unreleased, valid) reservations for a node.
  std::vector<Reservation> active_for_node(const NodeId& node_id) const;

  // Current slot usage on a node/executor.
  uint32_t used_slots(const NodeId& node_id, ExecutorType executor) const;

  // Current scratch usage on a node.
  uint64_t used_scratch(const NodeId& node_id) const;

  size_t active_count() const;

  // All currently active reservations (deterministic order by id).
  std::vector<Reservation> all_active() const;

 private:
  uint32_t used_executor_slots_locked(const NodeId& node_id,
                                      ExecutorType executor) const;
  std::map<ReservationId, Reservation> reservations_;
};

}  // namespace cf