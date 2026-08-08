#include "compute_fabric/resource/reservation.h"

#include "compute_fabric/core/random.h"
#include "compute_fabric/core/time_util.h"

namespace cf {

namespace {

const ExecutorCapability* find_executor(const NodeRecord& node,
                                        ExecutorType type) {
  for (const auto& e : node.capacity.executors) {
    if (e.type == type) return &e;
  }
  return nullptr;
}

}  // namespace

uint32_t ReservationRegistry::used_executor_slots_locked(
    const NodeId& node_id, ExecutorType executor) const {
  uint32_t used = 0;
  for (const auto& [id, r] : reservations_) {
    (void)id;
    if (r.node_id == node_id && r.executor == executor && !r.released) {
      used += r.slots;
    }
  }
  return used;
}

ReservationResult ReservationRegistry::try_reserve(
    const NodeRecord& node, CoordinatorEpoch epoch,
    const ComputeTaskId& task_id, ExecutorType executor, uint32_t slots,
    uint64_t scratch_bytes) {
  ReservationResult out;
  if (node.health == NodeHealth::Offline) {
    out.reasons.push_back("node offline");
    return out;
  }
  if (node.health == NodeHealth::Draining) {
    out.reasons.push_back("node draining");
    return out;
  }
  const auto* cap = find_executor(node, executor);
  if (cap == nullptr) {
    out.reasons.push_back("requested executor not present on node");
    return out;
  }
  if (!cap->healthy) {
    out.reasons.push_back("executor unhealthy");
    return out;
  }
  if (slots == 0) {
    out.reasons.push_back("zero slots requested");
    return out;
  }
  uint32_t used = used_executor_slots_locked(node.id, executor);
  if (used + slots > static_cast<uint32_t>(cap->slots)) {
    out.reasons.push_back("insufficient executor slots (" +
                          std::to_string(slots) + " needed, " +
                          std::to_string(cap->slots - used) + " free)");
    return out;
  }
  if (executor == ExecutorType::Cpu) {
    uint32_t used_cpu = 0;
    for (const auto& [id, r] : reservations_) {
      (void)id;
      if (r.node_id == node.id && !r.released) used_cpu += r.slots;
    }
    if (used_cpu + slots > static_cast<uint32_t>(node.capacity.cpu_worker_slots)) {
      out.reasons.push_back("insufficient CPU worker slots");
      return out;
    }
  }
  uint64_t used_scr = used_scratch(node.id);
  if (used_scr + scratch_bytes > node.capacity.host_memory_scheduling_bytes &&
      scratch_bytes > 0) {
    out.reasons.push_back("scratch budget exceeded");
    return out;
  }

  Reservation r;
  r.id = Id128::random();
  r.task_id = task_id;
  r.node_id = node.id;
  r.session_id = node.session;
  r.coordinator_epoch = epoch;
  r.session_epoch = node.session_epoch;
  r.executor = executor;
  r.slots = slots;
  r.scratch_bytes = scratch_bytes;
  r.created_at_ms = now_millis();
  out.reservation = r;
  reservations_[r.id] = r;
  return out;
}

Result<void> ReservationRegistry::release(const ReservationId& id) {
  auto it = reservations_.find(id);
  if (it == reservations_.end()) {
    return Error(ErrorCode::ReservationNotFound, "reservation not found");
  }
  if (it->second.released) {
    return Error(ErrorCode::ReservationNotFound, "reservation already released");
  }
  it->second.released = true;
  return {};
}

void ReservationRegistry::release_all_for_node(const NodeId& node_id) {
  for (auto& [id, r] : reservations_) {
    (void)id;
    if (r.node_id == node_id) r.released = true;
  }
}

size_t ReservationRegistry::invalidate_before(CoordinatorEpoch current_epoch) {
  size_t n = 0;
  for (auto& [id, r] : reservations_) {
    (void)id;
    if (r.coordinator_epoch != current_epoch && !r.released) {
      r.released = true;
      ++n;
    }
  }
  return n;
}

size_t ReservationRegistry::invalidate_session(
    const NodeId& node_id, const NodeSessionId& session) {
  size_t n = 0;
  for (auto& [id, r] : reservations_) {
    (void)id;
    if (r.node_id == node_id && r.session_id == session && !r.released) {
      r.released = true;
      ++n;
    }
  }
  return n;
}

const Reservation* ReservationRegistry::find(const ReservationId& id) const {
  auto it = reservations_.find(id);
  if (it == reservations_.end()) return nullptr;
  return &it->second;
}

std::vector<Reservation> ReservationRegistry::active_for_node(
    const NodeId& node_id) const {
  std::vector<Reservation> out;
  for (const auto& [id, r] : reservations_) {
    (void)id;
    if (r.node_id == node_id && !r.released) out.push_back(r);
  }
  return out;
}

uint32_t ReservationRegistry::used_slots(const NodeId& node_id,
                                         ExecutorType executor) const {
  return used_executor_slots_locked(node_id, executor);
}

uint64_t ReservationRegistry::used_scratch(const NodeId& node_id) const {
  uint64_t used = 0;
  for (const auto& [id, r] : reservations_) {
    (void)id;
    if (r.node_id == node_id && !r.released) used += r.scratch_bytes;
  }
  return used;
}

size_t ReservationRegistry::active_count() const {
  size_t n = 0;
  for (const auto& [id, r] : reservations_) {
    (void)id;
    if (!r.released) ++n;
  }
  return n;
}

std::vector<Reservation> ReservationRegistry::all_active() const {
  std::vector<Reservation> out;
  for (const auto& [id, r] : reservations_) {
    (void)id;
    if (!r.released) out.push_back(r);
  }
  return out;
}

}  // namespace cf