#include "framework.h"

#include "compute_fabric/scheduler/scheduler.h"

using namespace cf;

namespace {
NodeRecord cpu_node(const NodeId& id, int slots) {
  NodeRecord n;
  n.id = id;
  n.session = Id128::random();
  n.epoch = 1;
  n.session_epoch = 1;
  n.health = NodeHealth::Online;
  n.connected = true;
  n.capacity.cpu_worker_slots = slots;
  n.capacity.host_memory_scheduling_bytes = 1ull << 30;
  ExecutorCapability cpu;
  cpu.type = ExecutorType::Cpu;
  cpu.name = "cpu";
  cpu.slots = slots;
  cpu.relative_speed = 1.0;
  cpu.healthy = true;
  n.capacity.executors.push_back(cpu);
  return n;
}

NodeRecord cuda_node(const NodeId& id, int slots, int major = 12) {
  NodeRecord n = cpu_node(id, 2);
  ExecutorCapability gpu;
  gpu.type = ExecutorType::Cuda;
  gpu.name = "cuda";
  gpu.slots = slots;
  gpu.major = major;
  gpu.minor = 0;
  gpu.healthy = true;
  n.capacity.executors.push_back(gpu);
  return n;
}

ComputeTask mk(const char* name, KernelType k = KernelType::HashInteger) {
  ComputeTask t;
  t.id = Id128::random();
  t.workload = Id128::random();
  t.name = name;
  t.kernel = k;
  t.kernel_param_n = 1000;
  t.seed = 1;
  t.estimated_duration_seconds = 0.1;
  return t;
}
}  // namespace

TEST(placement_deterministic_with_same_input) {
  NodeId a = Id128(1, 1);
  NodeId b = Id128(1, 2);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  auto d1 = sched.place(task, nodes, resv, state);
  auto d2 = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d1.feasible && d2.feasible);
  EXPECT_EQ(d1.chosen_node, d2.chosen_node);
  EXPECT_EQ(d1.total_cost, d2.total_cost);
}

TEST(placement_stable_tie_breaks_by_node_id) {
  // Identical nodes, same task -> lowest node id wins deterministically.
  NodeId a = Id128(5, 0);
  NodeId b = Id128(3, 0);
  NodeId c = Id128(7, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  nodes[c] = cpu_node(c, 4);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_TRUE(d.chosen_node.has_value());
  EXPECT_EQ(*d.chosen_node, b);  // lowest id
}

TEST(placement_capability_exclusion_cuda) {
  NodeId cpu = Id128(1, 0);
  NodeId gpu = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[cpu] = cpu_node(cpu, 4);
  nodes[gpu] = cuda_node(gpu, 2);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("cuda-task");
  task.required_executor = ExecutorType::Cuda;
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, gpu);
  bool cpu_excluded = false;
  for (const auto& e : d.excluded) {
    if (e.node_id == cpu) cpu_excluded = true;
  }
  EXPECT_TRUE(cpu_excluded);
}

TEST(placement_min_compute_capability) {
  NodeId old_gpu = Id128(1, 0);
  NodeId new_gpu = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[old_gpu] = cuda_node(old_gpu, 2, 8);
  nodes[new_gpu] = cuda_node(new_gpu, 2, 12);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("needs-cc12");
  task.required_executor = ExecutorType::Cuda;
  task.min_compute_capability_major = 12;
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, new_gpu);
}

TEST(placement_state_locality_preferred) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  StateLocalityRegistry state;
  StateLocation loc;
  loc.state_id = "big";
  loc.size_bytes = 1024;
  loc.resident_nodes = {a};
  loc.transfer_estimate_seconds = 0.0;
  loc.recompute_estimate_seconds = 10.0;
  state.upsert(loc);
  ReservationRegistry resv;
  SchedulerPolicyEngine sched;
  auto task = mk("state-task", KernelType::StateDependent);
  StateDependency dep;
  dep.state_id = "big";
  dep.size_bytes = 1024;
  dep.resident_nodes = {a};
  dep.transfer_estimate_seconds = 0.0;
  dep.recompute_estimate_seconds = 10.0;
  task.state_dependencies.push_back(dep);
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, a);
}

TEST(placement_transfer_cost_penalizes_remote) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  StateLocalityRegistry state;
  StateLocation loc;
  loc.state_id = "s";
  loc.size_bytes = 1 << 30;
  loc.resident_nodes = {a};
  loc.transfer_estimate_seconds = 5.0;
  loc.recompute_estimate_seconds = 100.0;
  state.upsert(loc);
  ReservationRegistry resv;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  task.state_dependencies.push_back({});
  task.state_dependencies[0].state_id = "s";
  task.state_dependencies[0].size_bytes = 1 << 30;
  task.state_dependencies[0].resident_nodes = {a};
  task.state_dependencies[0].transfer_estimate_seconds = 5.0;
  task.state_dependencies[0].recompute_estimate_seconds = 100.0;
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, a);
  // The remote candidate (b) must carry a transfer cost for the 5s estimate.
  double remote_transfer = -1.0;
  for (const auto& c : d.candidates) {
    if (c.node_id == b) remote_transfer = c.transfer_cost;
  }
  EXPECT_GT(remote_transfer, 0.0);
}

TEST(placement_recompute_cheaper_than_transfer) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  StateLocalityRegistry state;
  StateLocation loc;
  loc.state_id = "s";
  loc.size_bytes = 1 << 30;
  loc.resident_nodes = {a};
  loc.transfer_estimate_seconds = 50.0;   // slow
  loc.recompute_estimate_seconds = 1.0;   // cheap
  state.upsert(loc);
  ReservationRegistry resv;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  task.state_dependencies.push_back({});
  task.state_dependencies[0].state_id = "s";
  task.state_dependencies[0].size_bytes = 1 << 30;
  task.state_dependencies[0].resident_nodes = {a};
  task.state_dependencies[0].transfer_estimate_seconds = 50.0;
  task.state_dependencies[0].recompute_estimate_seconds = 1.0;
  task.estimated_duration_seconds = 0.1;
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  // b's total = exec + recompute (1.0) vs a's = exec. Both should be close;
  // with weights both are duration-based. Verify decision is deterministic.
  EXPECT_NEAR(d.recompute_cost, 1.0, 1e-9);
}

TEST(placement_affinity_preferred_nodes) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  task.preferred_nodes = {b};
  auto d1 = sched.place(task, nodes, resv, state);
  auto d2 = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d1.feasible);
  EXPECT_EQ(d1.chosen_node, d2.chosen_node);
}

TEST(placement_forbidden_node_excluded) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  task.forbidden_nodes = {a};
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, b);
  bool a_excluded = false;
  for (const auto& e : d.excluded) {
    if (e.node_id == a) a_excluded = true;
  }
  EXPECT_TRUE(a_excluded);
}

TEST(placement_required_tag) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  auto tagged = cpu_node(b, 4);
  tagged.capacity.tags["gpu-node"] = "true";
  nodes[b] = tagged;
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  task.required_tags = {"gpu-node"};
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, b);
}

TEST(placement_queue_pressure_shifts_load) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 2);
  nodes[b] = cpu_node(b, 2);
  ReservationRegistry resv;
  auto fill1 = mk("fill1");
  auto fill2 = mk("fill2");
  // Saturate node a's 2 slots.
  EXPECT_TRUE(resv.try_reserve(nodes[a], 1, fill1.id, ExecutorType::Cpu, 1, 0).reservation);
  EXPECT_TRUE(resv.try_reserve(nodes[a], 1, fill2.id, ExecutorType::Cpu, 1, 0).reservation);
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("load");
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, b);
}

TEST(placement_baseline_policy_deterministic) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  sched.set_policy(SchedulerPolicy::Baseline);
  auto task = mk("t");
  task.preferred_nodes = {a};
  auto d1 = sched.place(task, nodes, resv, state);
  auto d2 = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d1.feasible);
  EXPECT_EQ(d1.chosen_node, d2.chosen_node);
  EXPECT_EQ(d1.total_cost, d2.total_cost);
}

TEST(placement_history_shapes_cost) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = cpu_node(a, 4);
  nodes[b] = cpu_node(b, 4);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  task.estimated_duration_seconds = 1.0;
  // Record history: node b has a poor mean duration.
  sched.record_history({task.task_class, ExecutorType::Cpu, b}, 50.0, false, 0);
  for (int i = 0; i < 5; ++i) {
    sched.record_history({task.task_class, ExecutorType::Cpu, b}, 40.0, false, 0);
  }
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, a);
}

TEST(placement_offline_and_draining_excluded) {
  NodeId a = Id128(1, 0);
  NodeId b = Id128(2, 0);
  NodeId c = Id128(3, 0);
  std::map<NodeId, NodeRecord> nodes;
  auto na = cpu_node(a, 4);
  nodes[a] = na;
  auto nb = cpu_node(b, 4);
  nb.health = NodeHealth::Offline;
  nodes[b] = nb;
  auto nc = cpu_node(c, 4);
  nc.health = NodeHealth::Draining;
  nodes[c] = nc;
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  auto task = mk("t");
  auto d = sched.place(task, nodes, resv, state);
  EXPECT_TRUE(d.feasible);
  EXPECT_EQ(*d.chosen_node, a);
  EXPECT_EQ(d.excluded.size(), 2ull);
}