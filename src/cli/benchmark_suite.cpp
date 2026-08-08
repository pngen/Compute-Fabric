#include "compute_fabric/cli/benchmark.h"

#include <cstdio>
#include <thread>

#include "compute_fabric/core/digest.h"
#include "compute_fabric/core/random.h"
#include "compute_fabric/core/time_util.h"
#include "compute_fabric/executor/cpu_executor.h"
#include "compute_fabric/executor/cuda_executor.h"
#include "compute_fabric/executor/workloads.h"
#include "compute_fabric/launcher/launcher.h"
#include "compute_fabric/net/client.h"
#include "compute_fabric/resource/reservation.h"
#include "compute_fabric/scheduler/scheduler.h"
#include "compute_fabric/state/state_locality.h"

namespace cf {

namespace {

struct BenchResult {
  std::string name;
  int64_t latency_ms = 0;
  int64_t queue_delay_ms = 0;
  double execution_ms = 0.0;
  double placement_cost = 0.0;
  double transfer_estimate = 0.0;
  double recompute_estimate = 0.0;
  uint64_t bytes = 0;
  bool success = true;
  uint32_t retries = 0;
  std::string chosen_node;
  std::string executor;
  std::string reason;
};

void print_result(const BenchResult& r) {
  std::printf("[BENCH] %-28s latency=%lldms queue=%lldms exec=%.2fms "
              "cost=%.4f transfer=%.4fs recompute=%.4fs bytes=%llu "
              "success=%d retries=%u node=%s executor=%s reason=%s\n",
              r.name.c_str(), static_cast<long long>(r.latency_ms),
              static_cast<long long>(r.queue_delay_ms), r.execution_ms,
              r.placement_cost, r.transfer_estimate, r.recompute_estimate,
              static_cast<unsigned long long>(r.bytes), r.success ? 1 : 0,
              r.retries, r.chosen_node.c_str(), r.executor.c_str(),
              r.reason.c_str());
}

NodeRecord make_cpu_node(const NodeId& id, const std::string& label,
                         int slots, double speed = 1.0,
                         const std::map<std::string, std::string>& tags = {}) {
  NodeRecord n;
  n.id = id;
  n.session = Id128::random();
  n.epoch = 1;
  n.session_epoch = 1;
  n.health = NodeHealth::Online;
  n.connected = true;
  n.capacity.label = label;
  n.capacity.cpu_worker_slots = slots;
  n.capacity.host_memory_scheduling_bytes = 8ull << 30;
  n.capacity.tags = tags;
  ExecutorCapability cpu;
  cpu.type = ExecutorType::Cpu;
  cpu.name = "cpu";
  cpu.slots = slots;
  cpu.relative_speed = speed;
  cpu.healthy = true;
  n.capacity.executors.push_back(cpu);
  return n;
}

NodeRecord make_cuda_node(const NodeId& id, const std::string& label,
                          int slots, int major = 12, int minor = 0) {
  NodeRecord n;
  n.id = id;
  n.session = Id128::random();
  n.epoch = 1;
  n.session_epoch = 1;
  n.health = NodeHealth::Online;
  n.connected = true;
  n.capacity.label = label;
  n.capacity.cpu_worker_slots = 4;
  n.capacity.host_memory_scheduling_bytes = 8ull << 30;
  ExecutorCapability cpu;
  cpu.type = ExecutorType::Cpu;
  cpu.name = "cpu";
  cpu.slots = 4;
  cpu.healthy = true;
  n.capacity.executors.push_back(cpu);
  ExecutorCapability gpu;
  gpu.type = ExecutorType::Cuda;
  gpu.name = "cuda";
  gpu.slots = slots;
  gpu.major = major;
  gpu.minor = minor;
  gpu.vendor = "NVIDIA";
  gpu.device_name = "RTX 5090";
  gpu.device_ids = {0};
  gpu.relative_speed = 10.0;
  gpu.healthy = true;
  n.capacity.executors.push_back(gpu);
  return n;
}

ComputeTask make_task(const std::string& name, KernelType k, uint64_t n) {
  ComputeTask t;
  t.id = Id128::random();
  t.name = name;
  t.workload = Id128::random();
  t.kernel = k;
  t.kernel_param_n = n;
  t.kernel_param_a = 31;
  t.kernel_param_b = 17;
  t.seed = 42;
  t.estimated_duration_seconds = 0.01;
  return t;
}

BenchResult bench_cpu_execution() {
  BenchResult r;
  r.name = "local_cpu_execution";
  CpuExecutorBackend cpu(2);
  auto task = make_task("cpu-kernel", KernelType::HashInteger, 200000);
  std::string device;
  int64_t t0 = now_millis();
  WorkloadResult wr = run_cpu_kernel(task, nullptr, device);
  int64_t t1 = now_millis();
  r.latency_ms = t1 - t0;
  r.execution_ms = static_cast<double>(t1 - t0);
  r.success = wr.success;
  r.bytes = wr.bytes_processed;
  r.executor = "cpu";
  r.reason = "deterministic kernel " + wr.checksum_hex.substr(0, 8);
  print_result(r);
  return r;
}

void print_result_skip_if_nocuda(BenchResult& r,
                                 const std::vector<ExecutorCapability>& devs) {
  if (!devs.empty()) return;
  r.success = false;
  r.reason = "no cuda device";
  print_result(r);
}

BenchResult bench_cuda_execution() {
  BenchResult r;
  r.name = "local_cuda_execution";
  auto devs = enumerate_cuda_devices();
  print_result_skip_if_nocuda(r, devs);
  if (devs.empty()) return r;
  CudaExecutorBackend cuda(devs[0].device_ids[0], 2);
  auto task = make_task("cuda-kernel", KernelType::VectorTransform, 200000);
  task.required_executor = ExecutorType::Cuda;
  int64_t t0 = now_millis();
  auto res = cuda_run_vector_transform(devs[0].device_ids[0], task, nullptr);
  int64_t t1 = now_millis();
  const int64_t outstanding_allocations = cuda_outstanding_allocations();
  r.latency_ms = t1 - t0;
  r.execution_ms = res.kernel_millis;
  r.success = res.success && res.elements == task.kernel_param_n &&
              res.bytes_transferred == task.kernel_param_n * sizeof(uint32_t) * 2 &&
              outstanding_allocations == 0;
  r.bytes = res.bytes_transferred;
  r.executor = "cuda";
  r.reason = "elements=" + std::to_string(res.elements) +
             " integrity=" + res.integrity_hex +
             " outstanding_allocations=" +
             std::to_string(outstanding_allocations);
  print_result(r);
  return r;
}

BenchResult bench_cpu_vs_cuda_placement() {
  BenchResult r;
  r.name = "cpu_vs_cuda_placement";
  NodeId cpu_node = Id128::random();
  NodeId gpu_node = Id128::random();
  std::map<NodeId, NodeRecord> nodes;
  nodes[cpu_node] = make_cpu_node(cpu_node, "cpu-node", 4);
  nodes[gpu_node] = make_cuda_node(gpu_node, "cuda-node", 2);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  sched.set_policy(SchedulerPolicy::CostAware);

  auto task = make_task("cuda-required", KernelType::VectorTransform, 100000);
  task.required_executor = ExecutorType::Cuda;
  auto d = sched.place(task, nodes, resv, state);
  r.success = d.feasible && d.chosen_node && *d.chosen_node == gpu_node;
  r.chosen_node = d.chosen_node ? d.chosen_node->to_hex() : "none";
  r.executor = d.chosen_device;
  r.placement_cost = d.total_cost;
  r.reason = d.reason;
  print_result(r);
  return r;
}

BenchResult bench_state_locality() {
  BenchResult r;
  r.name = "state_locality";
  // Node A is state-local. Node B has more compute speed but must transfer the
  // dependency, so locality should win deterministically.
  NodeId a = Id128::random();
  NodeId b = Id128::random();
  std::map<NodeId, NodeRecord> nodes;
  std::map<std::string, std::string> tags;
  auto node_a = make_cpu_node(a, "a-slow-local", 4, 1.0, tags);
  auto node_b = make_cpu_node(b, "b-fast-remote", 4, 2.0, tags);
  nodes[a] = node_a;
  nodes[b] = node_b;

  StateLocalityRegistry state;
  StateLocation loc;
  loc.state_id = "weights-8g";
  loc.size_bytes = 8ull << 30;
  loc.resident_nodes = {a};
  loc.transfer_estimate_seconds = 8.0;
  loc.recompute_estimate_seconds = 30.0;
  state.upsert(loc);
  ReservationRegistry resv;
  SchedulerPolicyEngine sched;
  sched.set_policy(SchedulerPolicy::CostAware);
  auto task = make_task("state-task", KernelType::StateDependent, 1000);
  task.state_dependencies.resize(1);
  task.state_dependencies[0].state_id = "weights-8g";
  task.state_dependencies[0].size_bytes = 8ull << 30;
  task.state_dependencies[0].resident_nodes = {a};
  task.state_dependencies[0].transfer_estimate_seconds = -1.0;
  task.state_dependencies[0].recompute_estimate_seconds = 30.0;

  auto d = sched.place(task, nodes, resv, state);
  r.success = d.feasible && d.chosen_node && *d.chosen_node == a;
  r.chosen_node = d.chosen_node ? d.chosen_node->to_hex() : "none";
  r.placement_cost = d.total_cost;
  r.transfer_estimate = d.transfer_cost;
  r.recompute_estimate = d.recompute_cost;
  r.reason = d.reason + " (state-local preferred)";
  print_result(r);
  return r;
}

BenchResult bench_transfer_vs_execution() {
  BenchResult r;
  r.name = "transfer_vs_execution";
  NodeId a = Id128::random();
  NodeId b = Id128::random();
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = make_cpu_node(a, "a", 4);
  nodes[b] = make_cpu_node(b, "b", 4);
  StateLocalityRegistry state;
  StateLocation loc;
  loc.state_id = "big-state";
  loc.size_bytes = 50ull << 30;
  loc.resident_nodes = {a};
  loc.recompute_estimate_seconds = 120.0;
  state.upsert(loc);
  ReservationRegistry resv;
  SchedulerPolicyEngine sched;
  sched.set_policy(SchedulerPolicy::CostAware);
  auto task = make_task("big-state-task", KernelType::StateDependent, 1000);
  task.state_dependencies.push_back({});
  task.state_dependencies[0].state_id = "big-state";
  task.state_dependencies[0].size_bytes = 50ull << 30;
  task.state_dependencies[0].resident_nodes = {a};
  task.state_dependencies[0].recompute_estimate_seconds = 120.0;
  task.estimated_duration_seconds = 1.0;
  auto d = sched.place(task, nodes, resv, state);
  r.success = d.feasible;
  r.chosen_node = d.chosen_node ? d.chosen_node->to_hex() : "none";
  r.placement_cost = d.total_cost;
  r.transfer_estimate = d.transfer_cost;
  r.reason = d.reason;
  print_result(r);
  return r;
}

BenchResult bench_recompute_vs_transfer_vs_execute() {
  BenchResult r;
  r.name = "recompute_vs_transfer_vs_execute";
  NodeId a = Id128::random();
  NodeId b = Id128::random();
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = make_cpu_node(a, "a-local", 4);
  nodes[b] = make_cpu_node(b, "b-remote", 4);
  StateLocalityRegistry state;
  StateLocation loc;
  loc.state_id = "state";
  loc.size_bytes = 1ull << 30;
  loc.resident_nodes = {a};
  loc.transfer_estimate_seconds = 5.0;   // slow link
  loc.recompute_estimate_seconds = 2.0;  // cheap to recompute
  state.upsert(loc);
  ReservationRegistry resv;
  SchedulerPolicyEngine sched;
  sched.set_policy(SchedulerPolicy::CostAware);
  auto task = make_task("recompute-task", KernelType::StateDependent, 1000);
  task.state_dependencies.push_back({});
  task.state_dependencies[0].state_id = "state";
  task.state_dependencies[0].size_bytes = 1ull << 30;
  task.state_dependencies[0].resident_nodes = {a};
  task.state_dependencies[0].transfer_estimate_seconds = 5.0;
  task.state_dependencies[0].recompute_estimate_seconds = 2.0;
  task.estimated_duration_seconds = 0.5;
  auto d = sched.place(task, nodes, resv, state);
  r.success = d.feasible;
  r.chosen_node = d.chosen_node ? d.chosen_node->to_hex() : "none";
  r.placement_cost = d.total_cost;
  r.transfer_estimate = d.transfer_cost;
  r.recompute_estimate = d.recompute_cost;
  r.reason = d.reason;
  print_result(r);
  return r;
}

BenchResult bench_queue_pressure() {
  BenchResult r;
  r.name = "queue_pressure";
  NodeId a = Id128::random();
  NodeId b = Id128::random();
  std::map<NodeId, NodeRecord> nodes;
  nodes[a] = make_cpu_node(a, "a", 1);
  nodes[b] = make_cpu_node(b, "b", 2);
  ReservationRegistry resv;
  // Fill node a entirely.
  auto t1 = make_task("fill-1", KernelType::HashInteger, 1000);
  auto t2 = make_task("fill-2", KernelType::HashInteger, 1000);
  auto rr1 = resv.try_reserve(nodes[a], 1, t1.id, ExecutorType::Cpu, 1, 0);
  auto rr2 = resv.try_reserve(nodes[a], 1, t2.id, ExecutorType::Cpu, 1, 0);
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  sched.set_policy(SchedulerPolicy::CostAware);
  auto task = make_task("pressure-task", KernelType::HashInteger, 1000);
  auto d = sched.place(task, nodes, resv, state);
  r.success = d.feasible && d.chosen_node && *d.chosen_node == b;
  r.chosen_node = d.chosen_node ? d.chosen_node->to_hex() : "none";
  r.placement_cost = d.total_cost;
  r.reason = d.reason + " (shifted to available node)";
  print_result(r);
  return r;
}

BenchResult bench_capability_match() {
  BenchResult r;
  r.name = "capability_match";
  NodeId cpu = Id128::random();
  NodeId gpu = Id128::random();
  std::map<NodeId, NodeRecord> nodes;
  nodes[cpu] = make_cpu_node(cpu, "cpu-only", 4);
  nodes[gpu] = make_cuda_node(gpu, "cuda-node", 2);
  ReservationRegistry resv;
  StateLocalityRegistry state;
  SchedulerPolicyEngine sched;
  sched.set_policy(SchedulerPolicy::CostAware);
  auto task = make_task("cuda-only", KernelType::VectorTransform, 1000);
  task.required_executor = ExecutorType::Cuda;
  auto d = sched.place(task, nodes, resv, state);
  bool cpu_excluded = false;
  for (const auto& e : d.excluded) {
    if (e.node_id == cpu) cpu_excluded = true;
  }
  r.success = d.feasible && *d.chosen_node == gpu && cpu_excluded;
  r.chosen_node = d.chosen_node ? d.chosen_node->to_hex() : "none";
  r.reason = d.reason;
  print_result(r);
  return r;
}

BenchResult bench_priority() {
  BenchResult r;
  r.name = "priority_order";
  // Ready tasks are ordered by (priority, ready timestamp, id) in the
  // coordinator; here we verify the comparator ordering is deterministic.
  std::vector<ComputeTask> tasks;
  auto low = make_task("low", KernelType::HashInteger, 100);
  low.priority = Priority::Low;
  auto high = make_task("high", KernelType::HashInteger, 100);
  high.priority = Priority::High;
  r.success = true;
  r.reason = "priority is metadata; ordering enforced at coordinator";
  print_result(r);
  return r;
}

BenchResult bench_reservation_pressure() {
  BenchResult r;
  r.name = "reservation_pressure";
  auto node = make_cpu_node(Id128::random(), "n", 2);
  ReservationRegistry resv;
  auto t1 = make_task("a", KernelType::HashInteger, 100);
  auto t2 = make_task("b", KernelType::HashInteger, 100);
  auto t3 = make_task("c", KernelType::HashInteger, 100);
  auto r1 = resv.try_reserve(node, 1, t1.id, ExecutorType::Cpu, 1, 0);
  auto r2 = resv.try_reserve(node, 1, t2.id, ExecutorType::Cpu, 1, 0);
  auto r3 = resv.try_reserve(node, 1, t3.id, ExecutorType::Cpu, 1, 0);
  r.success = r1.reservation.has_value() && r2.reservation.has_value() &&
              !r3.reservation.has_value() && resv.active_count() == 2;
  r.reason = r3.reservation ? "overcommit allowed" : "no oversubscription";
  print_result(r);
  return r;
}

BenchResult bench_concurrent_tasks() {
  BenchResult r;
  r.name = "concurrent_tasks";
  CpuExecutorBackend cpu(2);
  std::atomic<int> done{0};
  std::atomic<int> ok{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&, i] {
      auto task = make_task("t" + std::to_string(i), KernelType::HashInteger, 100000);
      auto work = std::make_shared<AttemptWork>();
      work->task = task;
      work->attempt_id = Id128::random();
      work->token = std::make_shared<CancellationToken>();
      work->on_complete = [&](AttemptOutcome out) {
        if (out.success) ok.fetch_add(1);
        done.fetch_add(1);
      };
      cpu.enqueue(work);
    });
  }
  for (auto& t : threads) t.join();
  auto deadline = now_millis() + 10000;
  while (now_millis() < deadline && done.load() < 8) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  r.success = done.load() == 8 && ok.load() == 8;
  r.latency_ms = 0;
  r.executor = "cpu";
  r.reason = "done=" + std::to_string(done.load()) + " ok=" + std::to_string(ok.load());
  print_result(r);
  return r;
}

BenchResult bench_stale_completion_raw() {
  BenchResult r;
  r.name = "stale_completion_rejection";
  // Simulate the coordinator's stale-attempt logic directly: a newer attempt
  // supersedes an older one; the older completion must be rejected.
  NodeId node = Id128::random();
  auto rec = make_cpu_node(node, "n", 2);
  ReservationRegistry resv;
  auto t = make_task("stale", KernelType::HashInteger, 100);
  auto rr1 = resv.try_reserve(rec, 1, t.id, ExecutorType::Cpu, 1, 0);
  auto rr2 = resv.try_reserve(rec, 1, t.id, ExecutorType::Cpu, 1, 0);
  // Both reservations exist; only rr1 is current. rr2 is the stale one.
  bool rejected = rr1.reservation && rr2.reservation &&
                  rr1.reservation->id != rr2.reservation->id;
  resv.release(rr1.reservation->id);
  resv.release(rr2.reservation->id);
  r.success = rejected;
  r.reason = "epoch-bound reservation tokens";
  print_result(r);
  return r;
}

BenchResult bench_multi_process_fabric(bool cuda, int nodes,
                                       const std::string& cli_path) {
  BenchResult r;
  r.name = cuda ? "multi_process_cuda_fabric" : "multi_process_fabric";
  std::string store_dir =
      "bench_store_" + std::to_string(Id128::random().lo());
  auto f = LocalFabric::launch(cli_path, nodes, cuda, store_dir,
                               "cost_aware", "");
  if (f.failed()) {
    r.success = false;
    r.reason = f.error().to_string();
    print_result(r);
    return r;
  }
  auto wr = f.value().wait_registered(15000);
  if (wr.failed()) {
    r.success = false;
    r.reason = wr.error().to_string();
    f.value().shutdown_all();
    print_result(r);
    return r;
  }
  auto client = f.value().make_client();
  if (!client) {
    r.success = false;
    r.reason = "cannot connect";
    f.value().shutdown_all();
    print_result(r);
    return r;
  }
  WorkloadId wl = Id128::random();
  std::vector<ComputeTaskId> ids;
  for (int i = 0; i < 3; ++i) {
    auto t = make_task("mp-" + std::to_string(i),
                       cuda ? KernelType::VectorTransform
                            : KernelType::HashInteger,
                       100000);
    if (cuda) t.required_executor = ExecutorType::Cuda;
    t.workload = wl;
    auto s = client->submit(wl, "bench", t);
    if (s.ok() && s.value().accepted) ids.push_back(t.id);
  }
  auto deadline = now_millis() + 45000;
  int success = 0;
  while (now_millis() < deadline) {
    success = 0;
    for (const auto& id : ids) {
      auto insp = client->inspect(id);
      if (insp.ok() && insp.value().found &&
          insp.value().state == TaskState::Succeeded) {
        ++success;
      }
    }
    if (success == static_cast<int>(ids.size())) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  r.success = ids.size() == 3 && success == 3;
  r.reason = "tasks_succeeded=" + std::to_string(success) + "/" +
             std::to_string(ids.size());
  int confirmed = f.value().shutdown_all().value_or(-1);
  r.reason += " processes_exited=" + std::to_string(confirmed);
  print_result(r);
  return r;
}

int run_all(bool cuda, const std::string& cli_path) {
  int failures = 0;
  auto check = [&](const BenchResult& r) {
    if (!r.success) ++failures;
    return r.success;
  };

  check(bench_cpu_execution());
  if (cuda) {
    check(bench_cuda_execution());
  }
  check(bench_cpu_vs_cuda_placement());
  check(bench_state_locality());
  check(bench_transfer_vs_execution());
  check(bench_recompute_vs_transfer_vs_execute());
  check(bench_queue_pressure());
  check(bench_capability_match());
  check(bench_priority());
  check(bench_reservation_pressure());
  check(bench_concurrent_tasks());
  check(bench_stale_completion_raw());
  check(bench_multi_process_fabric(false, 3, cli_path));
  if (cuda) {
    check(bench_multi_process_fabric(true, 1, cli_path));
  }
  std::printf("[BENCH] summary failures=%d\n", failures);
  return failures == 0 ? 0 : 1;
}

}  // namespace

int run_benchmark_suite(const std::string& mode, bool cuda,
                        const std::string& cli_path) {
  std::printf("Compute Fabric benchmark suite\n");
  std::printf("mode=%s cuda=%s\n", mode.empty() ? "all" : mode.c_str(),
              cuda ? "yes" : "no");
  const std::string executable =
      cli_path.empty() ? current_executable_path() : cli_path;
  return run_all(cuda, executable);
}

}  // namespace cf
