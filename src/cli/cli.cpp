#include "compute_fabric/cli/cli.h"
#include "compute_fabric/cli/benchmark.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "compute_fabric/core/digest.h"
#include "compute_fabric/core/random.h"
#include "compute_fabric/core/time_util.h"
#include "compute_fabric/core/version.h"
#include "compute_fabric/coordinator/coordinator.h"
#include "compute_fabric/executor/cpu_executor.h"
#include "compute_fabric/executor/cuda_executor.h"
#include "compute_fabric/launcher/launcher.h"
#include "compute_fabric/net/client.h"
#include "compute_fabric/node/node_runtime.h"
#include "compute_fabric/scheduler/scheduler.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <csignal>
#endif

namespace cf {

namespace {
bool interrupt_requested();
}

namespace {

struct CliArgs {
  std::map<std::string, std::string> flags;
  std::vector<std::string> positionals;
};

CliArgs parse_args(int argc, const char* const* argv) {
  CliArgs a;
  for (int i = 1; i < argc; ++i) {
    std::string s = argv[i];
    if (s.size() >= 2 && s[0] == '-' && s[1] == '-') {
      std::string key = s.substr(2);
      std::string val;
      auto eq = key.find('=');
      if (eq != std::string::npos) {
        val = key.substr(eq + 1);
        key = key.substr(0, eq);
      } else if (i + 1 < argc && argv[i + 1][0] != '-') {
        val = argv[++i];  // consume the following token as the value
      } else {
        val = "true";
      }
      a.flags[key] = val;
    } else {
      a.positionals.push_back(s);
    }
  }
  return a;
}

std::string flag(const CliArgs& a, const char* name,
                 const std::string& def = "") {
  auto it = a.flags.find(name);
  return it == a.flags.end() ? def : it->second;
}

bool has_flag(const CliArgs& a, const char* name) {
  return a.flags.count(name) > 0;
}

void print_usage(const char* prog) {
  std::printf(
      "Compute Fabric - distributed execution-placement runtime\n"
      "\n"
      "Usage: %s <command> [options]\n"
      "\n"
      "Commands:\n"
      "  --version                       print version\n"
      "  capabilities                    print executor capabilities\n"
      "  coordinator                     run the coordinator (blocking)\n"
      "  node                            run a node (blocking)\n"
      "  fabric local                    launch coordinator + N nodes locally\n"
      "  task submit                     submit a task\n"
      "  task inspect <task-id>          inspect a task\n"
      "  task list                       list tasks\n"
      "  task cancel <task-id>           cancel a task\n"
      "  workload inspect <workload-id>  inspect a workload\n"
      "  nodes                           list registered nodes\n"
      "  node drain <node-id>            drain/undrain a node\n"
      "  placement <task-spec>           query placement for a task\n"
      "  status                          fabric status\n"
      "  benchmark                       run the benchmark suite\n"
      "\n"
      "Common options:\n"
      "  --coordinator-host HOST         coordinator address (default 127.0.0.1)\n"
      "  --client-port PORT              coordinator client port\n"
      "  --node-port PORT                coordinator node port\n"
      "  --store-dir DIR                 durable state directory\n"
      "  --policy baseline|cost_aware    scheduler policy\n"
      "  --json                          JSON output where supported\n",
      prog);
}

int cmd_version() {
  std::printf("%s %s\n", project_name(), version_string());
  return 0;
}

int cmd_capabilities() {
  std::printf("capabilities:\n");
  std::printf("  cpu : %s\n", CpuExecutorBackend(2).available() ? "available" : "unavailable");
  std::vector<ExecutorCapability> cudas = enumerate_cuda_devices();
  if (cudas.empty()) {
    std::printf("  cuda: unavailable\n");
  } else {
    for (const auto& c : cudas) {
      std::printf("  cuda: device=%s cc=%d.%d mem=%llu slots=%d\n",
                  c.device_name.c_str(), c.major, c.minor,
                  static_cast<unsigned long long>(c.device_memory_bytes),
                  c.slots);
    }
  }
  return 0;
}

int cmd_coordinator(const CliArgs& a) {
  CoordinatorConfig cfg;
  cfg.node_port = static_cast<uint16_t>(std::stoi(flag(a, "node-port", "0")));
  cfg.client_port = static_cast<uint16_t>(std::stoi(flag(a, "client-port", "0")));
  cfg.store_dir = flag(a, "store-dir", ".compute_fabric_store");
  if (has_flag(a, "telemetry")) cfg.telemetry_path = flag(a, "telemetry");
  cfg.telemetry_human = has_flag(a, "telemetry-human");
  std::string policy = flag(a, "policy", "cost_aware");
  SchedulerPolicy p;
  if (!parse_scheduler_policy(policy, p)) {
    std::fprintf(stderr, "error: unknown policy '%s'\n", policy.c_str());
    return 2;
  }
  cfg.policy = p;

  Coordinator coord(cfg);
  auto r = coord.start();
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  std::printf("coordinator listening node_port=%u client_port=%u epoch=%llu\n",
              r.value().first, r.value().second,
              static_cast<unsigned long long>(coord.epoch()));
  std::fflush(stdout);
  install_interrupt_handler(nullptr);  // ensures console handler installed
  std::atomic<bool> finished{false};
  std::thread runner([&] {
    coord.run();
    finished.store(true);
  });
  while (!finished.load() && !interrupt_requested()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  if (interrupt_requested()) coord.stop();
  runner.join();
  coord.stop();
  return 0;
}

int cmd_node(const CliArgs& a) {
  NodeConfig cfg;
  cfg.coordinator_node_port =
      static_cast<uint16_t>(std::stoi(flag(a, "coordinator-port", "0")));
  cfg.coordinator_host = flag(a, "coordinator-host", "127.0.0.1");
  cfg.label = flag(a, "label", "node");
  cfg.address = flag(a, "address", "127.0.0.1");
  cfg.cpu_slots = std::stoi(flag(a, "cpu-slots", "2"));
  cfg.enable_cuda = has_flag(a, "cuda");
  cfg.cuda_device_ordinal = std::stoi(flag(a, "cuda-device", "0"));
  cfg.cuda_slots = std::stoi(flag(a, "cuda-slots", "2"));
  if (has_flag(a, "tag")) {
    std::string tag = flag(a, "tag");
    auto eq = tag.find('=');
    if (eq != std::string::npos) {
      cfg.tags[tag.substr(0, eq)] = tag.substr(eq + 1);
    }
  }
  if (cfg.coordinator_node_port == 0) {
    std::fprintf(stderr, "error: --coordinator-port is required\n");
    return 2;
  }

  NodeRuntime node(cfg);
  auto r = node.start();
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  std::printf("node %s registered (session epoch %llu, coordinator epoch %llu)\n",
              node.node_id().to_hex().c_str(),
              static_cast<unsigned long long>(node.session_epoch()),
              static_cast<unsigned long long>(node.coordinator_epoch()));
  std::fflush(stdout);
  install_interrupt_handler(nullptr);
  std::atomic<bool> finished{false};
  std::thread runner([&] {
    node.run();
    finished.store(true);
  });
  while (!finished.load() && !interrupt_requested()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  if (interrupt_requested()) node.stop();
  runner.join();
  node.stop();
  return 0;
}

ComputeTask parse_task_from_flags(const CliArgs& a) {
  ComputeTask t;
  auto id_hex = flag(a, "task-id", "");
  if (!id_hex.empty()) {
    auto r = Id128::from_hex(id_hex);
    if (r.ok()) t.id = r.value();
  } else {
    t.id = Id128::random();
  }
  auto wl_hex = flag(a, "workload", "");
  if (!wl_hex.empty()) {
    auto r = Id128::from_hex(wl_hex);
    if (r.ok()) t.workload = r.value();
  } else {
    t.workload = Id128::random();
  }
  t.name = flag(a, "name", "task");
  std::string kn = flag(a, "kernel", "hash_integer");
  if (!parse_kernel_type(kn, t.kernel)) t.kernel = KernelType::HashInteger;
  t.kernel_param_n = std::stoull(flag(a, "n", "100000"));
  t.kernel_param_a = std::stoull(flag(a, "a", "31"));
  t.kernel_param_b = std::stoull(flag(a, "b", "17"));
  t.seed = std::stoull(flag(a, "seed", "42"));
  std::string ex = flag(a, "executor", "any");
  if (!parse_executor_type(ex, t.required_executor)) t.required_executor = ExecutorType::Any_;
  std::string pr = flag(a, "priority", "normal");
  if (!parse_priority(pr, t.priority)) t.priority = Priority::Normal;
  t.estimated_duration_seconds = std::stod(flag(a, "duration", "0.05"));
  t.expected_input_bytes = std::stoull(flag(a, "input-bytes", "0"));
  t.expected_output_bytes = std::stoull(flag(a, "output-bytes", "0"));
  t.expected_transfer_gb = std::stod(flag(a, "transfer-gb", "0"));
  t.expected_recompute_cost_seconds = std::stod(flag(a, "recompute", "0"));
  t.retry.max_attempts = static_cast<uint32_t>(std::stoul(flag(a, "max-attempts", "1")));
  t.retry.retry_backoff_ms = std::stoull(flag(a, "retry-backoff-ms", "100"));
  if (has_flag(a, "retry-on-node-loss")) {
    t.retry.retry_on_node_loss = flag(a, "retry-on-node-loss") != "false";
  }
  if (has_flag(a, "min-cc-major")) {
    t.min_compute_capability_major = std::stoi(flag(a, "min-cc-major"));
  }
  if (has_flag(a, "scratch")) {
    t.scratch_memory_bytes = std::stoull(flag(a, "scratch"));
  }
  if (has_flag(a, "required-tag")) {
    t.required_tags.push_back(flag(a, "required-tag"));
  }
  if (has_flag(a, "state-id")) {
    cf::StateDependency dep;
    dep.state_id = flag(a, "state-id");
    dep.size_bytes = std::stoull(flag(a, "state-size", "0"));
    dep.transfer_estimate_seconds = std::stod(flag(a, "state-transfer", "-1"));
    dep.recompute_estimate_seconds = std::stod(flag(a, "state-recompute", "-1"));
    if (has_flag(a, "resident-node")) {
      auto id = Id128::from_hex(flag(a, "resident-node"));
      if (id.ok()) dep.resident_nodes.push_back(id.value());
    }
    t.state_dependencies.push_back(dep);
  }
  return t;
}

Result<std::unique_ptr<FabricClient>> make_client(const CliArgs& a) {
  std::string host = flag(a, "coordinator-host", "127.0.0.1");
  uint16_t port = static_cast<uint16_t>(std::stoi(flag(a, "client-port", "0")));
  if (port == 0) {
    return Error(ErrorCode::InvalidArgument, "--client-port required");
  }
  auto c = std::make_unique<FabricClient>(host, port);
  auto r = c->connect();
  if (r.failed()) return r.error();
  return c;
}

int cmd_task_submit(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  ComputeTask t = parse_task_from_flags(a);
  auto r = c.value()->submit(t.workload, flag(a, "workload-name", "cli"), t);
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  if (!r.value().accepted) {
    std::fprintf(stderr, "error: submit rejected: %s\n", r.value().error.c_str());
    return 2;
  }
  std::printf("submitted task %s\n", r.value().task_id.to_hex().c_str());
  return 0;
}

int cmd_task_inspect(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  if (a.positionals.empty()) {
    std::fprintf(stderr, "error: task id required\n");
    return 2;
  }
  auto id = Id128::from_hex(a.positionals[0]);
  if (id.failed()) {
    std::fprintf(stderr, "error: bad task id\n");
    return 2;
  }
  auto r = c.value()->inspect(id.value());
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  const auto& resp = r.value();
  if (!resp.found) {
    std::fprintf(stderr, "task not found: %s\n", a.positionals[0].c_str());
    return 1;
  }
  std::printf("task %s\n", resp.task_id.to_hex().c_str());
  std::printf("  name: %s\n", resp.name.c_str());
  std::printf("  class: %s\n", task_class_name(resp.task_class));
  std::printf("  state: %s\n", task_state_name(resp.state));
  std::printf("  attempts: %zu\n", resp.attempts.size());
  for (const auto& at : resp.attempts) {
    std::printf("    attempt %s state=%s node=%s device=%s integrity=%s\n",
                at.id.to_hex().c_str(), attempt_state_name(at.state),
                at.node_id.to_hex().c_str(), at.device.c_str(),
                at.output_integrity.c_str());
  }
  return 0;
}

int cmd_task_list(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  auto r = c.value()->list(flag(a, "state", ""));
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  for (const auto& e : r.value().tasks) {
    std::printf("%s %-12s %-10s %s\n", e.task_id.to_hex().c_str(),
                task_state_name(e.state), priority_name(e.priority),
                e.name.c_str());
  }
  return 0;
}

int cmd_task_cancel(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  if (a.positionals.empty()) {
    std::fprintf(stderr, "error: task id required\n");
    return 2;
  }
  auto id = Id128::from_hex(a.positionals[0]);
  if (id.failed()) {
    std::fprintf(stderr, "error: bad task id\n");
    return 2;
  }
  auto r = c.value()->cancel(id.value());
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  if (!r.value().found) {
    std::fprintf(stderr, "task not found\n");
    return 1;
  }
  std::printf("cancel %s: %s\n", r.value().cancelled ? "accepted" : "rejected",
              r.value().error.c_str());
  return r.value().cancelled ? 0 : 1;
}

int cmd_workload_inspect(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  if (a.positionals.empty()) {
    std::fprintf(stderr, "error: workload id required\n");
    return 2;
  }
  auto id = Id128::from_hex(a.positionals[0]);
  if (id.failed()) {
    std::fprintf(stderr, "error: bad workload id\n");
    return 2;
  }
  auto r = c.value()->workload_inspect(id.value());
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  std::printf("workload %s %s\n", r.value().workload_id.to_hex().c_str(),
              r.value().name.c_str());
  for (const auto& e : r.value().tasks) {
    std::printf("  %s %s\n", e.task_id.to_hex().c_str(),
                task_state_name(e.state));
  }
  return 0;
}

int cmd_nodes(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  auto r = c.value()->nodes();
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  for (const auto& e : r.value().nodes) {
    std::printf("%s %-8s %-12s cpu=%u/%u %s\n", e.node_id.to_hex().c_str(),
                node_health_name(e.health), e.label.c_str(), e.cpu_used,
                e.cpu_slots, e.address.c_str());
  }
  return 0;
}

int cmd_node_drain(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  if (a.positionals.empty()) {
    std::fprintf(stderr, "error: node id required\n");
    return 2;
  }
  auto id = Id128::from_hex(a.positionals[0]);
  if (id.failed()) {
    std::fprintf(stderr, "error: bad node id\n");
    return 2;
  }
  bool drain = flag(a, "undrain", "false") == "true" ? false : true;
  auto r = c.value()->drain(id.value(), drain);
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  if (!r.value().found) {
    std::fprintf(stderr, "node not found\n");
    return 1;
  }
  std::printf("drain %s: %s\n",
              r.value().accepted ? "accepted" : "rejected",
              r.value().error.c_str());
  return r.value().accepted ? 0 : 1;
}

int cmd_status(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  auto r = c.value()->status();
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  const auto& s = r.value();
  std::printf("coordinator epoch %llu policy %s\n",
              static_cast<unsigned long long>(s.coordinator_epoch),
              s.policy.c_str());
  std::printf("nodes=%u tasks=%u running=%u reserved=%u completed=%u\n",
              s.node_count, s.task_count, s.running_count, s.reserved_count,
              s.completed_count);
  for (const auto& e : s.nodes) {
    std::printf("  node %s %-8s %-12s cpu=%u/%u\n",
                e.node_id.to_hex().c_str(), node_health_name(e.health),
                e.label.c_str(), e.cpu_used, e.cpu_slots);
  }
  return 0;
}

int cmd_placement(const CliArgs& a) {
  auto c = make_client(a);
  if (c.failed()) {
    std::fprintf(stderr, "error: %s\n", c.error().to_string().c_str());
    return 2;
  }
  ComputeTask t = parse_task_from_flags(a);
  auto r = c.value()->placement_query(t);
  if (r.failed()) {
    std::fprintf(stderr, "error: %s\n", r.error().to_string().c_str());
    return 2;
  }
  const auto& d = r.value().decision;
  if (!d.feasible) {
    std::printf("placement infeasible: %s\n", d.reason.c_str());
    for (const auto& e : d.excluded) {
      std::printf("  excluded %s:", e.node_id.to_hex().c_str());
      for (const auto& rt : e.reasons) std::printf(" %s", rt.c_str());
      std::printf("\n");
    }
    return 1;
  }
  std::printf("placement: node=%s exec=%s\n", d.chosen_node->to_hex().c_str(),
              d.chosen_device.c_str());
  std::printf("  total=%f exec=%f queue=%f transfer=%f recompute=%f pressure=%f locality=%f\n",
              d.total_cost, d.execution_cost, d.queue_cost, d.transfer_cost,
              d.recompute_cost, d.pressure_cost, d.state_locality_score);
  for (const auto& e : d.excluded) {
    std::printf("  excluded %s:", e.node_id.to_hex().c_str());
    for (const auto& rt : e.reasons) std::printf(" %s", rt.c_str());
    std::printf("\n");
  }
  return 0;
}

int cmd_fabric_local(const CliArgs& a) {
  install_interrupt_handler(nullptr);
  int nodes = std::stoi(flag(a, "nodes", "2"));
  bool cuda_node = has_flag(a, "cuda-node");
  std::string store_dir = flag(a, "store-dir", ".compute_fabric_store_local");
  std::string policy = flag(a, "policy", "cost_aware");
  std::string telemetry_dir = flag(a, "telemetry-dir", "");

  std::string exe = current_executable_path();
  auto f = LocalFabric::launch(exe, nodes, cuda_node, store_dir, policy,
                               telemetry_dir);
  if (f.failed()) {
    std::fprintf(stderr, "error: %s\n", f.error().to_string().c_str());
    return 2;
  }

  auto wr = f.value().wait_registered(15000);
  if (wr.failed()) {
    std::fprintf(stderr, "error: %s\n", wr.error().to_string().c_str());
    f.value().shutdown_all();
    return 2;
  }

  auto client = f.value().make_client();
  if (!client) {
    std::fprintf(stderr, "error: cannot connect to coordinator\n");
    f.value().shutdown_all();
    return 2;
  }

  // Deterministic validation workload.
  WorkloadId wl = Id128::random();
  std::vector<ComputeTaskId> submitted;
  int failures = 0;
  for (int i = 0; i < 3; ++i) {
    ComputeTask t;
    t.id = Id128::random();
    t.workload = wl;
    t.name = "minimal-" + std::to_string(i);
    t.kernel = KernelType::HashInteger;
    t.kernel_param_n = 50000;
    t.kernel_param_a = 31;
    t.kernel_param_b = 17;
    t.seed = 42 + i;
    t.estimated_duration_seconds = 0.01;
    t.retry.max_attempts = 2;
    auto r = client->submit(wl, "fabric-local", t);
    if (r.failed() || !r.value().accepted) {
      std::fprintf(stderr, "submit failed: %s\n",
                   r.ok() ? r.value().error.c_str() : r.error().to_string().c_str());
      ++failures;
    } else {
      submitted.push_back(t.id);
    }
  }
  if (cuda_node) {
    ComputeTask t;
    t.id = Id128::random();
    t.workload = wl;
    t.name = "cuda-transform";
    t.required_executor = ExecutorType::Cuda;
    t.kernel = KernelType::VectorTransform;
    t.kernel_param_n = 200000;
    t.kernel_param_a = 31;
    t.kernel_param_b = 17;
    t.seed = 7;
    t.estimated_duration_seconds = 0.01;
    t.retry.max_attempts = 2;
    auto r = client->submit(wl, "fabric-local", t);
    if (r.failed() || !r.value().accepted) {
      std::fprintf(stderr, "cuda submit failed: %s\n",
                   r.ok() ? r.value().error.c_str() : r.error().to_string().c_str());
      ++failures;
    } else {
      submitted.push_back(t.id);
    }
  }

  // Wait for all tasks to reach a terminal state.
  auto deadline = now_millis() + 60000;
  bool all_terminal = false;
  while (now_millis() < deadline && !interrupt_requested()) {
    all_terminal = true;
    for (const auto& id : submitted) {
      auto insp = client->inspect(id);
      if (insp.failed() || !insp.value().found ||
          !is_terminal(insp.value().state)) {
        all_terminal = false;
        break;
      }
    }
    if (all_terminal) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  int success = 0;
  int failed = 0;
  for (const auto& id : submitted) {
    auto insp = client->inspect(id);
    if (insp.ok() && insp.value().found &&
        insp.value().state == TaskState::Succeeded) {
      ++success;
    } else {
      ++failed;
    }
  }
  auto st = client->status();
  std::printf("fabric local: tasks=%zu succeeded=%d failed=%d\n",
              submitted.size(), success, failed);
  if (st.ok()) {
    std::printf("fabric local: nodes=%u epoch=%llu\n", st.value().node_count,
                static_cast<unsigned long long>(st.value().coordinator_epoch));
  }

  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("fabric local: shutdown confirmed processes=%d\n", confirmed);
  if (interrupt_requested()) {
    std::fprintf(stderr, "fabric local: interrupted\n");
    return 130;
  }
  if (!all_terminal || failures > 0 || failed > 0 || confirmed < nodes + 1) {
    std::fprintf(stderr, "fabric local: validation FAILED\n");
    return 2;
  }
  std::printf("fabric local: validation PASSED\n");
  return 0;
}

}  // namespace

#if defined(_WIN32)
static volatile LONG g_interrupt = 0;
static BOOL WINAPI ctrl_handler(DWORD type) {
  (void)type;
  InterlockedExchange(&g_interrupt, 1);
  return TRUE;
}
#else
static volatile sig_atomic_t g_interrupt = 0;
static void interrupt_handler(int) { g_interrupt = 1; }
#endif

namespace {
bool interrupt_requested() {
#if defined(_WIN32)
  return InterlockedCompareExchange(&g_interrupt, 0, 0) != 0;
#else
  return g_interrupt != 0;
#endif
}
}  // namespace

void install_interrupt_handler(void (*fn)()) {
#if defined(_WIN32)
  InterlockedExchange(&g_interrupt, 0);
  SetConsoleCtrlHandler(ctrl_handler, TRUE);
#else
  g_interrupt = 0;
  std::signal(SIGINT, interrupt_handler);
  std::signal(SIGTERM, interrupt_handler);
#endif
  (void)fn;
}

int cli_main(int argc, const char* const* argv) {
  setvbuf(stdout, nullptr, _IONBF, 0);
  setvbuf(stderr, nullptr, _IONBF, 0);
  // Child-owning commands must convert console interruption into orderly
  // stack unwinding so LocalFabric destructors can reap every child.
  install_interrupt_handler(nullptr);
  CliArgs args = parse_args(argc, argv);

  if (has_flag(args, "version") || has_flag(args, "v")) return cmd_version();
  if (args.positionals.empty()) {
    print_usage(argv[0]);
    return 0;
  }

  const std::string& cmd = args.positionals[0];
  if (cmd == "capabilities") return cmd_capabilities();
  if (cmd == "coordinator") return cmd_coordinator(args);
  if (cmd == "node") {
    if (args.positionals.size() >= 2 && args.positionals[1] == "drain") {
      args.positionals.erase(args.positionals.begin(),
                             args.positionals.begin() + 2);
      return cmd_node_drain(args);
    }
    return cmd_node(args);
  }
  if (cmd == "fabric") {
    if (args.positionals.size() >= 2 && args.positionals[1] == "local") {
      return cmd_fabric_local(args);
    }
    print_usage(argv[0]);
    return 2;
  }
  if (cmd == "task") {
    if (args.positionals.size() < 2) {
      print_usage(argv[0]);
      return 2;
    }
    const std::string& sub = args.positionals[1];
    if (sub == "submit") return cmd_task_submit(args);
    if (sub == "inspect") return cmd_task_inspect(args);
    if (sub == "list") return cmd_task_list(args);
    if (sub == "cancel") return cmd_task_cancel(args);
    print_usage(argv[0]);
    return 2;
  }
  if (cmd == "workload") {
    if (args.positionals.size() >= 2 && args.positionals[1] == "inspect") {
      return cmd_workload_inspect(args);
    }
    print_usage(argv[0]);
    return 2;
  }
  if (cmd == "nodes") return cmd_nodes(args);
  if (cmd == "placement") return cmd_placement(args);
  if (cmd == "status") return cmd_status(args);
  if (cmd == "benchmark") {
    std::string mode = args.positionals.size() >= 2 ? args.positionals[1] : "";
    bool cuda = has_flag(args, "cuda") || mode == "cuda";
    return run_benchmark_suite(flag(args, "mode", mode), cuda);
  }
  print_usage(argv[0]);
  return 2;
}

}  // namespace cf
