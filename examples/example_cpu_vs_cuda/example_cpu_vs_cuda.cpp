#include <cstdio>

#include "../example_common.h"
#include "compute_fabric/executor/cuda_executor.h"

using namespace cf;
using namespace cf::example;

int main() {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) {
    std::printf("example_cpu_vs_cuda: no cuda device; SKIPPED\n");
    return 0;
  }
  auto f = LocalFabric::launch(cli_path(), 2, true, store_dir("cpucuda"), "cost_aware", "");
  if (f.failed()) {
    std::fprintf(stderr, "launch failed: %s\n", f.error().to_string().c_str());
    return 1;
  }
  if (f.value().wait_registered(20000).failed()) {
    std::fprintf(stderr, "nodes did not register\n");
    f.value().shutdown_all();
    return 1;
  }
  auto c = f.value().make_client();
  if (!c) {
    std::fprintf(stderr, "cannot connect\n");
    f.value().shutdown_all();
    return 1;
  }

  // A CUDA-required task must always land on the CUDA-capable node.
  auto t = make_task("must-use-cuda", 200000, KernelType::VectorTransform);
  t.required_executor = ExecutorType::Cuda;
  t.retry.max_attempts = 1;

  // Query placement first (explainable decision).
  auto pq = c->placement_query(t);
  if (pq.ok()) {
    std::printf("placement query: feasible=%d", pq.value().decision.feasible);
    if (pq.value().decision.chosen_node) {
      std::printf(" node=%s", pq.value().decision.chosen_node->to_hex().c_str());
    }
    for (const auto& e : pq.value().decision.excluded) {
      std::printf(" excluded=%s", e.node_id.to_hex().c_str());
    }
    std::printf("\n");
  }

  auto r = c->submit(t.workload, "cpucuda-example", t);
  if (!r.ok() || !r.value().accepted) {
    std::fprintf(stderr, "submit failed\n");
    f.value().shutdown_all();
    return 1;
  }
  bool ok = false;
  auto deadline = now_millis() + 30000;
  while (now_millis() < deadline) {
    auto insp = c->inspect(t.id);
    if (insp.ok() && insp.value().state == TaskState::Succeeded) {
      ok = true;
      break;
    }
    if (insp.ok() && is_terminal(insp.value().state)) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  auto insp = c->inspect(t.id);
  std::string device = insp.ok() && !insp.value().attempts.empty()
                           ? insp.value().attempts.back().device
                           : "-";
  std::printf("example_cpu_vs_cuda: state=%s device=%s\n",
              insp.ok() ? task_state_name(insp.value().state) : "unknown",
              device.c_str());
  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("example_cpu_vs_cuda: processes_exited=%d\n", confirmed);

  // The CUDA-required task must have executed on the CUDA executor.
  if (ok && device.find("cuda") == std::string::npos) {
    std::fprintf(stderr, "example_cpu_vs_cuda: task ran on %s, expected cuda\n",
                 device.c_str());
    return 1;
  }
  if (!ok) {
    std::fprintf(stderr, "example_cpu_vs_cuda FAILED\n");
    return 1;
  }
  std::printf("example_cpu_vs_cuda PASSED\n");
  return 0;
}