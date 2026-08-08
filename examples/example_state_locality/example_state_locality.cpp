#include <cstdio>

#include "../example_common.h"

using namespace cf;
using namespace cf::example;

int main() {
  auto f = LocalFabric::launch(cli_path(), 2, false, store_dir("loc"), "cost_aware", "");
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

  // Learn the two registered node ids.
  auto nodes = c->nodes();
  if (!nodes.ok() || nodes.value().nodes.size() < 2) {
    std::fprintf(stderr, "need 2 nodes\n");
    f.value().shutdown_all();
    return 1;
  }
  NodeId state_holder = nodes.value().nodes[0].node_id;

  // A task whose required state is resident on node 0 (8 GiB).
  auto t = make_task("state-task", 1000, KernelType::StateDependent);
  StateDependency dep;
  dep.state_id = "model-weights";
  dep.generation = "v7";
  dep.size_bytes = 8ull << 30;
  dep.resident_nodes = {state_holder};
  dep.transfer_estimate_seconds = 0.0;      // local
  dep.recompute_estimate_seconds = 30.0;    // expensive
  t.state_dependencies.push_back(dep);

  auto r = c->submit(t.workload, "loc-example", t);
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
  NodeId chosen = insp.ok() && !insp.value().attempts.empty()
                      ? insp.value().attempts.back().node_id
                      : Id128::zero();
  std::printf("example_state_locality: state holder=%s chosen=%s local=%s\n",
              state_holder.to_hex().c_str(), chosen.to_hex().c_str(),
              chosen == state_holder ? "yes" : "no");

  // Also query the explainable placement decision directly.
  auto pq = c->placement_query(t);
  if (pq.ok()) {
    const auto& d = pq.value().decision;
    std::printf("  transfer_cost=%.4f recompute_cost=%.4f locality_score=%.4f\n",
                d.transfer_cost, d.recompute_cost, d.state_locality_score);
  }

  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("example_state_locality: processes_exited=%d\n", confirmed);
  if (!ok || chosen != state_holder) {
    std::fprintf(stderr, "example_state_locality FAILED\n");
    return 1;
  }
  std::printf("example_state_locality PASSED\n");
  return 0;
}