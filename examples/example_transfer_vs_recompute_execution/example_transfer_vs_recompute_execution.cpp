#include <cstdio>

#include "../example_common.h"

using namespace cf;
using namespace cf::example;

int main() {
  auto f = LocalFabric::launch(cli_path(), 2, false, store_dir("xfr"), "cost_aware", "");
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
  auto nodes = c->nodes();
  if (!nodes.ok() || nodes.value().nodes.size() < 2) {
    std::fprintf(stderr, "need 2 nodes\n");
    f.value().shutdown_all();
    return 1;
  }
  NodeId holder = nodes.value().nodes[0].node_id;

  // Scenario A: transfer is cheaper than recompute -> stay on the local node.
  auto ta = make_task("transfer-task", 1000, KernelType::StateDependent);
  StateDependency depA;
  depA.state_id = "state-a";
  depA.size_bytes = 1ull << 30;
  depA.resident_nodes = {holder};
  depA.transfer_estimate_seconds = 0.5;   // cheap
  depA.recompute_estimate_seconds = 30.0; // expensive
  ta.state_dependencies.push_back(depA);
  ta.forbidden_nodes = {holder};  // force consideration of the remote node
  auto pqA = c->placement_query(ta);
  double transfer_cost = pqA.ok() ? pqA.value().decision.transfer_cost : -1;
  double recompute_cost = pqA.ok() ? pqA.value().decision.recompute_cost : -1;

  // Scenario B: recompute is cheaper than transfer.
  auto tb = make_task("recompute-task", 1000, KernelType::StateDependent);
  StateDependency depB;
  depB.state_id = "state-b";
  depB.size_bytes = 50ull << 30;
  depB.resident_nodes = {holder};
  depB.transfer_estimate_seconds = 40.0;  // slow link
  depB.recompute_estimate_seconds = 2.0;  // cheap to recompute
  tb.state_dependencies.push_back(depB);
  auto pqB = c->placement_query(tb);
  double transfer_cost_b = pqB.ok() ? pqB.value().decision.transfer_cost : -1;
  double recompute_cost_b = pqB.ok() ? pqB.value().decision.recompute_cost : -1;

  std::printf("example_transfer_vs_recompute:\n");
  std::printf("  scenario_a transfer=%.4f recompute=%.4f\n", transfer_cost,
              recompute_cost);
  std::printf("  scenario_b transfer=%.4f recompute=%.4f\n", transfer_cost_b,
              recompute_cost_b);

  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("example_transfer_vs_recompute: processes_exited=%d\n", confirmed);
  if (!pqA.ok() || !pqB.ok()) {
    std::fprintf(stderr, "example_transfer_vs_recompute FAILED\n");
    return 1;
  }
  std::printf("example_transfer_vs_recompute PASSED\n");
  return 0;
}