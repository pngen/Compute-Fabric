#include <cstdio>

#include "../example_common.h"

using namespace cf;
using namespace cf::example;

int main() {
  auto f = LocalFabric::launch(cli_path(), 3, false, store_dir("mp"), "cost_aware", "");
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

  std::vector<ComputeTaskId> ids;
  for (int i = 0; i < 4; ++i) {
    auto t = make_task(("mp-" + std::to_string(i)).c_str(), 80000);
    auto r = c->submit(t.workload, "mp-example", t);
    if (r.ok() && r.value().accepted) ids.push_back(t.id);
  }
  bool ok = wait_for_success(*c, ids, 45000);

  auto nodes = c->nodes();
  std::printf("example_multi_node_placement: registered nodes=%zu\n",
              nodes.ok() ? nodes.value().nodes.size() : 0);
  for (auto& id : ids) {
    auto insp = c->inspect(id);
    if (insp.ok() && !insp.value().attempts.empty()) {
      std::printf("  task %s -> node %s device %s state %s\n",
                  id.to_hex().c_str(),
                  insp.value().attempts.back().node_id.to_hex().c_str(),
                  insp.value().attempts.back().device.c_str(),
                  task_state_name(insp.value().state));
    }
  }
  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("example_multi_node_placement: processes_exited=%d\n", confirmed);
  if (!ok) {
    std::fprintf(stderr, "example_multi_node_placement FAILED\n");
    return 1;
  }
  std::printf("example_multi_node_placement PASSED\n");
  return 0;
}