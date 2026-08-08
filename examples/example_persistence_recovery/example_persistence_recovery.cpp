#include <cstdio>

#include "compute_fabric/coordinator/coordinator.h"
#include "compute_fabric/net/client.h"
#include "compute_fabric/node/node_runtime.h"

using namespace cf;

namespace {
std::string store_dir() {
  return "example_store_persist_" + std::to_string(cf::Id128::random().lo());
}
}  // namespace

int main() {
  bool ok = true;
  std::string dir = store_dir();
  ComputeTaskId committed_task;
  {
    CoordinatorConfig cfg;
    cfg.store_dir = dir;
    Coordinator coord(cfg);
    auto ports = coord.start();
    if (!ports.ok()) {
      std::fprintf(stderr, "coordinator start failed\n");
      return 1;
    }
    NodeConfig nc;
    nc.coordinator_node_port = ports.value().first;
    nc.cpu_slots = 2;
    NodeRuntime node(nc);
    if (node.start().failed()) {
      std::fprintf(stderr, "node start failed\n");
      coord.stop();
      return 1;
    }
    FabricClient c("127.0.0.1", ports.value().second);
    if (c.connect().failed()) {
      std::fprintf(stderr, "connect failed\n");
      node.stop();
      coord.stop();
      return 1;
    }
    ComputeTask t;
    t.id = Id128::random();
    t.workload = Id128::random();
    t.name = "persist-task";
    t.kernel = KernelType::HashInteger;
    t.kernel_param_n = 50000;
    t.seed = 3;
    auto r = c.submit(t.workload, "persist-example", t);
    if (!r.ok() || !r.value().accepted) {
      std::fprintf(stderr, "submit failed\n");
      ok = false;
    } else {
      committed_task = t.id;
      auto deadline = now_millis() + 20000;
      while (now_millis() < deadline) {
        auto insp = c.inspect(t.id);
        if (insp.ok() && insp.value().state == TaskState::Succeeded) break;
        if (insp.ok() && is_terminal(insp.value().state)) {
          ok = false;
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
    }
    node.stop();
    c.close();
    coord.stop();
  }

  // Restart with the same store: the committed task must be retained.
  {
    CoordinatorConfig cfg;
    cfg.store_dir = dir;
    Coordinator coord(cfg);
    auto ports = coord.start();
    if (!ports.ok()) {
      std::fprintf(stderr, "restart coordinator failed\n");
      return 1;
    }
    FabricClient c("127.0.0.1", ports.value().second);
    if (c.connect().failed()) {
      std::fprintf(stderr, "restart connect failed\n");
      coord.stop();
      return 1;
    }
    auto insp = c.inspect(committed_task);
    bool retained = insp.ok() && insp.value().found &&
                    insp.value().state == TaskState::Succeeded;
    std::printf("example_persistence_recovery: committed task retained=%s epoch=%llu\n",
                retained ? "yes" : "no",
                static_cast<unsigned long long>(coord.epoch()));
    if (!retained) ok = false;
    c.shutdown_coordinator();
    c.close();
    coord.stop();
  }

  if (!ok) {
    std::fprintf(stderr, "example_persistence_recovery FAILED\n");
    return 1;
  }
  std::printf("example_persistence_recovery PASSED\n");
  return 0;
}