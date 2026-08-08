#include "framework.h"

#include <thread>

#include "compute_fabric/coordinator/coordinator.h"
#include "compute_fabric/launcher/launcher.h"
#include "compute_fabric/net/client.h"
#include "compute_fabric/node/node_runtime.h"

using namespace cf;

namespace {

std::string store_dir() {
  return "test_store_stress_" + std::to_string(cf::Id128::random().lo());
}

std::string cli_path() {
#if defined(CF_CLI_PATH)
  return std::string(CF_CLI_PATH);
#else
  return current_executable_path();
#endif
}

}  // namespace

TEST(stress_repeated_create_destroy_fabric) {
  for (int i = 0; i < 3; ++i) {
    auto f = LocalFabric::launch(cli_path(), 2, false, store_dir(), "cost_aware", "");
    EXPECT_TRUE(f.ok());
    if (f.failed()) break;
    EXPECT_OK(f.value().wait_registered(15000));
    int confirmed = f.value().shutdown_all().value_or(-1);
    EXPECT_GE(confirmed, 3);
  }
}

TEST(stress_repeated_coordinator_node_cycles) {
  for (int i = 0; i < 3; ++i) {
    CoordinatorConfig cfg;
    cfg.store_dir = store_dir();
    Coordinator coord(cfg);
    auto ports = coord.start();
    EXPECT_TRUE(ports.ok());
    NodeConfig nc;
    nc.coordinator_node_port = ports.value().first;
    nc.cpu_slots = 2;
    NodeRuntime node(nc);
    EXPECT_OK(node.start());
    FabricClient c("127.0.0.1", ports.value().second);
    EXPECT_OK(c.connect());
    EXPECT_TRUE(c.wait_for_nodes(1, 10000));
    ComputeTask t;
    t.id = Id128::random();
    t.workload = Id128::random();
    t.name = "stress";
    t.kernel = KernelType::HashInteger;
    t.kernel_param_n = 20000;
    t.seed = 1;
    EXPECT_TRUE(c.submit(t.workload, "w", t).value().accepted);
    auto deadline = now_millis() + 15000;
    while (now_millis() < deadline) {
      auto insp = c.inspect(t.id);
      if (insp.ok() && insp.value().state == TaskState::Succeeded) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    auto insp = c.inspect(t.id);
    EXPECT_TRUE(insp.ok());
    EXPECT_EQ(insp.value().state, TaskState::Succeeded);
    node.stop();
    c.close();
    coord.stop();
  }
}

TEST(stress_burst_submissions_single_node) {
  CoordinatorConfig cfg;
  cfg.store_dir = store_dir();
  Coordinator coord(cfg);
  auto ports = coord.start();
  EXPECT_TRUE(ports.ok());
  NodeConfig nc;
  nc.coordinator_node_port = ports.value().first;
  nc.cpu_slots = 2;
  NodeRuntime node(nc);
  EXPECT_OK(node.start());
  FabricClient c("127.0.0.1", ports.value().second);
  EXPECT_OK(c.connect());
  EXPECT_TRUE(c.wait_for_nodes(1, 10000));

  std::vector<ComputeTaskId> ids;
  for (int i = 0; i < 12; ++i) {
    ComputeTask t;
    t.id = Id128::random();
    t.workload = Id128::random();
    t.name = "burst";
    t.kernel = KernelType::SyntheticLoop;
    t.kernel_param_n = 2000000;
    t.seed = i;
    auto r = c.submit(t.workload, "w", t);
    if (r.ok() && r.value().accepted) ids.push_back(t.id);
  }
  EXPECT_EQ(ids.size(), 12ull);
  auto deadline = now_millis() + 30000;
  int succeeded = 0;
  while (now_millis() < deadline) {
    succeeded = 0;
    for (const auto& id : ids) {
      auto insp = c.inspect(id);
      if (insp.ok() && insp.value().state == TaskState::Succeeded) ++succeeded;
    }
    if (succeeded == 12) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_EQ(succeeded, 12);
  node.stop();
  c.close();
  coord.stop();
}

TEST(stress_shutdown_idempotent) {
  CoordinatorConfig cfg;
  cfg.store_dir = store_dir();
  Coordinator coord(cfg);
  auto ports = coord.start();
  EXPECT_TRUE(ports.ok());
  NodeConfig nc;
  nc.coordinator_node_port = ports.value().first;
  nc.cpu_slots = 2;
  NodeRuntime node(nc);
  EXPECT_OK(node.start());
  node.stop();
  node.stop();  // repeated shutdown must be safe
  coord.stop();
  coord.stop();  // repeated shutdown must be safe
  EXPECT_TRUE(true);
}