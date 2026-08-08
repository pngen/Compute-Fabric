#include "framework.h"

#include <cstdio>
#include <cstdlib>
#include <thread>

#include "compute_fabric/launcher/launcher.h"
#include "compute_fabric/net/client.h"

using namespace cf;

namespace {

std::string cli_path() {
#if defined(CF_CLI_PATH)
  return std::string(CF_CLI_PATH);
#else
  return current_executable_path();
#endif
}

std::string store_dir() {
  return "test_store_mp_" + std::to_string(cf::Id128::random().lo());
}

ComputeTask mp_task(const char* name, uint64_t n = 50000) {
  ComputeTask t;
  t.id = Id128::random();
  t.workload = Id128::random();
  t.name = name;
  t.kernel = KernelType::HashInteger;
  t.kernel_param_n = n;
  t.kernel_param_a = 31;
  t.kernel_param_b = 17;
  t.seed = 42;
  return t;
}

}  // namespace

TEST(multi_process_one_node) {
  auto f = LocalFabric::launch(cli_path(), 1, false, store_dir(), "cost_aware", "");
  EXPECT_TRUE(f.ok());
  if (f.failed()) return;
  EXPECT_OK(f.value().wait_registered(20000));
  auto c = f.value().make_client();
  EXPECT_TRUE(c != nullptr);
  if (!c) return;
  auto task = mp_task("mp1");
  auto r = c->submit(task.workload, "w", task);
  EXPECT_TRUE(r.ok() && r.value().accepted);
  auto deadline = now_millis() + 30000;
  bool ok = false;
  while (now_millis() < deadline) {
    auto insp = c->inspect(task.id);
    if (insp.ok() && insp.value().state == TaskState::Succeeded) {
      ok = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(ok);
  int confirmed = f.value().shutdown_all().value_or(-1);
  EXPECT_GE(confirmed, 2);  // coordinator + 1 node
}

TEST(multi_process_three_nodes) {
  auto f = LocalFabric::launch(cli_path(), 3, false, store_dir(), "cost_aware", "");
  EXPECT_TRUE(f.ok());
  if (f.failed()) return;
  EXPECT_OK(f.value().wait_registered(20000));
  EXPECT_EQ(f.value().nodes().size(), 3ull);
  auto c = f.value().make_client();
  EXPECT_TRUE(c != nullptr);
  if (!c) return;
  // Submit 3 tasks; they should spread across nodes.
  std::vector<ComputeTaskId> ids;
  for (int i = 0; i < 3; ++i) {
    auto task = mp_task(("mp3-" + std::to_string(i)).c_str());
    auto r = c->submit(task.workload, "w", task);
    if (r.ok() && r.value().accepted) ids.push_back(task.id);
  }
  EXPECT_EQ(ids.size(), 3ull);
  auto deadline = now_millis() + 45000;
  int succeeded = 0;
  while (now_millis() < deadline) {
    succeeded = 0;
    for (const auto& id : ids) {
      auto insp = c->inspect(id);
      if (insp.ok() && insp.value().state == TaskState::Succeeded) ++succeeded;
    }
    if (succeeded == 3) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_EQ(succeeded, 3);
  int confirmed = f.value().shutdown_all().value_or(-1);
  EXPECT_GE(confirmed, 4);  // coordinator + 3 nodes
}

TEST(multi_process_shutdown_leaves_no_orphans) {
  auto f = LocalFabric::launch(cli_path(), 2, false, store_dir(), "cost_aware", "");
  EXPECT_TRUE(f.ok());
  if (f.failed()) return;
  EXPECT_OK(f.value().wait_registered(20000));
  int confirmed = f.value().shutdown_all().value_or(-1);
  EXPECT_GE(confirmed, 3);  // coordinator + 2 nodes
  // After shutdown, the coordinator and nodes must not be running.
  // (LocalFabric::shutdown_all sets running=false once processes exit.)
  EXPECT_FALSE(f.value().healthy());
  for (const auto& n : f.value().nodes()) {
    EXPECT_FALSE(n.running);
  }
}