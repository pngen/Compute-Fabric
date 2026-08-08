#include <cstdio>

#include "../example_common.h"

using namespace cf;
using namespace cf::example;

int main() {
  auto f = LocalFabric::launch(cli_path(), 2, false, store_dir("prio"), "cost_aware", "");
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

  // Submit a low-priority task first, then a critical one, on a single-slot
  // node so ordering is observable. Only one node is used to serialize.
  std::vector<ComputeTaskId> ids;
  auto low = make_task("background-task", 4000000);
  low.kernel = KernelType::SyntheticLoop;
  low.retry.max_attempts = 1;
  low.priority = Priority::Background;
  auto crit = make_task("critical-task", 50000);
  crit.kernel = KernelType::HashInteger;
  crit.retry.max_attempts = 1;
  crit.priority = Priority::Critical;

  auto r1 = c->submit(low.workload, "prio-example", low);
  auto r2 = c->submit(crit.workload, "prio-example", crit);
  if ((r1.ok() && r1.value().accepted)) ids.push_back(low.id);
  if ((r2.ok() && r2.value().accepted)) ids.push_back(crit.id);

  bool ok = wait_for_success(*c, ids, 30000);
  auto insp_crit = c->inspect(crit.id);
  auto insp_low = c->inspect(low.id);
  // The critical task must be dispatched before the background task because
  // the coordinator orders ready tasks by priority.
  bool ordered = ok && insp_crit.ok() && insp_low.ok();
  if (ordered) {
    auto crit_dispatch = insp_crit.value().attempts.empty()
                             ? 0
                             : insp_crit.value().attempts[0].dispatched_at_ms;
    auto low_dispatch = insp_low.value().attempts.empty()
                            ? 0
                            : insp_low.value().attempts[0].dispatched_at_ms;
    ordered = crit_dispatch > 0 && crit_dispatch <= low_dispatch;
  }
  std::printf("example_priority: critical=%s background=%s\n",
              insp_crit.ok() ? task_state_name(insp_crit.value().state) : "na",
              insp_low.ok() ? task_state_name(insp_low.value().state) : "na");
  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("example_priority: processes_exited=%d\n", confirmed);
  if (!ok || !ordered) {
    std::fprintf(stderr, "example_priority FAILED\n");
    return 1;
  }
  std::printf("example_priority PASSED\n");
  return 0;
}