#include <cstdio>

#include "../example_common.h"

using namespace cf;
using namespace cf::example;

int main() {
  auto f = LocalFabric::launch(cli_path(), 2, false, store_dir("retry"), "cost_aware", "");
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

  // Use a workload that stays active long enough for this example to observe
  // and terminate its first placement. A fast hash task can complete between
  // the 25 ms inspection polls and never demonstrate node-loss retry.
  auto t = make_task("retry-task", 20000000, KernelType::SyntheticLoop);
  t.estimated_duration_seconds = 1.0;
  t.retry.max_attempts = 3;
  t.retry.retry_on_node_loss = true;
  t.retry.retry_backoff_ms = 50;
  auto r = c->submit(t.workload, "retry-example", t);
  if (!r.ok() || !r.value().accepted) {
    std::fprintf(stderr, "submit failed\n");
    f.value().shutdown_all();
    return 1;
  }

  // Wait for the first attempt, then kill its node.
  auto deadline = now_millis() + 10000;
  bool captured = false;
  NodeId host;
  ExecutionAttempt first;
  while (now_millis() < deadline) {
    auto insp = c->inspect(t.id);
    if (insp.ok() && !insp.value().attempts.empty() &&
        insp.value().attempts[0].state == AttemptState::Started) {
      host = insp.value().attempts[0].node_id;
      first = insp.value().attempts[0];
      captured = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  if (!captured) {
    std::fprintf(stderr, "no attempt captured\n");
    f.value().shutdown_all();
    return 1;
  }
  // Find the label of the node hosting the attempt and kill that process.
  std::string host_label;
  auto nl = c->nodes();
  if (nl.ok()) {
    for (const auto& e : nl.value().nodes) {
      if (e.node_id == host) {
        host_label = e.label;
        break;
      }
    }
  }
  std::printf("killing node %s (label %s) hosting attempt %s\n",
              host.to_hex().c_str(), host_label.c_str(), first.id.to_hex().c_str());
  auto kr = f.value().kill_node_by_label(host_label);
  if (kr.failed()) {
    std::fprintf(stderr, "kill failed: %s\n", kr.error().to_string().c_str());
    f.value().shutdown_all();
    return 1;
  }

  std::vector<ComputeTaskId> ids{t.id};
  bool ok = wait_for_success(*c, ids, 45000);
  auto insp = c->inspect(t.id);
  std::printf("example_retry_on_node_loss: attempts=%zu state=%s\n",
              insp.ok() ? insp.value().attempts.size() : 0,
              insp.ok() ? task_state_name(insp.value().state) : "unknown");
  for (const auto& a : insp.value().attempts) {
    std::printf("  attempt %s node=%s state=%s\n", a.id.to_hex().c_str(),
                a.node_id.to_hex().c_str(), attempt_state_name(a.state));
  }
  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("example_retry_on_node_loss: processes_exited=%d\n", confirmed);
  if (!ok) {
    std::fprintf(stderr, "example_retry_on_node_loss FAILED\n");
    return 1;
  }
  std::printf("example_retry_on_node_loss PASSED\n");
  return 0;
}
