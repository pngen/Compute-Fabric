#pragma once

#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "compute_fabric/launcher/launcher.h"
#include "compute_fabric/net/client.h"

namespace cf::example {

inline std::string cli_path() {
#if defined(CF_CLI_PATH)
  return std::string(CF_CLI_PATH);
#else
  return current_executable_path();
#endif
}

inline std::string store_dir(const char* tag) {
  return std::string("example_store_") + tag + "_" +
         std::to_string(cf::Id128::random().lo());
}

inline ComputeTask make_task(const char* name, uint64_t n = 100000,
                             cf::KernelType k = cf::KernelType::HashInteger) {
  ComputeTask t;
  t.id = Id128::random();
  t.workload = Id128::random();
  t.name = name;
  t.kernel = k;
  t.kernel_param_n = n;
  t.kernel_param_a = 31;
  t.kernel_param_b = 17;
  t.seed = 42;
  t.estimated_duration_seconds = 0.01;
  t.retry.max_attempts = 3;
  t.retry.retry_on_node_loss = true;
  t.retry.retry_backoff_ms = 50;
  return t;
}

// Waits until every task id is terminal; returns true if all succeeded.
inline bool wait_for_success(FabricClient& c,
                             const std::vector<ComputeTaskId>& ids,
                             int64_t timeout_ms) {
  auto deadline = now_millis() + timeout_ms;
  while (now_millis() < deadline) {
    bool all_done = true;
    bool ok = true;
    for (const auto& id : ids) {
      auto insp = c.inspect(id);
      if (!insp.ok() || !insp.value().found) {
        all_done = false;
        break;
      }
      if (insp.value().state == TaskState::Succeeded) continue;
      if (is_terminal(insp.value().state)) {
        ok = false;
        continue;
      }
      all_done = false;
    }
    if (all_done) return ok;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

}  // namespace cf::example