#include <cstdio>
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
}  // namespace

int main() {
  std::string store_dir = "example_store_minimal_" +
                          std::to_string(cf::Id128::random().lo());
  auto f = LocalFabric::launch(cli_path(), 1, false, store_dir, "cost_aware", "");
  if (f.failed()) {
    std::fprintf(stderr, "launch failed: %s\n", f.error().to_string().c_str());
    return 1;
  }
  auto wr = f.value().wait_registered(20000);
  if (wr.failed()) {
    std::fprintf(stderr, "registration wait failed: %s\n",
                 wr.error().to_string().c_str());
    f.value().shutdown_all();
    return 1;
  }
  auto c = f.value().make_client();
  if (!c) {
    std::fprintf(stderr, "cannot connect to coordinator\n");
    f.value().shutdown_all();
    return 1;
  }

  ComputeTask task;
  task.id = Id128::random();
  task.workload = Id128::random();
  task.name = "example-minimal";
  task.kernel = KernelType::HashInteger;
  task.kernel_param_n = 100000;
  task.kernel_param_a = 31;
  task.kernel_param_b = 17;
  task.seed = 42;
  task.estimated_duration_seconds = 0.01;
  task.retry.max_attempts = 2;

  auto r = c->submit(task.workload, "example", task);
  if (r.failed() || !r.value().accepted) {
    std::fprintf(stderr, "submit failed\n");
    f.value().shutdown_all();
    return 1;
  }

  auto deadline = now_millis() + 30000;
  bool ok = false;
  while (now_millis() < deadline) {
    auto insp = c->inspect(task.id);
    if (insp.ok() && insp.value().state == TaskState::Succeeded) {
      ok = true;
      break;
    }
    if (insp.ok() && is_terminal(insp.value().state) &&
        insp.value().state != TaskState::Succeeded) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  auto insp = c->inspect(task.id);
  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("example_minimal_task: state=%s integrity=%s processes_exited=%d\n",
              insp.ok() ? task_state_name(insp.value().state) : "unknown",
              insp.ok() && !insp.value().attempts.empty()
                  ? insp.value().attempts.back().output_integrity.c_str()
                  : "-",
              confirmed);
  if (!ok) {
    std::fprintf(stderr, "example_minimal_task FAILED\n");
    return 1;
  }
  std::printf("example_minimal_task PASSED\n");
  return 0;
}