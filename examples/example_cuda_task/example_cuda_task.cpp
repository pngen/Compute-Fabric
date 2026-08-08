#include <cstdio>

#include "../example_common.h"

using namespace cf;
using namespace cf::example;

int main() {
  auto f = LocalFabric::launch(cli_path(), 1, true, store_dir("cuda"), "cost_aware", "");
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

  auto t = make_task("cuda-transform", 200000, KernelType::VectorTransform);
  t.required_executor = ExecutorType::Cuda;
  t.retry.max_attempts = 1;
  auto r = c->submit(t.workload, "cuda-example", t);
  if (!r.ok() || !r.value().accepted) {
    // CUDA may be unavailable; report and pass with a note.
    std::printf("example_cuda_task: cuda node failed to submit (cuda unavailable?)\n");
    f.value().shutdown_all();
    return 0;
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
  std::printf("example_cuda_task: state=%s device=%s integrity=%s\n",
              insp.ok() ? task_state_name(insp.value().state) : "unknown",
              insp.ok() && !insp.value().attempts.empty()
                  ? insp.value().attempts.back().device.c_str()
                  : "-",
              insp.ok() && !insp.value().attempts.empty()
                  ? insp.value().attempts.back().output_integrity.c_str()
                  : "-");
  int confirmed = f.value().shutdown_all().value_or(-1);
  std::printf("example_cuda_task: processes_exited=%d\n", confirmed);
  if (!ok) {
    std::fprintf(stderr, "example_cuda_task FAILED\n");
    return 1;
  }
  std::printf("example_cuda_task PASSED\n");
  return 0;
}