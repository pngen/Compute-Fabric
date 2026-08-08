#include "framework.h"

#include "compute_fabric/executor/cpu_executor.h"
#include "compute_fabric/executor/cuda_executor.h"

using namespace cf;

namespace {
ComputeTask kernel_task(const char* name, KernelType k, uint64_t n) {
  ComputeTask t;
  t.id = Id128::random();
  t.workload = Id128::random();
  t.name = name;
  t.kernel = k;
  t.kernel_param_n = n;
  t.kernel_param_a = 31;
  t.kernel_param_b = 17;
  t.seed = 42;
  return t;
}

AttemptOutcome run_sync(CpuExecutorBackend& cpu, const ComputeTask& task,
                        int64_t timeout_ms = 10000) {
  AttemptOutcome result;
  std::atomic<bool> done{false};
  auto work = std::make_shared<AttemptWork>();
  work->task = task;
  work->attempt_id = Id128::random();
  work->token = std::make_shared<CancellationToken>();
  work->on_complete = [&](AttemptOutcome out) {
    result = std::move(out);
    done.store(true);
  };
  cpu.enqueue(work);
  auto deadline = now_millis() + timeout_ms;
  while (!done.load() && now_millis() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return result;
}
}  // namespace

TEST(cpu_executor_actual_execution) {
  CpuExecutorBackend cpu(2);
  auto task = kernel_task("hash", KernelType::HashInteger, 100000);
  auto out = run_sync(cpu, task);
  EXPECT_TRUE(out.success);
  EXPECT_EQ(out.state, AttemptState::Succeeded);
  EXPECT_FALSE(out.output_integrity.empty());
  EXPECT_EQ(out.output_bytes.size(), 16ull);
  EXPECT_GT(out.duration_ms, 0.0);
  cpu.shutdown(false);
}

TEST(cpu_executor_deterministic_output) {
  CpuExecutorBackend cpu(2);
  auto task = kernel_task("hash", KernelType::HashInteger, 50000);
  auto out1 = run_sync(cpu, task);
  auto out2 = run_sync(cpu, task);
  EXPECT_TRUE(out1.success);
  EXPECT_TRUE(out2.success);
  EXPECT_EQ(out1.output_bytes, out2.output_bytes);
  EXPECT_EQ(out1.output_integrity, out2.output_integrity);
  cpu.shutdown(false);
}

TEST(cpu_executor_deterministic_across_kernel_types) {
  CpuExecutorBackend cpu(2);
  for (auto k : {KernelType::HashInteger, KernelType::VectorTransform,
                 KernelType::Checksum, KernelType::SyntheticLoop,
                 KernelType::StateDependent, KernelType::TensorArithmetic}) {
    auto task = kernel_task("k", k, 1000);
    auto out1 = run_sync(cpu, task);
    auto out2 = run_sync(cpu, task);
    EXPECT_TRUE(out1.success);
    EXPECT_EQ(out1.output_bytes, out2.output_bytes);
  }
  cpu.shutdown(false);
}

TEST(cpu_executor_cpu_and_cuda_digest_match) {
  // The CUDA VectorTransform must produce the same digest as the CPU
  // VectorTransform for identical parameters.
  CpuExecutorBackend cpu(2);
  auto task = kernel_task("vt", KernelType::VectorTransform, 50000);
  auto out = run_sync(cpu, task);
  EXPECT_TRUE(out.success);
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) {
    EXPECT_TRUE(true);  // no CUDA available; skip
  } else {
    auto res = cuda_run_vector_transform(devs[0].device_ids[0], task, nullptr);
    EXPECT_TRUE(res.success);
    EXPECT_EQ(res.integrity_hex, out.output_integrity);
  }
  cpu.shutdown(false);
}

TEST(cpu_executor_failure_reporting) {
  CpuExecutorBackend cpu(2);
  auto task = kernel_task("unsupported", KernelType::None, 100);
  auto out = run_sync(cpu, task);
  EXPECT_FALSE(out.success);
  EXPECT_EQ(out.failure_kind, FailureKind::ExecutorFailed);
  cpu.shutdown(false);
}

TEST(cpu_executor_cancellation) {
  CpuExecutorBackend cpu(2);
  auto task = kernel_task("big", KernelType::SyntheticLoop, 20u * 1000u * 1000u);
  auto work = std::make_shared<AttemptWork>();
  work->task = task;
  work->attempt_id = Id128::random();
  work->token = std::make_shared<CancellationToken>();
  std::atomic<bool> done{false};
  work->on_complete = [&](AttemptOutcome out) {
    EXPECT_TRUE(!out.success);  // cancelled
    EXPECT_EQ(out.failure_kind, FailureKind::Cancelled);
    done.store(true);
  };
  EXPECT_OK(cpu.enqueue(work));
  // Cancel immediately (the work is still queued or just-dequeued).
  EXPECT_OK(cpu.cancel_attempt(work->attempt_id));
  auto deadline = now_millis() + 10000;
  while (!done.load() && now_millis() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_TRUE(done.load());
  cpu.shutdown(false);
}

TEST(cpu_executor_cancel_before_enqueue_via_token) {
  CpuExecutorBackend cpu(2);
  auto task = kernel_task("big", KernelType::SyntheticLoop, 20u * 1000u * 1000u);
  auto work = std::make_shared<AttemptWork>();
  work->task = task;
  work->attempt_id = Id128::random();
  work->token = std::make_shared<CancellationToken>();
  work->token->request();
  std::atomic<bool> done{false};
  work->on_complete = [&](AttemptOutcome out) {
    EXPECT_TRUE(!out.success);
    EXPECT_EQ(out.state, AttemptState::Cancelled);
    done.store(true);
  };
  EXPECT_OK(cpu.enqueue(work));
  auto deadline = now_millis() + 10000;
  while (!done.load() && now_millis() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_TRUE(done.load());
  cpu.shutdown(false);
}

TEST(cpu_executor_slot_accounting) {
  CpuExecutorBackend cpu(2);
  auto stats = cpu.stats();
  EXPECT_EQ(stats.max_slots, 2u);
  EXPECT_EQ(stats.active, 0u);
  cpu.shutdown(false);
}

TEST(cpu_executor_can_execute_gating) {
  CpuExecutorBackend cpu(2);
  auto cuda_task = kernel_task("cuda", KernelType::VectorTransform, 100);
  cuda_task.required_executor = ExecutorType::Cuda;
  auto r = cpu.can_execute(cuda_task);
  EXPECT_TRUE(r.ok());
  EXPECT_FALSE(r.value());
  auto cpu_task = kernel_task("cpu", KernelType::HashInteger, 100);
  auto r2 = cpu.can_execute(cpu_task);
  EXPECT_TRUE(r2.ok());
  EXPECT_TRUE(r2.value());
  cpu.shutdown(false);
}

TEST(cpu_executor_repeated_lifecycle) {
  for (int i = 0; i < 3; ++i) {
    CpuExecutorBackend cpu(2);
    auto task = kernel_task("t", KernelType::HashInteger, 10000);
    auto out = run_sync(cpu, task);
    EXPECT_TRUE(out.success);
    cpu.shutdown(false);
  }
}