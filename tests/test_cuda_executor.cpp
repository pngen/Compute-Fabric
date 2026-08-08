#include "framework.h"

#include "compute_fabric/executor/cuda_executor.h"
#include "compute_fabric/executor/workloads.h"

using namespace cf;

namespace {
ComputeTask vt_task(uint64_t n, uint64_t seed = 42) {
  ComputeTask t;
  t.id = Id128::random();
  t.workload = Id128::random();
  t.name = "cuda-vt";
  t.required_executor = ExecutorType::Cuda;
  t.kernel = KernelType::VectorTransform;
  t.kernel_param_n = n;
  t.kernel_param_a = 31;
  t.kernel_param_b = 17;
  t.seed = seed;
  return t;
}
}  // namespace

TEST(cuda_device_discovery) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) {
    EXPECT_TRUE(true);  // no CUDA; skip
    return;
  }
  EXPECT_FALSE(devs[0].device_name.empty());
  EXPECT_GT(devs[0].major, 0);
  EXPECT_GT(devs[0].device_memory_bytes, 0ull);
}

TEST(cuda_actual_kernel_execution_and_verification) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) return;
  auto task = vt_task(200000);
  auto res = cuda_run_vector_transform(devs[0].device_ids[0], task, nullptr);
  EXPECT_TRUE(res.success);
  if (!res.success) {
    EXPECT_STR_CONTAINS(res.error_message, "");  // surface the error text
    return;
  }
  // Verify the digest matches the deterministic CPU VectorTransform result.
  std::string device;
  WorkloadResult wr = run_cpu_kernel(task, nullptr, device);
  EXPECT_TRUE(wr.success);
  EXPECT_EQ(res.integrity_hex, wr.checksum_hex);
  EXPECT_EQ(res.elements, 200000ull);
}

TEST(cuda_deterministic_output) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) return;
  auto task = vt_task(100000);
  auto res1 = cuda_run_vector_transform(devs[0].device_ids[0], task, nullptr);
  auto res2 = cuda_run_vector_transform(devs[0].device_ids[0], task, nullptr);
  EXPECT_TRUE(res1.success);
  EXPECT_TRUE(res2.success);
  EXPECT_EQ(res1.output, res2.output);
  EXPECT_EQ(res1.integrity_hex, res2.integrity_hex);
}

TEST(cuda_host_device_transfer) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) return;
  auto task = vt_task(50000);
  auto res = cuda_run_vector_transform(devs[0].device_ids[0], task, nullptr);
  EXPECT_TRUE(res.success);
  EXPECT_GT(res.bytes_transferred, 0ull);
}

TEST(cuda_backend_executes_attempt) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) return;
  CudaExecutorBackend cuda(devs[0].device_ids[0], 2);
  auto task = vt_task(100000);
  AttemptOutcome out;
  std::atomic<bool> done{false};
  auto work = std::make_shared<AttemptWork>();
  work->task = task;
  work->attempt_id = Id128::random();
  work->token = std::make_shared<CancellationToken>();
  work->on_complete = [&](AttemptOutcome o) {
    out = std::move(o);
    done = true;
  };
  EXPECT_OK(cuda.enqueue(work));
  auto deadline = now_millis() + 15000;
  while (!done.load() && now_millis() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_TRUE(done.load());
  EXPECT_TRUE(out.success);
  EXPECT_EQ(out.state, AttemptState::Succeeded);
  EXPECT_FALSE(out.output_integrity.empty());
  cuda.shutdown(false);
}

TEST(cuda_slot_accounting) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) return;
  CudaExecutorBackend cuda(devs[0].device_ids[0], 2);
  auto s = cuda.stats();
  EXPECT_EQ(s.max_slots, 2u);
  cuda.shutdown(false);
}

TEST(cuda_repeated_lifecycle_no_leaks) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) return;
  for (int i = 0; i < 3; ++i) {
    CudaExecutorBackend cuda(devs[0].device_ids[0], 2);
    auto task = vt_task(50000);
    AttemptOutcome out;
    std::atomic<bool> done{false};
    auto work = std::make_shared<AttemptWork>();
    work->task = task;
    work->attempt_id = Id128::random();
    work->token = std::make_shared<CancellationToken>();
    work->on_complete = [&](AttemptOutcome o) {
      out = std::move(o);
      done = true;
    };
    EXPECT_OK(cuda.enqueue(work));
    auto deadline = now_millis() + 15000;
    while (!done.load() && now_millis() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_TRUE(done.load());
    EXPECT_TRUE(out.success);
    cuda.shutdown(false);
  }
  EXPECT_EQ(cuda_outstanding_allocations(), 0ll);
}

TEST(cuda_capability_metadata) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) return;
  CudaExecutorBackend cuda(devs[0].device_ids[0], 2);
  auto cap = cuda.capability();
  EXPECT_EQ(cap.type, ExecutorType::Cuda);
  EXPECT_GT(cap.major, 0);
  EXPECT_FALSE(cap.device_name.empty());
  cuda.shutdown(false);
}

TEST(cuda_erroneous_task_rejected) {
  auto devs = enumerate_cuda_devices();
  if (devs.empty()) return;
  CudaExecutorBackend cuda(devs[0].device_ids[0], 2);
  // A CPU-only task must not be executable on CUDA.
  ComputeTask t;
  t.required_executor = ExecutorType::Cpu;
  t.kernel = KernelType::HashInteger;
  auto r = cuda.can_execute(t);
  EXPECT_TRUE(r.ok());
  EXPECT_FALSE(r.value());
  cuda.shutdown(false);
}