// CUDA kernels for Compute Fabric. Deterministic integer arithmetic only so
// results are byte-exact across CPU and GPU. Compiled by nvcc.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "compute_fabric/executor/cuda_executor.h"
#include "compute_fabric/executor/workloads.h"
#include "compute_fabric/core/digest.h"
#include "compute_fabric/core/time_util.h"

#if defined(__CUDACC__)
#include <cuda_runtime.h>
#endif

namespace cf {

namespace {
std::atomic<int64_t> g_cuda_allocations{0};
}  // namespace

int64_t cuda_outstanding_allocations() {
  return g_cuda_allocations.load();
}

constexpr uint32_t kMod = 0xFFFFFFFBu;  // prime near 2^32

// Mirrors the CPU wmix() exactly.
__device__ __host__ inline uint32_t wmix_u32(uint32_t x) {
  x ^= x >> 16;
  x *= 0x7FEB352Du;
  x ^= x >> 15;
  x *= 0x846CA68Bu;
  x ^= x >> 16;
  return x;
}

__global__ void vec_transform_kernel(const uint32_t* x, uint32_t* y,
                                     uint64_t n, uint32_t a, uint32_t b,
                                     uint32_t seed) {
  uint64_t i = static_cast<uint64_t>(blockIdx.x) * blockDim.x + threadIdx.x;
  if (i >= n) return;
  uint32_t xi = wmix_u32(static_cast<uint32_t>(i) + seed);
  uint64_t prod = static_cast<uint64_t>(xi) * a + b;
  y[i] = static_cast<uint32_t>(prod % kMod);
}

__global__ void reduce_checksum_kernel(const uint32_t* y, uint64_t n,
                                       uint64_t* out) {
  extern __shared__ uint64_t sh[];
  uint64_t i = static_cast<uint64_t>(blockIdx.x) * blockDim.x + threadIdx.x;
  uint32_t tid = threadIdx.x;
  uint64_t acc = 0;
  if (i < n) {
    acc = y[i];
  }
  sh[tid] = acc;
  __syncthreads();
  for (uint32_t s = blockDim.x / 2; s > 0; s >>= 1) {
    if (tid < s) sh[tid] += sh[tid + s];
    __syncthreads();
  }
  if (tid == 0) {
    out[blockIdx.x] = sh[0];
  }
}

void* cuda_alloc_checked(size_t bytes, const char* what, std::string& err) {
#if defined(__CUDACC__)
  void* p = nullptr;
  cudaError_t e = cudaMalloc(&p, bytes);
  if (e != cudaSuccess) {
    err = std::string(what) + ": " + cudaGetErrorString(e);
    return nullptr;
  }
  g_cuda_allocations.fetch_add(1);
  return p;
#else
  (void)bytes;
  (void)what;
  err = "cuda not compiled";
  return nullptr;
#endif
}

void cuda_free_checked(void* p) {
#if defined(__CUDACC__)
  if (p) {
    cudaError_t e = cudaFree(p);
    if (e == cudaSuccess) g_cuda_allocations.fetch_add(-1);
  }
#else
  (void)p;
#endif
}

std::vector<ExecutorCapability> enumerate_cuda_devices() {
  std::vector<ExecutorCapability> out;
#if defined(__CUDACC__)
  int count = 0;
  cudaError_t e = cudaGetDeviceCount(&count);
  if (e != cudaSuccess || count <= 0) return out;
  for (int dev = 0; dev < count; ++dev) {
    cudaDeviceProp props{};
    if (cudaGetDeviceProperties(&props, dev) != cudaSuccess) continue;
    ExecutorCapability c;
    c.type = ExecutorType::Cuda;
    c.name = "cuda";
    c.slots = 2;
    c.major = props.major;
    c.minor = props.minor;
    c.vendor = "NVIDIA";
    c.architecture = props.name;
    c.device_name = props.name;
    c.device_memory_bytes = props.totalGlobalMem;
    c.device_ids = {dev};
    c.relative_speed = 10.0;  // relative to baseline CPU; measured in benchmarks
    c.healthy = true;
    out.push_back(c);
  }
#else
  (void)0;
#endif
  return out;
}

CudaDeviceGuard::CudaDeviceGuard(int device_ordinal) {
#if defined(__CUDACC__)
  cudaError_t e = cudaGetDevice(&previous_);
  if (e == cudaSuccess) {
    e = cudaSetDevice(device_ordinal);
  }
  if (e != cudaSuccess) {
    error_ = std::string("cudaSetDevice: ") + cudaGetErrorString(e);
    return;
  }
  ok_ = true;
#else
  (void)device_ordinal;
  error_ = "cuda not compiled";
#endif
}

CudaDeviceGuard::~CudaDeviceGuard() {
#if defined(__CUDACC__)
  if (ok_) cudaSetDevice(previous_);
#else
  (void)0;
#endif
}

CudaRunResult cuda_run_vector_transform(
    int device_ordinal, const ComputeTask& task,
    const std::shared_ptr<CancellationToken>& token) {
  CudaRunResult r;
#if defined(__CUDACC__)
  CudaDeviceGuard guard(device_ordinal);
  if (!guard.ok()) {
    r.success = false;
    r.error_message = guard.error();
    return r;
  }

  uint64_t n = task.kernel_param_n > 0 ? task.kernel_param_n : 1;
  if (n > 4u * 1000u * 1000u) n = 4u * 1000u * 1000u;  // bounded allocations
  const uint32_t a = static_cast<uint32_t>(task.kernel_param_a & 0xFFFFFFFFu);
  const uint32_t b = static_cast<uint32_t>(task.kernel_param_b & 0xFFFFFFFFu);
  const uint32_t seed = static_cast<uint32_t>(task.seed & 0xFFFFFFFFu);

  const size_t bytes = static_cast<size_t>(n) * sizeof(uint32_t);
  std::string err;

  uint32_t* d_x = static_cast<uint32_t*>(cuda_alloc_checked(bytes, "cudaMalloc x", err));
  if (!d_x) {
    r.success = false;
    r.error_message = err;
    return r;
  }
  uint32_t* d_y = static_cast<uint32_t*>(cuda_alloc_checked(bytes, "cudaMalloc y", err));
  if (!d_y) {
    cuda_free_checked(d_x);
    r.success = false;
    r.error_message = err;
    return r;
  }
  uint32_t* d_partial = nullptr;
  uint32_t blocks = 0;
  const uint32_t threads = 256;
  if (n > 0) {
    blocks = static_cast<uint32_t>((n + threads - 1) / threads);
  }
  if (blocks > 4096) blocks = 4096;
  size_t part_bytes = static_cast<size_t>(blocks) * sizeof(uint64_t);
  d_partial = static_cast<uint32_t*>(
      cuda_alloc_checked(part_bytes, "cudaMalloc partial", err));
  if (!d_partial) {
    cuda_free_checked(d_x);
    cuda_free_checked(d_y);
    r.success = false;
    r.error_message = err;
    return r;
  }

  // Host input, deterministic.
  std::vector<uint32_t> h_x(static_cast<size_t>(n));
  for (uint64_t i = 0; i < n; ++i) {
    h_x[i] = wmix_u32(static_cast<uint32_t>(i) + seed);
  }

  cudaStream_t stream = nullptr;
  if (cudaStreamCreate(&stream) != cudaSuccess) {
    r.success = false;
    r.error_message = "cudaStreamCreate failed";
    cuda_free_checked(d_x);
    cuda_free_checked(d_y);
    cuda_free_checked(d_partial);
    return r;
  }
  r.bytes_transferred = bytes * 2;  // host->device + device->host

  int64_t t0 = now_micros();
  cudaError_t e = cudaMemcpyAsync(d_x, h_x.data(), bytes, cudaMemcpyHostToDevice, stream);
  if (e == cudaSuccess) {
    vec_transform_kernel<<<blocks, threads, 0, stream>>>(d_x, d_y, n, a, b, seed);
    e = cudaGetLastError();
  }
  if (e == cudaSuccess) {
    // Read back the full result array (real device->host transfer).
    std::vector<uint32_t> h_y(static_cast<size_t>(n));
    e = cudaMemcpyAsync(h_y.data(), d_y, bytes, cudaMemcpyDeviceToHost, stream);
    if (e == cudaSuccess) {
      // Device-side reduction for verification.
      reduce_checksum_kernel<<<blocks, threads, threads * sizeof(uint64_t), stream>>>(
          d_y, n, reinterpret_cast<uint64_t*>(d_partial));
      e = cudaGetLastError();
      if (e == cudaSuccess) {
        e = cudaStreamSynchronize(stream);
      }
      if (e == cudaSuccess) {
        std::vector<uint64_t> h_part(blocks);
        e = cudaMemcpy(h_part.data(), d_partial, part_bytes, cudaMemcpyDeviceToHost);
        if (e == cudaSuccess) {
          uint64_t dev_sum = 0;
          for (uint32_t i = 0; i < blocks; ++i) dev_sum += h_part[i];
          // Host-side digest from the actual device output, and verify the
          // device reduction matches a host summation of device output.
          uint64_t acc_lo = 0;
          uint64_t acc_hi = 0;
          for (uint64_t i = 0; i < n; ++i) {
            acc_lo += h_y[i];
            acc_hi ^= static_cast<uint64_t>(h_y[i]) << 1;
          }
          if (dev_sum != acc_lo) {
            r.success = false;
            r.error_message = "device reduction mismatch";
          } else {
          uint64_t buf[2] = {acc_lo, acc_hi};
          uint64_t h0 = bytes_hash(buf, sizeof(buf), seed);
          uint64_t h1 = mix64(h0 ^ n);
          r.output.resize(16);
          for (int i = 0; i < 8; ++i) {
            r.output[i] = static_cast<uint8_t>((h0 >> (56 - i * 8)) & 0xFF);
            r.output[8 + i] = static_cast<uint8_t>((h1 >> (56 - i * 8)) & 0xFF);
          }
          r.integrity_hex = hex_digest(r.output);
          r.success = true;
          r.elements = n;
          r.device_name = std::string("cuda:") + std::to_string(device_ordinal);
          }
        }
      }
    }
  }
  if (e != cudaSuccess) {
    r.success = false;
    r.error_message = std::string("cuda: ") + cudaGetErrorString(e);
  }
  r.kernel_millis = (now_micros() - t0) / 1000.0;

  cudaStreamDestroy(stream);
  cuda_free_checked(d_x);
  cuda_free_checked(d_y);
  cuda_free_checked(d_partial);
#else
  (void)device_ordinal;
  (void)task;
  (void)token;
  r.success = false;
  r.error_message = "cuda not compiled";
#endif
  return r;
}

CudaExecutorBackend::CudaExecutorBackend(int device_ordinal, int32_t slots,
                                         int32_t queue_limit)
    : device_ordinal_(device_ordinal),
      slots_(slots > 0 ? slots : 1),
      queue_limit_(queue_limit > 0 ? queue_limit : 64) {
#if defined(__CUDACC__)
  CudaDeviceGuard guard(device_ordinal_);
  if (!guard.ok()) return;
  cudaDeviceProp props{};
  if (cudaGetDeviceProperties(&props, device_ordinal_) != cudaSuccess) return;
  device_name_ = props.name;
  major_ = props.major;
  minor_ = props.minor;
  device_memory_bytes_ = props.totalGlobalMem;
  arch_ = props.name;
  available_ = true;
  for (int32_t i = 0; i < slots_; ++i) {
    workers_.emplace_back([this] { worker_loop(); });
  }
#else
  (void)0;
#endif
}

CudaExecutorBackend::~CudaExecutorBackend() { shutdown(false); }

ExecutorCapability CudaExecutorBackend::capability() const {
  ExecutorCapability c;
  c.type = ExecutorType::Cuda;
  c.name = "cuda";
  c.slots = slots_;
  c.major = major_;
  c.minor = minor_;
  c.vendor = "NVIDIA";
  c.architecture = arch_;
  c.device_name = device_name_;
  c.device_memory_bytes = device_memory_bytes_;
  c.device_ids = {device_ordinal_};
  c.relative_speed = 10.0;
  c.healthy = available_;
  return c;
}

Result<bool> CudaExecutorBackend::can_execute(const ComputeTask& task) const {
  if (!available_) return false;
  if (task.required_executor != ExecutorType::Cuda) return false;
  if (task.kernel != KernelType::VectorTransform) return false;
  if (task.min_compute_capability_major > 0) {
    if (major_ < task.min_compute_capability_major) return false;
    if (major_ == task.min_compute_capability_major && minor_ < task.min_compute_capability_minor)
      return false;
  }
  return true;
}

Result<void> CudaExecutorBackend::enqueue(std::shared_ptr<AttemptWork> work) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stopping_) return Error(ErrorCode::ShuttingDown, "cuda executor shutting down");
    if (queue_.size() >= static_cast<size_t>(queue_limit_)) {
      return Error(ErrorCode::NoCapacity, "cuda executor queue full");
    }
    queue_.push_back(std::move(work));
  }
  cv_.notify_one();
  return Error::success();
}

Result<void> CudaExecutorBackend::cancel_attempt(const ExecutionAttemptId& id) {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& w : queue_) {
    if (w->attempt_id == id) {
      if (w->token) w->token->request();
      return Error::success();
    }
  }
  auto it = active_tokens_.find(id);
  if (it != active_tokens_.end()) {
    it->second->request();
    return Error::success();
  }
  return Error(ErrorCode::TaskNotFound, "attempt not found in cuda executor");
}

bool CudaExecutorBackend::has_active(const ExecutionAttemptId& id) const {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& w : queue_) {
    if (w->attempt_id == id) return true;
  }
  return active_tokens_.count(id) > 0;
}

ExecutorRuntimeStats CudaExecutorBackend::stats() const {
  ExecutorRuntimeStats s;
  s.total_attempts = static_cast<uint64_t>(total_.load());
  s.succeeded = static_cast<uint64_t>(succeeded_.load());
  s.failed = static_cast<uint64_t>(failed_.load());
  s.active = static_cast<uint32_t>(active_.load());
  s.max_slots = static_cast<uint32_t>(slots_);
  s.allocations_outstanding = static_cast<uint64_t>(cuda_outstanding_allocations());
  std::lock_guard<std::mutex> lk(mu_);
  s.queued = static_cast<uint32_t>(queue_.size());
  return s;
}

void CudaExecutorBackend::worker_loop() {
  for (;;) {
    std::shared_ptr<AttemptWork> work;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait_for(lk, std::chrono::milliseconds(100), [this] {
        return stopping_ || !queue_.empty();
      });
      if (queue_.empty()) {
        if (stopping_) return;
        continue;
      }
      work = queue_.front();
      queue_.pop_front();
      active_tokens_[work->attempt_id] = work->token;
    }
    active_.fetch_add(1);
    run_one(work);
    active_.fetch_add(-1);
    {
      std::lock_guard<std::mutex> lk(mu_);
      active_tokens_.erase(work->attempt_id);
    }
  }
}

void CudaExecutorBackend::run_one(std::shared_ptr<AttemptWork> work) {
  total_.fetch_add(1);
  int64_t t0 = now_micros();
  AttemptOutcome out;
  out.device = "cuda:" + std::to_string(device_ordinal_);
  if (work->on_start) work->on_start();
  if (work->token && work->token->is_cancelled()) {
    out.success = false;
    out.state = AttemptState::Cancelled;
    out.failure_kind = FailureKind::Cancelled;
    out.failure_reason = "cancelled before execution";
  } else {
    CudaRunResult r = cuda_run_vector_transform(device_ordinal_, work->task, work->token);
    out.duration_ms = (now_micros() - t0) / 1000.0;
    if (r.success) {
      out.success = true;
      out.state = AttemptState::Succeeded;
      out.output_bytes = r.output;
      out.output_integrity = r.integrity_hex;
      out.bytes_transferred = r.bytes_transferred;
      succeeded_.fetch_add(1);
    } else {
      out.success = false;
      out.state = AttemptState::Failed;
      out.failure_kind = FailureKind::CudaError;
      out.failure_reason = r.error_message;
      failed_.fetch_add(1);
    }
  }
  if (work->on_complete) work->on_complete(std::move(out));
}

void CudaExecutorBackend::shutdown(bool drain) {
  bool expected = false;
  if (!stopping_.compare_exchange_strong(expected, true)) return;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (!drain) {
      for (auto& w : queue_) {
        if (w->token) w->token->request();
      }
    }
  }
  cv_.notify_all();
  for (auto& t : workers_) {
    if (t.joinable()) t.join();
  }
  workers_.clear();
#if defined(__CUDACC__)
  if (available_) {
    CudaDeviceGuard guard(device_ordinal_);
    (void)cudaDeviceSynchronize();
    (void)cudaDeviceReset();
  }
#endif
}

}  // namespace cf