#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/executor/cpu_executor.h"
#include "compute_fabric/executor/cuda_executor.h"
#include "compute_fabric/executor/executor.h"
#include "compute_fabric/net/channel.h"
#include "compute_fabric/net/socket.h"
#include "compute_fabric/protocol/messages.h"

namespace cf {

struct NodeConfig {
  std::string coordinator_host = "127.0.0.1";
  uint16_t coordinator_node_port = 0;
  std::string label;
  std::string address;
  int32_t cpu_slots = 2;
  uint64_t host_memory_scheduling_bytes = 8ull << 30;
  bool enable_cuda = false;
  int32_t cuda_device_ordinal = 0;
  int32_t cuda_slots = 2;
  int64_t heartbeat_interval_ms = 250;
  int64_t io_timeout_ms = 5000;
  std::map<std::string, std::string> tags;
};

// A Compute Fabric node runtime: registers with the coordinator, runs a CPU
// executor (and optionally a CUDA executor), executes dispatched attempts,
// reports started/completed/failed/cancelled lifecycle events, sends
// heartbeats, and shuts down cleanly.
class NodeRuntime {
 public:
  explicit NodeRuntime(NodeConfig config);
  ~NodeRuntime();

  NodeRuntime(const NodeRuntime&) = delete;
  NodeRuntime& operator=(const NodeRuntime&) = delete;

  // Connects and registers; blocks until registration is acknowledged.
  Result<void> start(int64_t register_timeout_ms = 10000);
  Result<void> try_register(const proto::NodeRegister& reg,
                            int64_t each_attempt_ms);

  // Blocks until stop() is called.
  void run();

  // Requests shutdown; joins all threads. Safe to call repeatedly.
  void stop();

  const NodeId& node_id() const { return node_id_; }
  NodeSessionId session() const { return session_; }
  SessionEpoch session_epoch() const { return session_epoch_; }
  CoordinatorEpoch coordinator_epoch() const { return epoch_; }
  bool connected() const { return connected_.load(); }

  ExecutorRuntimeStats cpu_stats() const;
  ExecutorRuntimeStats cuda_stats() const;

 private:
  void request_runtime_stop();
  void terminate_transport();
  void forget_attempt(const ExecutionAttemptId& id);
  void read_loop(net::Channel& channel);
  void heartbeat_loop();
  void completion_loop();
  void handle_dispatch(const proto::DispatchAttempt& msg);
  void handle_cancel(const proto::AttemptCancel& msg);
  void send_started(const ExecutionAttemptId& id, const std::string& device);
  void send_complete(const ExecutionAttemptId& id, const ComputeTaskId& task,
                     const ReservationId& resv, const AttemptOutcome& out);
  void send_failed(const ExecutionAttemptId& id, const ComputeTaskId& task,
                   const ReservationId& resv, FailureKind kind,
                   const std::string& reason, double duration_ms);
  void send_cancelled(const ExecutionAttemptId& id, const ComputeTaskId& task,
                      const ReservationId& resv, const std::string& reason);
  void send_release(const ReservationId& resv, const ComputeTaskId& task);
  Result<void> send_frame(const proto::Frame& frame, int timeout_ms = -1);

  NodeConfig config_;
  NodeId node_id_;
  NodeSessionId session_;
  CoordinatorEpoch epoch_ = 0;
  SessionEpoch session_epoch_ = 0;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> connected_{false};
  std::mutex stop_mu_;
  bool stop_done_ = false;
  std::mutex lifecycle_mu_;
  std::condition_variable lifecycle_cv_;

  std::unique_ptr<CpuExecutorBackend> cpu_;
  std::unique_ptr<CudaExecutorBackend> cuda_;

  std::mutex send_mu_;
  std::shared_ptr<net::Channel> channel_;

  std::thread heartbeat_thread_;
  std::thread read_thread_;
  std::thread completion_thread_;

  std::mutex comp_mu_;
  std::condition_variable comp_cv_;
  struct CompletionItem {
    ExecutionAttemptId attempt_id;
    ComputeTaskId task_id;
    ReservationId reservation_id;
    AttemptOutcome outcome;
  };
  std::deque<CompletionItem> comp_queue_;

  std::mutex active_mu_;
  std::map<ExecutionAttemptId, std::shared_ptr<CancellationToken>> active_tokens_;
};

}  // namespace cf
