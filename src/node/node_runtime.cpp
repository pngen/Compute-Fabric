#include "compute_fabric/node/node_runtime.h"

#include <algorithm>

#include "compute_fabric/core/digest.h"
#include "compute_fabric/core/random.h"
#include "compute_fabric/core/time_util.h"
#include "compute_fabric/executor/cpu_executor.h"
#include "compute_fabric/executor/cuda_executor.h"

namespace cf {

namespace {
constexpr size_t kMaxCompQueue = 4096;
constexpr uint32_t kMaxFrame = proto::kMaxFramePayload;

}  // namespace

NodeRuntime::NodeRuntime(NodeConfig config) : config_(std::move(config)) {
  node_id_ = Id128::random();
  cpu_ = std::make_unique<CpuExecutorBackend>(config_.cpu_slots);
  if (config_.enable_cuda) {
    cuda_ = std::make_unique<CudaExecutorBackend>(config_.cuda_device_ordinal,
                                                  config_.cuda_slots);
  }
}

NodeRuntime::~NodeRuntime() { stop(); }

ExecutorRuntimeStats NodeRuntime::cpu_stats() const {
  return cpu_ ? cpu_->stats() : ExecutorRuntimeStats{};
}
ExecutorRuntimeStats NodeRuntime::cuda_stats() const {
  return cuda_ ? cuda_->stats() : ExecutorRuntimeStats{};
}

Result<void> NodeRuntime::send_frame(const proto::Frame& frame,
                                     int timeout_ms) {
  std::lock_guard<std::mutex> lk(send_mu_);
  if (!channel_ || !channel_->valid()) {
    return Error(ErrorCode::NotConnected, "node channel not connected");
  }
  if (timeout_ms < 0) timeout_ms = static_cast<int>(config_.io_timeout_ms);
  return channel_->send_frame(frame, timeout_ms);
}

void NodeRuntime::request_runtime_stop() {
  connected_.store(false);
  stopping_.store(true);
  lifecycle_cv_.notify_all();
  comp_cv_.notify_all();
}

void NodeRuntime::terminate_transport() {
  request_runtime_stop();
  std::lock_guard<std::mutex> lk(send_mu_);
  if (channel_) channel_->close();
}

void NodeRuntime::forget_attempt(const ExecutionAttemptId& id) {
  std::lock_guard<std::mutex> lk(active_mu_);
  active_tokens_.erase(id);
}

Result<void> NodeRuntime::start(int64_t register_timeout_ms) {
  if (register_timeout_ms <= 0) {
    return Error(ErrorCode::InvalidArgument,
                 "registration timeout must be positive");
  }
  // Build capacity.
  proto::NodeRegister reg;
  reg.node_id = node_id_;
  reg.label = config_.label;
  reg.address = config_.address;
  reg.capacity.cpu_worker_slots = config_.cpu_slots;
  reg.capacity.host_memory_scheduling_bytes = config_.host_memory_scheduling_bytes;
  reg.capacity.tags = config_.tags;
  reg.capacity.address = config_.address;
  reg.capacity.label = config_.label;
  if (cpu_) {
    reg.capacity.executors.push_back(cpu_->capability());
  }
  if (cuda_) {
    reg.capacity.executors.push_back(cuda_->capability());
  }

  // Retry the connect + register cycle until the ack is received or the
  // deadline passes. Tolerates transient connection-delivery delays.
  const int64_t total_deadline = now_millis() + register_timeout_ms;
  Error last_err(ErrorCode::Timeout, "registration timed out");
  while (now_millis() < total_deadline) {
    const int64_t remaining_ms = total_deadline - now_millis();
    auto r = try_register(reg, std::min<int64_t>(3000, remaining_ms));
    if (r.ok()) {
      // Start loops.
      read_thread_ = std::thread([this] { read_loop(*channel_); });
      heartbeat_thread_ = std::thread([this] { heartbeat_loop(); });
      completion_thread_ = std::thread([this] { completion_loop(); });
      return Error::success();
    }
    last_err = r.error();
    if (channel_) channel_->close();
    channel_.reset();
    const int64_t retry_delay_ms =
        std::min<int64_t>(200, total_deadline - now_millis());
    if (retry_delay_ms > 0) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(retry_delay_ms));
    }
  }
  return last_err;
}

Result<void> NodeRuntime::try_register(const proto::NodeRegister& reg,
                                       int64_t each_attempt_ms) {
  if (each_attempt_ms <= 0) {
    return Error(ErrorCode::Timeout, "registration attempt timed out");
  }
  const int64_t attempt_deadline = now_millis() + each_attempt_ms;

  // Connect.
  const int connect_timeout_ms = static_cast<int>(std::max<int64_t>(
      1, std::min<int64_t>(config_.io_timeout_ms, each_attempt_ms)));
  auto sock = net::TcpSocket::connect(config_.coordinator_host,
                                      config_.coordinator_node_port,
                                      connect_timeout_ms);
  if (sock.failed()) return sock.error();
  channel_ = std::make_shared<net::Channel>(sock.take());

  // Hello.
  proto::Hello hello;
  hello.role = "node";
  proto::BinaryWriter w;
  proto::serialize(w, hello);
  int64_t remaining_ms = attempt_deadline - now_millis();
  if (remaining_ms <= 0) {
    return Error(ErrorCode::Timeout, "registration attempt timed out");
  }
  auto r = send_frame(proto::make_frame(proto::kHello, proto::SenderKind::Node,
                                        node_id_, 1, w.bytes()),
                      static_cast<int>(remaining_ms));
  if (r.failed()) return r;

  // Register.
  proto::BinaryWriter w2;
  proto::serialize(w2, reg);
  remaining_ms = attempt_deadline - now_millis();
  if (remaining_ms <= 0) {
    return Error(ErrorCode::Timeout, "registration attempt timed out");
  }
  r = send_frame(proto::make_frame(proto::kNodeRegister, proto::SenderKind::Node,
                                   node_id_, 2, w2.bytes()),
                 static_cast<int>(remaining_ms));
  if (r.failed()) return r;

  // Wait for ack.
  proto::NodeRegisterAck ack;
  bool got = false;
  while (now_millis() < attempt_deadline) {
    remaining_ms = attempt_deadline - now_millis();
    auto frame = channel_->recv_frame(
        static_cast<int>(std::min<int64_t>(500, remaining_ms)), kMaxFrame);
    if (frame.failed()) {
      if (frame.error().code() == ErrorCode::Timeout) continue;
      return frame.error();
    }
    if (frame.value().header.message_type == proto::kNodeRegisterAck) {
      proto::BinaryReader br(frame.value().payload);
      auto m = proto::deserialize_node_register_ack(br);
      if (m.failed()) continue;
      if (m.value().node_id != node_id_) {
        return Error(ErrorCode::InvalidState,
                     "registration ack was for a different node");
      }
      ack = m.value();
      got = true;
      break;
    }
  }
  if (!got) {
    return Error(ErrorCode::Timeout, "timed out waiting for registration ack");
  }
  if (!ack.error.empty()) {
    return Error(ErrorCode::InvalidState, "registration rejected: " + ack.error);
  }
  session_ = ack.session_id;
  epoch_ = ack.coordinator_epoch;
  session_epoch_ = ack.session_epoch;
  connected_.store(true);
  return Error::success();
}

void NodeRuntime::run() {
  std::unique_lock<std::mutex> lk(lifecycle_mu_);
  lifecycle_cv_.wait(lk, [this] { return stopping_.load(); });
}

void NodeRuntime::stop() {
  {
    std::lock_guard<std::mutex> lk(stop_mu_);
    if (stop_done_) return;
    stop_done_ = true;
  }
  request_runtime_stop();
  {
    std::lock_guard<std::mutex> lk(active_mu_);
    for (const auto& [id, token] : active_tokens_) {
      (void)id;
      if (token) token->request();
    }
  }
  {
    std::lock_guard<std::mutex> lk(send_mu_);
    if (channel_) channel_->close();
  }
  if (cpu_) cpu_->shutdown(false);
  if (cuda_) cuda_->shutdown(false);
  if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
  if (read_thread_.joinable()) read_thread_.join();
  if (completion_thread_.joinable()) completion_thread_.join();
  {
    std::lock_guard<std::mutex> lk(active_mu_);
    active_tokens_.clear();
  }
  {
    std::lock_guard<std::mutex> lk(send_mu_);
    channel_.reset();
  }
}

void NodeRuntime::heartbeat_loop() {
  while (!stopping_.load()) {
    {
      std::unique_lock<std::mutex> lk(lifecycle_mu_);
      if (lifecycle_cv_.wait_for(
              lk,
              std::chrono::milliseconds(
                  std::max<int64_t>(1, config_.heartbeat_interval_ms)),
              [this] { return stopping_.load(); })) {
        break;
      }
    }
    if (!connected_.load()) {
      request_runtime_stop();
      break;
    }
    proto::NodeHeartbeat hb;
    hb.node_id = node_id_;
    hb.session_id = session_;
    hb.session_epoch = session_epoch_;
    hb.coordinator_epoch = epoch_;
    auto cs = cpu_stats();
    hb.active_cpu_slots = cs.active;
    auto gs = cuda_stats();
    hb.active_gpu_slots = gs.active;
    proto::ExecutorRuntimeSnapshot sn;
    sn.name = "cpu";
    sn.active = cs.active;
    hb.executors.push_back(sn);
    if (cuda_) {
      sn.name = "cuda";
      sn.active = gs.active;
      hb.executors.push_back(sn);
    }
    proto::BinaryWriter w;
    proto::serialize(w, hb);
    auto r = send_frame(proto::make_frame(proto::kNodeHeartbeat,
                                          proto::SenderKind::Node, node_id_,
                                          0, w.bytes()));
    if (r.failed()) {
      request_runtime_stop();
      break;
    }
  }
}

void NodeRuntime::read_loop(net::Channel& channel) {
  for (;;) {
    if (stopping_.load()) break;
    auto frame = channel.recv_frame(static_cast<int>(config_.io_timeout_ms),
                                    kMaxFrame);
    if (frame.failed()) {
      if (frame.error().code() == ErrorCode::Timeout) continue;
      request_runtime_stop();
      break;
    }
    proto::BinaryReader r(frame.value().payload);
    switch (frame.value().header.message_type) {
      case proto::kDispatchAttempt: {
        auto m = proto::deserialize_dispatch_attempt(r);
        if (m.ok()) handle_dispatch(m.value());
        break;
      }
      case proto::kAttemptCancel: {
        auto m = proto::deserialize_attempt_cancel(r);
        if (m.ok()) handle_cancel(m.value());
        break;
      }
      case proto::kNodeHeartbeatAck: {
        auto m = proto::deserialize_node_heartbeat_ack(r);
        if (m.failed() || !m.value().accepted) {
          request_runtime_stop();
        }
        break;
      }
      case proto::kNodeShutdown: {
        auto m = proto::deserialize_node_shutdown(r);
        proto::NodeShutdownAck ack;
        ack.accepted = m.ok() && m.value().node_id == node_id_ &&
                       m.value().session_id == session_;
        proto::BinaryWriter w;
        proto::serialize(w, ack);
        auto sent = send_frame(proto::make_frame(
            proto::kNodeShutdownAck, proto::SenderKind::Node, node_id_,
            frame.value().header.request_id, w.bytes()));
        if (sent.failed() || ack.accepted) request_runtime_stop();
        break;
      }
      default:
        break;
    }
    if (stopping_.load()) break;
  }
}

void NodeRuntime::handle_dispatch(const proto::DispatchAttempt& msg) {
  // Validate epoch.
  if (msg.coordinator_epoch != epoch_) {
    send_failed(msg.attempt_id, msg.task.id, msg.reservation_id,
                FailureKind::Internal, "stale coordinator epoch", 0.0);
    return;
  }
  auto token = std::make_shared<CancellationToken>();
  {
    std::lock_guard<std::mutex> lk(active_mu_);
    active_tokens_[msg.attempt_id] = token;
  }

  ExecutorBackend* exec = nullptr;
  if (msg.task.required_executor == ExecutorType::Cuda && cuda_) {
    auto ok = cuda_->can_execute(msg.task);
    if (ok.ok() && ok.value()) exec = cuda_.get();
  }
  if (!exec && cpu_) {
    auto ok = cpu_->can_execute(msg.task);
    if (ok.ok() && ok.value()) exec = cpu_.get();
  }
  if (!exec) {
    forget_attempt(msg.attempt_id);
    send_failed(msg.attempt_id, msg.task.id, msg.reservation_id,
                FailureKind::ExecutorFailed, "no executor can run this task",
                0.0);
    return;
  }

  auto work = std::make_shared<AttemptWork>();
  work->task = msg.task;
  work->attempt_id = msg.attempt_id;
  work->node_id = node_id_;
  work->reservation_id = msg.reservation_id;
  work->coordinator_epoch = msg.coordinator_epoch;
  work->token = token;
  work->enqueued_at_ms = now_millis();
  auto self = this;
  work->on_start = [self, msg] {
    const std::string device =
        self->cuda_ && msg.task.required_executor == ExecutorType::Cuda
            ? "cuda:" + std::to_string(self->config_.cuda_device_ordinal)
            : "cpu";
    self->send_started(msg.attempt_id, device);
  };
  work->on_complete = [self, msg](AttemptOutcome out) {
    CompletionItem item;
    item.attempt_id = msg.attempt_id;
    item.task_id = msg.task.id;
    item.reservation_id = msg.reservation_id;
    item.outcome = std::move(out);
    bool queued = false;
    bool queue_saturated = false;
    {
      std::unique_lock<std::mutex> lk(self->comp_mu_);
      self->comp_cv_.wait_for(lk, std::chrono::milliseconds(5000),
                              [&] {
                                return self->stopping_.load() ||
                                       self->comp_queue_.size() < kMaxCompQueue;
                              });
      if (!self->stopping_.load() &&
          self->comp_queue_.size() < kMaxCompQueue) {
        self->comp_queue_.push_back(std::move(item));
        queued = true;
      } else if (!self->stopping_.load()) {
        queue_saturated = true;
      }
    }
    if (queued) {
      self->comp_cv_.notify_all();
      return;
    }

    // A terminal outcome must never disappear while its reservation remains
    // live. During ordinary shutdown the transport is already being closed,
    // so the coordinator will recover all attempts for this node. If the
    // bounded completion queue remains saturated during normal operation,
    // deliberately terminate the transport and runtime to trigger the same
    // node-loss recovery path.
    self->forget_attempt(msg.attempt_id);
    if (queue_saturated) self->terminate_transport();
  };

  auto r = exec->enqueue(work);
  if (r.failed()) {
    forget_attempt(msg.attempt_id);
    send_failed(msg.attempt_id, msg.task.id, msg.reservation_id,
                FailureKind::ExecutorFailed, r.error().message(), 0.0);
  }
}

void NodeRuntime::handle_cancel(const proto::AttemptCancel& msg) {
  std::lock_guard<std::mutex> lk(active_mu_);
  auto it = active_tokens_.find(msg.attempt_id);
  if (it != active_tokens_.end()) {
    it->second->request();
  }
}

void NodeRuntime::completion_loop() {
  for (;;) {
    if (stopping_.load()) break;
    CompletionItem item;
    {
      std::unique_lock<std::mutex> lk(comp_mu_);
      comp_cv_.wait_for(lk, std::chrono::milliseconds(100), [&] {
        return stopping_.load() || !comp_queue_.empty();
      });
      if (comp_queue_.empty()) {
        if (stopping_.load()) break;
        continue;
      }
      item = std::move(comp_queue_.front());
      comp_queue_.pop_front();
    }
    comp_cv_.notify_all();
    forget_attempt(item.attempt_id);
    if (item.outcome.state == AttemptState::Succeeded) {
      send_complete(item.attempt_id, item.task_id, item.reservation_id,
                    item.outcome);
    } else if (item.outcome.state == AttemptState::Cancelled) {
      send_cancelled(item.attempt_id, item.task_id, item.reservation_id,
                     item.outcome.failure_reason);
    } else {
      send_failed(item.attempt_id, item.task_id, item.reservation_id,
                  item.outcome.failure_kind, item.outcome.failure_reason,
                  item.outcome.duration_ms);
    }
  }
}

void NodeRuntime::send_started(const ExecutionAttemptId& id,
                               const std::string& device) {
  proto::AttemptStarted st;
  st.attempt_id = id;
  st.started_at_ms = now_millis();
  st.device = device;
  proto::BinaryWriter w;
  proto::serialize(w, st);
  send_frame(proto::make_frame(proto::kAttemptStarted, proto::SenderKind::Node,
                               node_id_, 0, w.bytes()));
}

void NodeRuntime::send_complete(const ExecutionAttemptId& id,
                                const ComputeTaskId& task,
                                const ReservationId& resv,
                                const AttemptOutcome& out) {
  proto::AttemptComplete co;
  co.attempt_id = id;
  co.task_id = task;
  co.reservation_id = resv;
  co.coordinator_epoch = epoch_;
  co.result_code = 0;
  co.output_size = out.output_bytes.size();
  co.output_integrity = out.output_integrity;
  co.duration_ms = out.duration_ms;
  co.cpu_millis = out.duration_ms;
  co.bytes_transferred = out.bytes_transferred;
  co.device = out.device;
  co.notes = out.notes;
  proto::BinaryWriter w;
  proto::serialize(w, co);
  send_frame(proto::make_frame(proto::kAttemptComplete, proto::SenderKind::Node,
                               node_id_, 0, w.bytes()));
  send_release(resv, task);
}

void NodeRuntime::send_failed(const ExecutionAttemptId& id,
                              const ComputeTaskId& task,
                              const ReservationId& resv, FailureKind kind,
                              const std::string& reason, double duration_ms) {
  proto::AttemptFailed fa;
  fa.attempt_id = id;
  fa.task_id = task;
  fa.reservation_id = resv;
  fa.coordinator_epoch = epoch_;
  fa.failure_kind = static_cast<uint8_t>(kind);
  fa.failure_reason = reason;
  fa.duration_ms = duration_ms;
  proto::BinaryWriter w;
  proto::serialize(w, fa);
  send_frame(proto::make_frame(proto::kAttemptFailed, proto::SenderKind::Node,
                               node_id_, 0, w.bytes()));
  send_release(resv, task);
}

void NodeRuntime::send_cancelled(const ExecutionAttemptId& id,
                                 const ComputeTaskId& task,
                                 const ReservationId& resv,
                                 const std::string& reason) {
  proto::AttemptCancelled ca;
  ca.attempt_id = id;
  ca.task_id = task;
  ca.reservation_id = resv;
  ca.coordinator_epoch = epoch_;
  ca.reason = reason;
  proto::BinaryWriter w;
  proto::serialize(w, ca);
  send_frame(proto::make_frame(proto::kAttemptCancelled, proto::SenderKind::Node,
                               node_id_, 0, w.bytes()));
  send_release(resv, task);
}

void NodeRuntime::send_release(const ReservationId& resv,
                               const ComputeTaskId& task) {
  proto::ReservationRelease rel;
  rel.reservation_id = resv;
  rel.task_id = task;
  rel.node_id = node_id_;
  rel.session_id = session_;
  rel.coordinator_epoch = epoch_;
  proto::BinaryWriter w;
  proto::serialize(w, rel);
  send_frame(proto::make_frame(proto::kReservationRelease,
                               proto::SenderKind::Node, node_id_, 0, w.bytes()));
}

}  // namespace cf
