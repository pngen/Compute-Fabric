#include "compute_fabric/coordinator/coordinator.h"

#include <algorithm>
#include <chrono>
#include <cstring>

#include "compute_fabric/core/digest.h"
#include "compute_fabric/core/logging.h"
#include "compute_fabric/core/random.h"
#include "compute_fabric/core/time_util.h"

namespace cf {

namespace {

constexpr size_t kMaxQueue = 8192;
constexpr int64_t kQueuePushTimeoutMs = 5000;
constexpr uint32_t kMaxFrame = proto::kMaxFramePayload;
constexpr int64_t kCoreWaitMs = 10;
constexpr int64_t kSweepIntervalMs = 20;
constexpr size_t kMaxPlacements = 1000;
constexpr int64_t kDispatchTimeoutMs = 5000;
constexpr int64_t kShutdownDrainTimeoutMs = 2000;
constexpr size_t kMaxOutboundFrames = 256;
constexpr size_t kMaxOutboundBytes = 16u << 20;  // 16 MiB per connection

size_t frame_wire_size(const proto::Frame& frame) {
  return static_cast<size_t>(proto::kHeaderSize) + frame.payload.size();
}

template <typename Conn>
void stop_connection(const std::shared_ptr<Conn>& conn) {
  if (!conn) return;
  conn->stop.store(true);
  {
    std::lock_guard<std::mutex> lk(conn->wmu);
    conn->outbound.clear();
    conn->outbound_bytes = 0;
  }
  conn->wcv.notify_all();
  if (conn->channel) conn->channel->close();
}

template <typename Conn>
bool queue_outbound(const std::shared_ptr<Conn>& conn, proto::Frame frame) {
  if (!conn || conn->stop.load()) return false;
  const size_t frame_bytes = frame_wire_size(frame);
  bool overflow = false;
  {
    std::lock_guard<std::mutex> lk(conn->wmu);
    if (conn->stop.load()) return false;
    overflow = conn->outbound.size() >= kMaxOutboundFrames ||
               frame_bytes > kMaxOutboundBytes ||
               conn->outbound_bytes > kMaxOutboundBytes - frame_bytes;
    if (!overflow) {
      conn->outbound.push_back(std::move(frame));
      conn->outbound_bytes += frame_bytes;
    }
  }
  if (overflow) {
    // A peer that cannot consume bounded control traffic is deterministically
    // disconnected rather than applying unbounded memory pressure to the
    // coordinator core.
    stop_connection(conn);
    return false;
  }
  conn->wcv.notify_all();
  return true;
}

template <typename Conn>
bool wait_for_outbound_drain(const std::shared_ptr<Conn>& conn,
                             int64_t deadline_ms) {
  if (!conn) return true;
  const int64_t remaining_ms = deadline_ms - now_millis();
  if (remaining_ms <= 0) return false;
  std::unique_lock<std::mutex> lk(conn->wmu);
  conn->wcv.wait_for(lk, std::chrono::milliseconds(remaining_ms), [&] {
    return conn->stop.load() ||
           (conn->outbound.empty() && !conn->writer_in_flight);
  });
  return conn->outbound.empty() && !conn->writer_in_flight;
}

}  // namespace

// ---------------------------------------------------------------------------
// IdempotencyCache
// ---------------------------------------------------------------------------
void IdempotencyCache::put(const std::string& key, uint16_t message_type,
                           std::vector<uint8_t> payload) {
  if (entries_.count(key)) return;
  entries_[key] = Entry{message_type, std::move(payload)};
  order_.push_back(key);
  while (order_.size() > kMaxEntries) {
    entries_.erase(order_.front());
    order_.pop_front();
  }
}

bool IdempotencyCache::get(const std::string& key, uint16_t& message_type,
                           std::vector<uint8_t>& payload) const {
  auto it = entries_.find(key);
  if (it == entries_.end()) return false;
  message_type = it->second.message_type;
  payload = it->second.payload;
  return true;
}

// ---------------------------------------------------------------------------
// CoreCommand
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------
Coordinator::Coordinator(CoordinatorConfig config)
    : config_(std::move(config)) {
  scheduler_.set_policy(config_.policy);
}

Coordinator::~Coordinator() { stop(); }

Result<std::pair<uint16_t, uint16_t>> Coordinator::start() {
  net::sockets_init();

  // Durable state.
  auto store = Store::open(config_.store_dir, /*create_if_missing=*/true);
  if (store.failed()) return store.error();
  store_ = std::move(store.value());
  auto loaded = store_->load();
  if (loaded.failed()) return loaded.error();
  durable_ = loaded.value();

  const bool fresh = durable_.epoch == 0;
  if (fresh) {
    durable_.epoch = 1;
  } else {
    // Coordinator restart: bump epoch, invalidating all prior execution
    // authority and reservations.
    durable_.epoch += 1;
  }

  // Rebuild task graph from durable state.
  for (const auto& [id, task] : durable_.tasks) {
    (void)id;
    auto r = graph_.add_task(task);
    if (r.failed()) {
      return Error(ErrorCode::CorruptionDetected, r.error().message());
    }
  }
  for (const auto& [id, state] : durable_.task_states) {
    if (graph_.has_task(id)) {
      graph_.restore_state(id, state);
    }
  }
  for (const auto& [tid, attempts] : durable_.attempts) {
    (void)tid;
    attempts_[tid] = attempts;
  }
  for (const auto& sp : durable_.placements) {
    last_placements_[sp.task_id] = sp.decision;
  }

  // Recovery: mark previously-active attempts Lost and requeue eligible work.
  for (const auto& [id, task] : durable_.tasks) {
    TaskState st = graph_.state(id);
    if (is_active(st)) {
      auto r = graph_.set_state(id, TaskState::Lost);
      if (r.ok()) {
        telemetry_task("recovery.task_requeued", id,
                       Json::make_object().set("prev_state", task_state_name(st)));
        if (task.retry.retry_on_node_loss && task.retry.max_attempts > 1) {
          graph_.set_state(id, TaskState::RetryPending);
          retry_ready_at_ms_[id] =
              now_millis() + static_cast<int64_t>(task.retry.retry_backoff_ms);
        } else {
          graph_.set_state(id, TaskState::Failed);
        }
      }
    }
  }

  // Bind listeners.
  auto nl = net::TcpListener::bind(config_.node_host, config_.node_port, 64);
  if (nl.failed()) return nl.error();
  node_listener_ = std::move(nl.value());
  auto cl = net::TcpListener::bind(config_.client_host, config_.client_port, 64);
  if (cl.failed()) return cl.error();
  client_listener_ = std::move(cl.value());

  config_.node_port = node_listener_.port();
  config_.client_port = client_listener_.port();

  telemetry_.configure(config_.telemetry_path, config_.telemetry_human);
  telemetry_.event("coordinator.start",
                   Json::make_object()
                       .set("node_port", static_cast<int64_t>(config_.node_port))
                       .set("client_port", static_cast<int64_t>(config_.client_port))
                       .set("policy", scheduler_policy_name(config_.policy)),
                   durable_.epoch);

  // Save bumped epoch.
  persist();

  {
    std::lock_guard<std::mutex> lk(retired_mu_);
    reaper_stopping_ = false;
  }
  connection_reaper_thread_ =
      std::thread([this] { connection_reaper_loop(); });
  core_running_.store(true);
  core_thread_ = std::thread([this] { core_loop(); });
  accept_node_thread_ = std::thread([this] { accept_node_loop(); });
  accept_client_thread_ = std::thread([this] { accept_client_loop(); });

  return std::make_pair(config_.node_port, config_.client_port);
}

void Coordinator::run() {
  // Block until stop() is called.
  while (!stopping_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

void Coordinator::persist() {
  durable_.tasks = graph_to_tasks();
  durable_.task_states = graph_states();
  durable_.attempts.clear();
  for (const auto& [tid, list] : attempts_) {
    for (const auto& a : list) {
      if (a.committed) durable_.attempts[tid].push_back(a);
    }
  }
  durable_.retry_counts.clear();
  for (const auto& [tid, list] : attempts_) {
    durable_.retry_counts[tid] = static_cast<uint32_t>(list.size());
  }
  durable_.placements.clear();
  for (const auto& [tid, sp] : placement_records_) {
    durable_.placements.push_back(sp);
  }
  if (store_) {
    auto r = store_->save(durable_);
    if (r.failed()) {
      Logger::instance().error("persist failed: %s", r.error().message().c_str());
    }
  }
}

void Coordinator::stop() {
  {
    std::lock_guard<std::mutex> lk(stop_mu_);
    if (stop_done_) return;
    stop_done_ = true;
  }
  stopping_.store(true);
  // Stop accepting work and ask the core thread to exit before touching its
  // connection maps. Shutdown is pushed to the front so an existing backlog
  // cannot make stop unbounded.
  {
    std::lock_guard<std::mutex> lk(core_mu_);
    CoreCommand cmd;
    cmd.kind = CoreCommand::Kind::Shutdown;
    core_queue_.push_front(std::move(cmd));
  }
  core_cv_.notify_all();
  node_listener_.close();
  client_listener_.close();

  // No new entries can be appended to the all-connections lists after the
  // acceptors have stopped.
  if (accept_node_thread_.joinable()) accept_node_thread_.join();
  if (accept_client_thread_.joinable()) accept_client_thread_.join();

  std::vector<std::shared_ptr<NodeConn>> node_conns;
  std::vector<std::shared_ptr<ClientConn>> client_conns;
  {
    std::lock_guard<std::mutex> lk(conn_list_mu_);
    node_conns = all_node_conns_;
    client_conns = all_client_conns_;
  }
  // Stop every accepted connection, including peers that never completed a
  // handshake or registration. This list is independent of the core-owned
  // lookup maps.
  for (const auto& conn : node_conns) {
    stop_connection(conn);
    retire_node_connection(conn);
  }
  for (const auto& conn : client_conns) {
    stop_connection(conn);
    retire_client_connection(conn);
  }

  if (core_thread_.joinable()) core_thread_.join();

  // The reaper is the only owner that joins connection threads. It drains all
  // retirements before exiting, eliminating concurrent joins between stop()
  // and normal disconnect cleanup.
  {
    std::lock_guard<std::mutex> lk(retired_mu_);
    reaper_stopping_ = true;
  }
  retired_cv_.notify_all();
  if (connection_reaper_thread_.joinable()) {
    connection_reaper_thread_.join();
  }

  {
    std::lock_guard<std::mutex> lk(core_mu_);
    core_queue_.clear();
  }
  node_conns_.clear();
  client_conns_.clear();
  {
    std::lock_guard<std::mutex> lk(conn_list_mu_);
    all_node_conns_.clear();
    all_client_conns_.clear();
  }

  telemetry_.event("coordinator.stop", Json::make_object(), durable_.epoch);
  telemetry_.flush();
}

// ---------------------------------------------------------------------------
// Thread loops
// ---------------------------------------------------------------------------
void Coordinator::accept_node_loop() {
  for (;;) {
    if (stopping_.load()) return;
    auto sock = node_listener_.accept(500);
    if (sock.failed()) {
      if (stopping_.load()) return;
      continue;
    }
    if (stopping_.load()) {
      sock.value().close();
      return;
    }
    auto conn = std::make_shared<NodeConn>();
    conn->channel = std::make_shared<net::Channel>(sock.take());
    conn->stop.store(false);
    {
      std::lock_guard<std::mutex> lk(conn_list_mu_);
      all_node_conns_.push_back(conn);
    }
    conn->writer = std::thread([this, conn] { node_writer(conn); });
    conn->reader = std::thread([this, conn] { node_reader(conn); });
  }
}

void Coordinator::accept_client_loop() {
  for (;;) {
    if (stopping_.load()) return;
    auto sock = client_listener_.accept(500);
    if (sock.failed()) {
      if (stopping_.load()) return;
      continue;
    }
    if (stopping_.load()) {
      sock.value().close();
      return;
    }
    auto conn = std::make_shared<ClientConn>();
    conn->id = Id128::random();
    conn->channel = std::make_shared<net::Channel>(sock.take());
    conn->stop.store(false);
    {
      std::lock_guard<std::mutex> lk(conn_list_mu_);
      all_client_conns_.push_back(conn);
    }
    conn->writer = std::thread([this, conn] { client_writer(conn); });
    conn->reader = std::thread([this, conn] {
      // handshake
      auto h = conn->channel->recv_frame(5000, kMaxFrame);
      if (h.failed()) {
        stop_connection(conn);
        retire_client_connection(conn);
        return;
      }
      proto::BinaryReader r(h.value().payload);
      auto hello = proto::deserialize_hello(r);
      if (h.value().header.message_type != proto::kHello || hello.failed() ||
          hello.value().role != "client" ||
          hello.value().protocol_version != proto::kProtocolVersion) {
        stop_connection(conn);
        retire_client_connection(conn);
        return;
      }
      CoreCommand connected;
      connected.kind = CoreCommand::Kind::ClientConnected;
      connected.client_id = conn->id;
      connected.client_conn = conn;
      {
        std::lock_guard<std::mutex> lk(core_mu_);
        if (stopping_.load()) {
          stop_connection(conn);
          retire_client_connection(conn);
          return;
        }
        core_queue_.push_back(std::move(connected));
      }
      core_cv_.notify_all();
      client_reader(conn);
    });
  }
}

void Coordinator::retire_node_connection(
    const std::shared_ptr<NodeConn>& conn) {
  if (!conn || conn->retirement_queued.exchange(true)) return;
  {
    std::lock_guard<std::mutex> lk(retired_mu_);
    retired_node_conns_.push_back(conn);
  }
  retired_cv_.notify_one();
}

void Coordinator::retire_client_connection(
    const std::shared_ptr<ClientConn>& conn) {
  if (!conn || conn->retirement_queued.exchange(true)) return;
  {
    std::lock_guard<std::mutex> lk(retired_mu_);
    retired_client_conns_.push_back(conn);
  }
  retired_cv_.notify_one();
}

void Coordinator::connection_reaper_loop() {
  for (;;) {
    std::shared_ptr<NodeConn> node_conn;
    std::shared_ptr<ClientConn> client_conn;
    {
      std::unique_lock<std::mutex> lk(retired_mu_);
      retired_cv_.wait(lk, [this] {
        return reaper_stopping_ || !retired_node_conns_.empty() ||
               !retired_client_conns_.empty();
      });
      if (!retired_node_conns_.empty()) {
        node_conn = std::move(retired_node_conns_.front());
        retired_node_conns_.pop_front();
      } else if (!retired_client_conns_.empty()) {
        client_conn = std::move(retired_client_conns_.front());
        retired_client_conns_.pop_front();
      } else if (reaper_stopping_) {
        return;
      }
    }

    // This dedicated thread is distinct from every connection reader/writer,
    // so it is the sole join owner and can never self-join.
    if (node_conn) {
      stop_connection(node_conn);
      if (node_conn->reader.joinable()) node_conn->reader.join();
      if (node_conn->writer.joinable()) node_conn->writer.join();
      std::lock_guard<std::mutex> lk(conn_list_mu_);
      all_node_conns_.erase(
          std::remove(all_node_conns_.begin(), all_node_conns_.end(),
                      node_conn),
          all_node_conns_.end());
    } else if (client_conn) {
      stop_connection(client_conn);
      if (client_conn->reader.joinable()) client_conn->reader.join();
      if (client_conn->writer.joinable()) client_conn->writer.join();
      std::lock_guard<std::mutex> lk(conn_list_mu_);
      all_client_conns_.erase(
          std::remove(all_client_conns_.begin(), all_client_conns_.end(),
                      client_conn),
          all_client_conns_.end());
    }
  }
}

void Coordinator::node_writer(std::shared_ptr<NodeConn> conn) {
  for (;;) {
    proto::Frame frame;
    {
      std::unique_lock<std::mutex> lk(conn->wmu);
      conn->wcv.wait_for(lk, std::chrono::milliseconds(100), [&] {
        return conn->stop.load() || !conn->outbound.empty();
      });
      if (conn->stop.load()) break;
      if (conn->outbound.empty()) continue;
      frame = std::move(conn->outbound.front());
      conn->outbound.pop_front();
      const size_t frame_bytes = frame_wire_size(frame);
      conn->outbound_bytes =
          frame_bytes <= conn->outbound_bytes
              ? conn->outbound_bytes - frame_bytes
              : 0;
      conn->writer_in_flight = true;
    }
    auto r = conn->channel->send_frame(frame, 5000);
    {
      std::lock_guard<std::mutex> lk(conn->wmu);
      conn->writer_in_flight = false;
    }
    conn->wcv.notify_all();
    if (r.failed()) {
      stop_connection(conn);
      break;
    }
  }
  conn->wcv.notify_all();
}

void Coordinator::client_writer(std::shared_ptr<ClientConn> conn) {
  for (;;) {
    proto::Frame frame;
    {
      std::unique_lock<std::mutex> lk(conn->wmu);
      conn->wcv.wait_for(lk, std::chrono::milliseconds(100), [&] {
        return conn->stop.load() || !conn->outbound.empty();
      });
      if (conn->stop.load()) break;
      if (conn->outbound.empty()) continue;
      frame = std::move(conn->outbound.front());
      conn->outbound.pop_front();
      const size_t frame_bytes = frame_wire_size(frame);
      conn->outbound_bytes =
          frame_bytes <= conn->outbound_bytes
              ? conn->outbound_bytes - frame_bytes
              : 0;
      conn->writer_in_flight = true;
    }
    auto r = conn->channel->send_frame(frame, 5000);
    {
      std::lock_guard<std::mutex> lk(conn->wmu);
      conn->writer_in_flight = false;
    }
    conn->wcv.notify_all();
    if (r.failed()) {
      stop_connection(conn);
      break;
    }
  }
  conn->wcv.notify_all();
}

void Coordinator::node_reader(std::shared_ptr<NodeConn> conn) {
  bool handshake_done = false;
  NodeId registered_node_id = Id128::zero();
  for (;;) {
    if (conn->stop.load()) break;
    auto frame = conn->channel->recv_frame(5000, kMaxFrame);
    if (frame.failed()) {
      if (frame.error().code() == ErrorCode::Timeout && handshake_done) {
        continue;
      }
      break;
    }
    if (!handshake_done) {
      if (frame.value().header.message_type != proto::kHello) break;
      proto::BinaryReader r(frame.value().payload);
      auto hello = proto::deserialize_hello(r);
      if (hello.failed() || hello.value().role != "node" ||
          hello.value().protocol_version != proto::kProtocolVersion) {
        break;
      }
      handshake_done = true;
      continue;
    }

    CoreCommand cmd;
    cmd.node_conn = conn;
    cmd.request_id = frame.value().header.request_id;
    proto::BinaryReader r(frame.value().payload);
    switch (frame.value().header.message_type) {
      case proto::kNodeRegister: {
        auto m = proto::deserialize_node_register(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::NodeRegister;
        cmd.reg = m.value();
        registered_node_id = m.value().node_id;
        break;
      }
      case proto::kNodeHeartbeat: {
        auto m = proto::deserialize_node_heartbeat(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::NodeHeartbeat;
        cmd.hb = m.value();
        break;
      }
      case proto::kNodeShutdown: {
        auto m = proto::deserialize_node_shutdown(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::NodeShutdownMsg;
        cmd.node_id = m.value().node_id;
        break;
      }
      case proto::kAttemptStarted: {
        auto m = proto::deserialize_attempt_started(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::AttemptStarted;
        cmd.st = m.value();
        break;
      }
      case proto::kAttemptProgress: {
        auto m = proto::deserialize_attempt_progress(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::AttemptProgress;
        cmd.pr = m.value();
        break;
      }
      case proto::kAttemptComplete: {
        auto m = proto::deserialize_attempt_complete(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::AttemptComplete;
        cmd.co = m.value();
        break;
      }
      case proto::kAttemptFailed: {
        auto m = proto::deserialize_attempt_failed(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::AttemptFailed;
        cmd.fa = m.value();
        break;
      }
      case proto::kAttemptCancelled: {
        auto m = proto::deserialize_attempt_cancelled(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::AttemptCancelled;
        cmd.ca = m.value();
        break;
      }
      case proto::kReservationRelease: {
        auto m = proto::deserialize_reservation_release(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ReservationRelease;
        cmd.rel = m.value();
        break;
      }
      default:
        break;
    }
    if (cmd.kind == CoreCommand::Kind::Sweep) continue;  // uninitialized
    {
      std::unique_lock<std::mutex> lk(core_mu_);
      core_cv_.wait_for(lk, std::chrono::milliseconds(kQueuePushTimeoutMs),
                        [&] {
                          return stopping_.load() ||
                                 core_queue_.size() < kMaxQueue;
                        });
      if (stopping_.load() || core_queue_.size() >= kMaxQueue) break;
      core_queue_.push_back(std::move(cmd));
    }
    core_cv_.notify_all();
  }
  stop_connection(conn);
  // Report disconnect.
  if (!stopping_.load() && registered_node_id != Id128::zero()) {
    CoreCommand cmd;
    cmd.kind = CoreCommand::Kind::NodeDisconnected;
    cmd.node_id = registered_node_id;
    cmd.node_conn = conn;
    {
      std::lock_guard<std::mutex> lk(core_mu_);
      core_queue_.push_back(std::move(cmd));
    }
    core_cv_.notify_all();
  }
  retire_node_connection(conn);
}

void Coordinator::client_reader(std::shared_ptr<ClientConn> conn) {
  for (;;) {
    if (conn->stop.load()) break;
    auto frame = conn->channel->recv_frame(5000, kMaxFrame);
    if (frame.failed()) {
      if (frame.error().code() != ErrorCode::Timeout) break;
      continue;
    }
    CoreCommand cmd;
    cmd.client_conn = conn;
    cmd.request_id = frame.value().header.request_id;
    proto::BinaryReader r(frame.value().payload);
    switch (frame.value().header.message_type) {
      case proto::kTaskSubmit: {
        auto m = proto::deserialize_task_submit(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientTaskSubmit;
        cmd.submit = m.value();
        break;
      }
      case proto::kTaskInspect: {
        auto m = proto::deserialize_task_inspect(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientTaskInspect;
        cmd.inspect = m.value();
        break;
      }
      case proto::kTaskList: {
        auto m = proto::deserialize_task_list(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientTaskList;
        cmd.list = m.value();
        break;
      }
      case proto::kTaskCancel: {
        auto m = proto::deserialize_task_cancel(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientTaskCancel;
        cmd.cancel = m.value();
        break;
      }
      case proto::kWorkloadInspect: {
        auto m = proto::deserialize_workload_inspect(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientWorkloadInspect;
        cmd.wl = m.value();
        break;
      }
      case proto::kFabricStatus: {
        auto m = proto::deserialize_fabric_status(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientFabricStatus;
        cmd.fs = m.value();
        break;
      }
      case proto::kPlacementQuery: {
        auto m = proto::deserialize_placement_query(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientPlacementQuery;
        cmd.pq = m.value();
        break;
      }
      case proto::kNodeList: {
        auto m = proto::deserialize_node_list(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientNodeList;
        break;
      }
      case proto::kNodeDrain: {
        auto m = proto::deserialize_node_drain(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientNodeDrain;
        cmd.drain = m.value();
        break;
      }
      case proto::kCoordinatorShutdown: {
        auto m = proto::deserialize_coordinator_shutdown(r);
        if (m.failed()) break;
        cmd.kind = CoreCommand::Kind::ClientCoordinatorShutdown;
        break;
      }
      default:
        break;
    }
    if (cmd.kind == CoreCommand::Kind::Sweep) continue;
    {
      std::unique_lock<std::mutex> lk(core_mu_);
      core_cv_.wait_for(lk, std::chrono::milliseconds(kQueuePushTimeoutMs),
                        [&] {
                          return stopping_.load() ||
                                 core_queue_.size() < kMaxQueue;
                        });
      if (stopping_.load() || core_queue_.size() >= kMaxQueue) break;
      core_queue_.push_back(std::move(cmd));
    }
    core_cv_.notify_all();
  }
  stop_connection(conn);
  // Client disconnected: remove from map.
  if (!stopping_.load()) {
    CoreCommand cmd;
    cmd.kind = CoreCommand::Kind::ClientDisconnected;
    cmd.client_id = conn->id;
    cmd.client_conn = conn;
    {
      std::lock_guard<std::mutex> lk(core_mu_);
      core_queue_.push_back(std::move(cmd));
    }
    core_cv_.notify_all();
  }
  retire_client_connection(conn);
}

// ---------------------------------------------------------------------------
// Core loop
// ---------------------------------------------------------------------------
void Coordinator::core_loop() {
  int64_t last_sweep = now_millis();
  for (;;) {
    CoreCommand cmd;
    bool has_cmd = false;
    {
      std::unique_lock<std::mutex> lk(core_mu_);
      core_cv_.wait_for(lk, std::chrono::milliseconds(kCoreWaitMs), [&] {
        return !core_queue_.empty();
      });
      if (!core_queue_.empty()) {
        cmd = std::move(core_queue_.front());
        core_queue_.pop_front();
        has_cmd = true;
      }
    }
    if (has_cmd) {
      if (cmd.kind == CoreCommand::Kind::Shutdown) {
        core_running_.store(false);
        return;
      }
      process(cmd);
    }
    int64_t now = now_millis();
    if (now - last_sweep >= kSweepIntervalMs) {
      last_sweep = now;
      sweep();
    }
  }
}

// ---------------------------------------------------------------------------
// Process
// ---------------------------------------------------------------------------
void Coordinator::process(const CoreCommand& cmd) {
  switch (cmd.kind) {
    case CoreCommand::Kind::NodeRegister: handle_node_register(cmd); break;
    case CoreCommand::Kind::NodeHeartbeat: handle_node_heartbeat(cmd); break;
    case CoreCommand::Kind::NodeShutdownMsg: {
      auto it = node_conns_.find(cmd.node_id);
      if (it != node_conns_.end() && it->second == cmd.node_conn) {
        proto::NodeShutdownAck ack;
        ack.accepted = true;
        proto::BinaryWriter w;
        proto::serialize(w, ack);
        respond_node(cmd, proto::kNodeShutdownAck, w.bytes());
        wait_for_outbound_drain(cmd.node_conn,
                                now_millis() + kShutdownDrainTimeoutMs);
        stop_connection(it->second);
      }
      break;
    }
    case CoreCommand::Kind::NodeDisconnected:
      handle_node_disconnect(cmd.node_id, cmd.node_conn);
      break;
    case CoreCommand::Kind::DispatchAck: handle_dispatch_ack(cmd); break;
    case CoreCommand::Kind::AttemptStarted: handle_attempt_started(cmd); break;
    case CoreCommand::Kind::AttemptProgress: break;
    case CoreCommand::Kind::AttemptComplete: handle_attempt_complete(cmd); break;
    case CoreCommand::Kind::AttemptFailed: handle_attempt_failed(cmd); break;
    case CoreCommand::Kind::AttemptCancelled: handle_attempt_cancelled(cmd); break;
    case CoreCommand::Kind::ReservationRelease: handle_reservation_release(cmd); break;
    case CoreCommand::Kind::ClientConnected: {
      if (!cmd.client_conn || cmd.client_conn->stop.load()) break;
      auto existing = client_conns_.find(cmd.client_id);
      if (existing != client_conns_.end() &&
          existing->second != cmd.client_conn) {
        stop_connection(existing->second);
      }
      client_conns_[cmd.client_id] = cmd.client_conn;
      break;
    }
    case CoreCommand::Kind::ClientTaskSubmit: handle_task_submit(cmd); break;
    case CoreCommand::Kind::ClientTaskInspect: handle_task_inspect(cmd); break;
    case CoreCommand::Kind::ClientTaskList: handle_task_list(cmd); break;
    case CoreCommand::Kind::ClientTaskCancel: handle_task_cancel(cmd); break;
    case CoreCommand::Kind::ClientWorkloadInspect: handle_workload_inspect(cmd); break;
    case CoreCommand::Kind::ClientFabricStatus: handle_fabric_status(cmd); break;
    case CoreCommand::Kind::ClientPlacementQuery: handle_placement_query(cmd); break;
    case CoreCommand::Kind::ClientNodeList: handle_node_list(cmd); break;
    case CoreCommand::Kind::ClientNodeDrain: handle_node_drain(cmd); break;
    case CoreCommand::Kind::ClientCoordinatorShutdown: handle_coordinator_shutdown(cmd); break;
    case CoreCommand::Kind::ClientDisconnected: {
      auto it = client_conns_.find(cmd.client_id);
      if (it != client_conns_.end() && it->second == cmd.client_conn) {
        client_conns_.erase(it);
      }
      break;
    }
    case CoreCommand::Kind::Sweep:
    case CoreCommand::Kind::Shutdown:
      break;
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
void Coordinator::respond_client(const CoreCommand& cmd,
                                 uint16_t message_type,
                                 const std::vector<uint8_t>& payload) {
  if (!cmd.client_conn) return;
  auto it = client_conns_.find(cmd.client_conn->id);
  if (it == client_conns_.end() || it->second != cmd.client_conn) return;
  auto frame = proto::make_frame(message_type, proto::SenderKind::Coordinator,
                                 Id128::zero(), cmd.request_id, payload);
  queue_outbound(cmd.client_conn, std::move(frame));
}

void Coordinator::send_to_node(const NodeId& node_id, uint16_t message_type,
                               const std::vector<uint8_t>& payload) {
  auto it = node_conns_.find(node_id);
  if (it == node_conns_.end() || !it->second || it->second->stop.load()) return;
  auto frame = proto::make_frame(message_type, proto::SenderKind::Coordinator,
                                 Id128::zero(), 0, payload);
  queue_outbound(it->second, std::move(frame));
}

void Coordinator::respond_node(const CoreCommand& cmd, uint16_t message_type,
                               const std::vector<uint8_t>& payload) {
  if (!cmd.node_conn) return;
  auto frame = proto::make_frame(message_type, proto::SenderKind::Coordinator,
                                 Id128::zero(), cmd.request_id, payload);
  queue_outbound(cmd.node_conn, std::move(frame));
}

std::string Coordinator::idem_key(const Id128& sender, uint64_t request_id) const {
  return sender.to_hex() + ":" + std::to_string(request_id);
}

void Coordinator::telemetry_node(const std::string& event, const NodeId& node,
                                 const Json& fields) {
  Json j = Json::make_object().set("node", node.to_hex());
  for (const auto& [k, v] : fields.object()) j.set(k, v);
  telemetry_.event(event, j, durable_.epoch);
}

void Coordinator::telemetry_task(const std::string& event, const ComputeTaskId& task,
                                 const Json& fields) {
  Json j = Json::make_object().set("task", task.to_hex());
  for (const auto& [k, v] : fields.object()) j.set(k, v);
  telemetry_.event(event, j, durable_.epoch);
}

// ---------------------------------------------------------------------------
// Node registration / heartbeat
// ---------------------------------------------------------------------------
void Coordinator::handle_node_register(const CoreCommand& cmd) {
  const auto& reg = cmd.reg;
  auto conn = cmd.node_conn;
  if (!conn || conn->stop.load()) return;

  proto::NodeRegisterAck ack;
  ack.node_id = reg.node_id;
  ack.coordinator_epoch = durable_.epoch;

  auto existing = nodes_.find(reg.node_id);
  auto old = node_conns_.find(reg.node_id);
  if (existing != nodes_.end() && existing->second.connected &&
      old != node_conns_.end() && old->second == conn) {
    // Idempotent replay on the current connection: return the established
    // authority rather than creating another session.
    ack.session_id = existing->second.session;
    ack.session_epoch = existing->second.session_epoch;
    proto::BinaryWriter w;
    proto::serialize(w, ack);
    respond_node(cmd, proto::kNodeRegisterAck, w.bytes());
    return;
  }

  // A new transport carrying the same stable NodeId supersedes the old one.
  // Session epochs make any late traffic from the previous transport stale.
  if (old != node_conns_.end() && old->second != conn) {
    stop_connection(old->second);
  }

  NodeRecord rec;
  rec.id = reg.node_id;
  rec.session = Id128::random();
  rec.epoch = durable_.epoch;
  rec.session_epoch =
      (existing != nodes_.end()) ? existing->second.session_epoch + 1 : 1;
  rec.health = NodeHealth::Online;
  rec.capacity = reg.capacity;
  rec.last_heartbeat_ms = now_millis();
  rec.register_ms = now_millis();
  rec.connected = true;

  nodes_[reg.node_id] = rec;
  conn->node_id = reg.node_id;
  conn->session = rec.session;
  node_conns_[reg.node_id] = conn;

  ack.node_id = reg.node_id;
  ack.session_id = rec.session;
  ack.session_epoch = rec.session_epoch;
  ack.error = "";

  proto::BinaryWriter w;
  proto::serialize(w, ack);
  respond_node(cmd, proto::kNodeRegisterAck, w.bytes());

  telemetry_node("node.register", reg.node_id,
                 Json::make_object()
                     .set("label", reg.label)
                     .set("executors", Json::make_array())
                     .set("session", rec.session.to_hex())
                     .set("session_epoch", static_cast<int64_t>(rec.session_epoch)));
}

void Coordinator::handle_node_heartbeat(const CoreCommand& cmd) {
  const auto& hb = cmd.hb;
  proto::NodeHeartbeatAck ack;
  auto it = nodes_.find(hb.node_id);
  if (it == nodes_.end()) {
    ack.accepted = false;
    ack.error = "unknown node";
  } else if (it->second.session != hb.session_id ||
             it->second.session_epoch != hb.session_epoch) {
    ack.accepted = false;
    ack.error = "stale session; re-register";
  } else if (hb.coordinator_epoch != durable_.epoch) {
    ack.accepted = false;
    ack.error = "stale coordinator epoch; re-register";
  } else {
    it->second.last_heartbeat_ms = now_millis();
    it->second.health = NodeHealth::Online;
    ack.accepted = true;
  }
  proto::BinaryWriter w;
  proto::serialize(w, ack);
  if (cmd.node_conn) {
    auto f = proto::make_frame(proto::kNodeHeartbeatAck,
                               proto::SenderKind::Coordinator, Id128::zero(),
                               cmd.request_id, w.bytes());
    queue_outbound(cmd.node_conn, std::move(f));
  }
}

void Coordinator::handle_node_disconnect(const NodeId& node_id,
                                         const std::shared_ptr<NodeConn>& conn) {
  // The lookup map is core-owned. Remove the stopped transport even when the
  // node was already declared offline by a heartbeat sweep; otherwise that
  // map entry would retain the reaped connection until coordinator shutdown.
  auto nc = node_conns_.find(node_id);
  if (nc == node_conns_.end() || nc->second != conn) return;
  node_conns_.erase(nc);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return;
  if (it->second.health == NodeHealth::Offline) return;
  it->second.connected = false;
  // Grace period before declaring offline (heartbeat timeout).
  it->second.last_heartbeat_ms = now_millis() - config_.heartbeat_timeout_ms + 100;
  telemetry_node("node.disconnect", node_id, Json::make_object());
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------
void Coordinator::schedule_ready_tasks() {
  std::vector<std::pair<ComputeTaskId, int64_t>> ready;
  for (const auto& id : graph_.task_ids()) {
    if (graph_.state(id) == TaskState::Ready) {
      auto rit = ready_at_ms_.find(id);
      int64_t t = rit != ready_at_ms_.end() ? rit->second : 0;
      ready.emplace_back(id, t);
    }
  }
  std::sort(ready.begin(), ready.end(), [this](const auto& a, const auto& b) {
    const ComputeTask* ta = graph_.find(a.first);
    const ComputeTask* tb = graph_.find(b.first);
    Priority pa = ta ? ta->priority : Priority::Normal;
    Priority pb = tb ? tb->priority : Priority::Normal;
    if (pa != pb) return pa < pb;
    if (a.second != b.second) return a.second < b.second;
    return a.first < b.first;
  });

  for (const auto& [task_id, unused] : ready) {
    (void)unused;
    if (stopping_.load()) return;
    const ComputeTask* task = graph_.find(task_id);
    if (!task) continue;
    // Re-check state (may have changed).
    if (graph_.state(task_id) != TaskState::Ready) continue;

    // Deadline check.
    if (task->has_deadline && now_millis() >= task->deadline_epoch_ms) {
      graph_.set_state(task_id, TaskState::Expired);
      telemetry_task("task.expire", task_id, Json::make_object());
      persist();
      continue;
    }

    auto decision = scheduler_.place(*task, nodes_, reservations_, state_locality_);
    last_placements_[task_id] = decision;
    placement_records_[task_id] = StoredPlacement{task_id, ++placement_sequence_, decision};

    if (!decision.feasible || !decision.chosen_node) {
      // Emit placement telemetry with exclusions.
      Json j = Json::make_object()
                   .set("feasible", false)
                   .set("reason", decision.reason)
                   .set("excluded_count", static_cast<int64_t>(decision.excluded.size()));
      telemetry_task("placement.decision", task_id, j);
      continue;
    }

    NodeId node_id = *decision.chosen_node;
    auto nit = nodes_.find(node_id);
    if (nit == nodes_.end() || !nit->second.connected) {
      continue;
    }
    ExecutorType executor = decision.chosen_executor.value_or(task->required_executor);
    uint32_t slots = executor == ExecutorType::Cuda ? task->required_gpu_slots
                                                    : task->required_cpu_slots;

    auto rr = reservations_.try_reserve(nit->second, durable_.epoch, task_id,
                                        executor, slots, task->scratch_memory_bytes);
    if (!rr.reservation) {
      Json j = Json::make_object().set("feasible", false)
                                 .set("reason", "reservation failed")
                                 .set("node", node_id.to_hex());
      telemetry_task("placement.decision", task_id, j);
      continue;
    }
    Reservation resv = rr.reservation.value();

    auto t1 = graph_.set_state(task_id, TaskState::Planning);
    if (t1.failed()) {
      reservations_.release(resv.id);
      continue;
    }
    auto t2 = graph_.set_state(task_id, TaskState::Reserved);
    if (t2.failed()) {
      reservations_.release(resv.id);
      continue;
    }
    task_reservation_[task_id] = resv.id;

    // Create attempt.
    ExecutionAttempt a;
    a.id = Id128::random();
    a.task_id = task_id;
    a.node_id = node_id;
    a.node_session = nit->second.session;
    a.coordinator_epoch = durable_.epoch;
    a.reservation_id = resv.id;
    a.executor = executor;
    a.attempt_number = static_cast<uint32_t>(attempts_[task_id].size());
    a.dispatched_at_ms = now_millis();
    a.state = AttemptState::Dispatched;
    a.device = decision.chosen_device;
    attempts_[task_id].push_back(a);
    attempt_by_reservation_[resv.id] = a;
    dispatch_at_ms_[task_id] = a.dispatched_at_ms;

    // Send dispatch.
    proto::DispatchAttempt da;
    da.task = *task;
    da.attempt_id = a.id;
    da.reservation_id = resv.id;
    da.coordinator_epoch = durable_.epoch;
    da.attempt_number = a.attempt_number;
    proto::BinaryWriter w;
    proto::serialize(w, da);
    send_to_node(node_id, proto::kDispatchAttempt, w.bytes());

    graph_.set_state(task_id, TaskState::Dispatching);

    // Telemetry.
    Json pd = Json::make_object()
                  .set("feasible", true)
                  .set("node", node_id.to_hex())
                  .set("executor", decision.chosen_device)
                  .set("reason", decision.reason)
                  .set("total_cost", decision.total_cost)
                  .set("execution_cost", decision.execution_cost)
                  .set("queue_cost", decision.queue_cost)
                  .set("transfer_cost", decision.transfer_cost)
                  .set("recompute_cost", decision.recompute_cost)
                  .set("pressure_cost", decision.pressure_cost)
                  .set("locality_score", decision.state_locality_score)
                  .set("candidate_count", static_cast<int64_t>(decision.candidates.size()));
    telemetry_task("placement.decision", task_id, pd);
    telemetry_task("reservation.granted", task_id,
                   Json::make_object()
                       .set("reservation", resv.id.to_hex())
                       .set("node", node_id.to_hex())
                       .set("slots", static_cast<int64_t>(slots)));
    telemetry_task("attempt.dispatch", task_id,
                   Json::make_object()
                       .set("attempt", a.id.to_hex())
                       .set("node", node_id.to_hex())
                       .set("executor", decision.chosen_device));
    persist();
  }
}

// ---------------------------------------------------------------------------
// Attempt handling
// ---------------------------------------------------------------------------
void Coordinator::handle_dispatch_ack(const CoreCommand& cmd) {
  // Dispatch ack from node: if rejected, retry.
  auto& da = cmd.da;
  if (da.accepted) return;
  // Find the task for this attempt.
  for (const auto& [tid, list] : attempts_) {
    for (const auto& a : list) {
      if (a.id == da.attempt_id) {
        release_task_reservation(tid);
        graph_.set_state(tid, TaskState::Ready);
        telemetry_task("attempt.dispatch_failed", tid,
                       Json::make_object().set("reason", da.error));
        persist();
        return;
      }
    }
  }
}

void Coordinator::handle_attempt_started(const CoreCommand& cmd) {
  const auto& st = cmd.st;
  ExecutionAttempt* active = find_active_attempt(st.attempt_id);
  if (!active) return;
  if (active->coordinator_epoch != durable_.epoch) return;
  active->state = AttemptState::Started;
  active->started_at_ms = st.started_at_ms;
  active->device = st.device;
  graph_.set_state(active->task_id, TaskState::Running);
  telemetry_task("attempt.start", active->task_id,
                 Json::make_object()
                     .set("attempt", st.attempt_id.to_hex())
                     .set("node", active->node_id.to_hex())
                     .set("executor", active->device));
  persist();
}

void Coordinator::handle_attempt_complete(const CoreCommand& cmd) {
  const auto& co = cmd.co;
  // Idempotency: already committed -> accept.
  ExecutionAttempt* active = find_active_attempt(co.attempt_id);
  if (active && active->committed) {
    proto::AttemptAck ack;
    ack.attempt_id = co.attempt_id;
    ack.accepted = true;
    proto::BinaryWriter w;
    proto::serialize(w, ack);
    if (cmd.node_conn) {
      auto f = proto::make_frame(proto::kAttemptCompleteAck,
                                 proto::SenderKind::Coordinator, Id128::zero(),
                                 cmd.request_id, w.bytes());
      queue_outbound(cmd.node_conn, std::move(f));
    }
    return;
  }
  if (!active) {
    // Unknown/stale attempt. Reject.
    proto::AttemptAck ack;
    ack.attempt_id = co.attempt_id;
    ack.accepted = false;
    ack.error = "stale_attempt";
    proto::BinaryWriter w;
    proto::serialize(w, ack);
    if (cmd.node_conn) {
      auto f = proto::make_frame(proto::kAttemptCompleteAck,
                                 proto::SenderKind::Coordinator, Id128::zero(),
                                 cmd.request_id, w.bytes());
      queue_outbound(cmd.node_conn, std::move(f));
    }
    telemetry_.event("attempt.stale_rejected",
                     Json::make_object().set("attempt", co.attempt_id.to_hex()),
                     durable_.epoch);
    return;
  }

  // Validate epoch, reservation, task lifecycle.
  bool valid = true;
  std::string reason;
  if (active->coordinator_epoch != durable_.epoch) {
    valid = false;
    reason = "stale coordinator epoch";
  } else if (active->state != AttemptState::Started &&
             active->state != AttemptState::Dispatched) {
    valid = false;
    reason = "attempt not active";
  } else if (active->reservation_id != co.reservation_id) {
    valid = false;
    reason = "reservation token mismatch";
  } else if (graph_.state(active->task_id) != TaskState::Running &&
             graph_.state(active->task_id) != TaskState::Dispatching) {
    valid = false;
    reason = "task lifecycle invalid for completion";
  }

  if (cancel_requested_[active->task_id]) {
    valid = false;
    reason = "task cancelled";
  }

  proto::AttemptAck ack;
  ack.attempt_id = co.attempt_id;
  if (!valid) {
    ack.accepted = false;
    ack.error = reason;
    if (reason == "task cancelled") {
      // Treat as cancelled/preempted.
      active->state = AttemptState::Cancelled;
      active->failure_kind = FailureKind::Cancelled;
      active->failure_reason = "cancelled";
      active->completed_at_ms = now_millis();
      graph_.set_state(active->task_id, TaskState::Preempted);
      release_task_reservation(active->task_id);
      telemetry_task("task.cancel", active->task_id, Json::make_object());
      persist();
    } else {
      telemetry_.event("attempt.stale_rejected",
                       Json::make_object().set("attempt", co.attempt_id.to_hex())
                                     .set("reason", reason),
                       durable_.epoch);
    }
    proto::BinaryWriter w;
    proto::serialize(w, ack);
    if (cmd.node_conn) {
      auto f = proto::make_frame(proto::kAttemptCompleteAck,
                                 proto::SenderKind::Coordinator, Id128::zero(),
                                 cmd.request_id, w.bytes());
      queue_outbound(cmd.node_conn, std::move(f));
    }
    return;
  }

  // Commit.
  active->state = AttemptState::Succeeded;
  active->committed = true;
  active->completed_at_ms = now_millis();
  active->output_size = co.output_size;
  active->output_integrity = co.output_integrity;
  active->cpu_millis = co.duration_ms;
  active->bytes_transferred = co.bytes_transferred;
  active->device = co.device;

  graph_.set_state(active->task_id, TaskState::Succeeded);
  release_task_reservation(active->task_id);
  cancel_requested_.erase(active->task_id);

  // Record history.
  const ComputeTask* task = graph_.find(active->task_id);
  if (task) {
    scheduler_.record_history({task->task_class, active->executor, active->node_id},
                              co.duration_ms / 1000.0, true, co.bytes_transferred);
  }

  ack.accepted = true;
  proto::BinaryWriter w;
  proto::serialize(w, ack);
  if (cmd.node_conn) {
    auto f = proto::make_frame(proto::kAttemptCompleteAck,
                               proto::SenderKind::Coordinator, Id128::zero(),
                               cmd.request_id, w.bytes());
    queue_outbound(cmd.node_conn, std::move(f));
  }

  telemetry_task("attempt.complete", active->task_id,
                 Json::make_object()
                     .set("attempt", co.attempt_id.to_hex())
                     .set("node", active->node_id.to_hex())
                     .set("executor", active->device)
                     .set("duration_ms", co.duration_ms)
                     .set("bytes", static_cast<int64_t>(co.bytes_transferred))
                     .set("integrity", co.output_integrity));
  telemetry_task("task.success", active->task_id, Json::make_object());

  on_task_succeeded_recursive(active->task_id);
  persist();
}

void Coordinator::handle_attempt_failed(const CoreCommand& cmd) {
  const auto& fa = cmd.fa;
  ExecutionAttempt* active = find_active_attempt(fa.attempt_id);
  if (!active) {
    proto::AttemptAck ack;
    ack.attempt_id = fa.attempt_id;
    ack.accepted = false;
    ack.error = "stale_attempt";
    proto::BinaryWriter w;
    proto::serialize(w, ack);
    if (cmd.node_conn) {
      auto f = proto::make_frame(proto::kAttemptFailedAck,
                                 proto::SenderKind::Coordinator, Id128::zero(),
                                 cmd.request_id, w.bytes());
      queue_outbound(cmd.node_conn, std::move(f));
    }
    return;
  }
  if (active->coordinator_epoch != durable_.epoch) return;
  if (active->state == AttemptState::Succeeded || active->committed) return;

  active->state = AttemptState::Failed;
  active->failure_kind = static_cast<FailureKind>(fa.failure_kind);
  active->failure_reason = fa.failure_reason;
  active->completed_at_ms = now_millis();

  const ComputeTask* task = graph_.find(active->task_id);
  TaskState current = graph_.state(active->task_id);
  bool can_retry = task && task->retry.max_attempts > 1 &&
                   (active->attempt_number + 1) < task->retry.max_attempts &&
                   (active->failure_kind != FailureKind::NodeLost ||
                    task->retry.retry_on_node_loss) &&
                   (active->failure_kind != FailureKind::ExecutorFailed ||
                    task->retry.retry_on_executor_failure);

  release_task_reservation(active->task_id);
  if (current != TaskState::Succeeded) {
    if (can_retry) {
      graph_.set_state(active->task_id, TaskState::RetryPending);
      retry_ready_at_ms_[active->task_id] =
          now_millis() + static_cast<int64_t>(task->retry.retry_backoff_ms);
      telemetry_task("task.retry", active->task_id,
                     Json::make_object()
                         .set("attempt", fa.attempt_id.to_hex())
                         .set("reason", fa.failure_reason));
    } else {
      graph_.set_state(active->task_id, TaskState::Failed);
      telemetry_task("task.fail", active->task_id,
                     Json::make_object()
                         .set("attempt", fa.attempt_id.to_hex())
                         .set("reason", fa.failure_reason)
                         .set("failure_kind", failure_kind_name(active->failure_kind)));
    }
  }

  proto::AttemptAck ack;
  ack.attempt_id = fa.attempt_id;
  ack.accepted = true;
  proto::BinaryWriter w;
  proto::serialize(w, ack);
  if (cmd.node_conn) {
    auto f = proto::make_frame(proto::kAttemptFailedAck,
                               proto::SenderKind::Coordinator, Id128::zero(),
                               cmd.request_id, w.bytes());
    queue_outbound(cmd.node_conn, std::move(f));
  }

  telemetry_task("attempt.fail", active->task_id,
                 Json::make_object()
                     .set("attempt", fa.attempt_id.to_hex())
                     .set("node", active->node_id.to_hex())
                     .set("reason", fa.failure_reason));
  persist();
}

void Coordinator::handle_attempt_cancelled(const CoreCommand& cmd) {
  const auto& ca = cmd.ca;
  ExecutionAttempt* active = find_active_attempt(ca.attempt_id);
  if (!active) return;
  if (active->coordinator_epoch != durable_.epoch) return;
  active->state = AttemptState::Cancelled;
  active->failure_kind = FailureKind::Cancelled;
  active->failure_reason = ca.reason;
  active->completed_at_ms = now_millis();
  release_task_reservation(active->task_id);
  graph_.set_state(active->task_id, TaskState::Preempted);
  telemetry_task("task.cancel", active->task_id, Json::make_object());
  persist();
}

void Coordinator::handle_reservation_release(const CoreCommand& cmd) {
  const auto& rel = cmd.rel;
  auto r = reservations_.find(rel.reservation_id);
  if (r && r->coordinator_epoch == durable_.epoch) {
    reservations_.release(rel.reservation_id);
  }
  proto::ReservationReleaseAck ack;
  ack.reservation_id = rel.reservation_id;
  ack.accepted = true;
  proto::BinaryWriter w;
  proto::serialize(w, ack);
  if (cmd.node_conn) {
    auto f = proto::make_frame(proto::kReservationReleaseAck,
                               proto::SenderKind::Coordinator, Id128::zero(),
                               cmd.request_id, w.bytes());
    queue_outbound(cmd.node_conn, std::move(f));
  }
}

// ---------------------------------------------------------------------------
// Client handlers
// ---------------------------------------------------------------------------
void Coordinator::handle_task_submit(const CoreCommand& cmd) {
  const auto& submit = cmd.submit;
  proto::TaskSubmitResponse resp;
  resp.task_id = submit.task.id;

  // Idempotency: same task id already present.
  if (graph_.has_task(submit.task.id)) {
    resp.accepted = true;
    proto::BinaryWriter w;
    proto::serialize(w, resp);
    respond_client(cmd, proto::kTaskSubmitResponse, w.bytes());
    return;
  }

  if (stopping_.load()) {
    resp.accepted = false;
    resp.error = "coordinator shutting down";
  } else if (submit.task.id.is_zero()) {
    resp.accepted = false;
    resp.error = "task id must be nonzero";
  } else {
    // Dependency validation.
    bool missing = false;
    for (const auto& d : submit.task.dependencies) {
      if (!graph_.has_task(d)) {
        missing = true;
        resp.error = "missing dependency: " + d.to_hex();
        break;
      }
    }
    if (!missing) {
      auto r = graph_.add_task(submit.task);
      if (r.failed()) {
        resp.accepted = false;
        resp.error = r.error().message();
      } else {
        // Workload record.
        if (!durable_.workloads.count(submit.workload_id)) {
          Workload wl;
          wl.id = submit.workload_id;
          wl.name = submit.workload_name;
          durable_.workloads[wl.id] = wl;
        }
        // State locality registration.
        for (const auto& dep : submit.task.state_dependencies) {
          StateLocation loc;
          loc.state_id = dep.state_id;
          loc.generation = dep.generation;
          loc.size_bytes = dep.size_bytes;
          loc.resident_nodes = dep.resident_nodes;
          loc.locality_quality = dep.locality_quality;
          loc.transfer_estimate_seconds = dep.transfer_estimate_seconds;
          loc.recompute_estimate_seconds = dep.recompute_estimate_seconds;
          state_locality_.upsert(loc);
        }

        TaskState initial = submit.task.dependencies.empty()
                                ? TaskState::Ready
                                : TaskState::Blocked;
        graph_.set_state(submit.task.id, initial);
        ready_at_ms_[submit.task.id] = now_millis();
        resp.accepted = true;
        telemetry_task("task.submit", submit.task.id,
                       Json::make_object()
                           .set("workload", submit.workload_id.to_hex())
                           .set("state", task_state_name(initial)));
        if (initial == TaskState::Ready) {
          telemetry_task("task.ready", submit.task.id, Json::make_object());
        }
        persist();
      }
    }
  }

  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kTaskSubmitResponse, w.bytes());
}

void Coordinator::handle_task_inspect(const CoreCommand& cmd) {
  const auto& inspect = cmd.inspect;
  proto::TaskInspectResponse resp;
  resp.task_id = inspect.task_id;
  const ComputeTask* task = graph_.find(inspect.task_id);
  if (!task) {
    resp.found = false;
    resp.error = "task not found";
  } else {
    resp.found = true;
    resp.name = task->name;
    resp.task_class = task->task_class;
    resp.state = graph_.state(inspect.task_id);
    resp.attempts = attempts_[inspect.task_id];
    resp.retry_count = static_cast<uint32_t>(attempts_[inspect.task_id].size());
    resp.workload_id_hex = task->workload.to_hex();
  }
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kTaskInspectResponse, w.bytes());
}

void Coordinator::handle_task_list(const CoreCommand& cmd) {
  const auto& list = cmd.list;
  proto::TaskListResponse resp;
  TaskState filter = TaskState::Submitted;
  bool has_filter = false;
  if (!list.filter_state.empty()) {
    TaskState st;
    if (parse_task_state(list.filter_state, st)) {
      filter = st;
      has_filter = true;
    }
  }
  for (const auto& id : graph_.task_ids()) {
    const ComputeTask* task = graph_.find(id);
    if (!task) continue;
    TaskState st = graph_.state(id);
    if (has_filter && st != filter) continue;
    proto::TaskListEntry e;
    e.task_id = id;
    e.name = task->name;
    e.state = st;
    e.task_class = task->task_class;
    e.priority = task->priority;
    resp.tasks.push_back(e);
  }
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kTaskListResponse, w.bytes());
}

void Coordinator::handle_task_cancel(const CoreCommand& cmd) {
  const auto& cancel = cmd.cancel;
  proto::TaskCancelResponse resp;
  const ComputeTask* task = graph_.find(cancel.task_id);
  if (!task) {
    resp.found = false;
    resp.error = "task not found";
  } else {
    resp.found = true;
    TaskState st = graph_.state(cancel.task_id);
    if (is_terminal(st)) {
      resp.cancelled = false;
      resp.error = "task already " + std::string(task_state_name(st));
    } else if (st == TaskState::Running) {
      // Cooperative cancellation.
      cancel_requested_[cancel.task_id] = true;
      resp.cancelled = true;
      resp.error = "cancel requested; will take effect cooperatively";
      // Best-effort cancel to the node.
      ExecutionAttempt* active = nullptr;
      for (auto& a : attempts_[cancel.task_id]) {
        if (a.state == AttemptState::Started || a.state == AttemptState::Dispatched) {
          active = &a;
          break;
        }
      }
      if (active) {
        proto::AttemptCancel ac;
        ac.attempt_id = active->id;
        ac.reason = "task cancelled by client";
        proto::BinaryWriter w;
        proto::serialize(w, ac);
        send_to_node(active->node_id, proto::kAttemptCancel, w.bytes());
      }
    } else {
      auto r = graph_.set_state(cancel.task_id, TaskState::Cancelled);
      if (r.failed()) {
        resp.cancelled = false;
        resp.error = r.error().message();
      } else {
        resp.cancelled = true;
        release_task_reservation(cancel.task_id);
        cancel_requested_.erase(cancel.task_id);
        telemetry_task("task.cancel", cancel.task_id, Json::make_object());
        persist();
      }
    }
  }
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kTaskCancelResponse, w.bytes());
}

void Coordinator::handle_workload_inspect(const CoreCommand& cmd) {
  const auto& wl = cmd.wl;
  proto::WorkloadInspectResponse resp;
  resp.workload_id = wl.workload_id;
  auto it = durable_.workloads.find(wl.workload_id);
  if (it == durable_.workloads.end()) {
    resp.found = false;
    resp.error = "workload not found";
  } else {
    resp.found = true;
    resp.name = it->second.name;
    for (const auto& id : graph_.task_ids()) {
      const ComputeTask* task = graph_.find(id);
      if (!task || task->workload != wl.workload_id) continue;
      proto::TaskListEntry e;
      e.task_id = id;
      e.name = task->name;
      e.state = graph_.state(id);
      e.task_class = task->task_class;
      e.priority = task->priority;
      resp.tasks.push_back(e);
    }
  }
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kWorkloadInspectResponse, w.bytes());
}

void Coordinator::handle_fabric_status(const CoreCommand& cmd) {
  proto::FabricStatusResponse resp;
  resp.coordinator_epoch = durable_.epoch;
  resp.policy = scheduler_policy_name(config_.policy);
  resp.node_count = 0;
  for (const auto& [id, rec] : nodes_) {
    (void)id;
    if (rec.connected) ++resp.node_count;
  }
  resp.task_count = static_cast<uint32_t>(graph_.size());
  resp.running_count = 0;
  resp.reserved_count = static_cast<uint32_t>(reservations_.active_count());
  resp.completed_count = 0;
  for (const auto& id : graph_.task_ids()) {
    TaskState st = graph_.state(id);
    if (is_terminal(st)) resp.completed_count++;
    if (st == TaskState::Running) resp.running_count++;
  }
  for (const auto& [id, rec] : nodes_) {
    proto::NodeStatusEntry e;
    e.node_id = id;
    e.label = rec.capacity.label;
    e.address = rec.capacity.address;
    e.health = rec.health;
    e.connected = rec.connected;
    e.cpu_slots = static_cast<uint32_t>(rec.capacity.cpu_worker_slots);
    e.cpu_used = reservations_.used_slots(id, ExecutorType::Cpu);
    e.session_epoch = rec.session_epoch;
    for (const auto& cap : rec.capacity.executors) {
      e.executors.push_back(cap.name + ":" + std::to_string(cap.slots));
    }
    resp.nodes.push_back(e);
  }
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kFabricStatusResponse, w.bytes());
}

void Coordinator::handle_placement_query(const CoreCommand& cmd) {
  const auto& pq = cmd.pq;
  proto::PlacementResponse resp;
  resp.found = true;
  resp.decision = scheduler_.place(pq.task, nodes_, reservations_, state_locality_);
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kPlacementResponse, w.bytes());
}

void Coordinator::handle_node_list(const CoreCommand& cmd) {
  proto::NodeListResponse resp;
  for (const auto& [id, rec] : nodes_) {
    proto::NodeStatusEntry e;
    e.node_id = id;
    e.label = rec.capacity.label;
    e.address = rec.capacity.address;
    e.health = rec.health;
    e.connected = rec.connected;
    e.cpu_slots = static_cast<uint32_t>(rec.capacity.cpu_worker_slots);
    e.cpu_used = reservations_.used_slots(id, ExecutorType::Cpu);
    e.session_epoch = rec.session_epoch;
    for (const auto& cap : rec.capacity.executors) {
      e.executors.push_back(cap.name + ":" + std::to_string(cap.slots));
    }
    resp.nodes.push_back(e);
  }
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kNodeListResponse, w.bytes());
}

void Coordinator::handle_node_drain(const CoreCommand& cmd) {
  const auto& drain = cmd.drain;
  proto::NodeDrainResponse resp;
  auto it = nodes_.find(drain.node_id);
  if (it == nodes_.end()) {
    resp.found = false;
    resp.error = "node not found";
  } else {
    resp.found = true;
    if (it->second.health == NodeHealth::Offline) {
      resp.accepted = false;
      resp.error = "node offline";
    } else {
      it->second.health = drain.drain ? NodeHealth::Draining : NodeHealth::Online;
      resp.accepted = true;
      telemetry_node("node.drain", drain.node_id,
                     Json::make_object().set("drain", drain.drain));
    }
  }
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kNodeDrainResponse, w.bytes());
}

void Coordinator::handle_coordinator_shutdown(const CoreCommand& cmd) {
  proto::CoordinatorShutdownResponse resp;
  resp.accepted = true;
  proto::BinaryWriter w;
  proto::serialize(w, resp);
  respond_client(cmd, proto::kCoordinatorShutdownResponse, w.bytes());

  // Gracefully ask connected nodes to exit.
  std::vector<std::shared_ptr<NodeConn>> shutdown_nodes;
  for (const auto& [id, conn] : node_conns_) {
    auto rec = nodes_.find(id);
    if (conn && !conn->stop.load() && rec != nodes_.end() &&
        rec->second.connected) {
      proto::NodeShutdown ns;
      ns.node_id = id;
      ns.session_id = conn->session;
      proto::BinaryWriter nw;
      proto::serialize(nw, ns);
      send_to_node(id, proto::kNodeShutdown, nw.bytes());
      shutdown_nodes.push_back(conn);
    }
  }

  // Writers are independent of the core thread. Give the small control-plane
  // frames one shared, bounded opportunity to leave before stop closes the
  // transports. The in-flight flag prevents an empty queue from being
  // mistaken for a completed send.
  const int64_t drain_deadline = now_millis() + kShutdownDrainTimeoutMs;
  wait_for_outbound_drain(cmd.client_conn, drain_deadline);
  for (const auto& conn : shutdown_nodes) {
    if (!wait_for_outbound_drain(conn, drain_deadline)) break;
  }

  stopping_.store(true);
  persist();
  // Ask the core loop to exit.
  {
    std::lock_guard<std::mutex> lk(core_mu_);
    CoreCommand shutdown;
    shutdown.kind = CoreCommand::Kind::Shutdown;
    core_queue_.push_front(std::move(shutdown));
  }
  core_cv_.notify_all();
}

// ---------------------------------------------------------------------------
// Sweep
// ---------------------------------------------------------------------------
void Coordinator::sweep() {
  int64_t now = now_millis();

  // Heartbeat timeouts -> node loss.
  std::vector<NodeId> lost;
  for (const auto& [id, rec] : nodes_) {
    if (rec.connected && (now - rec.last_heartbeat_ms) > config_.heartbeat_timeout_ms) {
      lost.push_back(id);
    }
    if (!rec.connected && (now - rec.last_heartbeat_ms) > config_.heartbeat_timeout_ms &&
        rec.health != NodeHealth::Offline) {
      lost.push_back(id);
    }
  }
  for (const auto& id : lost) {
    handle_node_loss(id);
  }

  // Dispatch timeout -> requeue.
  std::vector<ComputeTaskId> stale_dispatch;
  for (const auto& [tid, at] : dispatch_at_ms_) {
    if (graph_.state(tid) == TaskState::Dispatching &&
        (now - at) > kDispatchTimeoutMs) {
      stale_dispatch.push_back(tid);
    }
  }
  for (const auto& tid : stale_dispatch) {
    release_task_reservation(tid);
    graph_.set_state(tid, TaskState::Ready);
    ready_at_ms_[tid] = now;
    telemetry_task("attempt.dispatch_failed", tid,
                   Json::make_object().set("reason", "dispatch timeout"));
    persist();
  }

  // Retry backoff -> Ready.
  for (const auto& [tid, t] : retry_ready_at_ms_) {
    if (graph_.state(tid) == TaskState::RetryPending && t <= now) {
      graph_.set_state(tid, TaskState::Ready);
      ready_at_ms_[tid] = now;
      telemetry_task("recovery.task_requeued", tid, Json::make_object());
      persist();
    }
  }

  // Deadlines.
  for (const auto& id : graph_.task_ids()) {
    const ComputeTask* t = graph_.find(id);
    if (!t || !t->has_deadline) continue;
    if (graph_.state(id) == TaskState::Ready ||
        graph_.state(id) == TaskState::Blocked ||
        graph_.state(id) == TaskState::RetryPending) {
      if (now >= t->deadline_epoch_ms) {
        release_task_reservation(id);
        graph_.set_state(id, TaskState::Expired);
        telemetry_task("task.expire", id, Json::make_object());
        persist();
      }
    }
  }

  // Schedule ready tasks.
  schedule_ready_tasks();
}

void Coordinator::handle_node_loss(const NodeId& node_id) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return;
  if (it->second.health == NodeHealth::Offline) return;
  it->second.health = NodeHealth::Offline;
  it->second.connected = false;
  if (it->second.id != Id128::zero()) {
    auto conn = node_conns_.find(node_id);
    if (conn != node_conns_.end()) {
      stop_connection(conn->second);
    }
  }
  reservations_.release_all_for_node(node_id);
  telemetry_node("node.offline", node_id, Json::make_object());

  // Mark running attempts Lost and requeue.
  std::vector<ComputeTaskId> affected;
  for (const auto& [tid, list] : attempts_) {
    for (const auto& a : list) {
      if (a.node_id == node_id && !a.committed &&
          (a.state == AttemptState::Started || a.state == AttemptState::Dispatched)) {
        affected.push_back(tid);
      }
    }
  }
  for (const auto& tid : affected) {
    const ComputeTask* task = graph_.find(tid);
    if (!task) continue;
    bool can_retry = task->retry.retry_on_node_loss &&
                     task->retry.max_attempts > 1;
    graph_.set_state(tid, TaskState::Lost);
    if (can_retry) {
      graph_.set_state(tid, TaskState::RetryPending);
      retry_ready_at_ms_[tid] =
          now_millis() + static_cast<int64_t>(task->retry.retry_backoff_ms);
      telemetry_task("recovery.task_requeued", tid,
                     Json::make_object().set("reason", "node lost"));
    } else {
      graph_.set_state(tid, TaskState::Failed);
      telemetry_task("task.fail", tid,
                     Json::make_object().set("reason", "node lost"));
    }
    release_task_reservation(tid);
  }
  persist();
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
ExecutionAttempt* Coordinator::find_active_attempt(const ExecutionAttemptId& id) {
  for (auto& [tid, list] : attempts_) {
    (void)tid;
    for (auto& a : list) {
      if (a.id == id) return &a;
    }
  }
  return nullptr;
}

void Coordinator::release_task_reservation(const ComputeTaskId& task_id) {
  auto it = task_reservation_.find(task_id);
  if (it != task_reservation_.end()) {
    reservations_.release(it->second);
    attempt_by_reservation_.erase(it->second);
    task_reservation_.erase(it);
  }
}

void Coordinator::on_task_succeeded_recursive(const ComputeTaskId& id) {
  for (const auto& dep : graph_.dependents(id)) {
    if (graph_.state(dep) == TaskState::Blocked &&
        graph_.dependencies_satisfied(dep)) {
      graph_.set_state(dep, TaskState::Ready);
      ready_at_ms_[dep] = now_millis();
      telemetry_task("task.ready", dep, Json::make_object());
    }
    if (graph_.dependency_poisoned(dep) &&
        (graph_.state(dep) == TaskState::Blocked ||
         graph_.state(dep) == TaskState::Submitted)) {
      graph_.set_state(dep, TaskState::Lost);
      graph_.set_state(dep, TaskState::Failed);
      telemetry_task("task.fail", dep,
                     Json::make_object().set("reason", "dependency failed"));
      on_task_succeeded_recursive(dep);
    }
  }
}

std::map<ComputeTaskId, ComputeTask> Coordinator::graph_to_tasks() const {
  std::map<ComputeTaskId, ComputeTask> out;
  for (const auto& id : graph_.task_ids()) {
    const ComputeTask* t = graph_.find(id);
    if (t) out[id] = *t;
  }
  return out;
}

std::map<ComputeTaskId, TaskState> Coordinator::graph_states() const {
  std::map<ComputeTaskId, TaskState> out;
  for (const auto& id : graph_.task_ids()) {
    out[id] = graph_.state(id);
  }
  return out;
}

}  // namespace cf
