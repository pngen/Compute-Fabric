#pragma once

#include <atomic>
#include <cstddef>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/executor/executor.h"
#include "compute_fabric/net/channel.h"
#include "compute_fabric/net/socket.h"
#include "compute_fabric/persist/store.h"
#include "compute_fabric/protocol/messages.h"
#include "compute_fabric/resource/capacity.h"
#include "compute_fabric/resource/reservation.h"
#include "compute_fabric/scheduler/scheduler.h"
#include "compute_fabric/state/context_adapter.h"
#include "compute_fabric/state/state_locality.h"
#include "compute_fabric/task/dag.h"
#include "compute_fabric/telemetry/telemetry.h"

namespace cf {

struct CoordinatorConfig {
  std::string node_host = "127.0.0.1";
  uint16_t node_port = 0;    // 0 = auto-assign
  std::string client_host = "127.0.0.1";
  uint16_t client_port = 0;  // 0 = auto-assign
  std::string store_dir;
  SchedulerPolicy policy = SchedulerPolicy::CostAware;
  int64_t heartbeat_timeout_ms = 3000;
  int64_t dispatch_timeout_ms = 5000;
  std::string telemetry_path;
  bool telemetry_human = false;
  int64_t command_timeout_ms = 5000;  // socket I/O timeout
};

// A live node control connection.
struct NodeConn {
  NodeId node_id;
  NodeSessionId session;
  std::shared_ptr<net::Channel> channel;
  std::thread reader;
  std::thread writer;
  std::mutex wmu;
  std::condition_variable wcv;
  std::deque<proto::Frame> outbound;
  size_t outbound_bytes = 0;       // queued wire bytes, guarded by wmu
  bool writer_in_flight = false;  // guarded by wmu
  std::atomic<bool> stop{false};
  std::atomic<bool> retirement_queued{false};
};

// A live client control connection.
struct ClientConn {
  Id128 id;
  std::shared_ptr<net::Channel> channel;
  std::thread reader;
  std::thread writer;
  std::mutex wmu;
  std::condition_variable wcv;
  std::deque<proto::Frame> outbound;
  size_t outbound_bytes = 0;       // queued wire bytes, guarded by wmu
  bool writer_in_flight = false;  // guarded by wmu
  std::atomic<bool> stop{false};
  std::atomic<bool> retirement_queued{false};
};

// Command queue entry for the coordinator core thread. Owns all fields; the
// active `kind` selects which payload is populated.
struct CoreCommand {
  enum class Kind : uint8_t {
    NodeRegister,
    NodeHeartbeat,
    NodeShutdownMsg,
    NodeDisconnected,
    DispatchAck,
    AttemptStarted,
    AttemptProgress,
    AttemptComplete,
    AttemptFailed,
    AttemptCancelled,
    ReservationRelease,
    ClientConnected,
    ClientTaskSubmit,
    ClientTaskInspect,
    ClientTaskList,
    ClientTaskCancel,
    ClientWorkloadInspect,
    ClientFabricStatus,
    ClientPlacementQuery,
    ClientNodeList,
    ClientNodeDrain,
    ClientCoordinatorShutdown,
    ClientDisconnected,
    Sweep,
    Shutdown,
  };

  Kind kind = Kind::Sweep;
  std::shared_ptr<NodeConn> node_conn;
  std::shared_ptr<ClientConn> client_conn;
  uint64_t request_id = 0;
  NodeId node_id;
  NodeId client_id;

  proto::NodeRegister reg;
  proto::NodeHeartbeat hb;
  proto::DispatchAck da;
  proto::AttemptStarted st;
  proto::AttemptProgress pr;
  proto::AttemptComplete co;
  proto::AttemptFailed fa;
  proto::AttemptCancelled ca;
  proto::ReservationRelease rel;
  proto::TaskSubmit submit;
  proto::TaskInspect inspect;
  proto::TaskList list;
  proto::TaskCancel cancel;
  proto::WorkloadInspect wl;
  proto::FabricStatus fs;
  proto::PlacementQuery pq;
  proto::NodeDrain drain;
};

// Bounded idempotency cache keyed by (sender, request_id). Duplicate mutating
// requests return the cached response instead of being re-applied.
class IdempotencyCache {
 public:
  void put(const std::string& key, uint16_t message_type,
           std::vector<uint8_t> payload);
  bool get(const std::string& key, uint16_t& message_type,
           std::vector<uint8_t>& payload) const;

 private:
  struct Entry {
    uint16_t message_type;
    std::vector<uint8_t> payload;
  };
  std::map<std::string, Entry> entries_;
  std::deque<std::string> order_;
  static constexpr size_t kMaxEntries = 512;
};

struct CoordinatorOptions {
  std::shared_ptr<ContextStateProvider> context_provider;  // optional adapter
};

// Distributed coordinator: durable task graph, deterministic scheduler,
// bounded reservations, epoch-bound attempt authority, node registry,
// idempotent control plane, telemetry, and deterministic shutdown.
class Coordinator {
 public:
  explicit Coordinator(CoordinatorConfig config);
  ~Coordinator();

  Coordinator(const Coordinator&) = delete;
  Coordinator& operator=(const Coordinator&) = delete;

  // Binds sockets and starts threads. Returns the bound ports.
  Result<std::pair<uint16_t, uint16_t>> start();

  // Blocks until shutdown is requested (CoordinatorShutdown or stop()).
  void run();

  // Requests shutdown; joins all threads. Safe to call repeatedly.
  void stop();

  CoordinatorEpoch epoch() const { return durable_.epoch; }

  // Durable state for tests to inspect.
  const DurableState& durable() const { return durable_; }

 private:
  // ---- thread entry points ----
  void accept_node_loop();
  void accept_client_loop();
  void core_loop();
  void node_reader(std::shared_ptr<NodeConn> conn);
  void node_writer(std::shared_ptr<NodeConn> conn);
  void client_reader(std::shared_ptr<ClientConn> conn);
  void client_writer(std::shared_ptr<ClientConn> conn);
  void connection_reaper_loop();
  void retire_node_connection(const std::shared_ptr<NodeConn>& conn);
  void retire_client_connection(const std::shared_ptr<ClientConn>& conn);

  // ---- core processing ----
  void process(const CoreCommand& cmd);
  void sweep();
  void handle_node_register(const CoreCommand& cmd);
  void handle_node_heartbeat(const CoreCommand& cmd);
  void handle_node_disconnect(const NodeId& node_id,
                              const std::shared_ptr<NodeConn>& conn);
  void handle_attempt_started(const CoreCommand& cmd);
  void handle_attempt_complete(const CoreCommand& cmd);
  void handle_attempt_failed(const CoreCommand& cmd);
  void handle_attempt_cancelled(const CoreCommand& cmd);
  void handle_reservation_release(const CoreCommand& cmd);
  void handle_dispatch_ack(const CoreCommand& cmd);
  void handle_task_submit(const CoreCommand& cmd);
  void handle_task_inspect(const CoreCommand& cmd);
  void handle_task_list(const CoreCommand& cmd);
  void handle_task_cancel(const CoreCommand& cmd);
  void handle_workload_inspect(const CoreCommand& cmd);
  void handle_fabric_status(const CoreCommand& cmd);
  void handle_placement_query(const CoreCommand& cmd);
  void handle_node_list(const CoreCommand& cmd);
  void handle_node_drain(const CoreCommand& cmd);
  void handle_coordinator_shutdown(const CoreCommand& cmd);

  ExecutionAttempt* find_active_attempt(const ExecutionAttemptId& id);
  void release_task_reservation(const ComputeTaskId& task_id);
  void on_task_succeeded_recursive(const ComputeTaskId& id);
  std::map<ComputeTaskId, ComputeTask> graph_to_tasks() const;
  std::map<ComputeTaskId, TaskState> graph_states() const;
  void schedule_ready_tasks();
  void handle_node_loss(const NodeId& node_id);
  void persist();
  void respond_client(const CoreCommand& cmd, uint16_t message_type,
                      const std::vector<uint8_t>& payload);
  void respond_node(const CoreCommand& cmd, uint16_t message_type,
                    const std::vector<uint8_t>& payload);
  void send_to_node(const NodeId& node_id, uint16_t message_type,
                    const std::vector<uint8_t>& payload);
  std::string idem_key(const Id128& sender, uint64_t request_id) const;
  void telemetry_node(const std::string& event, const NodeId& node,
                      const Json& fields);
  void telemetry_task(const std::string& event, const ComputeTaskId& task,
                      const Json& fields);

  // ---- state ----
  CoordinatorConfig config_;
  DurableState durable_;
  std::unique_ptr<Store> store_;
  TaskGraph graph_;
  ReservationRegistry reservations_;
  StateLocalityRegistry state_locality_;
  SchedulerPolicyEngine scheduler_;
  std::map<NodeId, NodeRecord> nodes_;
  std::map<ComputeTaskId, std::vector<ExecutionAttempt>> attempts_;  // all
  std::map<ComputeTaskId, int64_t> retry_ready_at_ms_;
  std::map<ComputeTaskId, int64_t> ready_at_ms_;
  std::map<ComputeTaskId, int64_t> dispatch_at_ms_;
  std::map<ComputeTaskId, bool> cancel_requested_;
  std::map<ComputeTaskId, PlacementDecision> last_placements_;
  std::map<ComputeTaskId, StoredPlacement> placement_records_;
  std::map<ComputeTaskId, ReservationId> task_reservation_;
  std::map<ReservationId, ExecutionAttempt> attempt_by_reservation_;

  std::map<NodeId, std::shared_ptr<NodeConn>> node_conns_;
  std::map<Id128, std::shared_ptr<ClientConn>> client_conns_;
  IdempotencyCache idem_;

  // Every accepted connection (registered or not) is tracked here so shutdown
  // can join its reader/writer threads and never destroy a std::thread while
  // joinable.
  std::mutex conn_list_mu_;
  std::vector<std::shared_ptr<NodeConn>> all_node_conns_;
  std::vector<std::shared_ptr<ClientConn>> all_client_conns_;

  // Readers only enqueue retirement. A dedicated reaper owns all joins and
  // removes fully joined connections from the all-connections lists, so no
  // reader/core thread can ever join itself or block the core loop on a join.
  std::mutex retired_mu_;
  std::condition_variable retired_cv_;
  std::deque<std::shared_ptr<NodeConn>> retired_node_conns_;
  std::deque<std::shared_ptr<ClientConn>> retired_client_conns_;
  bool reaper_stopping_ = false;
  std::thread connection_reaper_thread_;

  net::TcpListener node_listener_;
  net::TcpListener client_listener_;
  std::thread accept_node_thread_;
  std::thread accept_client_thread_;
  std::thread core_thread_;

  std::mutex core_mu_;
  std::condition_variable core_cv_;
  std::deque<CoreCommand> core_queue_;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> core_running_{false};
  std::mutex stop_mu_;
  bool stop_done_ = false;

  Telemetry telemetry_;
  uint64_t placement_sequence_ = 0;
  std::shared_ptr<ContextStateProvider> context_provider_;

  friend struct CoreCommand;
};

}  // namespace cf
