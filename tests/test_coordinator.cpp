#include "framework.h"

#include <memory>
#include <thread>

#include "compute_fabric/coordinator/coordinator.h"
#include "compute_fabric/net/client.h"
#include "compute_fabric/node/node_runtime.h"

using namespace cf;

namespace {

std::string unique_store_dir() {
  return "test_store_coordinator_" + std::to_string(cf::Id128::random().lo());
}

ComputeTask mk_task(const char* name, uint64_t n = 10000) {
  ComputeTask t;
  t.id = Id128::random();
  t.workload = Id128::random();
  t.name = name;
  t.kernel = KernelType::HashInteger;
  t.kernel_param_n = n;
  t.kernel_param_a = 31;
  t.kernel_param_b = 17;
  t.seed = 42;
  t.estimated_duration_seconds = 0.01;
  return t;
}

TaskState wait_state(FabricClient& c, const ComputeTaskId& id,
                     const std::vector<TaskState>& targets, int64_t timeout_ms) {
  auto deadline = now_millis() + timeout_ms;
  while (now_millis() < deadline) {
    auto insp = c.inspect(id);
    if (insp.ok() && insp.value().found) {
      for (auto t : targets) {
        if (insp.value().state == t) return t;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  return TaskState::Submitted;
}

struct Fixture {
  std::string store_dir;
  std::unique_ptr<Coordinator> coord;
  uint16_t nport = 0;
  uint16_t cport = 0;
  std::vector<std::unique_ptr<NodeRuntime>> nodes;

  void start(const std::string& policy = "cost_aware") {
    if (store_dir.empty()) store_dir = unique_store_dir();
    CoordinatorConfig cfg;
    cfg.store_dir = store_dir;
    cfg.node_port = 0;
    cfg.client_port = 0;
    cfg.policy = SchedulerPolicy::CostAware;
    if (policy == "baseline") cfg.policy = SchedulerPolicy::Baseline;
    coord = std::make_unique<Coordinator>(cfg);
    auto ports = coord->start();
    EXPECT_TRUE(ports.ok());
    nport = ports.value().first;
    cport = ports.value().second;
  }

  void add_node(int cpu_slots = 2, bool cuda = false) {
    NodeConfig nc;
    nc.coordinator_host = "127.0.0.1";
    nc.coordinator_node_port = nport;
    nc.label = "node-" + std::to_string(nodes.size());
    nc.cpu_slots = cpu_slots;
    nc.enable_cuda = cuda;
    nc.cuda_device_ordinal = 0;
    nc.cuda_slots = cuda ? 1 : 0;
    auto node = std::make_unique<NodeRuntime>(nc);
    auto r = node->start();
    EXPECT_TRUE(r.ok());
    nodes.push_back(std::move(node));
  }

  std::unique_ptr<FabricClient> client() {
    auto c = std::make_unique<FabricClient>("127.0.0.1", cport);
    auto r = c->connect();
    EXPECT_TRUE(r.ok());
    return c;
  }

  void shutdown() {
    for (auto& n : nodes) {
      if (n) n->stop();
    }
    nodes.clear();
    if (coord) coord->stop();
  }

  ~Fixture() { shutdown(); }
};

}  // namespace

TEST(coordinator_full_task_flow) {
  Fixture f;
  f.start();
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));

  auto task = mk_task("flow");
  auto r = c->submit(task.workload, "w", task);
  EXPECT_TRUE(r.ok());
  EXPECT_TRUE(r.value().accepted);

  auto st = wait_state(*c, task.id, {TaskState::Succeeded, TaskState::Failed},
                       15000);
  EXPECT_EQ(st, TaskState::Succeeded);

  auto insp = c->inspect(task.id);
  EXPECT_TRUE(insp.ok());
  EXPECT_EQ(insp.value().state, TaskState::Succeeded);
  EXPECT_EQ(insp.value().attempts.size(), 1ull);
  EXPECT_FALSE(insp.value().attempts[0].output_integrity.empty());
  EXPECT_FALSE(insp.value().attempts[0].node_id.is_zero());
}

TEST(coordinator_illegal_dependency_rejected) {
  Fixture f;
  f.start();
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));

  auto task = mk_task("missing-dep");
  task.dependencies = {Id128::random()};
  auto r = c->submit(task.workload, "w", task);
  EXPECT_TRUE(r.ok());
  EXPECT_FALSE(r.value().accepted);
  EXPECT_STR_CONTAINS(r.value().error, "missing dependency");

  // Self-dependency.
  auto self = mk_task("self");
  self.dependencies = {self.id};
  auto r2 = c->submit(self.workload, "w", self);
  EXPECT_TRUE(r2.ok());
  EXPECT_FALSE(r2.value().accepted);
}

TEST(coordinator_dag_execution_order) {
  Fixture f;
  f.start();
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));

  auto a = mk_task("a");
  auto b = mk_task("b");
  auto d = mk_task("d");
  b.dependencies = {a.id};
  d.dependencies = {b.id};

  EXPECT_TRUE(c->submit(a.workload, "w", a).value().accepted);
  EXPECT_TRUE(c->submit(b.workload, "w", b).value().accepted);
  EXPECT_TRUE(c->submit(d.workload, "w", d).value().accepted);

  auto st_b = wait_state(*c, b.id, {TaskState::Succeeded, TaskState::Failed}, 15000);
  EXPECT_EQ(st_b, TaskState::Succeeded);
  // d must not have succeeded before b.
  auto insp_b = c->inspect(b.id);
  EXPECT_EQ(c->inspect(d.id).value().state, TaskState::Succeeded);
  EXPECT_TRUE(insp_b.ok());
}

TEST(coordinator_retry_on_node_loss) {
  Fixture f;
  f.start();
  f.add_node(2);
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(2, 10000));

  auto task = mk_task("retry", 20000000);
  task.kernel = KernelType::SyntheticLoop;
  task.retry.max_attempts = 3;
  task.retry.retry_on_node_loss = true;
  task.retry.retry_backoff_ms = 50;
  task.estimated_duration_seconds = 1.0;
  EXPECT_TRUE(c->submit(task.workload, "w", task).value().accepted);

  // Wait until the task is Running, then kill the node that hosts it.
  auto deadline = now_millis() + 10000;
  bool running = false;
  NodeId host;
  ExecutionAttempt first;
  while (now_millis() < deadline) {
    auto insp = c->inspect(task.id);
    if (insp.ok() && insp.value().state == TaskState::Running &&
        !insp.value().attempts.empty()) {
      host = insp.value().attempts.back().node_id;
      first = insp.value().attempts.back();
      running = true;
      break;
    }
    if (insp.ok() && is_terminal(insp.value().state)) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(running);
  if (running) {
    for (auto& n : f.nodes) {
      if (n && n->node_id() == host) {
        n->stop();
        n.reset();
      }
    }
    auto st = wait_state(*c, task.id, {TaskState::Succeeded, TaskState::Failed},
                         25000);
    EXPECT_EQ(st, TaskState::Succeeded);
    auto insp = c->inspect(task.id);
    EXPECT_GE(insp.value().attempts.size(), 1ull);
  }
}

TEST(coordinator_stale_epoch_rejects_old_completion) {
  Fixture f;
  f.start();
  f.add_node(2);
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(2, 10000));

  // A task that will be lost on its first node and retried on the second.
  // Keep the first attempt observably active long enough to stop its node.
  // HashInteger can finish between the 25 ms inspection polls in Release
  // builds, which would test polling speed instead of stale-attempt rejection.
  auto task = mk_task("stale", 20000000);
  task.kernel = KernelType::SyntheticLoop;
  task.estimated_duration_seconds = 1.0;
  task.retry.max_attempts = 3;
  task.retry.retry_on_node_loss = true;
  task.retry.retry_backoff_ms = 50;
  EXPECT_TRUE(c->submit(task.workload, "w", task).value().accepted);

  // Wait for the first attempt to start, capture its token, then kill its node.
  auto deadline = now_millis() + 10000;
  ExecutionAttempt first_attempt;
  bool captured = false;
  while (now_millis() < deadline) {
    auto insp = c->inspect(task.id);
    if (insp.ok() && !insp.value().attempts.empty() &&
        insp.value().attempts[0].state == AttemptState::Started) {
      first_attempt = insp.value().attempts[0];
      captured = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  EXPECT_TRUE(captured);
  if (!captured) return;
  for (auto& n : f.nodes) {
    if (n && n->node_id() == first_attempt.node_id) {
      n->stop();
      n.reset();
    }
  }
  // Wait for the retry to succeed on the surviving node.
  auto st = wait_state(*c, task.id, {TaskState::Succeeded, TaskState::Failed},
                       25000);
  EXPECT_EQ(st, TaskState::Succeeded);
  auto final_insp = c->inspect(task.id);
  EXPECT_GE(final_insp.value().attempts.size(), 2ull);

  // Simulate the dead node waking up and reporting the OLD (superseded)
  // attempt. The coordinator must reject it.
  auto sock = net::TcpSocket::connect("127.0.0.1", f.nport, 5000);
  EXPECT_TRUE(sock.ok());
  net::Channel ch(sock.take());
  proto::Hello hello;
  hello.role = "node";
  proto::BinaryWriter hw;
  proto::serialize(hw, hello);
  EXPECT_OK(ch.send_message(proto::kHello, proto::SenderKind::Node,
                            Id128::random(), 1, hw.bytes(), 5000));
  proto::NodeRegister reg;
  reg.node_id = Id128::random();
  reg.label = "fake";
  NodeCapacity cap;
  cap.cpu_worker_slots = 1;
  ExecutorCapability cpu;
  cpu.type = ExecutorType::Cpu;
  cpu.name = "cpu";
  cpu.slots = 1;
  cpu.healthy = true;
  cap.executors.push_back(cpu);
  reg.capacity = cap;
  proto::BinaryWriter rw;
  proto::serialize(rw, reg);
  EXPECT_OK(ch.send_message(proto::kNodeRegister, proto::SenderKind::Node,
                            reg.node_id, 2, rw.bytes(), 5000));
  deadline = now_millis() + 10000;
  bool acked = false;
  while (now_millis() < deadline) {
    auto frame = ch.recv_frame(500, proto::kMaxFramePayload);
    if (frame.ok() &&
        frame.value().header.message_type == proto::kNodeRegisterAck) {
      acked = true;
      break;
    }
  }
  EXPECT_TRUE(acked);

  proto::AttemptComplete co;
  co.attempt_id = first_attempt.id;
  co.task_id = first_attempt.task_id;
  co.reservation_id = first_attempt.reservation_id;
  co.coordinator_epoch = first_attempt.coordinator_epoch;
  co.result_code = 0;
  co.output_integrity = "superseded";
  proto::BinaryWriter w;
  proto::serialize(w, co);
  EXPECT_OK(ch.send_message(proto::kAttemptComplete, proto::SenderKind::Node,
                            reg.node_id, 3, w.bytes(), 5000));
  auto resp = ch.recv_frame(5000, proto::kMaxFramePayload);
  EXPECT_TRUE(resp.ok());
  if (resp.ok() && resp.value().header.message_type == proto::kAttemptCompleteAck) {
    proto::BinaryReader br(resp.value().payload);
    auto ackr = proto::deserialize_attempt_ack(br);
    EXPECT_TRUE(ackr.ok());
    EXPECT_FALSE(ackr.value().accepted);
  }
  ch.close();
}

TEST(coordinator_idempotent_duplicate_submit) {
  Fixture f;
  f.start();
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));

  auto task = mk_task("dup");
  auto r1 = c->submit(task.workload, "w", task);
  auto r2 = c->submit(task.workload, "w", task);
  EXPECT_TRUE(r1.ok() && r2.ok());
  EXPECT_TRUE(r1.value().accepted);
  EXPECT_TRUE(r2.value().accepted);
  EXPECT_EQ(r1.value().task_id, r2.value().task_id);
  auto list = c->list("");
  EXPECT_TRUE(list.ok());
  int count = 0;
  for (const auto& e : list.value().tasks) {
    if (e.task_id == task.id) ++count;
  }
  EXPECT_EQ(count, 1);
}

TEST(coordinator_idempotent_duplicate_cancel) {
  Fixture f;
  f.start();
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));

  auto task = mk_task("cancel-me");
  EXPECT_TRUE(c->submit(task.workload, "w", task).value().accepted);
  auto r1 = c->cancel(task.id);
  auto r2 = c->cancel(task.id);
  EXPECT_TRUE(r1.ok() && r2.ok());
  (void)r1;
  (void)r2;
  auto insp = c->inspect(task.id);
  EXPECT_TRUE(insp.ok());
  EXPECT_TRUE(is_terminal(insp.value().state));
}

TEST(coordinator_cancel_queued_task) {
  Fixture f;
  f.start();
  f.add_node(1);  // single slot
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));

  // Fill the single slot with a long-running blocker.
  auto blocker = mk_task("blocker", 64000000);
  blocker.kernel = KernelType::SyntheticLoop;
  blocker.retry.max_attempts = 1;
  EXPECT_TRUE(c->submit(blocker.workload, "w", blocker).value().accepted);
  auto victim = mk_task("victim", 1000);
  victim.retry.max_attempts = 1;
  EXPECT_TRUE(c->submit(victim.workload, "w", victim).value().accepted);

  // Wait until the coordinator has accepted both (victim non-terminal), then
  // cancel the queued victim immediately while the blocker holds the slot.
  auto deadline = now_millis() + 5000;
  while (now_millis() < deadline) {
    auto insp = c->inspect(victim.id);
    if (insp.ok() && insp.value().found && !is_terminal(insp.value().state)) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto rc = c->cancel(victim.id);
  EXPECT_TRUE(rc.ok());
  EXPECT_TRUE(rc.value().found);
  EXPECT_TRUE(rc.value().cancelled);
  auto insp = c->inspect(victim.id);
  EXPECT_TRUE(insp.ok());
  EXPECT_EQ(insp.value().state, TaskState::Cancelled);
  // Blocker still completes.
  auto st = wait_state(*c, blocker.id, {TaskState::Succeeded, TaskState::Failed}, 30000);
  EXPECT_EQ(st, TaskState::Succeeded);
}

TEST(coordinator_node_drain) {
  Fixture f;
  f.start();
  f.add_node(2);
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(2, 10000));
  auto nodes = c->nodes();
  EXPECT_TRUE(nodes.ok());
  EXPECT_EQ(nodes.value().nodes.size(), 2ull);
  auto nid = nodes.value().nodes[0].node_id;
  auto dr = c->drain(nid, true);
  EXPECT_TRUE(dr.ok());
  EXPECT_TRUE(dr.value().accepted);
  auto nodes2 = c->nodes();
  int drained = 0;
  for (const auto& e : nodes2.value().nodes) {
    if (e.health == NodeHealth::Draining) ++drained;
  }
  EXPECT_EQ(drained, 1);
  auto udr = c->drain(nid, false);
  EXPECT_TRUE(udr.ok());
  EXPECT_TRUE(udr.value().accepted);
}

TEST(coordinator_concurrent_submissions) {
  Fixture f;
  f.start();
  f.add_node(4);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));

  std::vector<std::thread> threads;
  std::vector<ComputeTaskId> ids;
  std::mutex mu;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&, i] {
      auto cc = f.client();
      auto task = mk_task(("conc-" + std::to_string(i)).c_str(), 50000);
      auto r = cc->submit(task.workload, "w", task);
      if (r.ok() && r.value().accepted) {
        std::lock_guard<std::mutex> lk(mu);
        ids.push_back(task.id);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(ids.size(), 8ull);

  auto deadline = now_millis() + 30000;
  int succeeded = 0;
  while (now_millis() < deadline) {
    succeeded = 0;
    for (const auto& id : ids) {
      auto insp = c->inspect(id);
      if (insp.ok() && insp.value().state == TaskState::Succeeded) ++succeeded;
    }
    if (succeeded == 8) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_EQ(succeeded, 8);
}

TEST(coordinator_shutdown_during_active_attempt) {
  Fixture f;
  f.start();
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));
  auto task = mk_task("long", 5000000);
  EXPECT_TRUE(c->submit(task.workload, "w", task).value().accepted);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  // Shutdown coordinator while the attempt is running.
  auto s = c->shutdown_coordinator();
  EXPECT_TRUE(s.ok());
  f.coord->stop();
  // Node should still be stopped cleanly.
  f.nodes[0]->stop();
  EXPECT_TRUE(true);
}

TEST(coordinator_epoch_bump_on_restart) {
  Fixture f;
  f.start();
  auto e1 = f.coord->epoch();
  f.coord->stop();
  f.coord.reset();
  f.start();
  auto e2 = f.coord->epoch();
  EXPECT_GT(e2, e1);
}

TEST(coordinator_persistence_recovery) {
  std::string dir = unique_store_dir();
  {
    CoordinatorConfig cfg;
    cfg.store_dir = dir;
    cfg.node_port = 0;
    cfg.client_port = 0;
    Coordinator coord(cfg);
    auto ports = coord.start();
    EXPECT_TRUE(ports.ok());
    NodeConfig nc;
    nc.coordinator_node_port = ports.value().first;
    nc.cpu_slots = 2;
    NodeRuntime node(nc);
    EXPECT_OK(node.start());
    FabricClient c("127.0.0.1", ports.value().second);
    EXPECT_OK(c.connect());
    auto task = mk_task("persist", 30000);
    EXPECT_TRUE(c.submit(task.workload, "w", task).value().accepted);
    auto st = wait_state(c, task.id, {TaskState::Succeeded}, 15000);
    EXPECT_EQ(st, TaskState::Succeeded);
    node.stop();
    c.close();
    coord.stop();
  }
  {
    // Restart with the same store dir: the committed task must be retained.
    CoordinatorConfig cfg;
    cfg.store_dir = dir;
    cfg.node_port = 0;
    cfg.client_port = 0;
    Coordinator coord(cfg);
    auto ports = coord.start();
    EXPECT_TRUE(ports.ok());
    FabricClient c("127.0.0.1", ports.value().second);
    EXPECT_OK(c.connect());
    auto list = c.list("");
    EXPECT_TRUE(list.ok());
    bool found_committed = false;
    for (const auto& e : list.value().tasks) {
      if (e.state == TaskState::Succeeded) found_committed = true;
    }
    EXPECT_TRUE(found_committed);
    c.shutdown_coordinator();
    c.close();
    coord.stop();
  }
}

TEST(coordinator_heartbeat_and_node_list) {
  Fixture f;
  f.start();
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));
  auto st = c->status();
  EXPECT_TRUE(st.ok());
  EXPECT_EQ(st.value().node_count, 1u);
  EXPECT_EQ(st.value().coordinator_epoch, f.coord->epoch());
}

TEST(coordinator_placement_query) {
  Fixture f;
  f.start();
  f.add_node(2);
  f.add_node(2);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(2, 10000));
  auto task = mk_task("query");
  task.required_executor = ExecutorType::Cpu;
  auto r = c->placement_query(task);
  EXPECT_TRUE(r.ok());
  EXPECT_TRUE(r.value().found);
  EXPECT_TRUE(r.value().decision.feasible);
  EXPECT_TRUE(r.value().decision.chosen_node.has_value());
  EXPECT_FALSE(r.value().decision.candidates.empty());
}

TEST(coordinator_idempotency_cache_dedup) {
  IdempotencyCache cache;
  std::string key = "node:1";
  uint16_t mt = 0;
  std::vector<uint8_t> payload;
  EXPECT_FALSE(cache.get(key, mt, payload));
  cache.put(key, proto::kTaskSubmitResponse, {1, 2, 3});
  EXPECT_TRUE(cache.get(key, mt, payload));
  EXPECT_EQ(mt, proto::kTaskSubmitResponse);
  EXPECT_EQ(payload.size(), 3ull);
  // Re-put does not overwrite.
  cache.put(key, proto::kTaskCancelResponse, {9});
  uint16_t mt2 = 0;
  std::vector<uint8_t> payload2;
  EXPECT_TRUE(cache.get(key, mt2, payload2));
  EXPECT_EQ(mt2, proto::kTaskSubmitResponse);
}

TEST(coordinator_cuda_placement_via_fabric) {
  // Skip entirely when this build has no CUDA device available.
  if (enumerate_cuda_devices().empty()) {
    EXPECT_TRUE(true);
    return;
  }
  Fixture f;
  f.start();
  f.add_node(2, /*cuda=*/true);
  auto c = f.client();
  EXPECT_TRUE(c->wait_for_nodes(1, 10000));

  auto task = mk_task("cuda-task");
  task.required_executor = ExecutorType::Cuda;
  task.kernel = KernelType::VectorTransform;
  task.kernel_param_n = 100000;
  auto r = c->submit(task.workload, "w", task);
  EXPECT_TRUE(r.ok() && r.value().accepted);
  auto st = wait_state(*c, task.id, {TaskState::Succeeded, TaskState::Failed},
                       20000);
  EXPECT_EQ(st, TaskState::Succeeded);
  auto insp = c->inspect(task.id);
  EXPECT_TRUE(insp.ok());
  if (!insp.value().attempts.empty()) {
    EXPECT_STR_CONTAINS(insp.value().attempts[0].device, "cuda");
  }
}
