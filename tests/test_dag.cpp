#include "framework.h"

#include "compute_fabric/task/dag.h"

using namespace cf;

namespace {
ComputeTask mk_task(const ComputeTaskId& id) {
  ComputeTask t;
  t.id = id;
  t.workload = Id128::random();
  t.name = "t";
  t.kernel = KernelType::HashInteger;
  t.kernel_param_n = 100;
  return t;
}

void succeed(TaskGraph& g, const ComputeTaskId& id) {
  EXPECT_OK(g.set_state(id, TaskState::Ready));
  EXPECT_OK(g.set_state(id, TaskState::Planning));
  EXPECT_OK(g.set_state(id, TaskState::Reserved));
  EXPECT_OK(g.set_state(id, TaskState::Dispatching));
  EXPECT_OK(g.set_state(id, TaskState::Running));
  EXPECT_OK(g.set_state(id, TaskState::Succeeded));
}
}  // namespace

TEST(dag_success_dependencies) {
  TaskGraph g;
  auto a = mk_task(Id128::random());
  auto b = mk_task(Id128::random());
  auto c = mk_task(Id128::random());
  b.dependencies = {a.id};
  c.dependencies = {b.id};
  EXPECT_OK(g.add_task(a));
  EXPECT_OK(g.add_task(b));
  EXPECT_OK(g.add_task(c));
  EXPECT_FALSE(g.dependencies_satisfied(b.id));
  EXPECT_FALSE(g.dependencies_satisfied(c.id));

  succeed(g, a.id);
  EXPECT_TRUE(g.dependencies_satisfied(b.id));
  EXPECT_FALSE(g.dependencies_satisfied(c.id));

  succeed(g, b.id);
  EXPECT_TRUE(g.dependencies_satisfied(c.id));
}

TEST(dag_self_dependency_rejected) {
  TaskGraph g;
  auto a = mk_task(Id128::random());
  a.dependencies = {a.id};
  EXPECT_FAIL(g.add_task(a));
}

TEST(dag_missing_dependency_forward_ref_allowed) {
  TaskGraph g;
  auto a = mk_task(Id128::random());
  auto b = mk_task(Id128::random());
  // Forward reference: b is allowed to be added later.
  a.dependencies = {b.id};
  EXPECT_OK(g.add_task(a));
  EXPECT_FALSE(g.dependencies_satisfied(a.id));
  EXPECT_OK(g.add_task(b));
  EXPECT_FALSE(g.dependencies_satisfied(a.id));
  succeed(g, b.id);
  EXPECT_TRUE(g.dependencies_satisfied(a.id));
}

TEST(dag_cycle_rejected) {
  TaskGraph g;
  auto a = mk_task(Id128::random());
  auto b = mk_task(Id128::random());
  // a depends on b (forward ref), then b depends on a -> cycle.
  a.dependencies = {b.id};
  EXPECT_OK(g.add_task(a));
  b.dependencies = {a.id};
  EXPECT_FAIL(g.add_task(b));
}

TEST(dag_multi_node_topological_order) {
  TaskGraph g;
  auto a = mk_task(Id128::random());
  auto b = mk_task(Id128::random());
  auto c = mk_task(Id128::random());
  auto d = mk_task(Id128::random());
  b.dependencies = {a.id};
  c.dependencies = {a.id};
  d.dependencies = {b.id, c.id};
  EXPECT_OK(g.add_task(a));
  EXPECT_OK(g.add_task(b));
  EXPECT_OK(g.add_task(c));
  EXPECT_OK(g.add_task(d));
  auto order_r = g.topological_order();
  EXPECT_TRUE(order_r.ok());
  auto order = order_r.value();
  EXPECT_EQ(order.size(), 4ull);
  // a must come before b and c; b and c before d.
  auto posA = std::find(order.begin(), order.end(), a.id);
  auto posB = std::find(order.begin(), order.end(), b.id);
  auto posC = std::find(order.begin(), order.end(), c.id);
  auto posD = std::find(order.begin(), order.end(), d.id);
  EXPECT_TRUE(posA < posB);
  EXPECT_TRUE(posA < posC);
  EXPECT_TRUE(posB < posD);
  EXPECT_TRUE(posC < posD);
}

TEST(dag_dependency_poison) {
  TaskGraph g;
  auto a = mk_task(Id128::random());
  auto b = mk_task(Id128::random());
  b.dependencies = {a.id};
  EXPECT_OK(g.add_task(a));
  EXPECT_OK(g.add_task(b));
  // Legal path to Failed for a.
  EXPECT_OK(g.set_state(a.id, TaskState::Ready));
  EXPECT_OK(g.set_state(a.id, TaskState::Planning));
  EXPECT_OK(g.set_state(a.id, TaskState::Reserved));
  EXPECT_OK(g.set_state(a.id, TaskState::Dispatching));
  EXPECT_OK(g.set_state(a.id, TaskState::Running));
  EXPECT_OK(g.set_state(a.id, TaskState::Failed));
  EXPECT_TRUE(g.dependency_poisoned(b.id));
}

TEST(dag_dependents_tracking) {
  TaskGraph g;
  auto a = mk_task(Id128::random());
  auto b = mk_task(Id128::random());
  b.dependencies = {a.id};
  EXPECT_OK(g.add_task(a));
  EXPECT_OK(g.add_task(b));
  auto deps = g.dependents(a.id);
  EXPECT_EQ(deps.size(), 1ull);
  EXPECT_EQ(deps[0], b.id);
}