#include "framework.h"

#include "compute_fabric/task/lifecycle.h"

using namespace cf;

#define TRANS(f, t) EXPECT_EQ(static_cast<int>(f.transition(t).value_or(TaskState::Submitted)), static_cast<int>(t))

TEST(lifecycle_normal_flow) {
  TaskLifecycle lc;
  EXPECT_EQ(lc.state(), TaskState::Submitted);
  EXPECT_OK(lc.transition(TaskState::Ready));
  EXPECT_OK(lc.transition(TaskState::Planning));
  EXPECT_OK(lc.transition(TaskState::Reserved));
  EXPECT_OK(lc.transition(TaskState::Dispatching));
  EXPECT_OK(lc.transition(TaskState::Running));
  EXPECT_OK(lc.transition(TaskState::Succeeded));
  EXPECT_TRUE(lc.is_terminal());
}

TEST(lifecycle_blocked_ready) {
  TaskLifecycle lc;
  EXPECT_OK(lc.transition(TaskState::Blocked));
  EXPECT_OK(lc.transition(TaskState::Ready));
  EXPECT_EQ(lc.state(), TaskState::Ready);
}

TEST(lifecycle_retry_flow) {
  TaskLifecycle lc;
  EXPECT_OK(lc.transition(TaskState::Ready));
  EXPECT_OK(lc.transition(TaskState::Planning));
  EXPECT_OK(lc.transition(TaskState::Reserved));
  EXPECT_OK(lc.transition(TaskState::Dispatching));
  EXPECT_OK(lc.transition(TaskState::Running));
  EXPECT_OK(lc.transition(TaskState::Failed));
  EXPECT_OK(lc.transition(TaskState::RetryPending));
  EXPECT_OK(lc.transition(TaskState::Ready));
}

TEST(lifecycle_illegal_transitions) {
  TaskLifecycle lc;
  // Submitted -> Running is illegal.
  EXPECT_FALSE(TaskLifecycle::is_legal(TaskState::Submitted, TaskState::Running));
  // Succeeded is terminal.
  EXPECT_FALSE(TaskLifecycle::is_legal(TaskState::Succeeded, TaskState::Running));
  // Running -> Submitted is illegal.
  EXPECT_FALSE(TaskLifecycle::is_legal(TaskState::Running, TaskState::Submitted));
  // Ready -> Running is illegal (must go through Planning/Reserved/Dispatching).
  EXPECT_FALSE(TaskLifecycle::is_legal(TaskState::Ready, TaskState::Running));
  // Cancelled is terminal.
  EXPECT_FALSE(TaskLifecycle::is_legal(TaskState::Cancelled, TaskState::Ready));
}

TEST(lifecycle_illegal_returns_typed_error) {
  TaskLifecycle lc;
  auto r = lc.transition(TaskState::Running);
  EXPECT_TRUE(r.failed());
  EXPECT_EQ(r.error().code(), ErrorCode::IllegalTransition);
}

TEST(lifecycle_cancel_paths) {
  TaskLifecycle lc;
  EXPECT_OK(lc.transition(TaskState::Blocked));
  EXPECT_OK(lc.transition(TaskState::Cancelled));
}

TEST(lifecycle_preempt_and_expire) {
  {
    TaskLifecycle lc;
    EXPECT_OK(lc.transition(TaskState::Ready));
    EXPECT_OK(lc.transition(TaskState::Planning));
    EXPECT_OK(lc.transition(TaskState::Reserved));
    EXPECT_OK(lc.transition(TaskState::Preempted));
    EXPECT_OK(lc.transition(TaskState::RetryPending));
  }
  {
    TaskLifecycle lc;
    EXPECT_OK(lc.transition(TaskState::Expired));
  }
}

TEST(lifecycle_state_names_roundtrip) {
  for (int i = 0; i <= static_cast<int>(TaskState::Expired); ++i) {
    TaskState s = static_cast<TaskState>(i);
    TaskState out;
    EXPECT_TRUE(parse_task_state(task_state_name(s), out));
    EXPECT_EQ(static_cast<int>(s), static_cast<int>(out));
  }
}

TEST(lifecycle_same_state_idempotent) {
  TaskLifecycle lc;
  EXPECT_OK(lc.transition(TaskState::Ready));
  auto r = lc.transition(TaskState::Ready);
  EXPECT_TRUE(r.ok());
}