#include "framework.h"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>

#if defined(_WIN32)
#include <direct.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "compute_fabric/persist/store.h"

using namespace cf;

namespace {
std::string temp_dir(const char* tag) {
  std::string base = "test_store_";
  base += tag;
  base += "_";
  base += std::to_string(cf::Id128::random().lo());
#if defined(_WIN32)
  _mkdir(base.c_str());
#else
  mkdir(base.c_str(), 0755);
#endif
  return base;
}

void rmrf(const std::string& dir) {
#if defined(_WIN32)
  std::string cmd = "rmdir /S /Q \"" + dir + "\" 2>nul";
  system(cmd.c_str());
#else
  std::string cmd = "rm -rf " + dir;
  system(cmd.c_str());
#endif
}

DurableState make_state() {
  DurableState s;
  s.epoch = 3;
  Workload wl;
  wl.id = Id128::random();
  wl.name = "w1";
  s.workloads[wl.id] = wl;
  ComputeTask t;
  t.id = Id128::random();
  t.workload = wl.id;
  t.name = "t1";
  t.kernel = KernelType::HashInteger;
  t.kernel_param_n = 100;
  s.tasks[t.id] = t;
  s.task_states[t.id] = TaskState::Succeeded;
  ExecutionAttempt a;
  a.id = Id128::random();
  a.task_id = t.id;
  a.node_id = Id128::random();
  a.node_session = Id128::random();
  a.coordinator_epoch = 3;
  a.reservation_id = Id128::random();
  a.state = AttemptState::Succeeded;
  a.committed = true;
  a.output_integrity = "deadbeef";
  s.attempts[t.id].push_back(a);
  s.retry_counts[t.id] = 1;
  return s;
}
}  // namespace

TEST(store_round_trip) {
  std::string dir = temp_dir("roundtrip");
  auto store = Store::open(dir, true);
  EXPECT_TRUE(store.ok());
  auto st = make_state();
  EXPECT_OK(store.value()->save(st));
  auto loaded = store.value()->load();
  EXPECT_TRUE(loaded.ok());
  auto& s = loaded.value();
  EXPECT_EQ(s.epoch, st.epoch);
  EXPECT_EQ(s.workloads.size(), st.workloads.size());
  EXPECT_EQ(s.tasks.size(), st.tasks.size());
  EXPECT_EQ(s.task_states.size(), st.task_states.size());
  EXPECT_EQ(s.attempts.size(), st.attempts.size());
  EXPECT_EQ(s.attempts.begin()->second[0].output_integrity, std::string("deadbeef"));
  EXPECT_EQ(s.retry_counts.size(), st.retry_counts.size());
  rmrf(dir);
}

TEST(store_atomic_rewrite) {
  std::string dir = temp_dir("atomic");
  auto store = Store::open(dir, true);
  EXPECT_TRUE(store.ok());
  auto st = make_state();
  EXPECT_OK(store.value()->save(st));
  st.epoch = 4;
  EXPECT_OK(store.value()->save(st));
  auto loaded = store.value()->load();
  EXPECT_TRUE(loaded.ok());
  EXPECT_EQ(loaded.value().epoch, 4ull);
  // No temp file left behind.
  std::ifstream tmp(dir + "/coordinator.store.tmp");
  EXPECT_FALSE(tmp.good());
  rmrf(dir);
}

TEST(store_corruption_detected) {
  std::string dir = temp_dir("corrupt");
  auto store = Store::open(dir, true);
  EXPECT_TRUE(store.ok());
  auto st = make_state();
  EXPECT_OK(store.value()->save(st));
  // Corrupt the file body.
  std::string path = dir + "/coordinator.store";
  std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
  f.seekp(20, std::ios::beg);
  char c = static_cast<char>(0xAA);
  f.write(&c, 1);
  f.close();
  auto loaded = store.value()->load();
  EXPECT_TRUE(loaded.failed());
  EXPECT_EQ(loaded.error().code(), ErrorCode::CorruptionDetected);
  rmrf(dir);
}

TEST(store_truncation_detected) {
  std::string dir = temp_dir("truncate");
  auto store = Store::open(dir, true);
  EXPECT_TRUE(store.ok());
  auto st = make_state();
  EXPECT_OK(store.value()->save(st));
  std::string path = dir + "/coordinator.store";
  std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
  f.seekp(0, std::ios::end);
  auto size = f.tellg();
  f.seekp(static_cast<std::streamoff>(size) - 10, std::ios::beg);
  f.close();
  // Truncate via truncate() on POSIX, or rewrite a shorter file on Windows.
#if defined(_WIN32)
  std::ifstream src(path, std::ios::binary);
  std::vector<char> data((std::istreambuf_iterator<char>(src)),
                         std::istreambuf_iterator<char>());
  src.close();
  data.resize(data.size() - 10);
  std::ofstream dst(path, std::ios::binary | std::ios::trunc);
  dst.write(data.data(), static_cast<std::streamsize>(data.size()));
  dst.close();
#else
  truncate(path.c_str(), static_cast<off_t>(size - 10));
#endif
  auto loaded = store.value()->load();
  EXPECT_TRUE(loaded.failed());
  EXPECT_EQ(loaded.error().code(), ErrorCode::CorruptionDetected);
  rmrf(dir);
}

TEST(store_restart_epoch_load) {
  std::string dir = temp_dir("epoch");
  auto store = Store::open(dir, true);
  EXPECT_TRUE(store.ok());
  auto st = make_state();
  st.epoch = 9;
  EXPECT_OK(store.value()->save(st));
  // Reopen (simulates coordinator restart).
  auto store2 = Store::open(dir, true);
  EXPECT_TRUE(store2.ok());
  auto loaded = store2.value()->load();
  EXPECT_TRUE(loaded.ok());
  EXPECT_EQ(loaded.value().epoch, 9ull);
  rmrf(dir);
}

TEST(store_missing_dir_created) {
  std::string base = temp_dir("nested");
  std::string dir = base + "/sub";
#if defined(_WIN32)
  _mkdir(dir.c_str());
#else
  mkdir(dir.c_str(), 0755);
#endif
  auto store = Store::open(dir, true);
  EXPECT_TRUE(store.ok());
  auto loaded = store.value()->load();
  EXPECT_TRUE(loaded.ok());
  rmrf(base);
}