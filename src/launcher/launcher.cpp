#include "compute_fabric/launcher/launcher.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <thread>

#include "compute_fabric/core/time_util.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

namespace cf {

namespace {

constexpr int64_t kCoordinatorReadyMs = 10000;
constexpr int64_t kGracefulShutdownMs = 3000;
constexpr int64_t kForcedShutdownMs = 3000;
constexpr int kConnectAttemptMs = 250;
constexpr int kRequestAttemptMs = 500;
constexpr int kPollIntervalMs = 25;

std::string describe_command(const std::string& exe_path,
                             const std::vector<std::string>& args) {
  std::string command = exe_path;
  for (const auto& arg : args) {
    command += " [";
    command += arg;
    command += "]";
  }
  return command;
}

int remaining_timeout(int64_t deadline_ms, int maximum_ms) {
  const int64_t remaining = deadline_ms - now_millis();
  if (remaining <= 0) return 0;
  return static_cast<int>(std::min<int64_t>(remaining, maximum_ms));
}

void sleep_for_poll(int64_t deadline_ms) {
  const int delay = remaining_timeout(deadline_ms, kPollIntervalMs);
  if (delay > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
  }
}

std::string exited_message(const std::string& name, const ChildProcess& child) {
  return name + " exited (pid " + std::to_string(child.sys_pid) +
         ", exit code " + std::to_string(child.exit_code) + ")";
}

#if defined(_WIN32)

Result<std::wstring> utf8_to_wide(const std::string& input) {
  if (input.find('\0') != std::string::npos) {
    return Error(ErrorCode::InvalidArgument,
                 "process path or argument contains an embedded NUL");
  }
  if (input.empty()) return std::wstring();

  const int count = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS,
                                        input.data(),
                                        static_cast<int>(input.size()), nullptr,
                                        0);
  if (count <= 0) {
    return Error(ErrorCode::InvalidArgument,
                 "process path or argument is not valid UTF-8 (Windows error " +
                     std::to_string(GetLastError()) + ")");
  }
  std::wstring output(static_cast<size_t>(count), L'\0');
  if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, input.data(),
                          static_cast<int>(input.size()), output.data(), count) !=
      count) {
    return Error(ErrorCode::InvalidArgument,
                 "cannot convert process path or argument to UTF-16 (Windows "
                 "error " +
                     std::to_string(GetLastError()) + ")");
  }
  return output;
}

std::string wide_to_utf8(const std::wstring& input) {
  if (input.empty()) return {};
  const int count = WideCharToMultiByte(
      CP_UTF8, WC_ERR_INVALID_CHARS, input.data(),
      static_cast<int>(input.size()), nullptr, 0, nullptr, nullptr);
  if (count <= 0) return {};
  std::string output(static_cast<size_t>(count), '\0');
  if (WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, input.data(),
                          static_cast<int>(input.size()), output.data(), count,
                          nullptr, nullptr) != count) {
    return {};
  }
  return output;
}

// Quotes one argument according to the parsing rules used by the Microsoft C
// runtime. In particular, backslashes preceding a quote or the closing quote
// are doubled so trailing directory separators survive intact.
std::wstring quote_windows_argument(const std::wstring& argument) {
  std::wstring quoted;
  quoted.push_back(L'"');
  size_t backslashes = 0;
  for (const wchar_t ch : argument) {
    if (ch == L'\\') {
      ++backslashes;
      continue;
    }
    if (ch == L'"') {
      quoted.append(backslashes * 2 + 1, L'\\');
      quoted.push_back(L'"');
      backslashes = 0;
      continue;
    }
    quoted.append(backslashes, L'\\');
    backslashes = 0;
    quoted.push_back(ch);
  }
  quoted.append(backslashes * 2, L'\\');
  quoted.push_back(L'"');
  return quoted;
}

#endif

}  // namespace

std::string current_executable_path() {
#if defined(_WIN32)
  std::wstring path(32768, L'\0');
  const DWORD length = GetModuleFileNameW(
      nullptr, path.data(), static_cast<DWORD>(path.size()));
  if (length == 0 || length >= path.size()) return "compute-fabric";
  path.resize(length);
  auto utf8 = wide_to_utf8(path);
  return utf8.empty() ? "compute-fabric" : utf8;
#else
  char buf[4096];
#if defined(__linux__)
  const ssize_t length = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
  if (length > 0) {
    buf[length] = '\0';
    return std::string(buf);
  }
#endif
  return "compute-fabric";
#endif
}

LocalFabric::~LocalFabric() {
  // Poll/reap even children already observed as exited. On Windows an exited
  // process still owns a kernel HANDLE until the reap path closes it.
  (void)shutdown_all();
}

uint16_t LocalFabric::pick_free_port() {
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 1);
  if (listener.failed()) return 0;
  const uint16_t port = listener.value().port();
  listener.value().close();
  return port;
}

Result<void> LocalFabric::spawn(const std::string& exe_path,
                                const std::vector<std::string>& args,
                                ChildProcess& out) {
  if (out.sys_pid != 0 || out.os_handle != 0) {
    return Error(ErrorCode::InvalidState,
                 "cannot replace an owned child process");
  }
  if (exe_path.find('\0') != std::string::npos) {
    return Error(ErrorCode::InvalidArgument,
                 "executable path contains an embedded NUL");
  }
  for (const auto& arg : args) {
    if (arg.find('\0') != std::string::npos) {
      return Error(ErrorCode::InvalidArgument,
                   "process argument contains an embedded NUL");
    }
  }

  out.command_line = describe_command(exe_path, args);
#if defined(_WIN32)
  auto executable = utf8_to_wide(exe_path);
  if (executable.failed()) return executable.error();

  std::wstring command_line = quote_windows_argument(executable.value());
  for (const auto& arg : args) {
    auto wide_arg = utf8_to_wide(arg);
    if (wide_arg.failed()) return wide_arg.error();
    command_line.push_back(L' ');
    command_line += quote_windows_argument(wide_arg.value());
  }

  STARTUPINFOW startup{};
  startup.cb = sizeof(startup);
  PROCESS_INFORMATION process{};
  const BOOL created = CreateProcessW(
      executable.value().c_str(), command_line.data(), nullptr, nullptr, FALSE,
      CREATE_NO_WINDOW, nullptr, nullptr, &startup, &process);
  if (!created) {
    const DWORD error = GetLastError();
    return Error(ErrorCode::LaunchError,
                 "CreateProcessW failed with Windows error " +
                     std::to_string(error));
  }

  CloseHandle(process.hThread);
  out.sys_pid = process.dwProcessId;
  out.os_handle = reinterpret_cast<uintptr_t>(process.hProcess);
#else
  std::vector<char*> argv;
  argv.reserve(args.size() + 2);
  argv.push_back(const_cast<char*>(exe_path.c_str()));
  for (const auto& arg : args) {
    argv.push_back(const_cast<char*>(arg.c_str()));
  }
  argv.push_back(nullptr);

  pid_t pid = 0;
  const int spawn_error =
      posix_spawn(&pid, exe_path.c_str(), nullptr, nullptr, argv.data(), environ);
  if (spawn_error != 0) {
    return Error(ErrorCode::LaunchError,
                 "posix_spawn failed: " +
                     std::string(std::strerror(spawn_error)));
  }
  out.sys_pid = static_cast<uint64_t>(pid);
#endif
  out.exit_code = -1;
  out.running = true;
  out.reaped = false;
  return {};
}

Result<LocalFabric> LocalFabric::launch(const std::string& exe_path,
                                        int node_count, bool cuda_node,
                                        const std::string& store_dir,
                                        const std::string& policy,
                                        const std::string& telemetry_dir) {
  if (node_count < 1) node_count = 1;

  LocalFabric fabric;
  fabric.node_port_ = fabric.pick_free_port();
  for (int attempt = 0; attempt < 8; ++attempt) {
    fabric.client_port_ = fabric.pick_free_port();
    if (fabric.client_port_ != 0 &&
        fabric.client_port_ != fabric.node_port_) {
      break;
    }
  }
  if (fabric.node_port_ == 0 || fabric.client_port_ == 0 ||
      fabric.node_port_ == fabric.client_port_) {
    return Error(ErrorCode::LaunchError, "cannot pick distinct free ports");
  }

  std::vector<std::string> coordinator_args = {
      "coordinator",
      "--node-port", std::to_string(fabric.node_port_),
      "--client-port", std::to_string(fabric.client_port_),
      "--store-dir", store_dir,
      "--policy", policy,
  };
  if (!telemetry_dir.empty()) {
    coordinator_args.push_back("--telemetry");
    coordinator_args.push_back(telemetry_dir + "/telemetry.ndjson");
  }
  auto spawned =
      fabric.spawn(exe_path, coordinator_args, fabric.coordinator_);
  if (spawned.failed()) return spawned.error();

  const int64_t ready_deadline = now_millis() + kCoordinatorReadyMs;
  Error readiness_error(ErrorCode::Timeout,
                        "coordinator did not answer a status request");
  bool coordinator_ready = false;
  while (now_millis() < ready_deadline) {
    auto running = fabric.process_running(fabric.coordinator_);
    if (running.failed()) {
      readiness_error = running.error();
      break;
    }
    if (!running.value()) {
      readiness_error =
          Error(ErrorCode::LaunchError,
                exited_message("coordinator", fabric.coordinator_));
      break;
    }

    FabricClient probe("127.0.0.1", fabric.client_port_);
    const int connect_ms =
        remaining_timeout(ready_deadline, kConnectAttemptMs);
    if (connect_ms <= 0) break;
    auto connected = probe.connect(connect_ms);
    if (connected.ok()) {
      const int status_ms =
          remaining_timeout(ready_deadline, kRequestAttemptMs);
      if (status_ms > 0) {
        auto status = probe.status(status_ms);
        if (status.ok() && status.value().error.empty()) {
          auto still_running = fabric.process_running(fabric.coordinator_);
          if (still_running.ok() && still_running.value()) {
            coordinator_ready = true;
            probe.close();
            break;
          }
          readiness_error =
              still_running.failed()
                  ? still_running.error()
                  : Error(ErrorCode::LaunchError,
                          exited_message("coordinator", fabric.coordinator_));
        } else {
          readiness_error =
              status.failed()
                  ? status.error()
                  : Error(ErrorCode::InvalidState, status.value().error);
        }
      }
    } else {
      readiness_error = connected.error();
    }
    probe.close();
    sleep_for_poll(ready_deadline);
  }

  if (!coordinator_ready) {
    auto final_state = fabric.process_running(fabric.coordinator_);
    std::string message;
    if (final_state.ok() && !final_state.value()) {
      message = exited_message("coordinator", fabric.coordinator_);
    } else {
      message = "coordinator did not become ready: " +
                readiness_error.to_string();
    }
    auto cleanup = fabric.shutdown_all();
    if (cleanup.failed()) {
      message += "; cleanup failed: " + cleanup.error().to_string();
    }
    return Error(ErrorCode::LaunchError, std::move(message));
  }

  for (int i = 0; i < node_count; ++i) {
    std::string label = "node-" + std::to_string(i);
    std::vector<std::string> node_args = {
        "node",
        "--coordinator-port", std::to_string(fabric.node_port_),
        "--label", label,
        "--cpu-slots", "2",
    };
    if (i == 0 && cuda_node) {
      node_args.push_back("--cuda");
      node_args.push_back("--cuda-slots");
      node_args.push_back("2");
    }

    ChildProcess node;
    auto node_spawned = fabric.spawn(exe_path, node_args, node);
    if (node_spawned.failed()) {
      std::string message = node_spawned.error().to_string();
      auto cleanup = fabric.shutdown_all();
      if (cleanup.failed()) {
        message += "; cleanup failed: " + cleanup.error().to_string();
      }
      return Error(ErrorCode::LaunchError, std::move(message));
    }
    fabric.nodes_.push_back(std::move(node));
    fabric.node_labels_.push_back(std::move(label));
  }
  return std::move(fabric);
}

Result<void> LocalFabric::wait_registered(int64_t timeout_ms) {
  if (timeout_ms <= 0) {
    return Error(ErrorCode::Timeout, "node registration deadline elapsed");
  }

  const int64_t deadline = now_millis() + timeout_ms;
  const size_t expected = node_labels_.size();
  size_t connected_expected = 0;
  Error last_error(ErrorCode::Timeout, "nodes did not register in time");
  std::unique_ptr<FabricClient> client;

  auto require_running = [this](ChildProcess& child,
                                const std::string& name) -> Result<void> {
    auto running = process_running(child);
    if (running.failed()) {
      return Error(ErrorCode::ProcessError,
                   name + " state query failed: " +
                       running.error().to_string());
    }
    if (!running.value()) {
      return Error(ErrorCode::LaunchError, exited_message(name, child));
    }
    return {};
  };

  while (now_millis() < deadline) {
    auto coordinator_ok = require_running(coordinator_, "coordinator");
    if (coordinator_ok.failed()) return coordinator_ok.error();
    for (size_t i = 0; i < nodes_.size(); ++i) {
      const std::string name =
          i < node_labels_.size() ? node_labels_[i]
                                  : "node-" + std::to_string(i);
      auto node_ok = require_running(nodes_[i], name);
      if (node_ok.failed()) return node_ok.error();
    }

    if (!client || !client->connected()) {
      client =
          std::make_unique<FabricClient>("127.0.0.1", client_port_);
      const int connect_ms = remaining_timeout(deadline, kConnectAttemptMs);
      if (connect_ms <= 0) break;
      auto connected = client->connect(connect_ms);
      if (connected.failed()) {
        last_error = connected.error();
        client->close();
        client.reset();
        sleep_for_poll(deadline);
        continue;
      }
    }

    const int request_ms = remaining_timeout(deadline, kRequestAttemptMs);
    if (request_ms <= 0) break;
    auto status = client->status(request_ms);
    if (status.failed()) {
      last_error = status.error();
      // A failed request may have consumed only part of a response. Never
      // continue issuing requests on that transport; reconnect next poll.
      client->close();
      client.reset();
      sleep_for_poll(deadline);
      continue;
    }
    if (!status.value().error.empty()) {
      last_error = Error(ErrorCode::InvalidState, status.value().error);
      sleep_for_poll(deadline);
      continue;
    }

    connected_expected = 0;
    for (const auto& expected_label : node_labels_) {
      const auto found = std::find_if(
          status.value().nodes.begin(), status.value().nodes.end(),
          [&expected_label](const proto::NodeStatusEntry& entry) {
            return entry.connected && entry.label == expected_label;
          });
      if (found != status.value().nodes.end()) ++connected_expected;
    }
    if (connected_expected == expected) {
      auto coordinator_still_running =
          require_running(coordinator_, "coordinator");
      if (coordinator_still_running.failed()) {
        return coordinator_still_running.error();
      }
      for (size_t i = 0; i < nodes_.size(); ++i) {
        const std::string name =
            i < node_labels_.size() ? node_labels_[i]
                                    : "node-" + std::to_string(i);
        auto node_still_running = require_running(nodes_[i], name);
        if (node_still_running.failed()) return node_still_running.error();
      }
      return {};
    }
    last_error = Error(ErrorCode::Timeout,
                       "connected expected nodes " +
                           std::to_string(connected_expected) + "/" +
                           std::to_string(expected));
    sleep_for_poll(deadline);
  }

  auto coordinator_ok = require_running(coordinator_, "coordinator");
  if (coordinator_ok.failed()) return coordinator_ok.error();
  for (size_t i = 0; i < nodes_.size(); ++i) {
    const std::string name =
        i < node_labels_.size() ? node_labels_[i]
                                : "node-" + std::to_string(i);
    auto node_ok = require_running(nodes_[i], name);
    if (node_ok.failed()) return node_ok.error();
  }
  return Error(ErrorCode::Timeout,
               "nodes did not register in time (connected expected nodes " +
                   std::to_string(connected_expected) + "/" +
                   std::to_string(expected) + "; last error: " +
                   last_error.to_string() + ")");
}

std::unique_ptr<FabricClient> LocalFabric::make_client() {
  auto client =
      std::make_unique<FabricClient>("127.0.0.1", client_port_);
  if (client->connect(5000).failed()) return nullptr;
  return client;
}

Result<int> LocalFabric::shutdown_all() {
  int total = coordinator_.sys_pid != 0 ? 1 : 0;
  for (const auto& node : nodes_) {
    if (node.sys_pid != 0) ++total;
  }
  if (total == 0) return 0;

  Error last_error;
  bool have_error = false;
  auto remember_error = [&last_error, &have_error](const Error& error) {
    last_error = error;
    have_error = true;
  };

  const int64_t graceful_deadline = now_millis() + kGracefulShutdownMs;
  auto coordinator_state = process_running(coordinator_);
  if (coordinator_state.failed()) {
    remember_error(coordinator_state.error());
  } else if (coordinator_state.value()) {
    FabricClient client("127.0.0.1", client_port_);
    const int connect_ms =
        remaining_timeout(graceful_deadline, kConnectAttemptMs);
    if (connect_ms > 0) {
      auto connected = client.connect(connect_ms);
      if (connected.ok()) {
        const int request_ms =
            remaining_timeout(graceful_deadline, kRequestAttemptMs);
        if (request_ms > 0) {
          auto requested = client.shutdown_coordinator(request_ms);
          if (requested.failed()) remember_error(requested.error());
        }
      } else {
        remember_error(connected.error());
      }
    }
    client.close();
  }

  auto poll_all = [this, &remember_error]() {
    bool all_reaped = true;
    auto poll_one = [this, &remember_error,
                     &all_reaped](ChildProcess& child) {
      if (child.sys_pid == 0 || child.reaped) return;
      auto running = process_running(child);
      if (running.failed()) {
        remember_error(running.error());
        all_reaped = false;
      } else if (running.value()) {
        all_reaped = false;
      }
    };
    poll_one(coordinator_);
    for (auto& node : nodes_) poll_one(node);
    return all_reaped;
  };

  while (!poll_all() && now_millis() < graceful_deadline) {
    sleep_for_poll(graceful_deadline);
  }
  (void)poll_all();

  auto terminate_one = [this, &remember_error](ChildProcess& child) {
    if (child.sys_pid == 0 || child.reaped) return;
    auto terminated = terminate(child);
    if (terminated.failed()) remember_error(terminated.error());
  };
  terminate_one(coordinator_);
  for (auto& node : nodes_) terminate_one(node);

  const int64_t forced_deadline = now_millis() + kForcedShutdownMs;
  while (!poll_all() && now_millis() < forced_deadline) {
    sleep_for_poll(forced_deadline);
  }
  (void)poll_all();

  int confirmed = 0;
  if (coordinator_.sys_pid != 0 && coordinator_.reaped) ++confirmed;
  for (const auto& node : nodes_) {
    if (node.sys_pid != 0 && node.reaped) ++confirmed;
  }
  if (confirmed == total) return confirmed;

  std::string message = "failed to reap all child processes (confirmed " +
                        std::to_string(confirmed) + "/" +
                        std::to_string(total) + ")";
  if (have_error) message += "; last error: " + last_error.to_string();
  return Error(ErrorCode::ProcessError, std::move(message));
}

Result<void> LocalFabric::kill_node_by_label(const std::string& label) {
  for (size_t i = 0; i < nodes_.size(); ++i) {
    if (i >= node_labels_.size() || node_labels_[i] != label) continue;
    auto terminated = terminate(nodes_[i]);
    if (terminated.failed()) return terminated.error();
    auto reaped =
        wait_process(nodes_[i], now_millis() + kForcedShutdownMs);
    if (reaped.failed()) {
      return Error(ErrorCode::ProcessError,
                   "failed to reap " + label + ": " +
                       reaped.error().to_string());
    }
    return {};
  }
  return Error(ErrorCode::NodeNotFound, "no node with label " + label);
}

Result<bool> LocalFabric::process_running(ChildProcess& child) {
  if (child.sys_pid == 0 || child.reaped) return false;
#if defined(_WIN32)
  if (child.os_handle == 0) {
    return Error(ErrorCode::ProcessError,
                 "owned process is missing its Windows handle (pid " +
                     std::to_string(child.sys_pid) + ")");
  }

  HANDLE handle = reinterpret_cast<HANDLE>(child.os_handle);
  const DWORD wait_result = WaitForSingleObject(handle, 0);
  if (wait_result == WAIT_TIMEOUT) {
    child.running = true;
    return true;
  }
  if (wait_result != WAIT_OBJECT_0) {
    const DWORD error = GetLastError();
    return Error(ErrorCode::ProcessError,
                 "WaitForSingleObject failed for pid " +
                     std::to_string(child.sys_pid) + " with Windows error " +
                     std::to_string(error));
  }

  DWORD exit_code = 0;
  if (!GetExitCodeProcess(handle, &exit_code)) {
    const DWORD error = GetLastError();
    return Error(ErrorCode::ProcessError,
                 "GetExitCodeProcess failed for pid " +
                     std::to_string(child.sys_pid) + " with Windows error " +
                     std::to_string(error));
  }
  child.exit_code = static_cast<int>(exit_code);
  child.running = false;
  if (!CloseHandle(handle)) {
    const DWORD error = GetLastError();
    return Error(ErrorCode::ProcessError,
                 "CloseHandle failed for exited pid " +
                     std::to_string(child.sys_pid) + " with Windows error " +
                     std::to_string(error));
  }
  child.os_handle = 0;
  child.reaped = true;
  return false;
#else
  int status = 0;
  pid_t result = 0;
  do {
    result = waitpid(static_cast<pid_t>(child.sys_pid), &status, WNOHANG);
  } while (result < 0 && errno == EINTR);

  if (result == 0) {
    child.running = true;
    return true;
  }
  if (result > 0) {
    if (WIFEXITED(status)) {
      child.exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      child.exit_code = 128 + WTERMSIG(status);
    } else {
      child.exit_code = -1;
    }
    child.running = false;
    child.reaped = true;
    return false;
  }
  if (errno == ECHILD) {
    // Another owner or an earlier poll already reaped it. There is no child
    // resource left to wait for, so retain the unknown exit code and converge
    // on the reaped state instead of reviving `running`.
    child.running = false;
    child.reaped = true;
    return false;
  }
  return Error(ErrorCode::ProcessError,
               "waitpid failed for pid " + std::to_string(child.sys_pid) +
                   ": " + std::string(std::strerror(errno)));
#endif
}

Result<void> LocalFabric::wait_process(ChildProcess& child,
                                       int64_t deadline_ms) {
  for (;;) {
    auto running = process_running(child);
    if (running.failed()) return running.error();
    if (!running.value()) return {};
    if (now_millis() >= deadline_ms) {
      return Error(ErrorCode::Timeout,
                   "timed out waiting for pid " +
                       std::to_string(child.sys_pid));
    }
    sleep_for_poll(deadline_ms);
  }
}

Result<void> LocalFabric::terminate(ChildProcess& child) {
  if (child.sys_pid == 0 || child.reaped) return {};
#if defined(_WIN32)
  if (child.os_handle == 0) {
    return Error(ErrorCode::ProcessError,
                 "cannot terminate pid " + std::to_string(child.sys_pid) +
                     ": Windows handle is missing");
  }
  HANDLE handle = reinterpret_cast<HANDLE>(child.os_handle);
  if (!TerminateProcess(handle, 1)) {
    const DWORD error = GetLastError();
    auto running = process_running(child);
    if (running.ok() && !running.value()) return {};
    return Error(ErrorCode::ProcessError,
                 "TerminateProcess failed for pid " +
                     std::to_string(child.sys_pid) + " with Windows error " +
                     std::to_string(error));
  }
#else
  if (kill(static_cast<pid_t>(child.sys_pid), SIGKILL) != 0) {
    const int error = errno;
    if (error == ESRCH) {
      auto running = process_running(child);
      if (running.ok() && !running.value()) return {};
    }
    return Error(ErrorCode::ProcessError,
                 "kill failed for pid " + std::to_string(child.sys_pid) +
                     ": " + std::string(std::strerror(error)));
  }
#endif
  child.running = true;
  return {};
}

}  // namespace cf
