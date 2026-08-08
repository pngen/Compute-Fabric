#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/net/client.h"

namespace cf {

// Path of the current executable (used to re-spawn coordinator/node processes).
std::string current_executable_path();

// Handle to a spawned child process. Move-only: moving transfers exclusive
// ownership of the process handle and leaves the source inert (running=false,
// handle=0), so a moved-from LocalFabric can never shut down live processes.
struct ChildProcess {
  std::string command_line;
  int exit_code = -1;
  bool running = false;
  bool reaped = false;
  uint64_t sys_pid = 0;
  uintptr_t os_handle = 0;  // Windows process handle (0 when not spawned)

  ChildProcess() = default;

  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;

  ChildProcess(ChildProcess&& o) noexcept
      : command_line(std::move(o.command_line)),
        exit_code(o.exit_code),
        running(o.running),
        reaped(o.reaped),
        sys_pid(o.sys_pid),
        os_handle(o.os_handle) {
    o.exit_code = -1;
    o.running = false;
    o.reaped = false;
    o.sys_pid = 0;
    o.os_handle = 0;
  }

  // Replacing an owned child would lose the only process handle. Child
  // ownership can be move-constructed but never move-assigned.
  ChildProcess& operator=(ChildProcess&&) = delete;
};

// A local multi-process fabric: real coordinator process + N real node
// processes communicating over localhost TCP. Guarantees zero orphaned
// processes on shutdown.
class LocalFabric {
 public:
  LocalFabric() = default;
  ~LocalFabric();

  LocalFabric(const LocalFabric&) = delete;
  LocalFabric& operator=(const LocalFabric&) = delete;
  LocalFabric(LocalFabric&&) noexcept = default;
  LocalFabric& operator=(LocalFabric&&) = delete;

  // exe_path: the compute-fabric executable. node_count: number of node
  // processes (>= 1). cuda_node: whether the first node is CUDA-capable.
  // Returns the fabric with coordinator + nodes already spawned (not yet
  // necessarily registered).
  static Result<LocalFabric> launch(const std::string& exe_path,
                                    int node_count, bool cuda_node,
                                    const std::string& store_dir,
                                    const std::string& policy,
                                    const std::string& telemetry_dir);

  uint16_t node_port() const { return node_port_; }
  uint16_t client_port() const { return client_port_; }

  // Blocks until all nodes are registered with the coordinator.
  Result<void> wait_registered(int64_t timeout_ms);

  // A client connected to the coordinator control plane.
  std::unique_ptr<FabricClient> make_client();

  // Gracefully shuts down the coordinator and all nodes, then waits for every
  // child process to exit. Returns the number of processes confirmed gone.
  Result<int> shutdown_all();

  // Terminates a node process by its label (e.g., "node-0"). Used by examples
  // to simulate node loss.
  Result<void> kill_node_by_label(const std::string& label);

  bool healthy() const { return coordinator_.running; }
  const std::vector<ChildProcess>& nodes() const { return nodes_; }

 private:
  uint16_t pick_free_port();
  Result<void> spawn(const std::string& exe_path,
                     const std::vector<std::string>& args,
                     ChildProcess& out);
  Result<bool> process_running(ChildProcess& p);
  Result<void> wait_process(ChildProcess& p, int64_t deadline_ms);
  Result<void> terminate(ChildProcess& p);

  uint16_t node_port_ = 0;
  uint16_t client_port_ = 0;
  ChildProcess coordinator_;
  std::vector<ChildProcess> nodes_;
  std::vector<std::string> node_labels_;
};

}  // namespace cf
