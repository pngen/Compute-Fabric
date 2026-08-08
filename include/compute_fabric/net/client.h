#pragma once

#include <cstdint>
#include <string>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/net/channel.h"
#include "compute_fabric/protocol/messages.h"

namespace cf {

// Synchronous client for the coordinator control plane. Used by the CLI,
// launcher, tests and examples.
class FabricClient {
 public:
  FabricClient(std::string host, uint16_t port);
  ~FabricClient();

  FabricClient(const FabricClient&) = delete;
  FabricClient& operator=(const FabricClient&) = delete;
  FabricClient(FabricClient&& o) noexcept;
  FabricClient& operator=(FabricClient&& o) noexcept;

  Result<void> connect(int timeout_ms = 10000);
  void close();

  Result<proto::TaskSubmitResponse> submit(const WorkloadId& workload,
                                           const std::string& workload_name,
                                           const ComputeTask& task);
  Result<proto::TaskInspectResponse> inspect(const ComputeTaskId& id);
  Result<proto::TaskListResponse> list(const std::string& filter_state);
  Result<proto::TaskCancelResponse> cancel(const ComputeTaskId& id);
  Result<proto::WorkloadInspectResponse> workload_inspect(const WorkloadId& id);
  Result<proto::FabricStatusResponse> status(int64_t timeout_ms = 10000);
  Result<proto::PlacementResponse> placement_query(const ComputeTask& task);
  Result<proto::NodeListResponse> nodes();
  Result<proto::NodeDrainResponse> drain(const NodeId& id, bool drain);
  Result<proto::CoordinatorShutdownResponse> shutdown_coordinator(
      int64_t timeout_ms = 10000);

  // Waits (polling) until the coordinator reports at least `n` nodes
  // connected, or the timeout elapses. Returns false on timeout.
  bool wait_for_nodes(size_t n, int64_t timeout_ms);

  bool connected() const { return channel_ != nullptr && channel_->valid(); }

 private:
  Result<proto::Frame> request(uint16_t message_type,
                               const std::vector<uint8_t>& payload,
                               int64_t timeout_ms);

  std::string host_;
  uint16_t port_;
  std::unique_ptr<net::Channel> channel_;
  Id128 client_id_;
  uint64_t next_request_id_ = 1;
};

}  // namespace cf
