#include "compute_fabric/net/client.h"

#include <limits>
#include <thread>

#include "compute_fabric/core/random.h"
#include "compute_fabric/core/time_util.h"

namespace cf {

FabricClient::FabricClient(std::string host, uint16_t port)
    : host_(std::move(host)), port_(port) {}

FabricClient::~FabricClient() { close(); }

FabricClient::FabricClient(FabricClient&& o) noexcept
    : host_(std::move(o.host_)),
      port_(o.port_),
      channel_(std::move(o.channel_)),
      client_id_(o.client_id_),
      next_request_id_(o.next_request_id_) {}

FabricClient& FabricClient::operator=(FabricClient&& o) noexcept {
  if (this != &o) {
    close();
    host_ = std::move(o.host_);
    port_ = o.port_;
    channel_ = std::move(o.channel_);
    client_id_ = o.client_id_;
    next_request_id_ = o.next_request_id_;
  }
  return *this;
}

Result<void> FabricClient::connect(int timeout_ms) {
  close();
  if (timeout_ms <= 0) {
    return Error(ErrorCode::Timeout, "connect deadline elapsed");
  }
  const int64_t deadline = now_millis() + timeout_ms;
  auto sock = net::TcpSocket::connect(host_, port_, timeout_ms);
  if (sock.failed()) return sock.error();

  auto new_channel = std::make_unique<net::Channel>(sock.take());
  const Id128 new_client_id = Id128::random();
  proto::Hello hello;
  hello.role = "client";
  proto::BinaryWriter w;
  proto::serialize(w, hello);

  const int64_t remaining = deadline - now_millis();
  if (remaining <= 0) {
    new_channel->close();
    return Error(ErrorCode::Timeout, "connect deadline elapsed before Hello");
  }
  const int hello_timeout = static_cast<int>(
      remaining > std::numeric_limits<int>::max()
          ? std::numeric_limits<int>::max()
          : remaining);
  auto sent = new_channel->send_message(
      proto::kHello, proto::SenderKind::Client, new_client_id, 0, w.bytes(),
      hello_timeout);
  if (sent.failed()) {
    Error error = sent.error();
    new_channel->close();
    return error;
  }

  channel_ = std::move(new_channel);
  client_id_ = new_client_id;
  next_request_id_ = 1;
  return Error::success();
}

void FabricClient::close() {
  if (channel_) channel_->close();
  channel_.reset();
}

Result<proto::Frame> FabricClient::request(uint16_t message_type,
                                           const std::vector<uint8_t>& payload,
                                           int64_t timeout_ms) {
  if (!channel_ || !channel_->valid()) {
    return Error(ErrorCode::NotConnected, "client not connected");
  }
  if (timeout_ms <= 0) {
    close();
    return Error(ErrorCode::Timeout, "request deadline elapsed");
  }
  const int64_t deadline = now_millis() + timeout_ms;
  uint64_t req = next_request_id_++;
  const int send_timeout = static_cast<int>(
      timeout_ms > std::numeric_limits<int>::max()
          ? std::numeric_limits<int>::max()
          : timeout_ms);
  auto r = channel_->send_message(message_type, proto::SenderKind::Client,
                                  client_id_, req, payload,
                                  send_timeout);
  if (r.failed()) {
    Error error = r.error();
    close();
    return error;
  }
  while (now_millis() < deadline) {
    const int64_t remaining = deadline - now_millis();
    const int receive_timeout =
        static_cast<int>(remaining < 500 ? remaining : 500);
    auto frame = channel_->recv_frame(receive_timeout,
                                      proto::kMaxFramePayload);
    if (frame.failed()) {
      if (frame.error().code() == ErrorCode::Timeout) continue;
      Error error = frame.error();
      close();
      return error;
    }
    if (frame.value().header.request_id == req) {
      if (frame.value().header.message_type == proto::kErrorResponse) {
        proto::BinaryReader br(frame.value().payload);
        auto e = proto::deserialize_error_response(br);
        if (e.ok()) {
          return Error(ErrorCode::Internal, e.value().error);
        }
      }
      return frame.value();
    }
  }
  close();
  return Error(ErrorCode::Timeout, "no response from coordinator");
}

Result<proto::TaskSubmitResponse> FabricClient::submit(
    const WorkloadId& workload, const std::string& workload_name,
    const ComputeTask& task) {
  proto::TaskSubmit m;
  m.workload_id = workload;
  m.workload_name = workload_name;
  m.task = task;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kTaskSubmit, w.bytes(), 10000);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_task_submit_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::TaskInspectResponse> FabricClient::inspect(const ComputeTaskId& id) {
  proto::TaskInspect m;
  m.task_id = id;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kTaskInspect, w.bytes(), 10000);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_task_inspect_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::TaskListResponse> FabricClient::list(const std::string& filter_state) {
  proto::TaskList m;
  m.filter_state = filter_state;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kTaskList, w.bytes(), 10000);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_task_list_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::TaskCancelResponse> FabricClient::cancel(const ComputeTaskId& id) {
  proto::TaskCancel m;
  m.task_id = id;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kTaskCancel, w.bytes(), 10000);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_task_cancel_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::WorkloadInspectResponse> FabricClient::workload_inspect(
    const WorkloadId& id) {
  proto::WorkloadInspect m;
  m.workload_id = id;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kWorkloadInspect, w.bytes(), 10000);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_workload_inspect_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::FabricStatusResponse> FabricClient::status(int64_t timeout_ms) {
  proto::FabricStatus m;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kFabricStatus, w.bytes(), timeout_ms);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_fabric_status_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::PlacementResponse> FabricClient::placement_query(
    const ComputeTask& task) {
  proto::PlacementQuery m;
  m.task = task;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kPlacementQuery, w.bytes(), 10000);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_placement_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::NodeListResponse> FabricClient::nodes() {
  proto::NodeList m;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kNodeList, w.bytes(), 10000);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_node_list_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::NodeDrainResponse> FabricClient::drain(const NodeId& id,
                                                     bool drain_flag) {
  proto::NodeDrain m;
  m.node_id = id;
  m.drain = drain_flag;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kNodeDrain, w.bytes(), 10000);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_node_drain_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

Result<proto::CoordinatorShutdownResponse> FabricClient::shutdown_coordinator(
    int64_t timeout_ms) {
  proto::CoordinatorShutdown m;
  proto::BinaryWriter w;
  proto::serialize(w, m);
  auto f = request(proto::kCoordinatorShutdown, w.bytes(), timeout_ms);
  if (f.failed()) return f.error();
  proto::BinaryReader r(f.value().payload);
  auto resp = proto::deserialize_coordinator_shutdown_response(r);
  if (resp.failed()) return resp.error();
  return resp.value();
}

bool FabricClient::wait_for_nodes(size_t n, int64_t timeout_ms) {
  auto deadline = now_millis() + timeout_ms;
  while (now_millis() < deadline) {
    auto s = status(deadline - now_millis());
    if (s.ok() && s.value().node_count >= n) return true;
    if (!connected()) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

}  // namespace cf
