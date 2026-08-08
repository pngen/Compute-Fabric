#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/core/status.h"
#include "compute_fabric/net/socket.h"
#include "compute_fabric/protocol/frame.h"

namespace cf::net {

// Framed message channel over a TcpSocket. Handles partial reads/writes,
// strict frame-size limits, header/payload checksums and protocol-version
// rejection. Recv is bounded by `max_frame_bytes` and a timeout.
class Channel {
 public:
  explicit Channel(TcpSocket socket) : socket_(std::move(socket)) {}

  // Sends one frame (serializes header + payload). Returns IoError on
  // partial-write failure.
  Result<void> send_frame(const proto::Frame& frame, int timeout_ms);

  // Reads one complete valid frame. Returns Timeout on idle, ConnectionClosed
  // on disconnect, MalformedFrame on bad framing/checksums.
  Result<proto::Frame> recv_frame(int timeout_ms, uint32_t max_frame_bytes);

  // Sends a frame built from a payload.
  Result<void> send_message(uint16_t message_type, proto::SenderKind kind,
                            const Id128& sender_id, uint64_t request_id,
                            const std::vector<uint8_t>& payload,
                            int timeout_ms);

  TcpSocket& socket() { return socket_; }
  void close();
  bool valid() const { return socket_.valid(); }

 private:
  TcpSocket socket_;
  std::mutex send_mu_;
  std::mutex recv_mu_;
  std::vector<uint8_t> recv_buffer_;
};

}  // namespace cf::net
