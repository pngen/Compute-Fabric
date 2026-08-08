#include "compute_fabric/net/channel.h"

#include <cstring>
#include <limits>

#include "compute_fabric/core/time_util.h"

namespace cf::net {

Result<void> Channel::send_frame(const proto::Frame& frame, int timeout_ms) {
  if (!valid()) return Error(ErrorCode::NotConnected, "channel closed");
  std::vector<uint8_t> wire(proto::kHeaderSize + frame.payload.size());
  proto::encode_header(frame.header, wire.data());
  if (!frame.payload.empty()) {
    std::memcpy(wire.data() + proto::kHeaderSize, frame.payload.data(),
                frame.payload.size());
  }
  std::lock_guard<std::mutex> lk(send_mu_);
  return socket_.send_all(wire.data(), wire.size(), timeout_ms);
}

Result<void> Channel::send_message(uint16_t message_type,
                                   proto::SenderKind kind,
                                   const Id128& sender_id,
                                   uint64_t request_id,
                                   const std::vector<uint8_t>& payload,
                                   int timeout_ms) {
  auto frame = proto::make_frame(message_type, kind, sender_id, request_id,
                                 payload);
  return send_frame(frame, timeout_ms);
}

Result<proto::Frame> Channel::recv_frame(int timeout_ms,
                                         uint32_t max_frame_bytes) {
  std::lock_guard<std::mutex> lk(recv_mu_);
  if (!valid()) return Error(ErrorCode::NotConnected, "channel closed");

  const int bounded_timeout = timeout_ms > 0 ? timeout_ms : 1;
  const int64_t deadline = now_millis() + bounded_timeout;
  for (;;) {
    size_t required_size = proto::kHeaderSize;
    Result<proto::FrameHeader> header = Error(
        ErrorCode::MalformedFrame, "frame header has not been received");

    if (recv_buffer_.size() >= proto::kHeaderSize) {
      header = proto::decode_header(recv_buffer_.data(), proto::kHeaderSize);
      if (header.failed()) {
        recv_buffer_.clear();
        return header.error();
      }
      if (header.value().payload_length > max_frame_bytes) {
        recv_buffer_.clear();
        return Error(ErrorCode::MalformedFrame,
                     "payload exceeds channel frame limit");
      }
      required_size = proto::kHeaderSize +
                      static_cast<size_t>(header.value().payload_length);
      if (recv_buffer_.size() == required_size) {
        proto::Frame frame;
        frame.header = header.value();
        frame.payload.assign(recv_buffer_.begin() + proto::kHeaderSize,
                             recv_buffer_.end());
        recv_buffer_.clear();
        auto validation = proto::validate_frame(frame);
        if (validation.failed()) return validation.error();
        return frame;
      }
    }

    const int64_t remaining = deadline - now_millis();
    if (remaining <= 0) {
      return Error(ErrorCode::Timeout, "recv timeout");
    }
    const size_t needed = required_size - recv_buffer_.size();
    const size_t old_size = recv_buffer_.size();
    recv_buffer_.resize(required_size);
    const int io_timeout = static_cast<int>(
        remaining > std::numeric_limits<int>::max()
            ? std::numeric_limits<int>::max()
            : remaining);
    auto received = socket_.recv_some(recv_buffer_.data() + old_size, needed,
                                      io_timeout);
    if (received.failed()) {
      recv_buffer_.resize(old_size);
      if (received.error().code() != ErrorCode::Timeout) {
        recv_buffer_.clear();
      }
      return received.error();
    }
    recv_buffer_.resize(old_size + received.value());
  }
}

void Channel::close() {
  socket_.close();
  std::lock_guard<std::mutex> lk(recv_mu_);
  recv_buffer_.clear();
}

}  // namespace cf::net
