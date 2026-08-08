#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "compute_fabric/core/status.h"

namespace cf::net {

struct SocketState;

// Friendly type for the platform socket handle.
#if defined(_WIN32)
using SocketHandle = uintptr_t;
#else
using SocketHandle = int;
#endif

// RAII TCP socket with timeout-based blocking I/O. All I/O is bounded.
class TcpSocket {
 public:
  TcpSocket() = default;
  ~TcpSocket();
  TcpSocket(TcpSocket&& o) noexcept;
  TcpSocket& operator=(TcpSocket&& o) noexcept;
  TcpSocket(const TcpSocket&) = delete;
  TcpSocket& operator=(const TcpSocket&) = delete;

  static Result<TcpSocket> connect(const std::string& host, uint16_t port,
                                   int timeout_ms);
  static Result<TcpSocket> from_raw(SocketHandle h, std::string peer);

  // Sends all bytes. `timeout_ms` bounds the entire operation, including all
  // partial writes, rather than applying independently to each native send.
  Result<void> send_all(const uint8_t* data, size_t len, int timeout_ms);
  // Receives up to `cap` bytes; returns an error on timeout or disconnect.
  Result<size_t> recv_some(uint8_t* data, size_t cap, int timeout_ms);

  // Unblocks active I/O, waits for it to drain, then closes exactly once.
  void close();

  bool valid() const;
  std::string peer() const { return peer_; }
  SocketHandle raw() const;

 private:
  static SocketHandle invalid_handle();
  std::shared_ptr<SocketState> state_;
  std::string peer_;
};

// TCP listener bound to a local address.
class TcpListener {
 public:
  TcpListener() = default;
  ~TcpListener();
  TcpListener(TcpListener&& o) noexcept;
  TcpListener& operator=(TcpListener&& o) noexcept;
  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;

  static Result<TcpListener> bind(const std::string& host, uint16_t port,
                                  int backlog = 64);

  Result<TcpSocket> accept(int timeout_ms);
  void close();
  uint16_t port() const { return port_; }
  bool valid() const;

 private:
  static SocketHandle invalid_handle();
  std::shared_ptr<SocketState> state_;
  uint16_t port_ = 0;
};

// One-time Winsock initialization (Windows only). Safe to call repeatedly.
void sockets_init();

}  // namespace cf::net
