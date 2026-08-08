#include "compute_fabric/net/socket.h"

#include <condition_variable>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>

#include "compute_fabric/core/time_util.h"

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#ifndef in_port_t
typedef uint16_t in_port_t;
#endif
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace cf::net {

struct SocketState {
  explicit SocketState(SocketHandle native_handle) : handle(native_handle) {}

  std::mutex mu;
  std::condition_variable cv;
  SocketHandle handle = static_cast<SocketHandle>(-1);
  size_t active_operations = 0;
  bool closing = false;
};

namespace {

SocketHandle kInvalid = static_cast<SocketHandle>(-1);

std::string last_error_str() {
#if defined(_WIN32)
  int err = WSAGetLastError();
  char buf[256] = {0};
  FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                 nullptr, err, 0, buf, sizeof(buf), nullptr);
  return std::string(buf) + " (wsa " + std::to_string(err) + ")";
#else
  return std::string(std::strerror(errno));
#endif
}

bool set_receive_timeout(SocketHandle h, int timeout_ms) {
  if (timeout_ms <= 0) timeout_ms = 1;
#if defined(_WIN32)
  // Windows SO_RCVTIMEO takes a DWORD timeout in milliseconds.
  DWORD ms = static_cast<DWORD>(timeout_ms);
  return setsockopt(h, SOL_SOCKET, SO_RCVTIMEO,
                    reinterpret_cast<const char*>(&ms), sizeof(ms)) == 0;
#else
  timeval tv{};
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;
  return setsockopt(h, SOL_SOCKET, SO_RCVTIMEO,
                    reinterpret_cast<const char*>(&tv), sizeof(tv)) == 0;
#endif
}

bool set_send_timeout(SocketHandle h, int timeout_ms) {
  if (timeout_ms <= 0) timeout_ms = 1;
#if defined(_WIN32)
  DWORD ms = static_cast<DWORD>(timeout_ms);
  return setsockopt(h, SOL_SOCKET, SO_SNDTIMEO,
                    reinterpret_cast<const char*>(&ms), sizeof(ms)) == 0;
#else
  timeval tv{};
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;
  return setsockopt(h, SOL_SOCKET, SO_SNDTIMEO,
                    reinterpret_cast<const char*>(&tv), sizeof(tv)) == 0;
#endif
}

bool set_socket_timeouts(SocketHandle h, int timeout_ms) {
  return set_receive_timeout(h, timeout_ms) &&
         set_send_timeout(h, timeout_ms);
}

// Low-latency control plane: disable Nagle so small frames (and partial
// writes in tests) are not artificially delayed waiting for ACKs.
void set_nodelay(SocketHandle h) {
  int one = 1;
  setsockopt(h, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one),
             sizeof(one));
}

bool resolve_host(const std::string& host, uint16_t port, sockaddr_storage& out,
                  socklen_t& out_len) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo* res = nullptr;
  std::string port_str = std::to_string(port);
  int rc = getaddrinfo(host.empty() ? "127.0.0.1" : host.c_str(),
                       port_str.c_str(), &hints, &res);
  if (rc != 0 || res == nullptr) return false;
  out_len = static_cast<socklen_t>(res->ai_addrlen);
  std::memcpy(&out, res->ai_addr, res->ai_addrlen);
  freeaddrinfo(res);
  return true;
}

class ActiveSocketOperation {
 public:
  explicit ActiveSocketOperation(const std::shared_ptr<SocketState>& state)
      : state_(state) {
    if (!state_) return;
    std::lock_guard<std::mutex> lk(state_->mu);
    if (state_->closing || state_->handle == kInvalid) return;
    handle_ = state_->handle;
    ++state_->active_operations;
    acquired_ = true;
  }

  ~ActiveSocketOperation() {
    if (!acquired_) return;
    {
      std::lock_guard<std::mutex> lk(state_->mu);
      --state_->active_operations;
    }
    state_->cv.notify_all();
  }

  ActiveSocketOperation(const ActiveSocketOperation&) = delete;
  ActiveSocketOperation& operator=(const ActiveSocketOperation&) = delete;

  explicit operator bool() const { return acquired_; }
  SocketHandle handle() const { return handle_; }

 private:
  std::shared_ptr<SocketState> state_;
  SocketHandle handle_ = kInvalid;
  bool acquired_ = false;
};

void native_shutdown(SocketHandle h) {
#if defined(_WIN32)
  ::shutdown(h, SD_BOTH);
#else
  ::shutdown(h, SHUT_RDWR);
#endif
}

void native_close(SocketHandle h) {
#if defined(_WIN32)
  closesocket(h);
#else
  ::close(h);
#endif
}

void close_state(const std::shared_ptr<SocketState>& state,
                 bool unblock_active_io) {
  if (!state) return;

  SocketHandle h = kInvalid;
  {
    std::unique_lock<std::mutex> lk(state->mu);
    if (state->handle == kInvalid) return;
    if (state->closing) {
      state->cv.wait(lk, [&] { return state->handle == kInvalid; });
      return;
    }
    state->closing = true;
    h = state->handle;
  }

  if (unblock_active_io) native_shutdown(h);

  {
    std::unique_lock<std::mutex> lk(state->mu);
    state->cv.wait(lk, [&] { return state->active_operations == 0; });
    native_close(h);
    state->handle = kInvalid;
    state->closing = false;
  }
  state->cv.notify_all();
}

bool state_valid(const std::shared_ptr<SocketState>& state) {
  if (!state) return false;
  std::lock_guard<std::mutex> lk(state->mu);
  return !state->closing && state->handle != kInvalid;
}

SocketHandle state_handle(const std::shared_ptr<SocketState>& state) {
  if (!state) return kInvalid;
  std::lock_guard<std::mutex> lk(state->mu);
  if (state->closing) return kInvalid;
  return state->handle;
}

}  // namespace

void sockets_init() {
#if defined(_WIN32)
  static bool initialized = [] {
    WSADATA data;
    WSAStartup(MAKEWORD(2, 2), &data);
    return true;
  }();
  (void)initialized;
#else
  (void)0;
#endif
}

SocketHandle TcpSocket::invalid_handle() { return kInvalid; }
SocketHandle TcpListener::invalid_handle() { return kInvalid; }

bool TcpSocket::valid() const { return state_valid(state_); }

SocketHandle TcpSocket::raw() const { return state_handle(state_); }

bool TcpListener::valid() const { return state_valid(state_); }

TcpSocket::~TcpSocket() { close(); }

TcpSocket::TcpSocket(TcpSocket&& o) noexcept
    : state_(std::move(o.state_)), peer_(std::move(o.peer_)) {}

TcpSocket& TcpSocket::operator=(TcpSocket&& o) noexcept {
  if (this != &o) {
    close();
    state_ = std::move(o.state_);
    peer_ = std::move(o.peer_);
  }
  return *this;
}

Result<TcpSocket> TcpSocket::from_raw(SocketHandle h, std::string peer) {
  if (h == invalid_handle()) {
    return Error(ErrorCode::NotConnected, "invalid accepted socket");
  }
#if defined(_WIN32)
  // A socket accepted from a non-blocking Winsock listener inherits the
  // listener's non-blocking mode. Channels use bounded blocking I/O, so
  // restore blocking mode before the first handshake read.
  u_long blocking = 0;
  if (ioctlsocket(h, FIONBIO, &blocking) != 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to restore accepted socket blocking mode: " + error);
  }
#endif
  if (!set_socket_timeouts(h, 5000)) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to configure accepted socket timeouts: " + error);
  }
  set_nodelay(h);
  TcpSocket s;
  s.state_ = std::make_shared<SocketState>(h);
  s.peer_ = std::move(peer);
  return s;
}

Result<TcpSocket> TcpSocket::connect(const std::string& host, uint16_t port,
                                     int timeout_ms) {
  sockets_init();
  sockaddr_storage addr{};
  socklen_t addr_len = 0;
  if (!resolve_host(host, port, addr, addr_len)) {
    return Error(ErrorCode::NetworkError, "cannot resolve " + host);
  }
#if defined(_WIN32)
  SOCKET h = socket(addr.ss_family, SOCK_STREAM, IPPROTO_TCP);
#else
  int h = socket(addr.ss_family, SOCK_STREAM, IPPROTO_TCP);
#endif
  if (h == static_cast<SocketHandle>(-1)) {
    return Error(ErrorCode::NetworkError,
                 "socket() failed: " + last_error_str());
  }
  // Non-blocking connect with timeout.
  int rc = 0;
#if defined(_WIN32)
  u_long mode = 1;
  if (ioctlsocket(h, FIONBIO, &mode) != 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to set socket non-blocking: " + error);
  }
#else
  int flags = fcntl(h, F_GETFL, 0);
  if (flags < 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to read socket flags before connect: " + error);
  }
  if (fcntl(h, F_SETFL, flags | O_NONBLOCK) != 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to set socket non-blocking: " + error);
  }
#endif
  rc = ::connect(h, reinterpret_cast<sockaddr*>(&addr), addr_len);
  if (rc != 0) {
#if defined(_WIN32)
    int err = WSAGetLastError();
    if (err == WSAEISCONN) {
      rc = 0;  // already connected
    } else if (err != WSAEWOULDBLOCK && err != WSAEINPROGRESS) {
      closesocket(h);
      return Error(ErrorCode::NetworkError,
                   "connect() failed: " + last_error_str());
    }
#endif
  }
  if (rc != 0) {
#if defined(_WIN32)
    int err = WSAGetLastError();
    if (err != WSAEWOULDBLOCK && err != WSAEINPROGRESS) {
      closesocket(h);
      return Error(ErrorCode::NetworkError,
                   "connect() failed: " + last_error_str());
    }
    fd_set wset;
    FD_ZERO(&wset);
    FD_SET(h, &wset);
    timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    int sel = select(0, nullptr, &wset, nullptr, &tv);
    if (sel == 0) {
      closesocket(h);
      return Error(ErrorCode::Timeout, "connect timed out to " + host + ":" +
                                           std::to_string(port));
    }
    if (sel < 0) {
      std::string error = last_error_str();
      closesocket(h);
      return Error(ErrorCode::NetworkError,
                   "connect readiness wait failed: " + error);
    }
    int soerr = 0;
    int len = sizeof(soerr);
    if (getsockopt(h, SOL_SOCKET, SO_ERROR,
                   reinterpret_cast<char*>(&soerr), &len) != 0) {
      std::string error = last_error_str();
      closesocket(h);
      return Error(ErrorCode::NetworkError,
                   "connect completion query failed: " + error);
    }
    if (soerr != 0) {
      closesocket(h);
      return Error(ErrorCode::NetworkError, "connect() failed on " + host);
    }
#else
    if (errno != EINPROGRESS && errno != EWOULDBLOCK) {
      ::close(h);
      return Error(ErrorCode::NetworkError,
                   "connect() failed: " + last_error_str());
    }
    fd_set wset;
    FD_ZERO(&wset);
    FD_SET(h, &wset);
    timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    int sel = select(h + 1, nullptr, &wset, nullptr, &tv);
    if (sel == 0) {
      ::close(h);
      return Error(ErrorCode::Timeout, "connect timed out to " + host + ":" +
                                           std::to_string(port));
    }
    if (sel < 0) {
      std::string error = last_error_str();
      ::close(h);
      return Error(ErrorCode::NetworkError,
                   "connect readiness wait failed: " + error);
    }
    int soerr = 0;
    socklen_t len = sizeof(soerr);
    if (getsockopt(h, SOL_SOCKET, SO_ERROR, &soerr, &len) != 0) {
      std::string error = last_error_str();
      ::close(h);
      return Error(ErrorCode::NetworkError,
                   "connect completion query failed: " + error);
    }
    if (soerr != 0) {
      ::close(h);
      return Error(ErrorCode::NetworkError, "connect() failed on " + host);
    }
#endif
  }
  // Restore blocking mode regardless of the connect path.
#if defined(_WIN32)
  u_long blocking = 0;
  if (ioctlsocket(h, FIONBIO, &blocking) != 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to restore socket blocking mode: " + error);
  }
#else
  int flags2 = fcntl(h, F_GETFL, 0);
  if (flags2 < 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to read socket flags after connect: " + error);
  }
  if (fcntl(h, F_SETFL, flags2 & ~O_NONBLOCK) != 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to restore socket blocking mode: " + error);
  }
#endif
  if (!set_socket_timeouts(h, 5000)) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to configure connected socket timeouts: " + error);
  }
  set_nodelay(h);
  TcpSocket s;
  s.state_ = std::make_shared<SocketState>(h);
  s.peer_ = host + ":" + std::to_string(port);
  return s;
}

Result<void> TcpSocket::send_all(const uint8_t* data, size_t len,
                                 int timeout_ms) {
  ActiveSocketOperation operation(state_);
  if (!operation) {
    return Error(ErrorCode::NotConnected, "socket not connected");
  }
  if (len == 0) return {};
  if (timeout_ms <= 0) {
    return Error(ErrorCode::Timeout, "send timeout");
  }
  const SocketHandle h = operation.handle();
  const int64_t deadline = now_millis() + static_cast<int64_t>(timeout_ms);
  size_t sent = 0;
  while (sent < len) {
    const int64_t remaining_ms = deadline - now_millis();
    if (remaining_ms <= 0) {
      return Error(ErrorCode::Timeout, "send timeout");
    }
    const int native_timeout_ms = static_cast<int>(
        remaining_ms > std::numeric_limits<int>::max()
            ? std::numeric_limits<int>::max()
            : remaining_ms);
    if (!set_send_timeout(h, native_timeout_ms)) {
      return Error(ErrorCode::NetworkError,
                   "failed to configure send timeout: " + last_error_str());
    }
#if defined(_WIN32)
    const size_t remaining = len - sent;
    const int chunk = static_cast<int>(
        remaining > static_cast<size_t>(std::numeric_limits<int>::max())
            ? std::numeric_limits<int>::max()
            : remaining);
    int n = ::send(h, reinterpret_cast<const char*>(data + sent), chunk, 0);
#else
    ssize_t n = ::send(h, data + sent, len - sent, MSG_NOSIGNAL);
#endif
    if (n == 0) {
      return Error(ErrorCode::ConnectionClosed, "connection closed during send");
    }
    if (n < 0) {
#if defined(_WIN32)
      int err = WSAGetLastError();
      if (err == WSAETIMEDOUT || err == WSAEWOULDBLOCK) {
        return Error(ErrorCode::Timeout, "send timeout");
      }
      if (err == WSAECONNRESET || err == WSAECONNABORTED ||
          err == WSAESHUTDOWN || err == WSAENOTSOCK) {
        return Error(ErrorCode::ConnectionClosed, "connection closed during send");
      }
#else
      if (errno == EINTR) continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return Error(ErrorCode::Timeout, "send timeout");
      }
      if (errno == EPIPE || errno == ECONNRESET || errno == ENOTCONN) {
        return Error(ErrorCode::ConnectionClosed, "connection closed during send");
      }
#endif
      return Error(ErrorCode::NetworkError,
                   "send failed: " + last_error_str());
    }
    sent += static_cast<size_t>(n);
  }
  return {};
}

Result<size_t> TcpSocket::recv_some(uint8_t* data, size_t cap,
                                    int timeout_ms) {
  if (cap == 0) return static_cast<size_t>(0);
  ActiveSocketOperation operation(state_);
  if (!operation) {
    return Error(ErrorCode::NotConnected, "socket not connected");
  }
  const SocketHandle h = operation.handle();
  if (!set_receive_timeout(h, timeout_ms)) {
    return Error(ErrorCode::NetworkError,
                 "failed to configure receive timeout: " + last_error_str());
  }
  if (cap > 0x7FFFFFFF) cap = 0x7FFFFFFF;
#if defined(_WIN32)
  int n = ::recv(h, reinterpret_cast<char*>(data), static_cast<int>(cap), 0);
  if (n == 0) return Error(ErrorCode::ConnectionClosed, "connection closed");
  if (n < 0) {
    int err = WSAGetLastError();
    if (err == WSAETIMEDOUT || err == WSAEWOULDBLOCK)
      return Error(ErrorCode::Timeout, "recv timeout");
    if (err == WSAECONNRESET || err == WSAECONNABORTED ||
        err == WSAESHUTDOWN || err == WSAENOTSOCK)
      return Error(ErrorCode::ConnectionClosed, "connection reset");
    return Error(ErrorCode::NetworkError, "recv failed: " + last_error_str());
  }
  return static_cast<size_t>(n);
#else
  ssize_t n = ::recv(h, data, cap, 0);
  if (n == 0) return Error(ErrorCode::ConnectionClosed, "connection closed");
  if (n < 0) {
    if (errno == EINTR)
      return Error(ErrorCode::Timeout, "recv interrupted");
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return Error(ErrorCode::Timeout, "recv timeout");
    if (errno == ECONNRESET || errno == ENOTCONN)
      return Error(ErrorCode::ConnectionClosed, "connection reset");
    return Error(ErrorCode::NetworkError, "recv failed: " + last_error_str());
  }
  return static_cast<size_t>(n);
#endif
}

void TcpSocket::close() {
  close_state(state_, true);
}

TcpListener::~TcpListener() { close(); }

TcpListener::TcpListener(TcpListener&& o) noexcept
    : state_(std::move(o.state_)), port_(o.port_) {
  o.port_ = 0;
}

TcpListener& TcpListener::operator=(TcpListener&& o) noexcept {
  if (this != &o) {
    close();
    state_ = std::move(o.state_);
    port_ = o.port_;
    o.port_ = 0;
  }
  return *this;
}

Result<TcpListener> TcpListener::bind(const std::string& host, uint16_t port,
                                      int backlog) {
  sockets_init();
#if defined(_WIN32)
  SOCKET h = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
  int h = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#endif
  if (h == static_cast<SocketHandle>(-1)) {
    return Error(ErrorCode::NetworkError, "socket() failed: " + last_error_str());
  }
  int one = 1;
  setsockopt(h, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one),
             sizeof(one));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
#if defined(_WIN32)
  addr.sin_addr.s_addr = INADDR_ANY;
#else
  addr.sin_addr.s_addr = INADDR_ANY;
#endif
  if (host != "0.0.0.0" && !host.empty()) {
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
  }
  if (::bind(h, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    std::string e = last_error_str();
#if defined(_WIN32)
    closesocket(h);
#else
    ::close(h);
#endif
    return Error(ErrorCode::NetworkError,
                 "bind failed on port " + std::to_string(port) + ": " + e);
  }
  if (::listen(h, backlog) != 0) {
    std::string e = last_error_str();
#if defined(_WIN32)
    closesocket(h);
#else
    ::close(h);
#endif
    return Error(ErrorCode::NetworkError, "listen failed: " + e);
  }

  // Non-blocking listener so accept() can be polled with a bounded deadline.
#if defined(_WIN32)
  u_long nbio = 1;
  if (ioctlsocket(h, FIONBIO, &nbio) != 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to set listener non-blocking: " + error);
  }
#else
  int fl = fcntl(h, F_GETFL, 0);
  if (fl < 0 || fcntl(h, F_SETFL, fl | O_NONBLOCK) != 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to set listener non-blocking: " + error);
  }
#endif

  // Determine the actual bound port (relevant when port == 0).
  sockaddr_in got{};
  socklen_t glen = sizeof(got);
  if (getsockname(h, reinterpret_cast<sockaddr*>(&got), &glen) != 0) {
    std::string error = last_error_str();
    native_close(h);
    return Error(ErrorCode::NetworkError,
                 "failed to query listener address: " + error);
  }

  TcpListener l;
  l.state_ = std::make_shared<SocketState>(h);
  l.port_ = ntohs(got.sin_port);
  return l;
}

Result<TcpSocket> TcpListener::accept(int timeout_ms) {
  // Bounded polling accept on a non-blocking listener. Avoids select()
  // quirks and is interruptible by close() during shutdown.
  auto deadline = now_millis() + timeout_ms;
  for (;;) {
    ActiveSocketOperation operation(state_);
    if (!operation) {
      return Error(ErrorCode::ConnectionClosed, "listener closed");
    }
#if defined(_WIN32)
    SOCKET c = ::accept(operation.handle(), nullptr, nullptr);
    if (c != INVALID_SOCKET) {
      return TcpSocket::from_raw(static_cast<SocketHandle>(c), "accepted");
    }
    int err = WSAGetLastError();
    if (err == WSAEWOULDBLOCK) {
      if (now_millis() >= deadline) return Error(ErrorCode::Timeout, "accept timeout");
      Sleep(2);
      continue;
    }
    if (err == WSAEINTR || err == WSAEINVAL || err == WSAENOTSOCK) {
      return Error(ErrorCode::ConnectionClosed, "accept interrupted");
    }
    return Error(ErrorCode::NetworkError, "accept failed: " + last_error_str());
#else
    int c = ::accept(operation.handle(), nullptr, nullptr);
    if (c >= 0) {
      return TcpSocket::from_raw(static_cast<SocketHandle>(c), "accepted");
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      if (now_millis() >= deadline) return Error(ErrorCode::Timeout, "accept timeout");
      usleep(2000);
      continue;
    }
    if (errno == EINTR) return Error(ErrorCode::ConnectionClosed, "accept interrupted");
    return Error(ErrorCode::NetworkError, "accept failed: " + last_error_str());
#endif
  }
}

void TcpListener::close() {
  close_state(state_, false);
}

}  // namespace cf::net
