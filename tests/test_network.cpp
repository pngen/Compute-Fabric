#include "framework.h"

#include <condition_variable>
#include <mutex>
#include <thread>

#include "compute_fabric/net/channel.h"
#include "compute_fabric/net/socket.h"
#include "compute_fabric/protocol/messages.h"

using namespace cf;

namespace {
std::vector<uint8_t> payload(const char* s) {
  return std::vector<uint8_t>(s, s + std::strlen(s));
}
}  // namespace

TEST(net_socket_connect_echo) {
  net::sockets_init();
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 8);
  EXPECT_TRUE(listener.ok());
  uint16_t port = listener.value().port();

  std::thread server([&] {
    auto client = listener.value().accept(5000);
    if (!client.ok()) {
      std::fprintf(stderr, "  [server] accept failed: %s\n",
                   client.error().to_string().c_str());
      return;
    }
    uint8_t buf[64] = {0};
    auto n = client.value().recv_some(buf, 64, 5000);
    if (n.failed()) {
      std::fprintf(stderr, "  [server] recv failed: %s\n",
                   n.error().to_string().c_str());
      return;
    }
    auto s = client.value().send_all(buf, n.value(), 5000);
    if (s.failed()) {
      std::fprintf(stderr, "  [server] send failed: %s\n",
                   s.error().to_string().c_str());
    }
  });

  auto sock = net::TcpSocket::connect("127.0.0.1", port, 5000);
  EXPECT_TRUE(sock.ok());
  if (sock.ok()) {
    const uint8_t byte = 0;
    auto expired = sock.value().send_all(&byte, 1, 0);
    EXPECT_TRUE(expired.failed());
    if (expired.failed()) {
      EXPECT_EQ(expired.error().code(), ErrorCode::Timeout);
    }
    const char* msg = "ping";
    auto sr = sock.value().send_all(reinterpret_cast<const uint8_t*>(msg), 4, 5000);
    EXPECT_OK(sr);
    uint8_t buf[64] = {0};
    auto r = sock.value().recv_some(buf, 64, 5000);
    if (r.failed()) {
      std::fprintf(stderr, "  [client] recv failed: %s\n",
                   r.error().to_string().c_str());
    }
    EXPECT_TRUE(r.ok());
    EXPECT_EQ(r.value(), 4ull);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), 4), std::string("ping"));
  }
  server.join();
  listener.value().close();
}

TEST(net_channel_frame_roundtrip) {
  net::sockets_init();
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 8);
  EXPECT_TRUE(listener.ok());
  uint16_t port = listener.value().port();
  std::mutex accept_mu;
  std::condition_variable accept_cv;
  bool server_accepted = false;

  std::thread server([&] {
    // Retry accept on timeout so the test tolerates thread-scheduling races.
    net::TcpSocket client;
    for (int i = 0; i < 20; ++i) {
      auto c = listener.value().accept(500);
      if (c.ok()) {
        client = c.take();
        break;
      }
    }
    if (!client.valid()) {
      std::fprintf(stderr, "  [frame] server accept failed\n");
      return;
    }
    net::Channel ch(std::move(client));
    {
      std::lock_guard<std::mutex> lk(accept_mu);
      server_accepted = true;
    }
    accept_cv.notify_one();
    // The client deliberately waits after accept. On Windows this proves the
    // accepted socket was restored to blocking mode instead of immediately
    // returning WSAEWOULDBLOCK from the first receive.
    auto frame = ch.recv_frame(1000, proto::kMaxFramePayload);
    if (frame.failed()) {
      std::fprintf(stderr, "  [frame] server recv_frame failed: %s\n",
                   frame.error().to_string().c_str());
    }
    EXPECT_TRUE(frame.ok());
    if (frame.ok()) {
      EXPECT_EQ(frame.value().header.message_type, proto::kHello);
      EXPECT_OK(ch.send_frame(frame.value(), 5000));
    }
  });

  auto sock = net::TcpSocket::connect("127.0.0.1", port, 5000);
  EXPECT_TRUE(sock.ok());
  if (!sock.ok()) {
    server.join();
    listener.value().close();
    return;
  }
  net::Channel ch(sock.take());
  {
    std::unique_lock<std::mutex> lk(accept_mu);
    EXPECT_TRUE(accept_cv.wait_for(lk, std::chrono::seconds(2),
                                   [&] { return server_accepted; }));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(75));
  auto f = proto::make_frame(proto::kHello, proto::SenderKind::Client,
                             Id128::random(), 42, payload("hi"));
  auto sr = ch.send_frame(f, 5000);
  if (sr.failed()) {
    std::fprintf(stderr, "  [client] send_frame failed: %s\n",
                 sr.error().to_string().c_str());
  }
  EXPECT_OK(sr);
  auto resp = ch.recv_frame(5000, proto::kMaxFramePayload);
  if (resp.failed()) {
    std::fprintf(stderr, "  [client] recv_frame failed: %s\n",
                 resp.error().to_string().c_str());
  }
  EXPECT_TRUE(resp.ok());
  EXPECT_EQ(resp.value().header.request_id, 42ull);
  server.join();
  listener.value().close();
}

TEST(net_channel_partial_read_handling) {
  net::sockets_init();
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 8);
  EXPECT_TRUE(listener.ok());
  uint16_t port = listener.value().port();

  // A large frame (>= 64 KiB) forces the receiver to assemble the payload
  // across many recv calls, exercising partial-read handling.
  const size_t big = 64 * 1024 + 13;
  std::vector<uint8_t> big_payload(big);
  for (size_t i = 0; i < big_payload.size(); ++i) big_payload[i] = static_cast<uint8_t>(i & 0xFF);

  std::thread server([&] {
    net::TcpSocket client;
    for (int i = 0; i < 20; ++i) {
      auto c = listener.value().accept(500);
      if (c.ok()) {
        client = c.take();
        break;
      }
    }
    if (!client.valid()) {
      std::fprintf(stderr, "  [partial] server accept failed\n");
      return;
    }
    auto f = proto::make_frame(proto::kTaskList, proto::SenderKind::Client,
                               Id128::random(), 9, big_payload);
    std::vector<uint8_t> wire(proto::kHeaderSize + f.payload.size());
    proto::encode_header(f.header, wire.data());
    std::memcpy(wire.data() + proto::kHeaderSize, f.payload.data(),
                f.payload.size());
    auto r = client.send_all(wire.data(), wire.size(), 5000);
    if (r.failed()) {
      std::fprintf(stderr, "  [partial] server send failed: %s\n",
                   r.error().to_string().c_str());
    }
  });

  auto sock = net::TcpSocket::connect("127.0.0.1", port, 5000);
  EXPECT_TRUE(sock.ok());
  if (!sock.ok()) {
    server.join();
    listener.value().close();
    return;
  }
  net::Channel ch(sock.take());
  auto resp = ch.recv_frame(5000, proto::kMaxFramePayload);
  if (resp.failed()) {
    std::fprintf(stderr, "  [partial] client recv_frame failed: %s\n",
                 resp.error().to_string().c_str());
  }
  EXPECT_TRUE(resp.ok());
  EXPECT_EQ(resp.value().header.request_id, 9ull);
  EXPECT_EQ(resp.value().header.message_type, proto::kTaskList);
  EXPECT_EQ(resp.value().payload.size(), big);
  server.join();
  listener.value().close();
}

TEST(net_channel_malformed_frame_rejected) {
  net::sockets_init();
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 8);
  EXPECT_TRUE(listener.ok());
  uint16_t port = listener.value().port();

  std::thread server([&] {
    auto client = listener.value().accept(5000);
    EXPECT_TRUE(client.ok());
    // Send garbage bytes.
    const char* garbage = "this is not a frame at all";
    client.value().send_all(reinterpret_cast<const uint8_t*>(garbage),
                            std::strlen(garbage), 5000);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  });

  auto sock = net::TcpSocket::connect("127.0.0.1", port, 5000);
  EXPECT_TRUE(sock.ok());
  net::Channel ch(sock.take());
  auto resp = ch.recv_frame(1000, proto::kMaxFramePayload);
  EXPECT_TRUE(resp.ok() == false);  // either malformed or closed
  server.join();
  listener.value().close();
}

TEST(net_channel_oversized_payload_rejected) {
  net::sockets_init();
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 8);
  EXPECT_TRUE(listener.ok());
  uint16_t port = listener.value().port();

  std::thread server([&] {
    auto client = listener.value().accept(5000);
    EXPECT_TRUE(client.ok());
    auto f = proto::make_frame(proto::kTaskSubmit, proto::SenderKind::Client,
                               Id128::random(), 1, payload("x"));
    f.header.payload_length = 1u << 25;  // 32 MiB > 8 MiB limit
    uint8_t wire[proto::kHeaderSize];
    proto::encode_header(f.header, wire);
    client.value().send_all(wire, proto::kHeaderSize, 5000);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  });

  auto sock = net::TcpSocket::connect("127.0.0.1", port, 5000);
  EXPECT_TRUE(sock.ok());
  net::Channel ch(sock.take());
  auto resp = ch.recv_frame(1000, proto::kMaxFramePayload);
  EXPECT_TRUE(resp.failed());
  if (resp.failed()) {
    EXPECT_EQ(resp.error().code(), ErrorCode::MalformedFrame);
  }
  server.join();
  listener.value().close();
}

TEST(net_channel_timeout) {
  net::sockets_init();
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 8);
  EXPECT_TRUE(listener.ok());
  uint16_t port = listener.value().port();

  std::thread server([&] {
    auto client = listener.value().accept(5000);
    EXPECT_TRUE(client.ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  });

  auto sock = net::TcpSocket::connect("127.0.0.1", port, 5000);
  EXPECT_TRUE(sock.ok());
  net::Channel ch(sock.take());
  auto resp = ch.recv_frame(200, proto::kMaxFramePayload);
  EXPECT_TRUE(resp.failed());
  if (resp.failed()) {
    EXPECT_EQ(resp.error().code(), ErrorCode::Timeout);
  }
  server.join();
  listener.value().close();
}

TEST(net_repeated_connects) {
  net::sockets_init();
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 8);
  EXPECT_TRUE(listener.ok());
  uint16_t port = listener.value().port();
  std::atomic<int> accepted{0};
  std::thread server([&] {
    for (int i = 0; i < 3; ++i) {
      auto c = listener.value().accept(5000);
      if (c.ok()) {
        accepted.fetch_add(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        c.take().close();
      }
    }
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  for (int i = 0; i < 3; ++i) {
    auto sock = net::TcpSocket::connect("127.0.0.1", port, 3000);
    if (sock.failed()) {
      std::fprintf(stderr, "  [reconnect] connect %d failed: %s\n", i,
                   sock.error().to_string().c_str());
    }
    EXPECT_TRUE(sock.ok());
    if (sock.ok()) sock.value().close();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server.join();
  listener.value().close();
  EXPECT_EQ(accepted.load(), 3);
}

TEST(net_accept_timeout_returns) {
  net::sockets_init();
  auto listener = net::TcpListener::bind("127.0.0.1", 0, 8);
  EXPECT_TRUE(listener.ok());
  int64_t t0 = now_millis();
  auto r = listener.value().accept(500);
  int64_t elapsed = now_millis() - t0;
  EXPECT_TRUE(r.failed());
  EXPECT_EQ(r.error().code(), ErrorCode::Timeout);
  EXPECT_GE(elapsed, 300);
  EXPECT_LE(elapsed, 3000);
  listener.value().close();
}

TEST(net_listener_connect_refused) {
  net::sockets_init();
  auto sock = net::TcpSocket::connect("127.0.0.1", 1, 500);
  EXPECT_TRUE(sock.failed());
}
