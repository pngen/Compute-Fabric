#include "framework.h"

#include "compute_fabric/core/identifiers.h"
#include "compute_fabric/protocol/wire.h"

using namespace cf;

TEST(id_roundtrip_hex) {
  Id128 a(0x0123456789ABCDEFull, 0xFEDCBA9876543210ull);
  std::string hex = a.to_hex();
  EXPECT_EQ(hex.size(), 32ull);
  auto b = Id128::from_hex(hex);
  EXPECT_TRUE(b.ok());
  EXPECT_EQ(a, b.value());
}

TEST(id_from_hex_bad) {
  EXPECT_FALSE(Id128::from_hex("short").ok());
  EXPECT_FALSE(Id128::from_hex("ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ").ok());
}

TEST(id_bytes_roundtrip) {
  Id128 a(0x1111111122222222ull, 0x3333333344444444ull);
  uint8_t buf[16];
  a.to_bytes(buf);
  Id128 b = Id128::from_bytes(buf);
  EXPECT_EQ(a, b);
}

TEST(id_zero_and_random) {
  EXPECT_TRUE(Id128::zero().is_zero());
  Id128 a = Id128::random();
  Id128 b = Id128::random();
  EXPECT_NE(a, b);
  EXPECT_FALSE(a.is_zero());
}

TEST(id_ordering_deterministic) {
  Id128 a(1, 0);
  Id128 b(2, 0);
  Id128 c(1, 1);
  EXPECT_TRUE(a < b);
  EXPECT_TRUE(a < c);
  EXPECT_TRUE(c < b);
}

TEST(id_wire_serialization) {
  proto::BinaryWriter w;
  Id128 a(0xDEADBEEFCAFEBABEull, 0x1234567890ABCDEFull);
  w.put_id(a);
  auto bytes = w.bytes();
  EXPECT_EQ(bytes.size(), 16ull);
  proto::BinaryReader r(bytes);
  Id128 b;
  EXPECT_TRUE(r.read_id(b));
  EXPECT_EQ(a, b);
  EXPECT_TRUE(r.remaining() == 0);
}

TEST(id_truncated_read_fails) {
  proto::BinaryWriter w;
  w.put_id(Id128::random());
  w.put_u8(0xAA);
  proto::BinaryReader r(w.bytes());
  Id128 a;
  uint64_t u = 0;
  uint32_t u32 = 0;
  EXPECT_TRUE(r.read_id(a));
  EXPECT_FALSE(r.read_u64(u));
  EXPECT_FALSE(r.read_u32(u32));
}

TEST(id_hash_stable) {
  Id128 a(1, 2);
  Id128 b(1, 2);
  std::hash<Id128> h;
  EXPECT_EQ(h(a), h(b));
}