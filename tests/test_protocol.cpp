#include "framework.h"

#include <cstring>

#include "compute_fabric/core/digest.h"
#include "compute_fabric/protocol/frame.h"
#include "compute_fabric/protocol/messages.h"
#include "compute_fabric/protocol/serialize.h"

using namespace cf;

namespace {
std::vector<uint8_t> payload_of(const std::string& s) {
  return std::vector<uint8_t>(s.begin(), s.end());
}
}  // namespace

TEST(protocol_frame_roundtrip) {
  auto f = proto::make_frame(proto::kTaskSubmit, proto::SenderKind::Client,
                             Id128::random(), 7, payload_of("hello"));
  EXPECT_EQ(f.header.magic, proto::kFrameMagic);
  EXPECT_EQ(f.header.version, proto::kProtocolVersion);
  EXPECT_OK(proto::validate_frame(f));
  uint8_t hdr[proto::kHeaderSize];
  proto::encode_header(f.header, hdr);
  auto decoded = proto::decode_header(hdr, proto::kHeaderSize);
  EXPECT_TRUE(decoded.ok());
  EXPECT_EQ(decoded.value().message_type, proto::kTaskSubmit);
  EXPECT_EQ(decoded.value().request_id, 7ull);
  EXPECT_EQ(decoded.value().sender_id, f.header.sender_id);
}

TEST(protocol_bad_magic_rejected) {
  auto f = proto::make_frame(proto::kTaskSubmit, proto::SenderKind::Client,
                             Id128::random(), 1, payload_of("x"));
  f.header.magic = 0xDEADBEEF;
  auto r = proto::validate_frame(f);
  EXPECT_TRUE(r.failed());
  EXPECT_EQ(r.error().code(), ErrorCode::MalformedFrame);
}

TEST(protocol_bad_version_rejected) {
  auto f = proto::make_frame(proto::kTaskSubmit, proto::SenderKind::Client,
                             Id128::random(), 1, payload_of("x"));
  f.header.version = 999;
  auto r = proto::validate_frame(f);
  EXPECT_TRUE(r.failed());
  EXPECT_EQ(r.error().code(), ErrorCode::ProtocolVersionMismatch);
}

TEST(protocol_bad_checksum_rejected) {
  auto f = proto::make_frame(proto::kTaskSubmit, proto::SenderKind::Client,
                             Id128::random(), 1, payload_of("x"));
  f.header.payload_checksum ^= 0xFFFFFFFF;
  auto r = proto::validate_frame(f);
  EXPECT_TRUE(r.failed());
}

TEST(protocol_oversized_payload_rejected) {
  auto f = proto::make_frame(proto::kTaskSubmit, proto::SenderKind::Client,
                             Id128::random(), 1, payload_of("x"));
  f.header.payload_length = proto::kMaxFramePayload + 1;
  auto r = proto::validate_frame(f);
  EXPECT_TRUE(r.failed());
}

TEST(protocol_header_checksum_detects_tamper) {
  auto f = proto::make_frame(proto::kTaskSubmit, proto::SenderKind::Client,
                             Id128::random(), 1, payload_of("x"));
  uint8_t hdr[proto::kHeaderSize];
  proto::encode_header(f.header, hdr);
  hdr[6] ^= 0xFF;  // tamper message_type byte
  auto d = proto::decode_header(hdr, proto::kHeaderSize);
  EXPECT_TRUE(d.failed());
}

TEST(protocol_short_header_rejected) {
  uint8_t tiny[10] = {0};
  auto d = proto::decode_header(tiny, 10);
  EXPECT_TRUE(d.failed());
}

TEST(protocol_message_hello_roundtrip) {
  proto::Hello m;
  m.role = "node";
  proto::BinaryWriter w;
  proto::serialize(w, m);
  proto::BinaryReader r(w.bytes());
  auto out = proto::deserialize_hello(r);
  EXPECT_TRUE(out.ok());
  EXPECT_EQ(out.value().role, std::string("node"));
}

TEST(protocol_task_serialization_roundtrip) {
  ComputeTask t;
  t.id = Id128::random();
  t.workload = Id128::random();
  t.name = "roundtrip-task";
  t.task_class = TaskClass::Inference;
  t.required_executor = ExecutorType::Cuda;
  t.min_compute_capability_major = 12;
  t.required_cpu_slots = 2;
  t.required_gpu_slots = 1;
  t.scratch_memory_bytes = 1 << 20;
  t.dependencies = {Id128::random(), Id128::random()};
  StateDependency dep;
  dep.state_id = "ctx/fred";
  dep.generation = "v3";
  dep.size_bytes = 4096;
  dep.resident_nodes = {Id128::random()};
  dep.transfer_estimate_seconds = 0.5;
  dep.recompute_estimate_seconds = 2.0;
  t.state_dependencies.push_back(dep);
  t.expected_input_bytes = 1000;
  t.expected_output_bytes = 2000;
  t.estimated_duration_seconds = 1.5;
  t.has_deadline = true;
  t.deadline_epoch_ms = 123456789;
  t.priority = Priority::High;
  t.retry.max_attempts = 3;
  t.retry.retry_on_node_loss = true;
  t.retry.retry_backoff_ms = 500;
  t.preferred_nodes = {Id128::random()};
  t.forbidden_nodes = {Id128::random()};
  t.required_tags = {"gpu"};
  t.preferred_tags = {"fast"};
  t.anti_affinity_group = "group-x";
  t.same_node_as_task = Id128::random();
  t.different_node_from_task = Id128::random();
  t.expected_transfer_gb = 2.5;
  t.expected_recompute_cost_seconds = 3.5;
  t.cost_estimate = 0.01;
  t.preemptible = true;
  t.kernel = KernelType::VectorTransform;
  t.kernel_param_n = 1000;
  t.kernel_param_a = 3;
  t.kernel_param_b = 4;
  t.seed = 99;
  t.user_tags = {{"model", "gpt"}, {"batch", "8"}};
  t.payload = {1, 2, 3, 4};

  proto::BinaryWriter w;
  proto::serialize_task(w, t);
  proto::BinaryReader r(w.bytes());
  auto out = proto::deserialize_task(r);
  EXPECT_TRUE(out.ok());
  auto& t2 = out.value();
  EXPECT_EQ(t2.id, t.id);
  EXPECT_EQ(t2.workload, t.workload);
  EXPECT_EQ(t2.name, t.name);
  EXPECT_EQ(t2.task_class, t.task_class);
  EXPECT_EQ(t2.required_executor, t.required_executor);
  EXPECT_EQ(t2.min_compute_capability_major, 12);
  EXPECT_EQ(t2.required_cpu_slots, 2u);
  EXPECT_EQ(t2.scratch_memory_bytes, 1u << 20);
  EXPECT_EQ(t2.dependencies.size(), 2ull);
  EXPECT_EQ(t2.state_dependencies.size(), 1ull);
  EXPECT_EQ(t2.state_dependencies[0].state_id, std::string("ctx/fred"));
  EXPECT_EQ(t2.state_dependencies[0].generation, std::string("v3"));
  EXPECT_EQ(t2.state_dependencies[0].transfer_estimate_seconds, 0.5);
  EXPECT_EQ(t2.estimated_duration_seconds, 1.5);
  EXPECT_TRUE(t2.has_deadline);
  EXPECT_EQ(t2.priority, Priority::High);
  EXPECT_EQ(t2.retry.max_attempts, 3u);
  EXPECT_EQ(t2.required_tags.size(), 1ull);
  EXPECT_EQ(t2.anti_affinity_group, std::string("group-x"));
  EXPECT_TRUE(t2.same_node_as_task.has_value());
  EXPECT_TRUE(t2.different_node_from_task.has_value());
  EXPECT_TRUE(t2.preemptible);
  EXPECT_EQ(t2.kernel, KernelType::VectorTransform);
  EXPECT_EQ(t2.user_tags.at("model"), std::string("gpt"));
  EXPECT_EQ(t2.payload.size(), 4ull);
}

TEST(protocol_attempt_serialization_roundtrip) {
  ExecutionAttempt a;
  a.id = Id128::random();
  a.task_id = Id128::random();
  a.node_id = Id128::random();
  a.node_session = Id128::random();
  a.coordinator_epoch = 5;
  a.reservation_id = Id128::random();
  a.executor = ExecutorType::Cuda;
  a.attempt_number = 2;
  a.dispatched_at_ms = 1000;
  a.started_at_ms = 1500;
  a.completed_at_ms = 1800;
  a.state = AttemptState::Succeeded;
  a.failure_kind = FailureKind::None;
  a.failure_reason = "";
  a.output_size = 16;
  a.output_integrity = "abcdef";
  a.cpu_millis = 12.5;
  a.bytes_transferred = 4096;
  a.device = "cuda:0";
  a.committed = true;

  proto::BinaryWriter w;
  proto::serialize_attempt(w, a);
  proto::BinaryReader r(w.bytes());
  auto out = proto::deserialize_attempt(r);
  EXPECT_TRUE(out.ok());
  auto& a2 = out.value();
  EXPECT_EQ(a2.id, a.id);
  EXPECT_EQ(a2.coordinator_epoch, 5ull);
  EXPECT_EQ(a2.output_integrity, std::string("abcdef"));
  EXPECT_EQ(a2.device, std::string("cuda:0"));
  EXPECT_TRUE(a2.committed);
}

TEST(protocol_placement_serialization_roundtrip) {
  PlacementDecision d;
  d.feasible = true;
  d.chosen_node = Id128::random();
  d.chosen_executor = ExecutorType::Cuda;
  d.chosen_device = "cuda:0";
  CandidateScore c;
  c.node_id = Id128::random();
  c.executor = ExecutorType::Cuda;
  c.device = "cuda:0";
  c.score = 1.25;
  c.execution_cost = 0.5;
  c.transfer_cost = 0.25;
  d.candidates.push_back(c);
  Exclusion e;
  e.node_id = Id128::random();
  e.reasons = {"offline", "capacity"};
  d.excluded.push_back(e);
  d.total_cost = 1.25;
  d.reason = "best";

  proto::BinaryWriter w;
  proto::serialize_placement(w, d);
  proto::BinaryReader r(w.bytes());
  auto out = proto::deserialize_placement(r);
  EXPECT_TRUE(out.ok());
  EXPECT_TRUE(out.value().feasible);
  EXPECT_EQ(out.value().chosen_node, d.chosen_node);
  EXPECT_EQ(out.value().candidates.size(), 1ull);
  EXPECT_EQ(out.value().candidates[0].executor, ExecutorType::Cuda);
  EXPECT_EQ(out.value().excluded.size(), 1ull);
  EXPECT_EQ(out.value().excluded[0].reasons.size(), 2ull);
  EXPECT_NEAR(out.value().total_cost, 1.25, 1e-9);
}

TEST(protocol_bounds_truncated_payload) {
  // A reader over a truncated buffer must fail cleanly.
  proto::BinaryWriter w;
  w.put_u32(1000);  // claims 1000-byte string
  proto::BinaryReader r(w.bytes());
  std::string s;
  EXPECT_FALSE(r.read_string(s, 1u << 20));
}

TEST(protocol_crc32_known_vector) {
  // CRC32("123456789") == 0xCBF43926.
  uint32_t crc = Crc32::compute("123456789", 9);
  EXPECT_EQ(crc, 0xCBF43926u);
}