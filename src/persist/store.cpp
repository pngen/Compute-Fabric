#include "compute_fabric/persist/store.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

#if defined(_WIN32)
#include <io.h>
#include <windows.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "compute_fabric/core/digest.h"
#include "compute_fabric/protocol/serialize.h"

namespace cf {

namespace {

constexpr uint32_t kStoreMagic = 0x43464653u;  // "CFFS"

bool file_exists(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  return f.good();
}

bool read_file(const std::string& path, std::vector<uint8_t>& data) {
  std::ifstream f(path, std::ios::binary | std::ios::ate);
  if (!f) return false;
  std::streamsize size = f.tellg();
  if (size < 0) return false;
  f.seekg(0, std::ios::beg);
  data.resize(static_cast<size_t>(size));
  if (!data.empty()) {
    f.read(reinterpret_cast<char*>(data.data()), size);
  }
  return f.good() || f.eof();
}

bool write_file_atomic(const std::string& path,
                       const std::vector<uint8_t>& data) {
  std::string tmp = path + ".tmp";
  {
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    if (!f) return false;
    f.write(reinterpret_cast<const char*>(data.data()),
            static_cast<std::streamsize>(data.size()));
    f.flush();
    if (!f) return false;
  }
#if defined(_WIN32)
  if (!MoveFileExA(tmp.c_str(), path.c_str(),
                   MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
    return false;
  }
#else
  if (std::rename(tmp.c_str(), path.c_str()) != 0) return false;
#endif
  return true;
}

struct StoreRecord {
  uint8_t tag;
  std::vector<uint8_t> payload;
};

constexpr uint8_t kRecEpoch = 1;
constexpr uint8_t kRecWorkload = 2;
constexpr uint8_t kRecTask = 3;
constexpr uint8_t kRecTaskState = 4;
constexpr uint8_t kRecAttempt = 5;
constexpr uint8_t kRecRetryCount = 6;
constexpr uint8_t kRecPlacement = 7;

}  // namespace

Store::Store(std::string path) : path_(std::move(path)) {}

Store::~Store() = default;

Result<void> Store::ensure_dir() const {
  std::string dir = path_;
  auto pos = dir.find_last_of("/\\");
  if (pos != std::string::npos) {
    dir = dir.substr(0, pos);
  } else {
    dir = ".";
  }
#if defined(_WIN32)
  if (CreateDirectoryA(dir.c_str(), nullptr) == 0) {
    DWORD err = GetLastError();
    if (err != ERROR_ALREADY_EXISTS) {
      return Error(ErrorCode::IoError, "cannot create store directory: " + dir);
    }
  }
#else
  if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
    return Error(ErrorCode::IoError, "cannot create store directory: " + dir);
  }
#endif
  return {};
}

Result<std::unique_ptr<Store>> Store::open(const std::string& store_dir,
                                           bool create_if_missing) {
  // store_dir may be a directory or a file path; treat as directory.
  std::string path = store_dir;
  if (!path.empty() && path.back() != '/' && path.back() != '\\') {
    path += "";
  }
  std::string file = path + "/coordinator.store";
  auto store = std::unique_ptr<Store>(new Store(file));
  auto r = store->ensure_dir();
  if (r.failed()) return r.error();
  if (!file_exists(file)) {
    if (!create_if_missing) {
      return Error(ErrorCode::IoError, "store does not exist: " + file);
    }
    // Initialize an empty store.
    DurableState empty;
    auto sv = store->save(empty);
    if (sv.failed()) return sv.error();
  }
  return store;
}

Result<DurableState> Store::load() {
  std::vector<uint8_t> bytes;
  if (!read_file(path_, bytes)) {
    return Error(ErrorCode::IoError, "cannot read store file: " + path_);
  }
  if (bytes.size() < 4 + 4 + 8 + 4) {
    return Error(ErrorCode::CorruptionDetected, "store file too small");
  }
  if (bytes.size() > kMaxFileSize) {
    return Error(ErrorCode::CorruptionDetected, "store file exceeds size bound");
  }
  proto::BinaryReader r(bytes);
  uint32_t magic = 0;
  uint32_t version = 0;
  uint64_t body_size = 0;
  uint32_t body_crc = 0;
  if (!r.read_u32(magic) || !r.read_u32(version) || !r.read_u64(body_size) ||
      !r.read_u32(body_crc)) {
    return Error(ErrorCode::CorruptionDetected, "store header truncated");
  }
  if (magic != kStoreMagic) {
    return Error(ErrorCode::CorruptionDetected, "store magic mismatch");
  }
  if (version != kFormatVersion) {
    return Error(ErrorCode::PersistenceError, "store format version unsupported");
  }
  if (r.remaining() != body_size) {
    return Error(ErrorCode::CorruptionDetected, "store body size mismatch (truncated)");
  }
  uint32_t actual_crc = Crc32::compute(bytes.data() + r.pos(), body_size);
  if (actual_crc != body_crc) {
    return Error(ErrorCode::CorruptionDetected, "store body checksum mismatch");
  }

  proto::BinaryReader br(bytes.data() + r.pos(), body_size);
  uint32_t record_count = 0;
  if (!br.read_u32(record_count) || record_count > (1u << 20)) {
    return Error(ErrorCode::CorruptionDetected, "store record count invalid");
  }
  DurableState state;
  std::vector<StoreRecord> records;
  records.reserve(record_count);
  for (uint32_t i = 0; i < record_count; ++i) {
    StoreRecord rec;
    if (!br.read_u8(rec.tag)) return Error(ErrorCode::CorruptionDetected, "record tag truncated");
    uint32_t plen = 0;
    if (!br.read_u32(plen) || plen > kMaxFileSize) {
      return Error(ErrorCode::CorruptionDetected, "record length invalid");
    }
    uint32_t pcrc = 0;
    if (!br.read_u32(pcrc)) return Error(ErrorCode::CorruptionDetected, "record crc truncated");
    if (br.remaining() < plen) {
      return Error(ErrorCode::CorruptionDetected, "record body truncated");
    }
    rec.payload.resize(plen);
    std::memcpy(rec.payload.data(), bytes.data() + r.pos() + br.pos(), plen);
    br.skip(plen);
    uint32_t c = Crc32::compute(rec.payload.data(), rec.payload.size());
    if (c != pcrc) {
      return Error(ErrorCode::CorruptionDetected, "record payload checksum mismatch");
    }
    records.push_back(std::move(rec));
  }

  for (const auto& rec : records) {
    proto::BinaryReader er(rec.payload);
    switch (rec.tag) {
      case kRecEpoch: {
        if (!er.read_u64(state.epoch)) {
          return Error(ErrorCode::CorruptionDetected, "epoch record malformed");
        }
        break;
      }
      case kRecWorkload: {
        auto wl = proto::deserialize_workload(er);
        if (wl.failed()) return Error(ErrorCode::CorruptionDetected, "bad workload record");
        state.workloads[wl.value().id] = wl.value();
        break;
      }
      case kRecTask: {
        auto t = proto::deserialize_task(er);
        if (t.failed()) return Error(ErrorCode::CorruptionDetected, "bad task record");
        state.tasks[t.value().id] = t.value();
        break;
      }
      case kRecTaskState: {
        Id128 id;
        uint8_t s = 0;
        if (!er.read_id(id) || !er.read_u8(s)) {
          return Error(ErrorCode::CorruptionDetected, "bad task state record");
        }
        state.task_states[id] = static_cast<TaskState>(s);
        break;
      }
      case kRecAttempt: {
        auto a = proto::deserialize_attempt(er);
        if (a.failed()) return Error(ErrorCode::CorruptionDetected, "bad attempt record");
        if (a.value().committed) {
          state.attempts[a.value().task_id].push_back(a.value());
        }
        break;
      }
      case kRecRetryCount: {
        Id128 id;
        uint32_t c = 0;
        if (!er.read_id(id) || !er.read_u32(c)) {
          return Error(ErrorCode::CorruptionDetected, "bad retry record");
        }
        state.retry_counts[id] = c;
        break;
      }
      case kRecPlacement: {
        StoredPlacement sp;
        if (!er.read_id(sp.task_id) || !er.read_u64(sp.sequence)) {
          return Error(ErrorCode::CorruptionDetected, "bad placement record");
        }
        auto d = proto::deserialize_placement(er);
        if (d.failed()) return Error(ErrorCode::CorruptionDetected, "bad placement record");
        sp.decision = d.value();
        state.placements.push_back(std::move(sp));
        break;
      }
      default:
        return Error(ErrorCode::CorruptionDetected, "unknown record tag");
    }
  }
  return state;
}

Result<void> Store::save(const DurableState& state) {
  proto::BinaryWriter body;
  std::vector<uint8_t> payload;
  {
    proto::BinaryWriter w;

    // Record count prefix (epoch + workloads + tasks + task_states +
    // committed attempts + retry counts + placements).
    uint32_t record_count = 1;
    record_count += static_cast<uint32_t>(state.workloads.size());
    record_count += static_cast<uint32_t>(state.tasks.size());
    record_count += static_cast<uint32_t>(state.task_states.size());
    for (const auto& [tid, attempts] : state.attempts) {
      (void)tid;
      record_count += static_cast<uint32_t>(attempts.size());
    }
    record_count += static_cast<uint32_t>(state.retry_counts.size());
    record_count += static_cast<uint32_t>(state.placements.size());
    w.put_u32(record_count);

    // Epoch
    {
      proto::BinaryWriter rec;
      rec.put_u64(state.epoch);
      payload = rec.take_bytes();
      w.put_u8(kRecEpoch);
      w.put_u32(static_cast<uint32_t>(payload.size()));
      w.put_u32(Crc32::compute(payload.data(), payload.size()));
      w.put_bytes(payload.data(), payload.size());
    }
    for (const auto& [id, wl] : state.workloads) {
      (void)id;
      proto::BinaryWriter rec;
      proto::serialize_workload(rec, wl);
      payload = rec.take_bytes();
      w.put_u8(kRecWorkload);
      w.put_u32(static_cast<uint32_t>(payload.size()));
      w.put_u32(Crc32::compute(payload.data(), payload.size()));
      w.put_bytes(payload.data(), payload.size());
    }
    for (const auto& [id, t] : state.tasks) {
      (void)id;
      proto::BinaryWriter rec;
      proto::serialize_task(rec, t);
      payload = rec.take_bytes();
      w.put_u8(kRecTask);
      w.put_u32(static_cast<uint32_t>(payload.size()));
      w.put_u32(Crc32::compute(payload.data(), payload.size()));
      w.put_bytes(payload.data(), payload.size());
    }
    for (const auto& [id, s] : state.task_states) {
      proto::BinaryWriter rec;
      rec.put_id(id);
      rec.put_u8(static_cast<uint8_t>(s));
      payload = rec.take_bytes();
      w.put_u8(kRecTaskState);
      w.put_u32(static_cast<uint32_t>(payload.size()));
      w.put_u32(Crc32::compute(payload.data(), payload.size()));
      w.put_bytes(payload.data(), payload.size());
    }
    for (const auto& [tid, attempts] : state.attempts) {
      for (const auto& a : attempts) {
        (void)tid;
        proto::BinaryWriter rec;
        proto::serialize_attempt(rec, a);
        payload = rec.take_bytes();
        w.put_u8(kRecAttempt);
        w.put_u32(static_cast<uint32_t>(payload.size()));
        w.put_u32(Crc32::compute(payload.data(), payload.size()));
        w.put_bytes(payload.data(), payload.size());
      }
    }
    for (const auto& [id, c] : state.retry_counts) {
      proto::BinaryWriter rec;
      rec.put_id(id);
      rec.put_u32(c);
      payload = rec.take_bytes();
      w.put_u8(kRecRetryCount);
      w.put_u32(static_cast<uint32_t>(payload.size()));
      w.put_u32(Crc32::compute(payload.data(), payload.size()));
      w.put_bytes(payload.data(), payload.size());
    }
    for (const auto& sp : state.placements) {
      proto::BinaryWriter rec;
      rec.put_id(sp.task_id);
      rec.put_u64(sp.sequence);
      proto::serialize_placement(rec, sp.decision);
      payload = rec.take_bytes();
      w.put_u8(kRecPlacement);
      w.put_u32(static_cast<uint32_t>(payload.size()));
      w.put_u32(Crc32::compute(payload.data(), payload.size()));
      w.put_bytes(payload.data(), payload.size());
    }
    body = w;
  }

  std::vector<uint8_t> file_bytes;
  {
    proto::BinaryWriter w;
    w.put_u32(kStoreMagic);
    w.put_u32(kFormatVersion);
    w.put_u64(body.size());
    w.put_u32(Crc32::compute(body.bytes().data(), body.bytes().size()));
    w.put_bytes(body.bytes().data(), body.bytes().size());
    file_bytes = w.take_bytes();
  }

  if (!write_file_atomic(path_, file_bytes)) {
    return Error(ErrorCode::IoError, "failed to write store file: " + path_);
  }
  return {};
}

}  // namespace cf