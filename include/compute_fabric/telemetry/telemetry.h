#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>

#include "compute_fabric/core/json.h"

namespace cf {

// Local telemetry: JSON Lines to an optional file plus human-readable stdout.
// Never sends anything externally. Schema version is fixed at 1.
class Telemetry {
 public:
  Telemetry() = default;
  ~Telemetry();

  Telemetry(const Telemetry&) = delete;
  Telemetry& operator=(const Telemetry&) = delete;

  // `json_path` empty disables file output. `human` enables stdout lines.
  void configure(const std::string& json_path, bool human);
  bool enabled() const { return configured_; }

  // Emits an event. Fields are merged into an envelope with schema, sequence,
  // unix timestamp (ms) and (optional) epoch.
  void event(const std::string& name, const Json& fields,
             uint64_t coordinator_epoch = 0);

  void flush();

  uint64_t sequence() const { return seq_; }

  static constexpr int kSchemaVersion = 1;

 private:
  void emit(const std::string& name, const Json& fields, uint64_t epoch);

  bool configured_ = false;
  bool human_ = false;
  std::string json_path_;
  std::ofstream file_;
  mutable std::mutex mu_;
  uint64_t seq_ = 0;
};

}  // namespace cf