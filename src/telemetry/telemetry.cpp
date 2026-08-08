#include "compute_fabric/telemetry/telemetry.h"

#include <cstdio>

#include "compute_fabric/core/time_util.h"

namespace cf {

Telemetry::~Telemetry() {
  std::lock_guard<std::mutex> lk(mu_);
  if (file_.is_open()) file_.flush();
}

void Telemetry::configure(const std::string& json_path, bool human) {
  std::lock_guard<std::mutex> lk(mu_);
  json_path_ = json_path;
  human_ = human;
  if (!json_path_.empty()) {
    file_.open(json_path_, std::ios::out | std::ios::app);
  }
  configured_ = true;
}

void Telemetry::event(const std::string& name, const Json& fields,
                      uint64_t coordinator_epoch) {
  std::lock_guard<std::mutex> lk(mu_);
  ++seq_;
  Json envelope = Json::make_object()
                      .set("schema", kSchemaVersion)
                      .set("seq", static_cast<int64_t>(seq_))
                      .set("ts", static_cast<int64_t>(unix_millis()))
                      .set("event", name);
  if (coordinator_epoch != 0) {
    envelope.set("coordinator_epoch", static_cast<int64_t>(coordinator_epoch));
  }
  for (const auto& [k, v] : fields.object()) {
    envelope.set(k, v);
  }
  std::string line = envelope.dump();
  if (file_.is_open()) {
    file_ << line << "\n";
  }
  if (human_) {
    std::printf("telemetry %s\n", line.c_str());
    std::fflush(stdout);
  }
}

void Telemetry::flush() {
  std::lock_guard<std::mutex> lk(mu_);
  if (file_.is_open()) file_.flush();
}

}  // namespace cf