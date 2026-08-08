#include "compute_fabric/core/time_util.h"

#include <chrono>
#include <ctime>

namespace cf {

int64_t now_millis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             Clock::now().time_since_epoch())
      .count();
}

int64_t now_micros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             Clock::now().time_since_epoch())
      .count();
}

int64_t unix_millis() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             WallClock::now().time_since_epoch())
      .count();
}

int64_t realtime_millis() { return unix_millis(); }

std::string unix_iso8601() {
  auto now = WallClock::now();
  std::time_t t = WallClock::to_time_t(now);
  std::tm tm{};
#if defined(_WIN32)
  gmtime_s(&tm, &t);
#else
  gmtime_r(&t, &tm);
#endif
  char buf[40];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return std::string(buf);
}

double seconds_between(TimePoint start, TimePoint end) {
  return std::chrono::duration<double>(end - start).count();
}

}  // namespace cf