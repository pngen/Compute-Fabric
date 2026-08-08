#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace cf {

using Clock = std::chrono::steady_clock;
using TimePoint = std::chrono::steady_clock::time_point;
using WallClock = std::chrono::system_clock;
using WallTimePoint = std::chrono::system_clock::time_point;

// Monotonic milliseconds since an arbitrary origin.
int64_t now_millis();

// Monotonic microseconds since an arbitrary origin.
int64_t now_micros();

// Wall-clock Unix timestamp in milliseconds.
int64_t unix_millis();

// Wall-clock Unix timestamp in seconds as a string for telemetry.
std::string unix_iso8601();

// Realtime wall timestamp in milliseconds (for telemetry).
int64_t realtime_millis();

double seconds_between(TimePoint start, TimePoint end);

}  // namespace cf