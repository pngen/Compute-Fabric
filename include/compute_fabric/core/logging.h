#pragma once

#include <cstdarg>
#include <cstdio>
#include <string>

namespace cf {

// Logging to stderr with a level prefix. Deterministic and bounded.
enum class LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3 };

class Logger {
 public:
  static Logger& instance();
  void set_level(LogLevel level) { level_ = level; }
  LogLevel level() const { return level_; }
  void set_tag(const std::string& tag) { tag_ = tag; }

  void log(LogLevel level, const char* fmt, ...);
  void debug(const char* fmt, ...);
  void info(const char* fmt, ...);
  void warn(const char* fmt, ...);
  void error(const char* fmt, ...);

 private:
  Logger() = default;
  LogLevel level_ = LogLevel::Info;
  std::string tag_;
};

}  // namespace cf