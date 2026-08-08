#include "compute_fabric/core/logging.h"

#include <cstdio>
#include <cstring>

namespace cf {

Logger& Logger::instance() {
  static Logger logger;
  return logger;
}

void Logger::log(LogLevel level, const char* fmt, ...) {
  if (level < level_) return;
  const char* prefix = "";
  switch (level) {
    case LogLevel::Debug: prefix = "DEBUG"; break;
    case LogLevel::Info: prefix = "INFO"; break;
    case LogLevel::Warn: prefix = "WARN"; break;
    case LogLevel::Error: prefix = "ERROR"; break;
  }
  char buf[2048];
  va_list args;
  va_start(args, fmt);
  std::vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  if (!tag_.empty()) {
    std::fprintf(stderr, "[%s] {%s} %s\n", prefix, tag_.c_str(), buf);
  } else {
    std::fprintf(stderr, "[%s] %s\n", prefix, buf);
  }
  std::fflush(stderr);
}

void Logger::debug(const char* fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char buf[2048];
  std::vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  log(LogLevel::Debug, "%s", buf);
}

void Logger::info(const char* fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char buf[2048];
  std::vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  log(LogLevel::Info, "%s", buf);
}

void Logger::warn(const char* fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char buf[2048];
  std::vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  log(LogLevel::Warn, "%s", buf);
}

void Logger::error(const char* fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char buf[2048];
  std::vsnprintf(buf, sizeof(buf), fmt, args);
  va_end(args);
  log(LogLevel::Error, "%s", buf);
}

}  // namespace cf