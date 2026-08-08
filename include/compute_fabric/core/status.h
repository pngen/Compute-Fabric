#pragma once

#include <cstdint>
#include <string>
#include <utility>

namespace cf {

// Typed error code. Values are stable across releases.
enum class ErrorCode : uint32_t {
  Ok = 0,
  InvalidArgument = 1,
  OutOfBounds = 2,
  MalformedFrame = 3,
  ProtocolVersionMismatch = 4,
  Timeout = 5,
  NetworkError = 6,
  ConnectionClosed = 7,
  NotConnected = 8,
  IllegalTransition = 9,
  TaskNotFound = 10,
  WorkloadNotFound = 11,
  DependencyCycle = 12,
  SelfDependency = 13,
  MissingDependency = 14,
  DependencyFailed = 15,
  DependencyCancelled = 16,
  NodeNotFound = 17,
  NodeOffline = 18,
  NodeDraining = 19,
  StaleSession = 20,
  StaleEpoch = 21,
  StaleAttempt = 22,
  NoCapacity = 23,
  ReservationConflict = 24,
  ReservationNotFound = 25,
  ExecutorUnavailable = 26,
  CapabilityMismatch = 27,
  NoSchedulableNode = 28,
  PersistenceError = 29,
  CorruptionDetected = 30,
  IoError = 31,
  CudaError = 32,
  Unsupported = 33,
  Internal = 34,
  Cancelled = 35,
  DeadlineExceeded = 36,
  AlreadyExists = 37,
  InvalidState = 38,
  DuplicateRequest = 39,
  ShuttingDown = 40,
  LaunchError = 41,
  ProcessError = 42,
  IdempotentReplay = 43,
};

const char* error_code_name(ErrorCode code);
const char* error_code_message(ErrorCode code);

class Error {
 public:
  Error() : code_(ErrorCode::Ok) {}
  Error(ErrorCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  static Error success() { return Error(); }

  bool ok() const { return code_ == ErrorCode::Ok; }
  bool failed() const { return !ok(); }
  ErrorCode code() const { return code_; }
  const std::string& message() const { return message_; }

  std::string to_string() const {
    return std::string(error_code_name(code_)) + ": " + message_;
  }

 private:
  ErrorCode code_;
  std::string message_;
};

// Minimal Result<T> with value-or-error semantics. No exceptions.
template <typename T>
class Result {
 public:
  Result(T value) : data_(std::move(value)), error_() {}
  Result(Error error) : data_(), error_(std::move(error)) {}

  static Result ok(T value) { return Result(std::move(value)); }
  static Result err(ErrorCode code, std::string message) {
    return Result(Error(code, std::move(message)));
  }
  static Result err(const Error& e) { return Result(e); }

  bool ok() const { return error_.ok(); }
  bool failed() const { return error_.failed(); }
  const Error& error() const { return error_; }
  T& value() { return data_.value(); }
  const T& value() const { return data_.value(); }
  T take() { return std::move(data_.value()); }
  T value_or(T fallback) const {
    return ok() ? data_.value() : std::move(fallback);
  }

 private:
  struct MaybeValue {
    MaybeValue() {}
    MaybeValue(T v) : filled(true), v(std::move(v)) {}
    bool filled = false;
    T v{};
    T& value() { return v; }
    const T& value() const { return v; }
  };
  MaybeValue data_;
  Error error_;
};

// Specialization for void results.
template <>
class Result<void> {
 public:
  Result() : error_() {}
  Result(Error error) : error_(std::move(error)) {}
  static Result success() { return Result(); }
  static Result err(ErrorCode code, std::string message) {
    return Result(Error(code, std::move(message)));
  }
  static Result err(const Error& e) { return Result(e); }
  bool ok() const { return error_.ok(); }
  bool failed() const { return error_.failed(); }
  const Error& error() const { return error_; }

 private:
  Error error_;
};

inline Error make_error(ErrorCode code, std::string message) {
  return Error(code, std::move(message));
}

}  // namespace cf