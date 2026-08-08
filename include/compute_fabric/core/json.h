#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace cf {

// Minimal JSON DOM + writer. Deterministic object ordering (std::map).
// Used for telemetry (JSON Lines) and CLI output. Not a full parser.
class Json {
 public:
  using Array = std::vector<Json>;
  using Object = std::map<std::string, Json>;

  enum class Type { Null, Bool, Int, Double, String, Array, Object };

  Json() : value_(NullValue{}) {}
  Json(std::nullptr_t) : value_(NullValue{}) {}
  Json(bool b) : value_(b) {}
  Json(int v) : value_(static_cast<int64_t>(v)) {}
  Json(int64_t v) : value_(v) {}
  Json(uint64_t v) : value_(static_cast<int64_t>(v)) {}
  Json(double v) : value_(v) {}
  Json(const char* s) : value_(std::string(s)) {}
  Json(std::string s) : value_(std::move(s)) {}

  static Json make_object() {
    Json j;
    j.value_ = Object{};
    return j;
  }
  static Json make_array() {
    Json j;
    j.value_ = Array{};
    return j;
  }

  Type type() const { return static_cast<Type>(value_.index()); }

  bool is_null() const { return type() == Type::Null; }
  bool is_bool() const { return type() == Type::Bool; }
  bool is_int() const { return type() == Type::Int; }
  bool is_double() const { return type() == Type::Double; }
  bool is_string() const { return type() == Type::String; }
  bool is_array() const { return type() == Type::Array; }
  bool is_object() const { return type() == Type::Object; }

  bool as_bool() const { return std::get<bool>(value_); }
  int64_t as_int() const {
    if (std::holds_alternative<int64_t>(value_)) return std::get<int64_t>(value_);
    if (std::holds_alternative<double>(value_)) return static_cast<int64_t>(std::get<double>(value_));
    return 0;
  }
  double as_double() const {
    if (std::holds_alternative<double>(value_)) return std::get<double>(value_);
    if (std::holds_alternative<int64_t>(value_)) return static_cast<double>(std::get<int64_t>(value_));
    return 0.0;
  }
  const std::string& as_string() const { return std::get<std::string>(value_); }

  Array& array() { return std::get<Array>(value_); }
  const Array& array() const { return std::get<Array>(value_); }
  Object& object() { return std::get<Object>(value_); }
  const Object& object() const { return std::get<Object>(value_); }

  Json& set(const std::string& key, Json value) {
    object()[key] = std::move(value);
    return *this;
  }
  Json& push(Json value) {
    array().push_back(std::move(value));
    return *this;
  }

  // Compact serialization (deterministic).
  std::string dump() const;
  // Pretty serialization with 2-space indentation.
  std::string dump_pretty() const;

 private:
  struct NullValue {};
  using Value =
      std::variant<NullValue, bool, int64_t, double, std::string, Array, Object>;

  Value value_;
};

}  // namespace cf