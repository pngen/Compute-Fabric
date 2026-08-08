#include "compute_fabric/core/json.h"

#include <cinttypes>
#include <cstdio>
#include <sstream>

namespace cf {

namespace {

void escape_string(std::string& out, const std::string& s) {
  out.push_back('"');
  for (char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\b': out += "\\b"; break;
      case '\f': out += "\\f"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned>(c));
          out += buf;
        } else {
          out.push_back(c);
        }
    }
  }
  out.push_back('"');
}

void dump_rec(std::string& out, const Json& j, int indent, bool pretty) {
  switch (j.type()) {
    case Json::Type::Null: out += "null"; break;
    case Json::Type::Bool: out += j.as_bool() ? "true" : "false"; break;
    case Json::Type::Int: {
      char buf[32];
      std::snprintf(buf, sizeof(buf), "%" PRId64, j.as_int());
      out += buf;
      break;
    }
    case Json::Type::Double: {
      double d = j.as_double();
      if (d == static_cast<int64_t>(d)) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%" PRId64 ".0", static_cast<int64_t>(d));
        out += buf;
      } else {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.9g", d);
        out += buf;
      }
      break;
    }
    case Json::Type::String: escape_string(out, j.as_string()); break;
    case Json::Type::Array: {
      out.push_back('[');
      bool first = true;
      for (const auto& item : j.array()) {
        if (!first) out.push_back(',');
        first = false;
        if (pretty) {
          out.push_back('\n');
          out.append(indent * 2, ' ');
        }
        dump_rec(out, item, pretty ? indent + 1 : 0, pretty);
      }
      if (pretty && !j.array().empty()) {
        out.push_back('\n');
        out.append((indent - 1) * 2, ' ');
      }
      out.push_back(']');
      break;
    }
    case Json::Type::Object: {
      out.push_back('{');
      bool first = true;
      for (const auto& [k, v] : j.object()) {
        if (!first) out.push_back(',');
        first = false;
        if (pretty) {
          out.push_back('\n');
          out.append(indent * 2, ' ');
        }
        escape_string(out, k);
        out.push_back(':');
        if (pretty) out.push_back(' ');
        dump_rec(out, v, pretty ? indent + 1 : 0, pretty);
      }
      if (pretty && !j.object().empty()) {
        out.push_back('\n');
        out.append((indent - 1) * 2, ' ');
      }
      out.push_back('}');
      break;
    }
  }
}

}  // namespace

std::string Json::dump() const {
  std::string out;
  dump_rec(out, *this, 0, false);
  return out;
}

std::string Json::dump_pretty() const {
  std::string out;
  dump_rec(out, *this, 1, true);
  return out;
}

}  // namespace cf