#pragma once

namespace cf {

// Version and public metadata. Kept in sync with CMake project version.
struct Version {
  int major;
  int minor;
  int patch;
};

constexpr int CF_VERSION_MAJOR = 1;
constexpr int CF_VERSION_MINOR = 0;
constexpr int CF_VERSION_PATCH = 0;

inline Version version() {
  return Version{CF_VERSION_MAJOR, CF_VERSION_MINOR, CF_VERSION_PATCH};
}

inline const char* version_string() { return "1.0.0"; }
inline const char* project_name() { return "Compute Fabric"; }

}  // namespace cf
