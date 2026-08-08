#include "framework.h"

#include <cstring>
#include <exception>

namespace cftest {

std::vector<TestCase>& registry() {
  static std::vector<TestCase> reg;
  return reg;
}

void register_test(const char* name, void (*fn)()) {
  registry().push_back({name, fn});
}

Context& context() {
  static Context ctx;
  return ctx;
}

void report_failure(const char* file, int line, const std::string& msg) {
  ++context().failures;
  std::fprintf(stderr, "    FAIL %s:%d: %s\n", file, line, msg.c_str());
  std::fflush(stderr);
}

int run_all(const char* filter) {
  int failed_tests = 0;
  int checks = 0;
  std::fprintf(stderr, "[SUITE] starting with %zu registered tests\n",
               cftest::registry().size());
  std::fflush(stderr);
  for (const auto& tc : registry()) {
    if (filter && std::strstr(tc.name, filter) == nullptr) continue;
    context().name = tc.name;
    context().failures = 0;
    context().checks = 0;
    std::fprintf(stderr, "[TEST] %s\n", tc.name);
    std::fflush(stderr);
    try {
      tc.fn();
    } catch (const std::exception& e) {
      std::fprintf(stderr, "  -> EXCEPTION during test: %s\n", e.what());
      std::fflush(stderr);
      ++context().failures;
      ++failed_tests;
    } catch (...) {
      std::fprintf(stderr, "  -> UNKNOWN EXCEPTION during test\n");
      std::fflush(stderr);
      ++context().failures;
      ++failed_tests;
    }
    checks += context().checks;
    if (context().failures > 0) {
      std::fprintf(stderr, "  -> %d failure(s) in %d check(s)\n",
                   context().failures, context().checks);
      ++failed_tests;
    }
  }
  std::fprintf(stderr, "=== %zu tests, %d checks, %d failed test(s) ===\n",
               registry().size(), checks, failed_tests);
  std::fflush(stderr);
  return failed_tests;
}

}  // namespace cftest