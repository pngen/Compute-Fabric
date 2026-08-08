#pragma once

#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace cftest {

struct TestCase {
  const char* name;
  void (*fn)();
};

std::vector<TestCase>& registry();
void register_test(const char* name, void (*fn)());

// Current test context.
struct Context {
  const char* name = "";
  int failures = 0;
  int checks = 0;
};
Context& context();
void report_failure(const char* file, int line, const std::string& msg);

// Runs all registered tests; returns the number of failed tests.
int run_all(const char* filter = nullptr);

}  // namespace cftest

#define TEST(name)                                                          \
  static void test_##name();                                                \
  [[maybe_unused]] static const bool cf_reg_##name =                        \
      (cftest::register_test(#name, &test_##name), true);                   \
  static void test_##name()

#define EXPECT_TRUE(cond)                                                   \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    if (!(cond)) {                                                          \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_TRUE failed: ") + #cond);  \
    }                                                                       \
  } while (0)

#define EXPECT_FALSE(cond)                                                  \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    if (cond) {                                                             \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_FALSE failed: ") + #cond); \
    }                                                                       \
  } while (0)

#define EXPECT_EQ(a, b)                                                     \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    auto va = (a);                                                          \
    auto vb = (b);                                                          \
    if (!(va == vb)) {                                                      \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_EQ failed: ") + #a + " == " + #b); \
    }                                                                       \
  } while (0)

#define EXPECT_NE(a, b)                                                     \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    auto va = (a);                                                          \
    auto vb = (b);                                                          \
    if (va == vb) {                                                         \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_NE failed: ") + #a + " != " + #b); \
    }                                                                       \
  } while (0)

#define EXPECT_OK(expr)                                                     \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    auto r_ = (expr);                                                       \
    if (r_.failed()) {                                                      \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_OK failed: ") + #expr +    \
                                 " -> " + r_.error().to_string());          \
    }                                                                       \
  } while (0)

#define EXPECT_FAIL(expr)                                                   \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    auto r_ = (expr);                                                       \
    if (r_.ok()) {                                                          \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_FAIL failed (was ok): ") + #expr); \
    }                                                                       \
  } while (0)

#define EXPECT_STR_CONTAINS(haystack, needle)                               \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    std::string h_ = (haystack);                                            \
    std::string n_ = (needle);                                              \
    if (h_.find(n_) == std::string::npos) {                                 \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_STR_CONTAINS failed: '") + \
                                 n_ + "' not in '" + h_ + "'");             \
    }                                                                       \
  } while (0)

#define EXPECT_NEAR(a, b, eps)                                              \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    double va_ = (a);                                                       \
    double vb_ = (b);                                                       \
    if (va_ < vb_ - (eps) || va_ > vb_ + (eps)) {                           \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_NEAR failed: ") + #a +     \
                                 " vs " + #b);                              \
    }                                                                       \
  } while (0)

#define EXPECT_GT(a, b)                                                     \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    if (!((a) > (b))) {                                                     \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_GT failed: ") + #a + " > " + #b); \
    }                                                                       \
  } while (0)

#define EXPECT_GE(a, b)                                                     \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    if (!((a) >= (b))) {                                                    \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_GE failed: ") + #a + " >= " + #b); \
    }                                                                       \
  } while (0)

#define EXPECT_LT(a, b)                                                     \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    if (!((a) < (b))) {                                                     \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_LT failed: ") + #a + " < " + #b); \
    }                                                                       \
  } while (0)

#define EXPECT_LE(a, b)                                                     \
  do {                                                                      \
    ++cftest::context().checks;                                             \
    if (!((a) <= (b))) {                                                    \
      cftest::report_failure(__FILE__, __LINE__,                            \
                             std::string("EXPECT_LE failed: ") + #a + " <= " + #b); \
    }                                                                       \
  } while (0)