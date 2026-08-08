#include "framework.h"

int main(int argc, const char* const* argv) {
  const char* filter = nullptr;
  if (argc > 1) filter = argv[1];
  int failed = cftest::run_all(filter);
  return failed == 0 ? 0 : 1;
}
