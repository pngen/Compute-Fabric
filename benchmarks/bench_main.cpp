#include <cstdio>

#include "compute_fabric/cli/benchmark.h"

int main(int argc, const char* const* argv) {
  std::string mode;
  std::string cli_path;
#if defined(CF_CLI_PATH)
  cli_path = CF_CLI_PATH;
#endif
  bool cuda = false;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--cuda") cuda = true;
    else if (a == "--quick" || a == "quick") mode = "quick";
    else mode = a;
  }
  return cf::run_benchmark_suite(mode, cuda, cli_path);
}
