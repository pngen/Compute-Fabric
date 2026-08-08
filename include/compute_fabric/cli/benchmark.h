#pragma once

#include <string>

namespace cf {

// Runs the deterministic benchmark suite. mode: "" (all) or "quick".
// Returns process exit code.
int run_benchmark_suite(const std::string& mode, bool cuda,
                        const std::string& cli_path = {});

}  // namespace cf
