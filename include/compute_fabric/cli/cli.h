#pragma once

#include <cstdint>
#include <string>

namespace cf {

// Entry point for the compute-fabric CLI. Returns a process exit code.
int cli_main(int argc, const char* const* argv);

// Installs Ctrl+C / SIGINT handling. The callback parameter is retained for
// source compatibility; interruption is observed by the blocking CLI commands
// and converted into an orderly runtime stop.
void install_interrupt_handler(void (*fn)());

}  // namespace cf
