# Contributing

Thank you for considering contributing to Compute Fabric.

## Ground rules

- Compute Fabric is a serious systems infrastructure project. Favor
  correctness, explicit ownership, deterministic behavior, inspectability, and
  honest evidence over cleverness.
- Keep placement and execution vendor-neutral. Backend-specific behavior belongs
  behind the executor interfaces.
- Do not claim functionality that is not implemented or tested. Unsupported
  capabilities must report unsupported.
- Do not weaken tests or extend timeouts to hide lifecycle defects; fix the
  implementation.
- Keep release builds warning-clean with warnings-as-errors enabled.

## Development workflow

1. Fork the repository and create a feature branch.
2. Make focused changes with clear commit messages.
3. Build and run the CPU test suite:

   ```powershell
   cmake -S . -B build -DCOMPUTE_FABRIC_ENABLE_CUDA=OFF
   cmake --build build --config Release
   ctest --test-dir build -C Release --output-on-failure
   ```

4. If the change affects CUDA, build and test with a real supported NVIDIA GPU:

   ```powershell
   cmake -S . -B build-cuda -G Ninja `
     -DCMAKE_BUILD_TYPE=Release `
     -DCOMPUTE_FABRIC_ENABLE_CUDA=ON `
     -DCMAKE_CUDA_ARCHITECTURES=120
   cmake --build build-cuda
   ctest --test-dir build-cuda --output-on-failure
   ```

5. Run affected examples and benchmark modes, then open a pull request.

## Code style

- Use modern C++20 and follow the surrounding style.
- Prefer the standard library and narrow OS abstractions; avoid unnecessary
  mandatory dependencies.
- Preserve exclusive ownership of process handles, sockets, connections,
  reservations, attempts, and accelerator resources.
- Keep platform-specific behavior behind narrow interfaces.
- Add comments only when they clarify non-obvious invariants.

## Testing expectations

- New invariants should be covered by native tests in `tests/`.
- Multi-node behavior should be exercised through the real multi-process
  fabric, not only in-process fixtures.
- Lifecycle changes must demonstrate bounded startup, shutdown, and zero orphan
  processes.
- CUDA claims must correspond to real hardware execution with deterministic
  verification and leak checks.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0 (see `LICENSE`). No CLA is required.
