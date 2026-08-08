# Compute Fabric

Compute Fabric answers one question:

> **Where should the next computation run?**

It is a vendor-neutral distributed execution-placement runtime for
heterogeneous AI compute. It accepts bounded task descriptions, chooses an
eligible executor using capability, capacity, queue pressure, placement
preferences, and state-locality signals, and tracks each execution attempt
through completion.

Compute Fabric occupies a specific layer in a larger stack:

- **FlashTier** — where the bytes live.
- **Context Fabric** — where accumulated reusable computational state lives.
- **Compute Fabric** — where the next computation should execute.

It is not Kubernetes, Slurm, Ray, an AIGOS clone, or a general-purpose
orchestration platform.

## What is implemented

- A coordinator with a durable task DAG, attempt history, reservations,
  idempotent client requests, node health, drain control, retry handling, and
  deterministic baseline or cost-aware placement.
- CPU and NVIDIA CUDA executor backends behind one executor contract.
- Capability and constraint filtering for CPU/CUDA type, CUDA compute
  capability, slots, scratch memory, required tags, and forbidden nodes, plus
  scoring preferences for nodes and tags.
- Placement costs for execution, queueing, state transfer, recomputation,
  resource pressure, and affinity. Tasks can carry standalone state-dependency
  metadata, and the code defines a narrow provider interface for future
  Context Fabric integration.
- A versioned, checksummed framed TCP control protocol with bounded frames and
  bounded I/O.
- Durable coordinator recovery, atomic store rewrites, structured telemetry,
  task cancellation, node-loss retry, and stale attempt/session rejection.
- `LocalFabric`, which starts a real coordinator and real node child processes
  on localhost and owns their shutdown and reap lifecycle.
- Native tests, nine executable examples, and CPU/CUDA benchmark modes.

The included workloads are deterministic validation kernels. Compute Fabric
does not yet provide a general arbitrary-code packaging or deployment layer.

## Supported configurations

The build permits Windows x64 and Linux x64. The release evidence for this
workspace was collected on Windows x64; Linux was not rerun in this validation
pass. Host code is compiled with warnings treated as errors by default.

CUDA is explicit: when `COMPUTE_FABRIC_ENABLE_CUDA=ON`, configuration fails if
the CUDA toolkit is unavailable instead of silently substituting the CPU
backend. The CUDA path requires CUDA 12.8 or newer and defaults to architecture
`120` (Blackwell). It has been validated with CUDA 12.9 on an NVIDIA GeForce
RTX 5090 at compute capability 12.0.

### CPU-only build

```powershell
cmake -S . -B build -DCOMPUTE_FABRIC_ENABLE_CUDA=OFF
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

### CUDA build

On Windows, run these commands from an x64 Visual Studio developer shell when
using Ninja:

```powershell
cmake -S . -B build-cuda -G Ninja `
  -DCMAKE_BUILD_TYPE=Release `
  -DCOMPUTE_FABRIC_ENABLE_CUDA=ON `
  -DCMAKE_CUDA_ARCHITECTURES=120
cmake --build build-cuda
ctest --test-dir build-cuda --output-on-failure
```

For other CUDA architectures, set `CMAKE_CUDA_ARCHITECTURES` explicitly.

## CLI quick start

The examples below use the Windows Release path. With a single-config
generator, use the executable directly from its build directory.

```powershell
# Inspect compiled executor capabilities.
.\build\Release\compute-fabric.exe capabilities

# Start a self-validating two-node CPU fabric.
.\build\Release\compute-fabric.exe fabric local --nodes 2 `
  --store-dir .compute_fabric_store_local

# Start a self-validating fabric whose first node has a CUDA executor.
.\build-cuda\compute-fabric.exe fabric local --nodes 2 --cuda-node `
  --store-dir .compute_fabric_store_cuda

# Run benchmark modes.
.\build\Release\compute_fabric_benchmarks.exe
.\build-cuda\compute_fabric_benchmarks.exe --cuda
```

The CLI also exposes standalone `coordinator` and `node` processes, task
submit/inspect/list/cancel commands, fabric and node status, node drain, and
placement queries. Run `compute-fabric` without arguments for the command
summary.

## Architecture

1. Clients send task and control requests to the coordinator.
2. The coordinator core thread owns task, node, connection, reservation, and
   placement state; reader threads only enqueue validated commands.
3. The scheduler excludes incompatible nodes, scores eligible candidates, and
   creates an epoch- and session-bound reservation and execution attempt.
4. A node executes the attempt on its bounded CPU or CUDA backend and reports
   start, completion, integrity, transfer, and release information.
5. Durable state records committed progress. On coordinator restart, the epoch
   advances so stale authority cannot commit work.

Connections, process handles, and CUDA allocations have explicit owners.
Shutdown first drains bounded control messages, then closes transports, joins
threads, and reaps child processes; remaining children are force-terminated
within a bounded fallback window.

## Examples

The build produces examples for minimal execution, multi-node placement,
state-locality placement, node-loss retry, priority, persistence recovery,
CUDA-required execution through `LocalFabric`, mandatory CUDA placement, and
placement-query cost breakdowns for modeled transfer-versus-recompute
scenarios. CUDA examples must be run from a CUDA-enabled build to exercise the
GPU path.

The benchmark suite is validation-oriented rather than a general performance
study. It combines real local CPU/CUDA execution and real local child-process
fabrics with synthetic in-memory placement and cost scenarios.

## Current limitations

- The demonstrated multi-process fabric is local-host only. Cross-machine
  deployment has not been validated here.
- The TCP control plane does not currently provide TLS, peer authentication,
  or authorization; do not expose its ports to untrusted networks.
- Durable state is a local coordinator store, not a replicated consensus or
  high-availability service.
- Nodes exit when an established coordinator transport is permanently lost;
  an external supervisor must restart them.
- Preferred nodes and tags influence placement scores. The task schema's
  anti-affinity, same-node, and different-node fields are serialized metadata
  but are not yet enforced by placement.
- `LocalFabric` discovers free ports before spawning the coordinator, leaving
  a small bind-after-probe race with unrelated local processes.
- CUDA validation currently targets NVIDIA CUDA and Blackwell `sm_120`; other
  vendors would require another executor backend, and other CUDA architectures
  require an explicit build setting and validation.
- Placement and execution are the product boundary. Cluster provisioning,
  container scheduling, service discovery, and arbitrary application rollout
  are intentionally out of scope.
