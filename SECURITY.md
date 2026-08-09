# Security

## Reporting a vulnerability

Compute Fabric is infrastructure exposed to network input. If you believe you
have found a security vulnerability, please report it privately to the project
maintainers rather than opening a public issue.

Use GitHub's private security-advisory flow for this repository when available.
Otherwise, contact the maintainer through the GitHub profile and request a
private reporting channel before sharing sensitive details.

Provide, if possible:

- A description of the vulnerability and its impact.
- The affected version, operating system, and configuration.
- Steps to reproduce, including a minimal proof of concept when practical.
- Any suggested fix.

Do not include secrets, private keys, or real customer data in the report.

## Supported versions

Security fixes are applied to the latest release and the `master` branch.

| Version | Supported |
|---------|-----------|
| 1.x     | Yes       |

## Trust model

Compute Fabric 1.x operates under an explicit **trusted-cluster network**
model:

- The TCP control plane has no authentication, authorization, or encryption.
- The coordinator is a single configured authority; it is not
  Byzantine-tolerant or highly available.
- Persisted coordinator state is checksummed but not encrypted at rest.
- Telemetry is written only to operator-configured local files and is not sent
  to an external service.

Deploy Compute Fabric only on networks whose participants are trusted. Do not
expose coordinator or node ports to untrusted networks.

## Guarantees we maintain

- Framed protocol parsing enforces size bounds, checksums, and typed decoding.
- Socket operations, connection queues, process startup, and shutdown paths are
  bounded.
- Connection, process-handle, reservation, attempt, and CUDA-allocation
  ownership is explicit and tested.
- Stale coordinator epochs and node sessions cannot commit execution results.
- Malformed input and unsupported capabilities return errors rather than
  silently changing execution backends.

## Future work

Authenticated and encrypted transports, replicated coordinator state, and
cross-host deployment validation remain future work. Until then, the trust
model above applies.
