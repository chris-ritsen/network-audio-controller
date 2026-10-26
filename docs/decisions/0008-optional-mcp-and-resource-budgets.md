# Optional MCP and low resource operation

Date: 2026-10-22

Status: Accepted requirements; implementation available, Raspberry Pi hardware
qualification pending.

## Context

Chris requires separate HTTP and MCP benchmarks because most users will not
run MCP, and requires NetAudio to be practical on Raspberry Pi class hardware.
The first native deployment reported combined service CPU and daemon resident
memory shortly after startup. Those measurements do not establish component
cost, settled memory use, or low-end hardware suitability.

## Decision

MCP is an optional capability of the shared daemon. HTTP and HTTPS use the same
domain services without requiring the MCP catalog, OAuth endpoints, or MCP app
session task. The implementation selects MCP with `[daemon] mcp = true`; an
absent setting disables it. Existing installations using MCP must explicitly
enable it before adopting this default. Service metadata and discovery advertise
only enabled capabilities. Splitting processes is unnecessary merely to make
MCP optional and would duplicate domain state or require another IPC contract.

Report HTTP-only, MCP enabled without clients, and active MCP separately.
Distinguish daemon and optional audio workers, resident and proportional memory,
service memory accounting, CPU as a fraction of one core, startup, steady state,
and request load. Benchmark complete servers in isolated environments with
stated device inventory and workloads. Retain full evidence outside Git.

Chris selected a Raspberry Pi 4 with 1 GB RAM as the target on 2026-10-22.
The deployment must leave room for its operating system and other work. Being
below 1 GB on a desktop is not sufficient qualification.
Actual hardware, numerical budgets and sustained workload results remain
explicit until qualified; constrained desktop runs are only supporting evidence.

On 2026-10-23 Chris requested further MCP optimization. The journal owns one
bounded overview read model, invalidated by committed event, issue and gap
changes. It is constructed on demand and reused without polling. Its event
projection omits raw device payloads that overview presentation does not use;
the authoritative journal and ordinary history reads retain complete records.
MCP keeps one current overview and device-list presentation, keyed by source
record identity, journal identity, management state and local calendar context.
Weak source references avoid retaining old device payloads. Diagnostics whose
freshness depends on elapsed time, host observations and request timestamps
are evaluated on every request. Implementation acceptance remains separate
from this optimization requirement.

## Consequences

On 2026-10-26 Chris required the daemon to be as lean as possible, with every
retained megabyte justified, and described approximately 100 MB total as his
expectation. This is an optimization target for the representative installation,
not evidence of an achieved budget or a limit that may discard observations.
Retained evidence must remain complete. Allocation profiles must distinguish
useful retained data, temporary representations, and allocator retention. Avoid
periodic allocator trimming or restarts as memory management. Implementation
changes and measured savings remain subject to working-behavior acceptance.

Clock history retains immutable evidence as serialized JSON, shared by its
dependent observations. Reads decode it only when a consumer needs to inspect
its fields; serialization passes the original evidence through. This preserves
all evidence while avoiding a permanently allocated JSON tree per sample.
Diagnostic responses should serialize from retained typed records instead of
constructing a second expanded tree of the complete history.

Ordinary HTTP users avoid initializing unused MCP services. MCP users enable
the capability explicitly and retain its current routes and authentication.
Performance changes must preserve device information, history, event ordering,
and live audio. New tests wait for Chris's acceptance of working behavior.
