# Native host audio providers

Date: 2026-10-22

Status: Proposed implementation of the accepted native-daemon requirement.

## Context

The existing daemon observes JACK, PulseAudio, local sound cards and peer
daemons. It controls explicitly requested local connections and settings,
measures signal levels, and traces the combined host and network audio graph.
JACK calls can block inside a native client library. PulseAudio has its own
callback and object ownership requirements. Neither library should become a
startup dependency of an unrelated network-device operation.

## Decision

Keep typed observations, graph construction and control planning in the shared
Rust core. Give JACK and PulseAudio separate native worker executables and
process lifetimes. A shared worker library owns bounded framing and transport;
the daemon owns each child, its request queue, generation, deadlines and
cleanup. Library absence or a stalled provider makes that capability explicitly
unavailable without taking down device discovery or HTTP service.

Workers subscribe before acquiring initial state, publish ordered changes, and
resynchronize explicitly after lost continuity. Native audio callbacks and
socket or filesystem events drive updates. Workers never start an audio server.
Configuration and peer discovery retain their existing public contracts.

Commands use stable identities, fresh preflight and readback. Reconnection never
replays a write. A deadline after dispatch preserves an uncertain result. Meter
inputs and recording callbacks have explicit owners and bounded resources;
they neither produce audio nor modify unrelated connections.

Finite measurements use a separately owned JACK worker process for each
request. Its inputs activate before connecting; connection notifications and
fresh identity checks gate capture. The real-time callback uses preallocated
state and counts the requested frames. Source changes, xruns and discontinuous
frame clocks invalidate the reading. Consumer cancellation closes the owned
inputs and reaps the worker. Multi-source targets measure the actual combined
signal rather than estimating it from separately recorded source peaks.

Optional level recording follows connected output identities from the graph
provider and uses a separate recording process. Configuration changes and xruns
end that capture and create a new one from current source evidence. A failed
unchanged configuration waits for a source event or an explicit read request;
there is no periodic restart. Callback-owned accumulators publish whole batches
through a fixed-capacity queue. Overflow stops capture with an explicit error.
The control thread serializes records after leaving the real-time callback.

JACK cycle timestamps map capture coverage to UTC second boundaries. Partial
windows, invalid samples and missing coverage remain explicit. Full records,
including amplitude, energy, frame count and source identities, are appended to
SQLite before updating the existing rolling level-history projection. Only
complete valid windows count as recorded seconds. Cursor pages expose retained
records without deleting older observations. An unclosed session is marked as
interrupted on startup; its unobserved stop time is not invented. Retaining full
records consumes disk space; storage failure is reported and stops that capture.

Signal tracing shares typed graph construction, name resolution, traversal and
level interpretation through the core library. Node identities retain distinct
device records even when names collide. Edges distinguish observed connections,
reported subscriptions, configured mappings and inferred application routing.
Presentation limits never discard the underlying graph. Missing observations
remain unknown rather than becoming claims of silence or working audio.

Optional external display, volume-stage and power observations use bounded
native Redis reads only when requested. Display records require a matching,
unexpired observer lease. These observations do not grant control authority.
Peer measurements retain separate capture windows; a large trace uses bounded
batches and reports that timing limitation instead of claiming simultaneity. Peer
readings preserve their reported fields; absent capture timestamps and window
metadata remain absent. A zero-frame capture never establishes silence.

Network summaries reuse the same diagnostic, journal and host-condition views.
Reported late-packet totals identify how many devices supplied observations;
unobserved devices never contribute invented zero counts. Names shared by
different managed servers or device records remain distinct.

## Consequences

Provider isolation allows bounded cleanup even when a native library stalls and
keeps optional libraries out of the daemon executable. It adds child-process
supervision and an internal typed transport. Release packaging must install
matching worker binaries with the daemon and verify their build identities.

In-process worker threads were considered, but cannot bound a stuck native
call's lifetime. Shell commands and Python helper processes would leave the
requested native rewrite incomplete. Separate workers reuse the native
libraries directly and preserve domain logic in one shared implementation.

This record does not accept new behavior on Chris's behalf. Existing observable
contracts must be exercised with isolated providers, then the complete working
implementation remains subject to Chris's acceptance before new tests encode it.

The systemd service uses `KillMode=mixed`: SIGTERM reaches the daemon first so
it can drain recorder output and stop its owned workers through their protocol.
The service manager still force-kills remaining cgroup members after its stop
deadline. The default control-group SIGTERM races the daemon's drain by killing
workers simultaneously; the native cutover exposed that race and retained an
explicit missing-final-record error rather than claiming complete coverage.

During worker shutdown, the supervisor continues consuming the bounded output
queue while awaiting process exit. A full queue must not block a graph worker
inside its native callbacks and prevent a recorder from deactivating. The same
exit and forced-cleanup deadlines apply. A dense private graph reproduced the
shutdown failure that a small virtual graph did not expose.

Host-audio consumers finish before their graph providers shut down. Recording
and measurement workers close their inputs and persist final results while the
observers continue consuming JACK graph changes. The provider processes then
close, within the existing overall deadline. Simultaneous cancellation allowed
the JACK observer's client-close request to block the recorder's deactivation;
a traced private graph with concurrent port churn reproduced that failure.
Consumer failure still drains the remaining owners and providers and is
reported explicitly. No delay or longer deadline substitutes for this ordering.
