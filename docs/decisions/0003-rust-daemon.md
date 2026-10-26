# Rust daemon with unchanged public interfaces

Date: 2026-10-20

Status: Accepted requirement; native implementation deployed on www on
2026-10-22. User acceptance and untested platform qualification remain separate.

## Context

The daemon implements HTTP, MCP, discovery and device orchestration in Python,
with protocol operations already shared through the Rust core. HTTP handlers
also own domain state, and MCP invokes those handlers through captured HTTP
responses. This couples service ownership to a transport and adds repeated
serialization and allocation.

Chris requested a Rust rewrite, explicitly retained the Python CLI, required a
clean cutover without API or CLI consumer changes, and identified daemon and
web-server efficiency as the purpose.

## Decision

Run the daemon and its HTTP, MCP, OAuth, discovery, monitoring and device
services in Rust. Retain the Python CLI and its command syntax. Reuse the
existing Rust protocol core. Preserve existing external routes, parameters,
payloads, status codes, event semantics, authentication, configuration and
discovery contracts.

Chris also authorized correcting defects during the rewrite. Preserve intended
consumer behavior rather than reproducing authorization races, lost updates,
invalid JSON or erroneous routing. Record intentional corrections separately
from compatibility failures in the comparison evidence.

HTTP and MCP call shared typed service operations. Device state has one owner;
subscribers receive ordered changes from a coherent snapshot. Replaceable
meter observations may be coalesced without discarding durable operation
results. External command acceptance and observed completion remain distinct.

The candidate runs separately during development. It does not proxy to a
Python engine. The installed Python daemon remains authoritative until the
replacement passes interface comparisons and operational verification.
Cutover replaces the daemon once, retains its public listeners and persisted
identities, and removes the superseded Python server implementation.

## Consequences

Consumers retain their current interfaces. Native protocol code and cached
read models can be shared without a Python process bridge. CPU, memory,
request latency and idle work must be measured against the existing daemon;
the implementation language alone is not performance evidence.

The replacement must include existing managed-device, host-audio and
monitoring behavior before cutover. A partial Rust server or a Rust listener
backed by Python does not satisfy this decision. Existing native audio
endpoint behavior and unrelated working-tree changes must be preserved.

SAP discovery receives the interface index and destination from the operating
system alongside each packet. A wildcard listener alone cannot establish which
joined interface delivered a packet. Only packets addressed to a configured
group on a joined interface enter the inventory. Network notifications update
memberships; announcement receipts reset individual monotonic expiry deadlines.
The service has no periodic interface scan or expiry sweep. Its REST, MCP and
SSE views share the same inventory and preserve the existing flow schema.

Managed inventory has a separate source capability gap: the existing Managed
API schema advertises no subscription root, while the established Controller
session provides signal-presence publications rather than a complete inventory
change stream. The candidate must not reproduce periodic inventory refreshes
or claim continuous freshness from bounded snapshots. On 2026-10-21 Chris
selected startup snapshots, explicit reads and command readback for managed
inventory. The service exposes observation time, refresh failure and stale
state. It does not retain the Python inventory refresh loop. Complete
event-driven inventory coverage remains pending a supported observer.

Native managed transports validate certificates and hostnames using system
trust plus explicitly supplied certificates. They do not reproduce the Python
Controller client's disabled certificate verification. Self-signed services
require trust configuration before qualification or cutover. Redirects and
automatic command retries remain disabled; a failed Controller exchange closes
its session and retains an uncertain result instead of replaying the command.

Controller identify and reboot use fresh managed permissions and an unambiguous
primary device identity within the selected domain. The transport records
whether a device request was fully written, not merely emitted by the core.
An interrupted write remains uncertain. A matching protocol acknowledgement
does not establish a visible identify indication or a completed reboot; HTTP,
MCP and the operation journal retain that distinction.

Controller status has separate ownership and freshness from Managed API
inventory. Bounded status reads reuse the validated session and core codecs.
The profile owns observations by domain, inventory identity and primary
Controller identity; profile replacement or membership changes discard them.
A failed read, ambiguous identity, disconnected owner or expired receipt
removes command-authorizing fields while retaining explicitly stale observation
evidence. Inventory refresh does not renew status receipt time. Individual
expiry deadlines publish state changes without sending background queries.
Optional switch-status failure remains visible beside available interface data.

Explicit clock refresh reads the full Controller clock record while keeping
Managed API preferred-leader configuration authoritative at every consumer.
The Controller-reported preference remains separate provenance. Explicit
transmit-flow inspection reuses the bounded ARC inventory state machine and
canonical flow projection, verifies channel counts around acquisition, and
retains its own receipt lifetime. Inventory refresh cannot renew either receipt;
failure, owner loss and expiry invalidate the corresponding current fields.

Managed preferred-leader write previews refresh the owning Managed API inventory
and recheck device ownership. They do not acquire a Controller connection merely
to preview the preference owned by the Managed API.

Optional Redis and D-Bus publication consume the same ordered read model as HTTP
and MCP. Redis keeps the existing record keys, publishes changes transactionally,
and uses RESP3 disconnect notifications with bounded reconnect backoff. Recovery
acquires a coherent snapshot; it does not poll device state or change Redis server
configuration. D-Bus retains its names, paths, properties and change signals.
Unavailable typed properties return an explicit observation error and are absent
from GetAll; property invalidation replaces fabricated zero or false observations.

Explicit capture and dissection observe existing device transports. Capture keeps
complete compressed packet payloads, source receipt timestamps, actual source and
destination addresses, session links and correlation evidence in the existing
SQLite schema. Its bounded writer stops capture visibly on storage failure or
overflow while device control remains independent. Existing database upgrades are
additive. Native dissection reports the shared core's decoded header and complete
payload; private research fact annotations remain a capture CLI concern.

The Python daemon command executes the packaged native binary. The same package
build installs the core library, daemon, endpoint and host-audio workers; the
daemon embeds its web assets and reports a source build identity. No Python
server is started by this command.

Managed AES67, analog, interface and redundancy configuration use those status observations and
shared domain planners. Each operation reads lock and settings again before
execution, checks the preview against those fresh facts, and sends at most one
write. Network preflight also reads platform capabilities and interface state,
plus switch choices when applicable. Network readback verifies both interfaces
and the previously selected redundancy status family so unrelated network
changes are visible. Command-result readback is a bounded set of queries, with
no automatic address change, reboot or retry. If the write exchange
fails, a replacement session may perform that read without replaying the write.
Receipts retain uncertain transmission and acknowledgement even when readback
observes the requested value. Configured AES67 state is distinct from effective
state and does not establish persistence or trigger a reboot. Configuration-only
confirmation remains a partial lifecycle observation in the operation journal.

Managed encoding and sample-rate pull-up use the same owner and fresh lock/status
preflight, with explicit managed operation permissions. Their successful result
requires both the device-reported requested value and the effective value to
match. A pending requested value remains a configuration-only observation and
an unsuccessful result, not effective-state confirmation. A bounded readback
does not poll for convergence or infer media readiness.

Managed configuration clear requires a fresh unlocked status and sends one
command. Its matching execution report remains distinct from effective settings
and reboot completion. A missing response remains uncertain: reading a retained
last-executed mode cannot establish which request caused it. Clear invalidates
cached Controller settings and performs bounded managed inventory readback.
An explicitly requested follow-up reboot requires a matching clear report,
successful inventory refresh, unchanged Controller identity and fresh reset
permission. Its receipt is retained separately, and neither command is replayed.
Factory reset remains unsupported over the existing managed transport.

Managed device operations select their provider from the coherent inventory
before opening a transport, and recheck ownership before queued direct writes.
Managed-only and merged records expose the same managed-control requirement to
shared capability planners; a missing direct discovery record cannot select
direct-device permission rules for an enrolled target.
An enrolled device's name and subscriptions come from its managed inventory;
direct discovery still contributes independently observed hardware and channel
details. This prevents an older direct record from hiding managed command
readback. Stale managed observations remain visibly stale and cannot authorize
a command. This correction is recorded separately in compatibility evidence.

The daemon's retained meter history owns managed signal subscriptions, with one
session per server and domain. Complete inventory observations establish
read-only signal membership; a missing or partial device list cannot establish
absence. Membership remains available after inventory expiry, while signal
receipts have their own deadlines and never refresh inventory or authorize
commands. Direct and managed meters retain separate source ownership. Session
removal cannot erase a replacement owner's sample. The candidate bounds active
sessions separately from command capacity and reports queued or exhausted
subscriptions. Reconnection follows detected failure with a finite budget; an
explicit complete inventory read can rearm an exhausted subscription. The
resource lifecycle was qualified in isolated transport fixtures; real managed
appliance qualification remains separate.

Managed latency uses the same ARC codec, latency projection and completion
rules as direct devices. ARC acknowledgement codes remain explicit; receiving
an ARC response alone is not acceptance. Fresh Controller latency observations
share the existing identity-scoped status cache and expiry lifecycle, without
replacing Managed API channel inventory or Conmon-owned audio status. One
bounded readback may follow an uncertain write; it never replays that write.
Configured latency matching the request is reported separately from command
acceptance. The retained completion contract requires both, without claiming
packet timing, audio delivery or persistence from a configuration response.

Managed AES67 multicast prefix uses the shared prefix capability and command
policy with fresh lock and AES67 permission checks. Its Controller observation
owns only the prefix field, even when the ARC response also reports latency or
AES67 values. A no-op has no invented acknowledgement; rejected ARC requests
remain failures even if subsequent readback matches. Prefix confirmation does
not assert effective media state or persistence.

Managed channel rename retains the established modern ARC frontend probe and
uses Managed API inventory for channel identity and name readback. The command
requires fresh unlocked state, a supported frontend and a stable channel
identity. One bounded complete inventory read follows the single ARC write;
channel or primary device identity changes cannot confirm it. Confirmation
binds stable channel identity and name, excluding changing signal-presence
observations. Blank-name reset remains unsupported because the existing reset
codec has no established managed transport. Journal provenance distinguishes
Controller acknowledgement from Managed API observation.

Managed sample-rate changes use the shared topology safety model. A bounded
Controller acquisition reads channel counts, complete channel and transmit-flow
inventories, then checks counts and sample rate again. Zero-count directions
need no inventory request. Controller topology has separate source ownership
and expiry; it does not replace Managed API channel identity or subscription
records. Unknown target capacity retains the existing destructive-confirmation
rule. With unknown target capacity, the existing rule permits a destructive
change after explicit confirmation and validates the complete observed topology
for internal consistency; it cannot confirm exact expected preservation. Receipts
state that verification scope and leave preservation confirmation unknown.
A single write is followed by a bounded fresh rate and topology acquisition;
matching rate alone is not success. Uncertain writes are never replayed. A no-op
confirms only the current rate and has no invented acknowledgement. Diagnostic
exports are not attempted for managed devices. Synthetic qualification and user
acceptance remain distinct from real device and media interoperability.


### Mixed preset ownership and managed resource replacement

The native implementation uses one preset planner and ordered executor for direct
and managed targets. Direct targets retain their device-operation locks; managed
targets share a configuration lease and use the existing managed command owner.
Every target is planned before any write. Managed capture reads authoritative
GraphQL channels and subscriptions plus Controller routing and requested settings.
Unknown subscriptions cannot be treated as cleared. Unsupported managed writers
remain explicit, and capture reports omitted settings. Full command receipts and
final observations remain attached to the parent preset run. This mechanism is
implemented for evaluation; Chris's acceptance of its behavior remains pending.

Managed sources own immutable credential and trust snapshots. Their revisions are
included in confirmation conditions. File notifications reload configuration and
its referenced resources, retire changed sessions and status ownership, and take
one replacement inventory snapshot. Identical content does not reconnect or fetch.
Invalid replacement resources make the source unavailable. Invalid configuration
retires active source access while preserving explicitly stale observations.
Directory replacement rearms watches; missed notifications require a bounded
resource resnapshot. This adds a source lifecycle boundary without polling and
prevents a session from combining a retired trust configuration with a newly read
credential. It does not make an already issued remote command reversible or
atomic with an external file editor.


### Native wireless ownership

The candidate moves Shure framing, device observations and setting validation into
ordinary shared Rust core modules. The daemon owns one serialized TCP session per
observed MAC address. Linux discovery uses kernel neighbor notifications and a
bounded initial snapshot. A dump overlapping notifications is discarded and
reacquired within one deadline, avoiding replay of older notifications over newer
dump records. Lost source continuity retires affected sessions and permits bounded
resynchronization; failure leaves wireless discovery explicitly unavailable while
other daemon capabilities continue. No periodic neighbor scans or property reads
are introduced. Other operating systems require their own native observer.

A session requests initial properties and owns its meter subscription. It becomes
ready after channel identity reports arrive. Commands use fresh property preflight,
one write and a matching report. Timeouts preserve whether the complete write was
sent and never replay it. A command remains owned after its caller disconnects.
Durable command records distinguish reported configuration from physical behavior.
A short failure episode has bounded retries; a later disconnect after a stable
connection receives a new retry budget. Shutdown releases meter subscriptions.

Wireless inventory is retained in the native journal database. Existing saved JSON
is imported once when no native records exist, retained unchanged, and presented as
offline history. Configured wireless-to-Dante mappings reload through file events;
invalid mappings are reported and do not become guessed matches. The file-watch
mechanism is shared with managed credential and trust observation, while each
capability owns its configuration and errors.

This implements the accepted native-rewrite requirement for evaluation. The session
policy and OS observers remain subject to Chris's acceptance. Synthetic peers
qualify software behavior, not physical radio, audio, network reconfiguration or
model interoperability. The existing Python backend remains installed until the
complete daemon is ready for cutover.

## Native cutover, 2026-10-22

The ordinary Python CLI now executes the packaged native daemon. The installed
service uses the existing HTTP and TLS listeners, identities, configuration and
SQLite records. The superseded Python server modules are removed; Python remains
for the CLI, standalone device APIs and configuration utilities. The existing
JavaScript browser application is retained. Host JACK and Pulse workers are
separate native children with owned lifecycles.

The release installs from one wheel containing the core, daemon, endpoint and
host workers. Runtime and database backups support rollback. Stopping NetAudio
also stops the dependent Surface Dante manager through its Requires relationship;
deployment restores previously active dependents and checks their actual state.
JACK and Pulse services do not need to restart for this daemon cutover.

Local Linux package qualification, consumer reads and the connected browser were
exercised. Synthetic managed transport qualification does not establish physical
managed-device behavior, and this cutover does not qualify other operating systems.
API major 1 is defined by the separate stable-public-API decision and its served
OpenAPI contract. Memory and request latency improved in local observations; CPU
improvement has not been established.
