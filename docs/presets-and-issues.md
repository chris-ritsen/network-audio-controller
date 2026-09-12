# Presets and monitoring issues

## Preset round trips

NetAudio presets use a versioned canonical configuration inside a conventional
Dante Controller XML envelope. The conventional fields remain readable by
other tools, while the NetAudio extension retains categories that the envelope
cannot express without loss. Parsing older XML remains supported. Unknown root
and device attributes and elements are retained for a later save.

The canonical preset can contain device identity and name, redundancy mode,
preferred leader, external word-clock selection, pull-up and clock source,
sample rate, encoding and latency, every IPv4 interface, transmitter and
receiver channel names, canonical transmit flows, native and external RTP
receiver subscriptions, codec gain, HA bridge state, and unknown fields.

`netaudio preset save` and the browser Presets page save selected routing,
audio, and network categories from fresh state. `netaudio preset show` or
`netaudio preset load --dry-run` resolves device identity and produces a
target-aware plan with current and requested values. Each action is classified
as `change`, `unchanged`, `unsupported`, `unavailable`, or `ambiguous`.
Only `change` actions can reach a writer. Missing fresh state or capability
evidence is `unavailable`, while an already matching value is `unchanged` and
sends no request. The browser preview
requires an explicit target for every device or an explicit skip, binds the
review to the XML digest, and checks identity again before applying.

Application uses a fixed dependency order: device name and redundancy, format
and clock settings, channel names, transmit flows, receiver subscriptions,
codec gain, then interface configuration. Each result records a state such as
`skipped`, `applied`, `acknowledged`, `confirmed`, or `failed`, along with
request, acknowledgement, effective state, and message where available. Fresh
readback is required for a confirmed result.

Planning reads device name, redundancy, sample rate, encoding, active latency,
preferred leader, pull-up, clock source, channel names, transmit flows, native
receiver subscriptions, codec gain, and every requested supported interface
from their fresh readback paths. Existing transmit flows are compared through
the canonical flow model. A matching flow is unchanged. A flow with the same
identity and different state is ambiguous and preserved because replacing it
could disrupt unrelated routing. External RTP receiver subscriptions are
unavailable for application because current receiver readback does not expose
the source and session identity required for a trustworthy comparison.
External word-clock selection, HA bridge state without an
advertised understood capability, and unknown categories are preserved and
skipped.

## Unified issues

The daemon derives typed issues from observations it already holds and does not
send remediation commands. Initial detection covers address and subnet
conflicts, clock synchronization and pull-up mismatch, subscription failure,
degraded receiver-flow health, licensing failure, safe/upgrade/reboot-required
states, configuration partition and persistence faults, stale or missing
telemetry, and requested-versus-effective divergence.

Each issue keeps a stable digest-based identity, severity, open/resolved state,
observed/unobservable evidence state, first and last seen timestamps, resolution time, occurrence count,
device/interface/channel/flow scope, raw source fields, evidence source,
evidence class, and suggested action. Subscription status and receiver-flow
health remain separate issue kinds and retain their independent raw values.

An issue resolves only after fresh evidence relevant to that issue explicitly
shows the condition is absent. Missing fields, failed queries, removal of a
snapshot, and an offline device make affected open issues unobservable without
changing their last supporting-evidence timestamp or resolving them.
`occurrence_count` is the number of relevant observations that supported the
issue; `last_seen` is the timestamp of the newest such observation. Unrelated
device observations change neither value. Cross-device address and pull-up
checks refresh only when a device in their recorded dependency set changes.
Active and resolved issues persist in `event-journal.json` with the journal,
using the same retention limit for resolved history. Silent count and timestamp
refreshes are persisted even when their material content does not warrant an
`issue_updated` event.

Use the CLI to inspect issues:

```console
netaudio issues list --state open
netaudio issues list --state resolved
```

Filters include device, kind, severity, state, and limit. `GET /issues` exposes
the same current and historical records. Issue open, update, and resolution
transitions become `issue_opened`, `issue_updated`, and `issue_resolved` journal
events, so they also appear through the event-journal API and existing SSE
`monitoring_event` stream. The browser Events page shows current issues and
resolved history alongside the journal.

Clearing the event journal removes retained transition events while preserving
the issue engine and its baselines. This avoids inventing a new transition from
an unchanged observation.

Issue conclusions are classified per record as direct observation, derived
state, or inference. Detection and lifecycle tests use deterministic snapshots;
they do not establish that a condition occurred on a natural device network or
that a suggested action would repair it.
