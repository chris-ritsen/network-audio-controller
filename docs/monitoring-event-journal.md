# Monitoring event journal

The netaudio daemon retains a bounded local timeline of parsed telemetry
transitions and user-initiated configuration operations. The journal is the
canonical mutation audit trail for CLI, HTTP, browser, and preset operations; it
does not issue additional protocol requests. It is stored as
`event-journal.json` beside the active netaudio configuration and is rewritten
atomically when an event is added or the journal is cleared.

Each event has a sequence number, UTC timestamp, stable device identity, event
kind, severity, optional interface/channel/flow identity, previous and current
values, supporting raw state, observation source, and derivation status. The
existing mDNS server name is the preferred device identity; managed inventory ID
or MAC address is used only when a server name is unavailable. JSON exports keep
those fields unchanged. Human-readable output summarizes the same records.
Configuration records also include an operation ID, correlation ID, optional
parent preset-run ID, operation and lifecycle phase, requested and effective
values, acknowledgement result and transport, final state, and separate
persistence-request and persistence-confirmation fields. Supporting evidence is
depth-, item-, and string-bounded, and credential-like fields are redacted.

## Event inputs

The first version supports:

- `device_disappeared` and `device_reappeared` from daemon lifecycle
  observations.
- `clock_role_changed`, `leader_identity_changed`, and
  `ptp_port_state_changed` from existing clock status fields.
- `mute_state_changed` for receiver and transmitter channels.
- `subscription_failed` and `subscription_recovered` from existing subscription
  status. Recovery is emitted only when a previous failure is followed by an
  explicit connected state. Missing status is not recovery.
- `late_packet_count_increased` and `late_packet_counter_reset` from heartbeat
  connection-health state.
- `receiver_flow_latency_high` and `receiver_flow_latency_recovered` from
  current latency, with average and peak latency retained as supporting
  measurements.
- `interface_error_counter_increased` and `interface_error_counter_reset` from
  heartbeat interface statistics.
- `interface_utilization_high` and `interface_utilization_recovered` when both
  reported traffic rates and a known link speed are available.
- `issue_opened`, `issue_updated`, and `issue_resolved` for transitions from the
  unified issue engine. The event retains the issue ID, kind, evidence class,
  raw source fields, and suggested action.
- `configuration_operation` for transmit-flow creation and deletion, external
  RTP subscription and association removal, receive/transmit/unicast
  performance, receive-flow default slots, configuration storage, and each
  mutation applied by a preset.
- `preset_run` for the parent preset lifecycle and its final counts of
  confirmed, partial, inconsistent, rejected, unavailable, failed, unchanged,
  and skipped actions. Unchanged or skipped actions contribute only to this
  summary.

An explicit clock-synchronization boolean is deferred because current device
state does not expose one consistently. Clock role and PTP port states are
reported without relabeling them as synchronization. Interface-utilization
events are omitted when link speed is unavailable. Other missing inputs remain
unknown and do not generate healthy, recovered, synchronized, zero, or cleared
states.

## Observed and derived records

Events marked `observed` represent a change in an already-parsed state or
counter. Events marked `derived` represent entry into or recovery from a named
threshold condition. Derived warning text and severity do not replace the raw
measurement. Device errors, failed queries, managed-status errors, and
field-source metadata present at observation time are retained under
`raw.observation_context`.

Configuration operations record only meaningful lifecycle transitions:
`requested`, `request_acknowledged`, `request_rejected`,
`effective_state_confirmed`, `partial_unobservable`, `inconsistent`,
`transport_or_validation_failure`, `persistence_request_acknowledged`, and
`persistence_confirmed`. A transport acknowledgement never establishes the
effective state. Missing fresh readback produces `partial_unobservable` rather
than success or failure. A configuration-storage acknowledgement leaves
`persistence_confirmation` null until an independent observation exists. Fresh
parsed readback updates the application device and passes through the same
journal observation path as ordinary telemetry, allowing subscription and issue
transitions to arise from observed state instead of request intent. Verification
polls are retained in bounded terminal evidence and do not each become events.

Preset child operations use their own operation IDs and the parent preset-run ID
as their shared correlation ID. This keeps each actual mutation reviewable while
avoiding entries for settings that preflight proved unchanged.

Receiver-flow latency uses the configured flow latency when available, or the
device's existing receiver-flow latency setting. The default warning boundary
is 80 percent of that setting and the recovery boundary is 60 percent.
Interface utilization uses the larger of the reported transmit and receive bit
rates divided by known link capacity. Its default warning and recovery
boundaries are 80 and 60 percent. The gap between warning and recovery provides
hysteresis.

The daemon configuration accepts these optional keys:

```toml
[daemon]
event_history_limit = 1000
event_flow_latency_warning_ratio = 0.8
event_flow_latency_recovery_ratio = 0.6
event_interface_utilization_warning_percent = 80.0
event_interface_utilization_recovery_percent = 60.0
```

Recovery values must be lower than warning values. Invalid settings prevent
daemon startup rather than silently changing threshold behavior.

## Retention, access, and clearing

The default retention limit is 1,000 events. Insertion order determines
eviction: adding an event beyond the limit removes the oldest retained event.
Sequence numbers remain monotonic while the journal file exists. The daemon
loads the retained records after restart; transition baselines are rebuilt from
fresh observations so startup does not turn unknown state into a transition.

Use `netaudio events list` to inspect recent entries. It accepts `--device`,
`--kind`, `--severity`, `--since`, and `--limit`. Use `netaudio events export`
to write stable JSON to stdout or `--file`. The daemon also serves
`GET /event-journal`. The browser Events page presents the retained history as
a timestamp, device, and event table with cumulative Information, Warning, and
Error filtering. Search narrows the visible rows, selecting a row reveals its
transition and supporting evidence, and Save downloads the complete unfiltered
JSON journal.

`netaudio events clear` and `DELETE /event-journal` remove only retained local
events. Clearing does not send a device command, clear a hardware counter, or
alter the in-memory event or issue transition baselines. Active and resolved
issue records remain available through `netaudio issues list` and `GET /issues`.
A later unchanged observation remains deduplicated.

See [Presets and monitoring issues](presets-and-issues.md) for issue kinds,
lifecycle rules, and interfaces.
