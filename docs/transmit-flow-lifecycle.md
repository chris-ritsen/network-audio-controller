# Transmit-flow lifecycle

NetAudio represents transmitter flows with one versioned schema across its
Python API, CLI, daemon HTTP API, browser, and presets. The schema records the
media mode, flow type and name; ordered flow-slot to transmitter-channel
mappings; sample rate, encoding and frames per packet; primary and secondary
IPv4/UDP destinations; interface and redundancy constraints; flow identity;
protocol requirements and capabilities; and raw extension fields retained from
readback.

Readback is available with:

```console
netaudio flow inspect
```

The command returns the canonical schema in structured output and a compact
table for human-readable output. Records that cannot be represented remain in
the inventory as `unparsed_records` with their raw record and error instead of
being discarded.

Use `netaudio flow plan SPECIFICATION.json` to validate a canonical request
without sending it. Use `netaudio flow apply SPECIFICATION.json --yes` to send a
supported direct-device create operation and verify fresh readback. Use
`netaudio flow delete --slot FLOW --yes` to delete a supported flow and verify
fresh absent readback. The daemon exposes equivalent operations at:

- `GET /transmit-flows/{device}`
- `POST /transmit-flows/plan`
- `POST /transmit-flows/create`
- `POST /transmit-flows/delete`

The browser's device transmit page uses those endpoints to inspect, plan,
create, and delete flows. Every mutation requires explicit confirmation.

## Completion states

The operation result preserves request acknowledgement, device confirmation,
persistence confirmation, and effective-state confirmation as separate fields.
`request_acknowledgement` contains only the received ARC response, parse state,
result code, and response-bound allocation data. The current ARC transports
provide no independent device-confirmation signal, so `device_confirmation`
remains null. `effective_state_confirmation` is true only after matching fresh
inventory, false only for affirmative contradictory inventory, and null when
verification remains unavailable or times out. `persistence_confirmation`
remains null because this lifecycle performs no persistence verification.
It reports `confirmed`, `deleted`, `partial`, `pending`, `inconsistent`,
`rejected`, or `unsupported` rather than treating a reply as proof of effective
state. The mutation is sent exactly once. Post-write verification polls fresh
inventory for up to five seconds at 250 ms intervals and retains every
preflight and post-write observation in `verification_observations`. An
acknowledged timeout is `partial`; a timeout without an acknowledgement is
`pending`. `inconsistent` is reserved for fresh affirmative contradiction.
Creation correlates device-allocated modern flows through the acknowledgement
identity when present. Deletion requires fresh absent readback.

Unrelated flows are compared through parser-established identity and durable
configuration fields. Raw records remain in the observations for diagnosis,
while freshness, extensions, ordering, and diagnostic values do not create a
false inconsistency. Actual unrelated creation, deletion, or durable
configuration change is contradictory.

Persistence confirmation remains unknown for direct ARC operations. RTP packet
reception, clock lock, and decoded audio remain false until separately tested;
control-plane readback does not establish them.

## Supported cohorts and fail-closed behavior

Two direct native-Dante multicast cohorts have promoted, digest-bound payloads:

- ARC `0x2729`: explicit flow identifiers, create, readback, and delete.
- ARC `0x2809`: device-allocated creation and global-flow-2 deletion. The
  request option word accepts only observed values `0`, `1`, and `113`.

The planner rejects unsupported fields before sending a request. The current
serializers do not author names, selected destination sockets, frames per
packet, or explicit interface/redundancy constraints. RTP/AES67 transmitter
authoring, ARC `0x2801` mutation, other `0x2809` delete identifiers, and managed
DDM transmitter-flow mutation remain unsupported. Managed devices never fall
back to direct UDP.

## Evidence provenance

`tests/fixtures/transmit_flow_lifecycle/provenance.json` binds every promoted
payload to its SHA-256 digest, size, evidence class, source, run, device,
software versions where retained, capture frame and timestamp where retained,
scope, transformations, and exclusions.

The `0x2809` create request and acknowledgement came from the bounded
`netaudio-aes3-allocation-20260906.G5wpTA` run using Dante Controller 4.16.1.5
through its visible interface and an AVIO AES3. The source capture digest,
frames, timestamps, and device versions are recorded in the provenance file.
The other promoted payloads are historical live fixtures whose missing original
capture metadata is declared explicitly. Record-length generalization for the
legacy request is classified as inferred.

Offline tests establish model validation, exact serializer bytes, parser
rejection, acknowledgement handling, lifecycle state transitions, canonical
comparison, and interface behavior. They do not establish acceptance on other
device versions, media transport, clock synchronization, or decoded audio.
