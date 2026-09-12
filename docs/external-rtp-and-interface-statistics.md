# External RTP discovery and interface statistics

NetAudio can listen for SAP version 1 announcements on UDP port 9875 and keep
an in-memory inventory of the SDP audio flows they describe. The listener joins
`239.255.255.255` on each configured active IPv4 interface. It rejects IPv6,
encrypted, compressed, malformed, and structurally incomplete announcements.
Inventory entries expire after one hour without a refresh.

Use the CLI to listen briefly and display the inventory:

```console
netaudio flow external-list --listen-seconds 2
```

The daemon exposes the same inventory at `GET /external-flows` and publishes
`external_flow_changed` server-sent events when an announcement is added,
refreshed, replaced, deleted, or expired. Entries retain their SAP origin,
unsigned SDP session ID, announcement interface, source socket, timestamps,
authentication bytes, raw SDP, parsed fields, and routing errors.

A routable entry needs an audio media description, a usable IPv4 connection
address, a valid UDP port, and a matching `L16`, `L24`, or `L32` RTP map with a
positive sample rate and channel count. Session structure and routability are
reported separately, so a valid metadata-only SDP remains visible with its
routing errors.

## Direct receiver subscription

The direct external receiver operation uses ARC opcode `0x3201`. Select a
discovered flow by its SAP source IPv4 and SDP session ID, then supply parallel
receiver-channel and one-based flow-slot lists:

```console
netaudio -n receiver flow subscribe-external \
  --source 192.0.2.44 \
  --session-id 123456789012 \
  --receiver-channels 1,2 \
  --flow-slots 1,2 \
  --yes
```

Slot zero removes that receiver association within the batch. An all-zero batch
is therefore a valid receiver-association removal request, but its
acknowledgement does not establish that the device deleted an unused receiver
flow. Receiver IDs must be unique; their order is not significant, and multiple
receivers may select the same external flow slot. A secondary destination is
encoded only when the SDP advertises one and the caller declares receiver
support with `--receiver-multiple-interfaces`.

The equivalent daemon operation is `POST /external-flows/subscribe`:

```json
{
  "rx_device": "receiver.local.",
  "source_ipv4": "192.0.2.44",
  "session_id": 123456789012,
  "receiver_channel_ids": [1, 2],
  "flow_slot_assignments": [1, 2],
  "receiver_supports_multiple_interfaces": false
}
```

The result reports request acknowledgement separately from subscription
readback, RTP packet reception, clock lock, and decoded audio. The latter four
remain false until independently confirmed. Managed-only devices fail closed
because this operation has no verified managed transport.

## Interface statistics

`netaudio device network-status` now decodes ConMon `0x0040` as outer interface
groups containing nested raw records. Each group selects the first record whose
two discriminator bytes are zero. Selected records expose transmit and receive
byte rates, checked 64-bit bit-rate derivations, cumulative error counters,
speed, discriminator, extension bytes, and the complete raw record.

The observation also retains the complete header record and body, packet
source, local receive time, transport name, and freshness. For protocol
versions at or above `0x0713`, capability bits report utilization, errors, and
clear-error support. Older versions use the observed default mask of three.
Error displays use a local baseline; a device counter decrease establishes a
new baseline.

The generic eight-byte `0x0041` request and the retained 32-byte `0x073a`
variant are both available internally. NetAudio does not expose them as a
proven remote clear operation.

## Evidence and verification limits

The `0x0040` structure is classified as observed from retained packets, but
their original run, timestamp, and frame metadata were not retained. SAP/SDP
and `0x3201` are classified as inferred and are tested with deterministic,
digest-bound synthetic fixtures. The current offline tests establish parsing,
validation, state transitions, and serializer layout. They do not establish
natural announcement compatibility, receiver acceptance, readback, RTP
reception, clock lock, or decoded audio. Those outcomes require separate live
evidence before interoperability is claimed.
