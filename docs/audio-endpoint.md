# Linux audio endpoint

`netaudio audio serve --configuration endpoint.json` runs a bidirectional
native-v2 endpoint. The network format is 48 kHz signed 24-bit PCM, with one
owned transmit flow and one receive flow per endpoint. Native audio adapters
and explicit WAVE files are separate input/output choices. Linux wheels include
the JACK adapter; it dynamically loads the installed JACK library and never
starts a JACK server or connects ports automatically.

Each process requires a distinct endpoint name, identity, local clock identity,
state directory and JACK client name. The selected PTPv1 parent and interface
must be explicit. Full endpoints currently use the default clock domain; the
shared transport itself isolates complete domains for its consumers. A shared transport allows processes in the same network
namespace to use the same host interface.

## Shared timing

Create a directory owned by the endpoint user with mode 0700. Start
`netaudio audio clock-serve --socket /absolute/private/directory/clock.sock`
under that same user. The service exclusively binds UDP 319 and 320, requiring
the corresponding bind permission, such as Linux `CAP_NET_BIND_SERVICE`.
An existing owner of these ports prevents startup.

Set this field in every participating endpoint configuration:

```json
{
  "clock_transport": {
    "type": "shared",
    "socket": "/absolute/private/directory/clock.sock"
  }
}
```

The service owns the sockets, kernel timestamps and multicast memberships.
Endpoints keep independent timing disciplines and media ownership. Dispatch
checks interface, complete clock domain, parent and requester identity.
Duplicate local clock identities on the same interface/domain are rejected.
One endpoint can disconnect without stopping its siblings. Service loss stops
its consumers; restarting an endpoint requires fresh clock acquisition.
The service does not reconnect or replay an uncertain request automatically.

Omitting `clock_transport` selects the existing exclusive mode. Shared mode
never falls back to exclusive binding. The implementation currently uses Linux
software timestamps and a selected PTPv1 parent; it does not implement PTPv2,
leader election, hardware timestamps or a universal host clock.

Both processes support systemd notification. Start the shared transport before
its endpoints. Endpoint readiness and discovery wait through the existing
60-second media-port reuse quarantine. A `Type=notify` service receives one
startup timeout extension covering that known deadline. Supervisors without
notification support must allow the wait, for example with a 90-second startup
deadline. Shutdown during the wait is cancellable.

## Channel geometry

The optional `profile` object has these fields:

```json
{
  "profile": {
    "transmit_channels": 8,
    "receive_channels": 8,
    "flow_slots": 8,
    "frames_per_packet": 48,
    "receive_latency_nanoseconds": 20000000,
    "maximum_media_payload_bytes": 1472
  }
}
```

This example describes configuration, not a guaranteed capacity. Channel
inventory, receive mappings, flow bitmaps, discovery and native adapters use
the same profile. Directional channel counts may differ. One receive flow can
use its slots for multiple local receiver channels; independent source flows
are not yet supported.

Packetization must fit the selected payload budget and declared range of
6–48 frames. With a 1472-byte payload budget, 24-bit samples and six frames per
packet, at most 81 slots fit one packet. That is packet geometry, not measured
throughput or a universal Dante channel limit. Control-record geometry and
bounded audio storage impose separate validated limits. Increasing channel
counts reduces the packetization that fits a normal Ethernet payload.

Native input/output channel maps must match the directional endpoint counts.
JACK publishes unconnected `send_1` through `send_N` inputs and `receive_1`
through `receive_N` outputs. The adapter requires its configured device rate and
callback size to match the running JACK server. Independent device clocks use
resampling; they do not change the network format. Network-synchronized mode
requires a 48 kHz device clock already synchronized by the caller.

Changing format, channel geometry or the selected device requires an explicit
process replacement. Automatic device reopening, multiple flows, other network
sample rates/widths and macOS/Windows network endpoint support remain separate
work. Short isolated runs do not qualify sustained hardware performance or
physical round-trip latency.
