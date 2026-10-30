# Linux audio endpoint

`netaudio audio serve --configuration endpoint.json` runs a bidirectional
native-v2 endpoint. The profile selects 8000–192000 Hz signed 16-, 24- or 32-bit PCM,
with independently owned transmit and receive flows. Native audio adapters
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

Realtime-to-monotonic conversion takes five independently bracketed clock
observations at startup and for each timestamp. It uses the narrowest observed
interval while checking every observation against that selection and the
startup reference. The existing 2 microsecond mapping-change allowance and
10 microsecond timestamp-uncertainty limit remain enforced. A scheduling delay
in one observation does not set the precision of the whole acquisition;
consistently uncertain observations still fail. A detected clock step or invalid
provider still invalidates the follower and its media generation.

The shutdown `clock_mapping` summary records acquisition and observation counts,
maximum raw and selected observation uncertainties, and inconsistent
acquisitions. Selected uncertainty excludes the separately applied mapping
allowance; timestamp uncertainty includes it and the startup reference's
uncertainty. These measurements describe software clock conversion, not hardware
timestamp accuracy or measured physical path latency.

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
    "sample_rate": 48000,
    "sample_bits": 24,
    "transmit_channels": 8,
    "receive_channels": 8,
    "flow_slots": 2,
    "transmit_flows": 4,
    "receive_flows": 4,
    "frames_per_packet": 48,
    "receive_latency_nanoseconds": 20000000,
    "maximum_media_payload_bytes": 1472
  }
}
```

This example describes configuration, not a guaranteed capacity. Channel
inventory, receive mappings, flow bitmaps, discovery and native adapters use
the same profile. Directional channel and flow capacities may differ. Each
flow has its own allocation handle, port, generation and lifecycle. Flow
capacities default to eight and describe configured resources, not throughput.

Receive grouping uses source identity and blocks of local channels bounded by
`flow_slots`; smaller advertised source slot capacity splits those blocks. A
source slot can feed multiple receivers in its group. One capture adapter feeds
all transmit flows. Received flows are placed on a common sample timeline into
their configured host channels. Clearing a group leaves sibling groups running.
Each directional flow reserves two media ports for independent reuse quarantine.

Packetization must fit the selected payload budget and declared range of
6–48 frames, with the minimum increasing at higher sample rates to preserve
the sender spacing budget. With a 1472-byte payload budget, 24-bit samples and six frames per
packet, at most 81 slots fit one packet. That is packet geometry, not measured
throughput or a universal Dante channel limit. Control-record geometry and
bounded audio storage impose separate validated limits. Increasing channel
counts reduces the packetization that fits a normal Ethernet payload.

Native input/output channel maps must match the directional endpoint counts.
JACK publishes unconnected `send_1` through `send_N` inputs and `receive_1`
through `receive_N` outputs. The adapter requires its configured device rate and
callback size to match the running JACK server. Independent device clocks use
resampling; they do not change the network format. Network-synchronized mode
requires a device clock at the configured network rate already synchronized by
the caller. Native float callbacks and resampling do not preserve all 32-bit
integer precision.

Native `buffered_frames` applies separately to capture priming and playback
priming. At 48000 Hz, 960 frames represent 20 ms in each direction. These host
buffers are additional to configured receive latency. Source timestamp placement,
sample-quantum alignment, callback/graph scheduling and any resampling delay also
affect the observed path. Receive latency alone is not an end-to-end latency
measurement.

Changing format, channel geometry or the selected device requires an explicit
process replacement. Automatic device reopening and macOS/Windows network endpoint support remain
separate work. Short isolated runs do not qualify sustained hardware performance or
physical round-trip latency.

Partial receive changes preserve the common host sample timeline. Native
independent-clock conversion fences changed channels until the resampler has
replaced its retained input history, emitting local silence at that boundary.
Sibling channels retain their filter state and pacing. This bounded transition
is separate from network loss and from the native floating-point precision
limit; output receipts retain per-channel received and silence accounting.

Receive recovery invalidates buffered samples by generation without scanning
the full ring on the media thread. Each receive flow uses an additional 64 KiB
of generation storage. Retired samples remain inaccessible; missing output
still produces explicit silence, and capture-age and playback deadlines remain
enforced.

The shutdown summary includes event-loop scheduling measurements. Preparation
measures reconciliation and deadline setup. Individual future polls and selected
handlers record their actual execution intervals, excluding time suspended at an
await. Setting `scheduling_thread_observations` to `true` in the endpoint JSON
configuration also records thread CPU time and voluntary/involuntary context
switches for the enclosing selection poll and preparation. This diagnostic mode
defaults to `false` because its system calls materially increase endpoint CPU
use. The shutdown summary records the selected mode; unmeasured thread fields
are null.
Pending deadline polls retain their maximum overrun separately from completed
polls. These observations distinguish owned execution, cached readiness delay and
off-CPU time; they do not identify the preempting task or guarantee operating-system
latency.

When a timer's monotonic target has elapsed, the endpoint first attempts to consume
its nonblocking kernel expiration directly. Other ready work cannot make it wait
solely for the runtime's cached I/O readiness to refresh. One-shot validation,
capture-age limits and missed-playout rejection remain enforced.

## Sustained qualification

One isolated Linux software-fixture run of the clean `c18374d` endpoint passed
a 600.103-second armed interval. Three endpoints each used eight transmit and
receive channels, four flows per direction, two slots per flow, 48 kHz PCM24,
48-frame packets, 128-frame dummy-JACK callbacks, 960 buffered frames and
10 ms receive latency. The endpoint and JACK processes used `SCHED_FIFO`
priority 20, with network-synchronized native clocks. All 24 recorded channels
matched their sources exactly through 28,804,864 frames per channel
(600.101 seconds). Recorder xruns and final endpoint fault counters were zero.

The preceding run of the same production binary stopped on a shared-output
sample deadline after 436.066 seconds. The successful interval does not explain
or repair that intermittent failure. It qualifies only this finite private
software profile; hardware, other rates, higher channel counts, other platforms
and user acceptance remain separate.
