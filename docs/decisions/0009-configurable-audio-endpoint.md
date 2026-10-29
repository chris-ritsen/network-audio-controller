# Configurable endpoint channel geometry

Status: requested capability accepted October 28, 2026; implementation proposed
and awaiting user acceptance.

Chris requested simultaneous virtual endpoints, more than two channels, and
measured capacity across sample rates and latency settings. The current endpoint
fixes its network format to two channels at 48 kHz with signed 24-bit PCM.

Make directional channel counts, per-flow slot capacity, packetization, receive latency and
the media payload budget explicit endpoint configuration. Discovery, inventory,
allocation, receive mappings and native adapters use the same geometry. Keep the
current one-flow-per-direction transport explicit. Its selected 48 kHz, 24-bit
network format remains separate from the native device rate and sample format.

Channel pages retain the protocol's reserved table and continuation semantics.
Receive bitmaps cover every configured receiver. Media and control packet
geometry limit a configured flow independently of endpoint channel counts.
Packetization preferences remain within the declared source range and UDP budget.
Reject configurations outside the implemented resource budget before opening
audio devices or publishing services.

Audio callbacks use preallocated sample storage and bounded queues. A channel
count does not introduce callback allocation, device auto-connection or a second
controller. Discovery and service readiness wait for the initial media-port reuse quarantine
to expire, so a newly published source can accept its first allocation. The
lease wait remains cancellable and observes clock/interface failures.
Independent instances retain separate identities, state directories,
JACK names and flow ownership. Channel and format changes require explicit
instance replacement; automatic device recovery is a separate decision.

Linux wheel builds include the dynamically loaded JACK adapter by default so an
ordinary package rebuild retains that capability. Explicit backend selection
remains available. Other platform builds retain their native backend defaults.

This supports measured capacity beyond stereo without implying universal Dante
limits or cross-platform qualification. Multiple flows, other network formats,
automatic device reopening and native virtual soundcard publication remain
separate work. Increasing constants or compiling a separate binary per channel
count was rejected because concurrent endpoints need independent configuration.
