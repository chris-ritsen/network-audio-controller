# Linux H.264 multicast endpoint

`netaudio video serve-multicast --configuration endpoint.json` runs a finite,
explicitly configured multicast endpoint. `netaudio video multicast-schema`
prints its complete JSON schema. Linux builds require the `video` feature,
GStreamer development libraries and the OpenH264 encoder, H.264 parser,
RTP packetizer, libav decoder and video conversion plugins.

This profile supports one ordinary unprotected IPv4 H.264 source and configured
receivers. The source reads tightly packed 1920×1080 BGRA frames at 30 fps.
Receivers write decoded NV12 frames when `record_frames` is true. It does not
automatically configure a desktop source, a display, a virtual camera, or a
third-party device. Existing `video share` and `video listen` commands provide
the separate desktop/display adapter workflow.

## Configuration and operation

Set a unique `device_name`, explicit local `arc_bind`, finite
`run_duration_seconds`, and `retirement_timeout_milliseconds`. Evidence can go
to standard output, a new file, or both. Each evidence entry has an incarnation,
revision and observation timestamp. Each worker needs a distinct new output
directory; existing evidence and media files are never overwritten.

The optional `source` supplies source/channel/flow identities, channel and flow
names, a finite raw input file, output directory and transmitter configuration.
The selected source has one local channel and one local flow, both numbered
one. Its DNS-SD service channel identifier is a separate namespace. Group,
port, interface, codec, RTP identity and source authority are supplied explicitly
in the transmitter configuration. A create request selects that owned resource;
it does not choose arbitrary endpoints or formats.

Each entry in `receivers` supplies its identities, channel name, expected source
device/channel/flow names, service channel and flow identifiers, destination,
source tuple, actual SPS/PPS, image/codec limits, authority, discovery deadline
and output policy. The receiver local channel and flow identifiers each cover
the configured interval starting at one. Global identifiers remain independent.
Run `netaudio video transmit --prepare-only` with the source configuration to
obtain actual encoder parameters before supplying a receiver configuration.

Creation uses the selected Media-4 ARC `0x2601` form. A separate `0x2600` read
reports current source allocation. Named `0x3410` Set intent is accepted before
a source appears; media starts only after matching retained channel and flow
discovery. A malformed page cannot partially update intent; an unsupported
mixed page leaves all intent unchanged. Repeated receiver rows use the final
row, and repeated unchanged intent does not restart a worker.

Source publication uses the channel and flow service types from the clean
protocol contract. The ARC listener and advertised video service ports are
different contracts. The runtime does not implement the separate advertised
video session negotiation service. Automatic Controller device admission and
ordinary unicast route construction remain unqualified.

## State and retirement

Accepted intent, allocation, requested publication, socket ownership, group
membership, packet admission, decoded frames and output acceptance are distinct
observations. Readiness requires a real output sink to accept a decoded frame.
With frame recording disabled, this profile can observe and decode packets but
does not claim output readiness. Cumulative observations describe what occurred
within a generation; `terminal` and worker ownership describe its retirement.
They are not proof of current physical display output.

Clear cancels only the selected receiver and awaits its provider retirement.
While an old worker still owns a joined socket, current receive-flow inventory
can retain that flow; receiver channel state independently reports cleared or
replacement intent. Another receiver remains independent. Source deletion via
`0x2602` removes the configured source flow, withdraws its publications and
retires its media worker. A remote receiver detects withdrawal or expiry and
retires its own media resources.

Cancelled or completed contexts do not authorize reuse of an old network tuple.
The source group is single-use within this runtime. After a receiver has joined
and retired, another Set requires a fresh configuration and explicit fresh
source/group authority. Restarting the process alone does not prove that old
network traffic has drained. Mutation replies are never automatically replayed;
an absent reply requires current readback before deciding what happened.

Shutdown has an explicit overall deadline in addition to provider deadlines.
If retirement cannot be established, the process reports unknown quiescence
and fails. It does not report a successful retirement after timing out.

## Qualification

The external private fixture exercised source create/current/delete, named
Set/Clear, two independent receivers, actual multicast delivery, reference
decoded frame hashes, discovery withdrawal and resource retirement. This is a
bounded lifecycle qualification, not sustained reliability or hardware
interoperability. An earlier fixture run hit the source send-deadline guard;
that failure remains relevant to throughput qualification. No media guard was
relaxed. Controller/Studio interoperability, live display/camera output for this
role and user acceptance remain outstanding.
