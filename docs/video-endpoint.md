# Linux H.264 endpoint

`netaudio video serve --configuration endpoint.json` runs a finite,
explicitly configured endpoint. `netaudio video endpoint-schema`
prints its complete JSON schema. Linux builds require the `video` feature,
GStreamer development libraries and the OpenH264 encoder, H.264 parser,
RTP packetizer, libav decoder and video conversion plugins.

This profile supports one ordinary unprotected IPv4 H.264 source and configured
receivers, using multicast or supplied-authority unicast acquisition. The source
reads tightly packed 1920×1080 BGRA frames at 30 fps.
When `record_frames` is true, a receiver writes one append-only decoded NV12
payload artifact and paired JSONL descriptor/completion index. Its receiver
configuration must set positive `recording.maximum_pending_frames` and
`recording.maximum_pending_bytes`; these are independent of decoder limits.
It does not automatically configure a desktop source, a display, a virtual
camera, or a third-party device. Existing `video share` and `video listen` commands provide
the separate desktop/display adapter workflow.

At this geometry, the raw source requires 248,832,000 bytes per second and each
recorded NV12 receiver produces 93,312,000 bytes per second. Packet archives,
indexes and filesystem synchronization add work beyond those payload rates.
Qualify the complete storage path with the intended concurrent streams; nominal
device bandwidth alone does not establish that frame deadlines will be met.

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

Every source specifies `transport.kind`, and every receiver specifies
`acquisition.kind`. A multicast receiver includes `source_flow`,
`source_local_flow` and `source_association_slot` in its acquisition object.
A unicast receiver supplies a `control` object with its separate `control_bind`,
actual `source_control` endpoint, `receiver_host_label`, `expected_allocation`
(global flow identifier and ownership value), distinct `allocation_transaction`
and `stop_transaction`, and a positive `operation_timeout_milliseconds`.
Its configured media destination is reserved before allocation. The expected
allocation describes the explicitly owned source profile; it cannot be obtained
by guessing a foreign allocation generation.

A unicast source transport supplies its actual `control_bind`,
`permitted_requester`, `receiver_host_label` and nonzero `ownership_value`.
The source reserves its media socket before publishing the channel. Its allowed
requester, receiver label, service channel and configured destination must all
match the allocation request. A source owns one configured channel and flow;
this describes the selected resource profile rather than a universal limit.

Multicast creation uses the selected Media-4 ARC `0x2601` form. A separate `0x2600` read
reports current source allocation. Named `0x3410` Set intent is accepted before
a source appears. Multicast media starts after matching retained channel and
flow discovery. Unicast resolves the channel and actual source-control service,
reserves its destination, and performs one allocation before configured media.
A malformed page cannot partially update intent; an unsupported
mixed page leaves all intent unchanged. Repeated receiver rows use the final
row, and repeated unchanged intent does not restart a worker.

Multicast publishes the channel and named flow; unicast publishes the channel
with its actual source-control port and no multicast flow association. The ARC
listener, source-control service, source media socket and receiver media socket
have separate ownership. Unicast source allocation and owned STOP use the ready
selected contracts. Exact codec parameters, source tuple, destination and
freshness authority remain supplied configuration. Neither an allocation ACK
nor the first RTP packet creates those values.

This integration uses the fully configured session branch. Guarded in-band
parameter acquisition remains separate work. Automatic foreign-source
acquisition also has a contract gap: associating the current allocation with the
permitted RTP sender IPv4 address and UDP source port before first-packet
admission. Automatic Controller device admission remains unqualified.

## State and retirement

Accepted intent, allocation, requested publication, socket ownership, group
membership, packet admission, decoded frames and output acceptance are distinct
observations. Readiness requires a real output sink to accept a decoded frame.
With frame recording disabled, this profile can observe and decode packets but
does not claim output readiness. Cumulative observations describe what occurred
within a generation; `terminal` and worker ownership describe its retirement.
They are not proof of current physical display output.

Clear commits removal of the selected named intent. Its receiver then retires
the provider and recording writer. A unicast receiver separately attempts one
STOP for its actually acquired source allocation; the STOP result describes
source control processing, not remote media quiescence. A recording writer
retains its generation output path
until it has ended and joined; a successor generation cannot reuse it. A writer
failure creates an incomplete manifest with its accepted and paired-record
boundaries. An ingress rejection records its unaccepted frame separately from
accepted-but-unwritten work. A retirement deadline records unknown quiescence
and still waits for the owned writer to join before the endpoint returns an
error.
While an old worker still owns a joined socket, current receive-flow inventory
can retain that flow; receiver channel state independently reports cleared or
replacement intent. Another receiver remains independent. Source deletion via
`0x2602` removes the configured source flow, withdraws its publications and
retires its media worker. A remote receiver detects withdrawal or expiry and
retires its own media resources.

Cancelled or completed contexts do not authorize reuse of an old network tuple.
Source allocations and receiver destinations are single-use within this finite
runtime. After a destination has been owned and retired, another Set requires
fresh configuration and explicit source/destination authority. Restarting the
process alone does not prove that old network traffic has drained. Clients do
not automatically retry mutations. The owned source retains each valid
source-control transaction and returns its recorded reply for an exact duplicate
request; different bytes with the same transaction are refused. An absent reply
requires current readback before deciding what happened.

Shutdown has an explicit overall deadline in addition to provider deadlines.
If retirement cannot be established, the process reports unknown quiescence
and fails. It does not report a successful retirement after timing out.

## Qualification

The October 30 private unicast fixture exercised deferred named Set, fresh
channel discovery with the actual source-control port, destination reservation,
allocation, configured receive, current inventory and repeated unchanged Set.
Explicit Clear produced one owned STOP, independent media and recording
retirement, and refusal to reuse the retired destination. Its source sent 120
complete frames. All 119 recorded frames matched a separate decode of captured
RTP; the remaining submitted frame was explicitly canceled by Clear. The
recorded first-to-last RTP span was 3.933333 seconds, with maximum recording
completion lateness 2.663502 milliseconds against the unchanged 50 millisecond
allowance. This establishes the finite cancellation lifecycle, not sustained
unicast reliability or foreign interoperability.

The first two private attempts stopped on external observer defects. Their
evidence is retained. The third completed lifecycle capture initially failed a
sustained-run validator that required all submitted frames to be recorded;
separate offline lifecycle validation accounted for the deliberate cancellation
without repeating media or changing the recording deadline. Protected services
and host interfaces remained unchanged.

An external control probe exercised the production client in a separate private
network namespace. Lost allocation and STOP replies, malformed and oversized
replies, cancellation after send, and an unexpected allocation handle retained
unknown outcomes. STOP results `0x0102`, `0x0103` and `0x03f0` retained their raw
values and uncertainty, and a second STOP was refused without sending another
request. Stray peer and transaction replies did not extend the fixed operation
deadline. These observations qualify the selected software control states;
they do not establish a foreign peer's behavior or physical retirement.

The following earlier media qualifications cover the multicast profile.

The external private fixture exercised source create/current/delete, named
Set/Clear, two independent receivers, actual multicast delivery, reference
decoded frame hashes, discovery withdrawal and resource retirement. This is a
bounded lifecycle qualification, not sustained reliability or hardware
interoperability. An earlier fixture run hit the source send-deadline guard;
that failure remains relevant to throughput qualification. No media guard was
relaxed.

The recording-supervisor candidate also completed an isolated 600-second,
18,000-frame source interval. Both software receivers retained 17,970
consecutive reference-matching frames after joining, with every accepted frame
paired in the index and drained. Two receiver-A recording completions exceeded
the unchanged 50-millisecond allowance, with a maximum lateness of 53.699263
milliseconds. This is complete media-integrity evidence but failed timing
qualification.

A subsequent private recording-stage diagnostic completed the same source
interval. Each receiver again retained 17,970 consecutive frames, all matching
the independently decoded packet archive. The maximum recording completion
lateness was 30.237088 and 36.983894 milliseconds, below the unchanged
50-millisecond allowance. Both recordings drained and all owned processes
exited cleanly. No timing witness triggered, so this observation does not
explain the earlier intermittent delays. It used a diagnostic binary, and each
receiver's first-to-last timestamp span was 598.966667 seconds after joining;
it does not qualify a common full 600-second receiver interval. Controller/Studio
interoperability, live display/camera output for this role and user acceptance
remain outstanding.

A clean recording-supervisor build subsequently passed a predeclared common
600-second receiver interval at 1920×1080 and 30 fps. After a 1,920-frame startup
allowance, both receivers retained the same 18,001 consecutive RTP timestamps
through source frame 19,920. All 19,891 retained frames per receiver, including
their accepted startup suffixes, matched the independently decoded packet
archive. All 19,921 source frames and 31,895 archived datagrams reconciled, and
the complete source file matched its generated hash after capture. Maximum
recording completion lateness was 27.611529 and 26.007805 milliseconds; no frame
exceeded the unchanged 50-millisecond allowance. Both recordings drained and
the owned fixture processes retired.

This qualifies that finite private software and storage profile. It does not
explain the earlier intermittent source or recording delays, qualify hardware
or other storage, or establish Controller/Studio interoperability. An offline
cadence-report error was corrected against the retained capture; the original
failed analysis and separate corrected result were preserved without a media
rerun. User acceptance remains outstanding.
