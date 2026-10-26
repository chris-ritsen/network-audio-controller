# Native audio adapters

Status: proposed; implementation and host qualification in progress.

The endpoint currently binds its media lifecycle to WAVE files. This prevents
applications from sending and receiving live audio and ties its validation to
recordings. Desktop audio clocks also need not share the network clock.

Separate operating-system audio from network protocol, flow ownership and clock
discipline. Use explicit native backend and device selection through CPAL for
ALSA, PulseAudio, PipeWire, WASAPI and CoreAudio, with optional ASIO. Use the JACK
API for one duplex client with named, initially unconnected ports. File adapters
remain explicitly selected recording tools. Never start an audio server or
silently connect the endpoint to physical speakers.

Callbacks exchange frames through bounded single-producer/single-consumer queues.
Conversion and adaptive resampling happen outside the callbacks. Independent
device clocks require band-limited resampling and bounded drift correction;
an explicitly network-synchronized device may preserve integer PCM exactly.
Output consumption is acknowledged only after the audio callback consumes it,
and route generations invalidate queued audio before replacement playback.

This reuses maintained platform implementations and keeps audio callbacks free
of application locks and allocation. It adds native build dependencies and
requires qualification of latency, discontinuities, device loss and clock drift.
Opening existing native devices does not install a virtual soundcard. System
device publication on Windows and macOS requires separate platform integration.
The network endpoint's Linux timestamp provider must also be ported before
claiming a complete Windows or macOS endpoint.

A JACK-only implementation was rejected because other operating systems and
Linux audio systems are required. File pipes and subprocess audio bridges were
rejected because they obscure clock ownership, errors and stream lifecycle.

Native device channel maps use explicit one-based channel numbers. A mono input
can feed both network channels; selected outputs must be distinct, and unselected
device outputs receive silence. This supports stereo endpoint channels within
larger native devices without inventing a stereo device format.

Default builds include the platform's native API. JACK, PulseAudio, PipeWire and
ASIO are optional Cargo features; Python builds select them explicitly through
`NETAUDIO_AUDIO_BACKENDS`. Device inventory reports the compiled choices. This
keeps ordinary CoreAudio and WASAPI packages independent of Linux audio libraries
and makes optional development-library requirements explicit.
