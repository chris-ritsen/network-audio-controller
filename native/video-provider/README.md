# H.264 parameter configuration provider

This package builds an isolated FFmpeg and GStreamer gst-libav provider with an
explicit native H.264 parameter-acceptance result. It does not install libraries
or plugins into the system. The ordinary configured endpoint path does not opt
into this capability. Runtime bootstrap activation is a separate integration.

Build with Python 3.12 or newer, a C toolchain, GNU make, pkg-config, NASM, Meson,
Ninja, and GStreamer development packages:

```sh
python scripts/build_video_provider.py --directory /absolute/new/build-directory
```

The directory must be new and outside the product checkout. `--source-cache`
can name a directory containing `ffmpeg.tar.xz` and `gst-libav.tar.xz`; hashes
and byte lengths are checked regardless of download or cache use. Source and
patch pins are in `sources.json`. A mismatch or patch failure stops the build.
Logs, staged binaries, exact artifact hashes, and the process environment are
retained in the build directory. Build failure leaves evidence for inspection.

The build profile contains the software H.264 decoder and its native libraries.
It is not a general-purpose FFmpeg installation. Use the `build.json` environment
only for the process owning this provider; do not export it into user services
or a login environment. The application must inspect the loaded plugin path and
receipt capability. Missing capability is an error, not permission to use the
system decoder as if it supplied the same receipt.

## Provider contract

Construct `avdec_h264` with `require-parameter-configuration=true`. This property
is immutable after construction. The mode uses exactly one native decode thread
and requires full resolution. Default construction retains the ordinary path.

Supply `video/x-h264` CAPS with `stream-format=avc`, `alignment=au`, exact positive
display width/height and cadence, and `codec_data` containing the canonical V1
configuration record. The record contains one SPS followed by its linked PPS,
four-byte media NAL lengths, exact profile/compatibility/level bytes from the SPS,
and no trailing extension fields. Each complete parameter NAL is at most 4096
bytes, matching the endpoint's bound and the pinned native parameter storage.
Nonzero NAL reference priorities are accepted. Baseline, Main, and supported
eight-bit High parameters use the same envelope; this is not a generic importer
for arbitrary avcC extension records or multiple parameter-set collections.

The endpoint's core validator remains responsible for its selected profile,
cadence, color, geometry and reference-frame policy. The native decoder parses
the exact SPS/PPS into its own parameter state without tolerant parsing retries.
Missing or unlinked parameters, incomplete syntax, extra residual syntax,
invalid framing, incompatible geometry and unsupported provider capability fail
configuration. Native acceptance does not promise successful entropy decoding,
output format negotiation, playout, or physical presentation.

While the owned pipeline is paused, send the CAPS event directly to the actual
decoder sink pad. A successful event alone is insufficient. Read the decoder's
`parameter-configuration` property and require a non-null structure named
`netaudio-h264-parameter-configuration-v1`. It contains:

- `generation`: a monotonically increasing native-context generation within
  this particular GStreamer element; it is not a wire or source epoch.
- `codec-data`: a copy of the bytes accepted by that native context.
- `width` and `height`: geometry obtained from its native parsed parameters and
  checked against the requested CAPS.
- `threads`: the actual native thread count, which is one in this profile.
- `native-version`: the pinned FFmpeg build version carrying this capability.

Bind this snapshot to the owned element identity, exact candidate parameters,
current source/allocation/interface/clock generation and expiry. The property is
a current snapshot, not an independently reusable authorization token. The
consumer owns serialization of configuration, activation, submission and
retirement; a copied structure does not prevent a later reconfiguration.

No picture is submitted during configuration. After the runtime commits its
current ACTIVE session, give appsrc the identical CAPS and submit four-byte
length-prefixed access units directly to this decoder. Do not insert h264parse:
its later CAPS negotiation can reopen the native context. Preserve original
access-unit identity and arrival time. Require the same receipt immediately
before submission and reject any later generation change or revocation.

Identical in-band SPS/PPS are allowed. Different parameter sets, parameter-set
extensions, packet side-data replacement and in-band configuration records
revoke native acceptance and fail. No permissive reconfiguration path runs in
this mode. The wrapper clears its receipt before reporting the native failure.
Explicit CAPS reconfiguration creates another native generation; failed
configuration clears the cached CAPS and receipt. Retirement clears the receipt.
`notify::parameter-configuration` accompanies configuration, retirement and
native revocation; bus errors and matching output remain separate observations.

The runtime must recheck cancellation, ownership, source currentness and candidate
expiry after asynchronous preparation. It must retain the provider and its
setup/retirement worker until quiescence is established. Neither this native
capability nor its bounded offline probe implements those runtime transitions.

## Maintenance and licensing

These are maintained NetAudio provider extensions to the exact upstream releases
named in the manifest, not capabilities claimed for an unmodified system plugin.
The bridge uses FFmpeg's AVOptions and GStreamer's GObject properties; it does not
inspect private native layouts from application code. Upgrades require reviewing
and rebuilding both patches together and repeating native acceptance/rejection,
revocation, unchanged-generation decoding and configured-path comparisons.

The upstream archives retain their copyright notices and LGPL license files.
The patches modify those LGPL sources and are distributed under their respective
upstream terms. The build disables external autodetection and does not enable
GPL or nonfree components. Preserve source, patches, notices and applicable
license material with any distribution of the resulting native libraries.
