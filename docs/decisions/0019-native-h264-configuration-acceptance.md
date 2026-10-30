# Native H.264 configuration acceptance

Status: isolated provider capability requirement accepted October 30, 2026;
mechanism proposed and awaiting Chris's implementation acceptance.

The guarded receiver must obtain actual compatible parameter acceptance from the
same decoder before ACTIVE and recovery submission. The installed gst-libav
provider returns successful CAPS handling even after FFmpeg reports extradata
failure. Its strict-compliance property does not repair this. Native inspection
also found swallowed parameter-parser errors, so exposing error strictness alone
would not establish the required result.

Use a versioned opt-in capability in pinned FFmpeg and gst-libav source builds.
Parse one bounded canonical SPS/PPS pair directly into the native decoder's own
state with the existing native parsers, without permissive retries. Export native
acceptance through AVOptions and an instance-generation-bound GStreamer receipt
containing the exact parameter bytes and native geometry. Reject later parameter
replacement and revoke the receipt on failure, reconfiguration or retirement.
Keep the ordinary configured path outside this opt-in mode.

The product owns the patches, source pins, build entry point and private prefix.
No global library installation is part of this decision. Strict mode selects one
software decode thread and the endpoint's finite parameter bounds. The V1
configuration envelope has one SPS/PPS pair and no avcC extension records. This
adds maintenance obligations when either upstream decoder changes; it avoids
private-layout access, debug-log interpretation and fabricated acceptance.

The eventual runtime binds the receipt to its current source and candidate,
commits ACTIVE, and submits AVC access units directly to the same instance.
Inserting h264parse after preparation can replace CAPS and reopen the decoder;
that would invalidate the receipt. Actual decoded output remains a later
observation. Recording and unicast decisions 0016–0018 remain proposed.

Replacing the backend would broaden the change without removing the need for an
explicit native result. Treating CAPS success, pipeline state, parser success or
pre-ACTIVE decoding as the result was rejected. The October 30 delegated scope
authorized isolated provider source changes and offline validation, with no live
installation or devices and no repository behavior tests before acceptance.
