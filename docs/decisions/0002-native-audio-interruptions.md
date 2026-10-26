# Native audio interruptions

Status: proposed; implementation and isolated qualification in progress.

A JACK xrun terminated the endpoint even while its server, sample rate and buffer
size remained available. The same error also represented shutdown and format
changes, preventing useful diagnosis.

Classify callback faults at their source. An xrun or capture/playback queue gap
gates callbacks immediately and wakes the endpoint. Suspend both media directions,
invalidate their sample and receipt generations, then re-prime the existing
streams. Preserve the JACK client, ports, connections and remote flow ownership.
Only fresh output consumption can restore receive-active status. Acknowledging
one interruption must not clear a later interruption or terminal failure.

Permit at most eight interruptions in a ten-second recovery window. Exhaustion
stops the endpoint with an explicit error. Device/server disappearance, format
changes, invalid samples and receipt corruption remain distinct terminal faults:
their stream cannot safely continue without reopening and revalidating ownership.
This decision does not add automatic device replacement or server reconnection.

Service readiness means control sockets, discovery and the selected adapter are
open. Report clock availability separately through event-driven service status;
an active process does not establish media readiness or a successful route.

Ignoring xruns was rejected because it would conceal sample discontinuity.
Restarting the process for each xrun was rejected because it would destroy ports
and flow ownership. Bounded in-place recovery preserves those resources, but
requires explicit generation fencing and separate device-loss recovery work.
