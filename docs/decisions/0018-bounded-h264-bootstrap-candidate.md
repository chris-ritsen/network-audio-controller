# Bounded H.264 bootstrap candidate

Status: virtual audio/video endpoint requirement accepted October 29, 2026;
core bootstrap mechanism proposed October 30, 2026 and awaiting acceptance.

The configured receiver requires complete codec parameters before reception.
Ready session-join revision 3, VJ12–VJ17, permits a separate bounded acquisition
stage when the source, destination, interface and current allocation are already
authoritative. Learning those authorities from incoming RTP would violate that
contract. Completing this library stage does not activate an endpoint.

Use one synchronous core bootstrap owner per explicit attempt. Its immutable
inputs identify the accepted allocation, permitted media peer, exclusive fresh
destination and nonreuse basis, receiving interface, owner and clock generations,
authority lifetime, supported codec policy and finite limits. These are caller
proof obligations. The core cannot authenticate a caller's ownership assertion or
verify an operating-system socket reservation. Missing authority remains a
configuration problem for the integrating owner.

Share RTP framing, NAL assembly and full SPS/PPS syntax/reference validation with
the configured path. Validate complete parameter NALs as they arrive. The first
admitted packet starts quarantine age and total byte/packet accounting; complete
parameter-only units do not restart it. Bound both individual assembly and total
acquisition. The owner retains at most one incomplete access unit and one parameter
pair. There is no asynchronous queue: one completed candidate transfers directly
to the caller and terminates the attempt.

Require consistent provisional PT/SSRC, contiguous sequence numbers, compatible
parameters and one complete marker-delimited IDR access unit with validated slice
headers. Display and coded geometry are bounded separately, allowing only normal
macroblock padding beyond the maximum visible dimensions. Explicit cadence and
color supply the required interpretation. Conflicting declared in-band values
reject; non-fixed VUI timing does not manufacture a fixed frame rate.

A conflict, invalid syntax/reference, bound failure, expiry or context change
discards every provisional value. The invalidating packet cannot begin another
attempt. The owner explicitly constructs any subsequent attempt under current
fresh-destination authority; local generation changes never prove wire exclusion.
The exposed deadline supports an owned deadline event without periodic polling.

Success is only `ReadyForProvider`: an immutable authority and parameter snapshot
with one assembled recovery unit, its identity and final packet arrival. The
H.264 parser validates syntax and slice headers; it does not decode entropy-coded
picture data or prove successful output. A later integration must validate the
candidate's current context, obtain actual compatible provider acceptance and
commit activation atomically. It must separately establish matching decoded
output and sink readiness. No network return, provider job, recording, inventory
success, destination reuse or physical effect occurs in this component.

This preserves the configured receiver and gives other core consumers the same
acquisition capability. The cost is explicit caller authority and expiry handling,
plus owned parameter and recovery storage. A second RTP/parser implementation and
an endpoint configuration filled with provisional placeholders were rejected
because they duplicate validation or blur activation. Recording decision 0016 and
unicast decision 0017 remain proposed; this decision accepts neither mechanism.
