# Own H.264 unicast acquisition

Status: requested virtual-video endpoint requirement accepted October 29,
2026; integration mechanism proposed October 30, 2026 and awaiting acceptance.

The own multicast endpoint already owns named receiver intent, current inventory,
provider generations and retirement. Its advertised channel service does not
implement unicast source allocation. The ready session-join and
lifecycle-acceptance contracts, both revision 3, supply that selected protocol
scope when current source and receiver authority are provided explicitly.

Use one endpoint lifecycle for both transports. A typed source transport owns
either multicast creation or a separately bound unicast source-control service.
A typed receiver acquisition supplies either retained multicast discovery and
membership or channel discovery, a reserved unicast destination and one owned
allocation transaction. Both consume the same configured media workers,
inventory projection, cancellation and retirement machinery. A separate full
unicast runtime would duplicate these responsibilities.

The selected source owns one explicitly configured channel and flow. Its
unicast configuration identifies the permitted control requester, receiver host,
destination and actual allocation generation. It reserves its media socket
before advertising the actual control port. A matching allocation commits that
owned resource before returning its handle and interface result. It does not
claim encoder output, receiver decoding or presentation. Exact duplicate
requests can receive their retained result; transaction reuse with different
request bytes cannot allocate or stop another resource.

The receiver reserves its destination before allocation and uses an explicit
immutable source-media tuple, codec configuration and authority lifetime. It
requires the expected owned allocation result before configured decoding. It
does not infer source identity or image parameters from the control port,
allocation handle, discovery codec or first RTP packet. This uses the fully
configured session branch. Guarded in-band parameter acquisition and automatic
foreign-source endpoint acquisition remain separate work.

Allocation and STOP have distinct single-use transaction identities and bounded
reply waits. A lost or malformed response retains an unknown outcome. There is
no automatic retry, inferred rollback or destination reuse. STOP uses only an
actually acquired handle; its control result remains distinct from local media
and recording retirement. The configured finite source lifetime and observable
local ownership do not manufacture remote completion after a lost reply.

This shares lifecycle implementation while adding explicit control state and
resource ownership. It also replaces the transport-specific serving command
with a common endpoint command and requires each configured resource to name
its transport. Recording remains the proposed mechanism in decision 0016;
using it during private integration does not accept that design or establish
physical output readiness. Earlier finite qualifications and intermittent
failures retain their original scope.
