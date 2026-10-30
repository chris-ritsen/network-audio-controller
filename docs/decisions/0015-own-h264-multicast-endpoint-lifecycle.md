# Own H.264 multicast endpoint lifecycle

Status: requested virtual-video endpoint requirement accepted October 29,
2026; implementation proposed and awaiting user acceptance.

The configured receiver can join an explicitly supplied multicast group, while
the original configured transmitter only supported an explicitly supplied
unicast destination. Neither direction is a virtual video endpoint. They do
not own a Media-4 resource lifecycle, publish a named source, accept named
receiver intent, or expose truthful current state. Treating their setup or a
successful socket bind as an active endpoint would mislead Controller and
NetAudio consumers.

The endpoint owns one bounded ordinary unprotected IPv4 H.264 multicast source
and independently owned receivers. It treats creation, current inventory,
DNS-SD publication, named Set/Clear intent, socket membership, encoded RTP,
decoded output, withdrawal and source deletion as separate transitions. It
uses the selected Media-4 0x2601/0x2602 forms and the one-slot source profile;
group, port, interface, codec input and provider authority are explicit owned
configuration and never inferred from a request, ACK, or first packet.

Its media resource model retains configured capacity, current allocation,
intent, actual group membership, provider activity, packet receipt, decoded
output and sink publication independently. Events emitted by the media workers
update that state directly. There is no status-file polling and no readiness or
processing claim from decoder setup, a successful join, a source ACK, or a
DNS-SD announcement. A receiver may report the exact accepted named intent
before media is ready, but current dynamic state remains unresolved until the
relevant observed transition occurs.

The endpoint withdraws its publications before resource reuse and fences each
worker and callback with its source or receiver generation. A Clear retires
only that receiver. A 0x2602 request authorizes source deletion even while
another receiver remains joined: multicast has neither receiver liveness nor a
remote-membership reference count. Locally owned source resources may wait for
their own final reference to retire, followed by a fresh current absence
observation. Lost control replies leave the outcome unknown and require current
readback; mutations are never blindly replayed.

This decision excludes ordinary Controller-selected unicast route-to-session
construction. It also excludes H.265, audio header reuse,
unicast receiver returns, synchronized physical presentation, and claims about
third-party admission.

Scope clarification, October 30, 2026: the original explanation incorrectly
treated reviewed own-unicast response/finalization and session semantics as
missing. `dante-studio-own-h264-session-join` revision 3 and
`dante-studio-own-h264-lifecycle-acceptance` revision 3 are ready and complete
for their selected own-unicast, supplied-authority scope. The multicast-only
implementation decision remains unchanged. The unresolved foreign-source
contract is the association of a current allocation with its permitted RTP
sender IPv4 address and UDP source port before first-packet admission, as
bounded by session-join clause VJ12.
