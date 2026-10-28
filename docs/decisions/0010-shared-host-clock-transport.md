# Shared host clock transport

Status: shared backend requirement accepted October 28, 2026; implementation
proposed and awaiting user acceptance.

Chris requested one backend per host so simultaneous virtual endpoints can share
PTP ports. Each existing endpoint owns UDP 319 and 320 exclusively, which prevents
ordinary independent processes from coexisting in one network namespace.

Provide an explicit shared transport service with one exclusive socket pair and
a private local Unix socket. Consumers register their actual interface address
and index, complete PTPv1 domain, local identity and selected parent. Memberships
are reference counted by interface index and multicast group, including address
aliases on the same interface. Dispatch also
checks the complete domain and identity, because multicast groups are buckets.
Independent endpoint followers retain their own calibration, fit, freshness,
generations and media ownership. This shares transport without asserting that
different interfaces, domains or endpoint clock identities are one clock.

Preserve kernel software receive and transmit timestamps. Transmit identifiers
belong to the service socket and are translated to the exact submitting consumer.
The service accepts only that consumer's owned Delay Request. Bounded channels
and write deadlines disconnect a slow consumer rather than silently lose timing
observations. Registration rejects an already owned clock identity on the same
interface and domain. The last consumer releases its memberships.

Transport loss stops the endpoint and retires its owned media through normal
shutdown. An explicit restart performs fresh acquisition; the service does not
replay requests or restore prior clock eligibility. This does not establish a
durable same-name device incarnation guarantee or automatic endpoint restart.
The existing selected-parent PTPv1 profile and software timestamp qualification
remain the supported timing scope. PTPv2, leader election and hardware timestamp
backends require separate implementation and qualification.

Keep exclusive transport as an explicit configuration for a single endpoint.
Shared mode never falls back to exclusive ports. A host service failure affects
its consumers, while a single endpoint disconnect does not stop the service or
other consumers. Independent followers duplicate a small amount of fitting work;
a shared calibrated clock publication would need its own identity and consumer
contract before replacing that discipline boundary.

Rejected alternatives are network namespaces as a product requirement,
SO_REUSEPORT packet balancing, duplicate independently bound PTP sockets, and one
unqualified host-wide clock. The broker is a NetAudio component and requires no
Surface or Redis dependency.

Wire requirements derive from the pinned clean
`dante-ordinary-ptpv1-clock-contract` revision 1 and its
`dante-ordinary-clock-time-basis` revision 1 dependency. IPC, queues, ownership and
lifecycle are independent software design.
