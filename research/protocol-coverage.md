# Protocol coverage

This checklist tracks the protocol capability surface identified in Höllentor,
not its interface, licensing, scenes, or host-network utilities. Its implementation
is not evidence for NetAudio's encoders. Source presence, captured packet equality,
and verified device behavior are different levels of evidence; none alone proves
complete interoperability.

## Coverage requirements

| Capability | Current implementation and remaining verification |
| --- | --- |
| mDNS service discovery | Existing discovery lifecycle |
| Periodic discovery refresh | Persistent daemon DNS-SD browser delegates refresh scheduling to zeroconf; finite library scans close at their requested timeout; timed client-specific verification remains |
| Directed discovery refresh | Shared one-shot DNS-SD PTR query, library method, CLI and HTTP; standard-derived and deterministic tests; matching AES3 and A32 service replies observed in bounded live capture |
| Interface selection and socket binding | Rust/C/Python source-address selection and fail-closed discovery implemented; loopback tests plus own-socket inspection and successful read-only name queries to nine physical devices on one Mac; alternate-interface behavior remains unverified |
| ARC framing and transaction matching | Raw transport matches source IPv4 and transaction ID; typed Rust parsers validate lengths and supported protocol/opcode/result combinations; raw replies are not typed-operation confirmation |
| Identity and device names | Existing implementation |
| Device and channel capabilities | Existing implementation |
| Legacy TX channel enumeration | Existing implementation |
| Legacy RX channel enumeration and subscription readback | Existing implementation |
| ARC 2.8.9 and 2.8.15 channel enumeration | Existing implementation |
| Receive-page fetching | Existing paging accumulators |
| Legacy routing and clearing | Existing implementation |
| ARC 2.8.15 subscription pages | Existing implementation |
| ARC 2.8.9 audio subscription pages | Encoder now reproduces four independent Controller exchanges; accepted replies observed; standalone causal route verification remains |
| Same-device routing variants | 2.8.15 shorthand exists; 2.8.9 shorthand remains unverified and rejected |
| Legacy RX channel rename | Existing implementation |
| Alternate legacy TX rename | Required variant and independent encoding remain unestablished |
| ARC 2.8.9 RX and TX rename | Existing implementation |
| TX flow inventory | Legacy and ARC status formats; actual queried revision reported |
| RX flow inventory | Existing implementation |
| ARC 2.8.9 populated multicast allocation | Rust encoder and acknowledgment parser; zero default options word; guarded Python workflow, CLI and HTTP; all three AES3 channel selections causally verified through NetAudio with restoration |
| Empty multicast allocation | Not established; no captured valid request yet |
| Parameterized multicast deletion | Legacy support and captured ARC 2.8.9 flow 2 support; additional identifiers remain unverified |
| Network addressing/configuration reads | Existing implementation; nine physical devices answered read-only checks |
| DHCP/static writes | Existing implementation |
| Switched/redundant mode writes | Capture-derived A32 Switched/Redundant and AD4D Switched/Split/Redundant variants; fresh preflight and configured-state readback; other variants remain unsupported; see [network evidence](network-configuration.md) |
| Reboot | Existing implementation |
| CMC discovery/liveness | Existing implementation |
| AES67-labelled multicast management | Existing AES67 configuration operations; any additional capability beyond multicast allocation remains to be established |

## Multicast allocation

`create_multicast_flow_2809` is a shared Rust command. Unlike legacy creation,
its captured request has no caller-selected global flow slot. The acknowledgment
provides a global identifier, which must be verified against fresh inventory.
The observed media-local identifier is 2; additional identifiers are not inferred
from the global identifier.

The Python `flow allocate` command and `POST /flows/allocate` require explicit
confirmation. The optional `request_options_word` defaults to zero; explicit
values 1 and 113 also reproduce captured exchanges. A bounded Controller 4.16.1.5
experiment on one AES3 recorded zero for channel 1 and both channels, and 113
for channel 2. This rules out a simple device-family mapping. The differing bits
and cross-device interchangeability remain unknown. NetAudio subsequently verified
zero for each of channel 1, channel 2 and both channels on that same AES3. Other
products, firmware versions, identifiers and channel counts remain unverified.
The workflow checks advertised ARC 2.8.9,
available channels, free capacity and media-local identity under the device's
topology lock. It sends one allocation attempt, checks the acknowledgment, and
requires matching readback while preserving preexisting flow identities and
channel assignments. An uncertain outcome is an error, not an automatic retry
or deletion. Managed-control allocation is not yet enabled.

Evidence: [allocation exchanges](../tests/fixtures/multicast_creation_2809.json).
The [AES3 experiment exchanges](../tests/fixtures/multicast_creation_2809_aes3.json)
retain three digest-bound request/acknowledgment pairs with source capture hashes,
frames, timestamps and versions. Controller creation and subsequent inventory
are observed; NetAudio's guarded legacy deletion restored each temporary flow.
No NetAudio allocation was sent during that experiment.
The subsequent [NetAudio verification](../tests/fixtures/multicast_allocation_2809_aes3.json)
is **causal** for the three same-target selections: one allocation attempt per
case, successful acknowledgment, matching fresh inventory, guarded deletion and
empty readback after each. Final stable device and interface settings matched
baseline. The source capture, installed wheel/core, exact frames and local
readback/restoration artifacts are digest-bound; actual multicast media was not
captured or analyzed.
Captured byte equality is **observed**; generalization of length fields is
**inferred**. Neither upgrades acceptance of untested channel counts or options.

## Subscription pages

The shared subscription-page encoder now accepts ARC 2.8.9 for audio only.
That revision retains separate copies of strings for each record, matching the
recorded Controller requests. ARC 2.8.15 retains its independently supported
string sharing and media variants. Unsupported revision/media combinations and
unverified 2.8.9 same-device shorthand fail closed.

Evidence: [subscription exchanges](../tests/fixtures/subscription_2809.json).
The request/reply exchanges are **observed** during a Controller preset load.
They do not establish that the operation alone, without the accompanying preset
reconciliation, causes the requested final routing state. The general routing
workflow has not been silently switched to this newly encoded variant.

## Directed discovery

`netaudio discovery refresh --address <IPv4>` and `POST /discovery/refresh`
with an `address` field request DNS-SD service records from one explicit address.
Omitting the address requests multicast discovery. These operations reuse the
running discovery transport and its normal response listeners; they do not reset
inventory, mark devices online, start another browser or claim discovery success
merely because a query was requested. The daemon's discovery transport honors
the configured local interface. The library browser exposes `refresh_discovery`
while running.

The query behavior is **documented** by
[RFC 6762 sections 5.4–5.5](https://datatracker.ietf.org/doc/html/rfc6762#section-5.5)
and uses [python-zeroconf's public query objects and sender](https://python-zeroconf.readthedocs.io/en/stable/api.html).
The deterministic tests check the actual encoded questions, explicit destination,
one-shot behavior, no implicit browser startup and invalid-address rejection.
Direct queries do not promise discovery across routers; responders may reject
off-link sources. A bounded [read-only verification](directed-discovery-verification.json)
**observed** matching service replies after directed queries to AES3 and A32;
AES3 replied to multicast and A32 replied to the requester. Both were already
known, so this does not establish a new inventory transition. Other products
and network conditions remain **unknown**. No competing implementation supplies
encoding facts.

## Interface selection audit

The 2026-09-06 source audit found that discovery source selection did not reach
the ARC transport and that the library browser could fall back to default
interfaces. The shared Rust `Client::new` and ABI 6 C constructor now accept an
optional local address. Explicit addresses must be unicast IPv4; binding errors
are returned without retrying on an unspecified source. An omitted address still
allows OS source selection. Native host-MAC lookup follows an explicitly selected
address rather than the default route.

Python `CoreClient` exposes `local_ip`. Cached transport clients, daemon probes,
standalone flow and lock helpers, and instrumented inventory pass the configured
source. Cache identity includes that address. Interface resolution is fresh on
each access and rejects a missing interface or one without IPv4. Daemon and
library discovery use the resolved address without falling back.

The native tests inspect the actual loopback socket's bound address and ephemeral
port, and verify invalid/unavailable-source errors. FFI tests cover constructor
validation and null output on failure. `tests/test_interface_selection.py` checks
Python forwarding, cache separation, interface disappearance and address changes,
and no client/browser creation on resolution failure. These are **observed**
offline implementation results, not newly established Dante wire behavior.
A subsequent [bounded read-only run](source-selection-verification.json)
**observed** the selected address on all nine client sockets using `lsof`, and
matching device-name replies from nine physical devices. Their advertised ARC
revisions were 2.7.41, 2.8.1 and 2.8.9. The native artifact, runner, timestamp and
local results are digest-bound. No device writes or captures were performed.
The separate lock/unlock socket inherits the parent's bound address; its binding
is covered offline, not by a physical lock operation.
OS-specific interface pinning and alternate-interface behavior remain **unknown**;
one selected source on one Mac does not prove interface pinning on every OS.

## Coverage audit boundaries

The continuous library discovery regression in
[`test_browser_lifecycle.py`](../tests/test_browser_lifecycle.py) verifies that a
zero-timeout scan retains its active DNS-SD browser until explicit close. The
finite-scan regression verifies closure at the requested timeout. Together with
the daemon's persistent `AsyncServiceBrowser`, these establish lifecycle
integration, not a timed observation of zeroconf's query-renewal scheduler.

ARC response validation is split deliberately. `Client::wait_for_response`
correlates source IPv4 and transaction ID; it does not establish that a raw reply
confirms a particular operation. `protocol::response_envelope` validates framing
length, and typed parsers use `validate_response_envelope` or explicit supported
protocol/opcode checks. The
[`response tests`](../tests/test_core_responses.py) and native parser tests check
those boundaries. Raw transport tests must not be described as full semantic
validation, nor should raw-returning research operations be confused with the
product's verified-write workflows.

The existing
[`ARC page tests`](../tests/test_modern_arc_capture.py) reproduce captured
requests and merge all four transmitter and six receiver pages, including a
short final receiver page. They also reject no-progress pagination, conflicting
records and the page limit. This is stronger evidence for receive-page fetching
than a single successful packet parse. The
[`command tests`](../tests/test_core_commands.py) separately compare DHCP,
static-address and reboot commands against preserved Controller requests; they
do not constitute new live device-write verification in this audit.

## Remaining experimental gates

Physical device writes require exact target/treatment authorization, exclusive
leases, bounded manifests, baseline and fresh readback, and restoration. No
physical mutation is authorized by this checklist. Empty allocations, alternate
rename forms, same-device subscription variants and switch-mode setters must not
be enabled using competing-application layouts or guessed device behavior.

iOS's vendored core was rebuilt from source commit
`a63ca7d45384f61ed3417f0a1ba794ddcdd41394`, with ABI 6 and digest-bound provenance.
Its regenerated golden files cover 63 command and 37 response cases, including
ten capture-bound cases in each. The Swift constructor accepts an explicit local
source address and rejects invalid or unavailable sources. The full simulator
suite passed all 347 tests, including the three new source-address tests.
This verifies the vendored core and simulator integration, not a physical-phone
deployment or a TestFlight release. No public release was performed.
