# Dante Controller Subscription-Status Findings

Status: experimentally verified and implemented through 2026-08-14

## Goal and clean-room boundary

The goal is to enumerate how shipping Dante Controller presents every subscription status by exposing controlled values from authentic A32 firmware running under QEMU. Controller is used only as an ordinary UI oracle. No Controller executable, resource, decompiled output, or internal status table is an allowed input.

The canonical raw UI transcription for values `0x0000-0x003f` is outside this repository at:

`../emulation/runs/controller-status-grid-4vm-20260812-01/devices/A32-000001/attempt-05/manual-confirmed-controller-state/controller-ui-observations-00-3f.md`

Its SHA-256 is `757d487ac332a791b42e416b25df32f85b6a9b3c4807b26ed31240d830e9f754`. The contemporaneous Controller/network capture has SHA-256 `d08a8c33594b943309f833f2b810521e2d304783885c65d76ebc4b24873333dd`. The authentic parsed ARC receiver records have SHA-256 `395dc0d926dee4d3202b4c6b47ae081fa1ec40e9ed0d8d947561d4597588faa6`.

## NetAudio implementation

The independently phrased built-in catalog is in:

`packages/netaudio/src/netaudio/dante/subscription_status_catalog.py`

Its implementation-time SHA-256 recorded in the provenance session is `ab4d1f029eeec12b92d0cc58ac0f2d48dcd5effde3014cd263f4e7771bbe67b1`. It covers every value from `0x0000` through `0x003f`. External user labels remain an override layer; they are no longer required for ordinary NetAudio installs to understand this verified range.

That checksum identifies the catalog at promotion time, not an immutable current-file hash. The 2026-08-12 clean UI observation for `0x0007` recorded no label and no symbol. NetAudio therefore does not assign a flow-setup meaning to `0x0007`. A later unbound remark that it was an idle wait state is not a verified fact.

The implementation deliberately does not reuse Controller's literal prose. Raw UI vocabulary remains in the observation artifact, while NetAudio ships independently phrased state, label, and detail values. The adjacent `rx_status_code` is serialized and displayed as a separate uncharacterized receiver-state value instead of being looked up in the subscription-result catalog.

Focused implementation tests are in `tests/test_subscription_status_catalog.py` and `tests/test_packet_dissector.py`.

## Proven ARC record fields

ARC `0x3000` receiver records contain two adjacent, independent 16-bit fields:

| Record offset | NetAudio field | Proven meaning |
| --- | --- | --- |
| `+12` | `rx_status_code` | Receiver-state bitmask; this is not the subscription-result enum |
| `+14` | `subscription_status_code` | Subscription-result enum rendered as labels, details, and symbols by Controller |

Source-channel-name-relative offset `+64` controls the low byte of `subscription_status_code`. Values `0x0000-0x003f` were already enumerated through Controller. The observed set includes connected-self at `0x0004`, connected-unicast at `0x0009`, and connected-multicast at `0x000a`; emulated audio transport is not required for these Controller presentation observations.

The source of the high byte of `subscription_status_code` has not been identified. Do not infer that the adjacent `rx_status_code` supplies it.

## Corrected four-guest grid

The four additional guests A32-000005 through A32-000008 were restored from the invalid receiver-state experiment and reconfigured against the correct field on 2026-08-13. Their coherent subscription ring is:

| Receiving guest | Retained source | Subscription status range |
| --- | --- | --- |
| A32-000005 | `Output NN@A32-000006` | `0x0000-0x003f` |
| A32-000006 | `Output NN@A32-000007` | `0x0040-0x007f` |
| A32-000007 | `Output NN@A32-000008` | `0x0080-0x00bf` |
| A32-000008 | `Output NN@A32-000005` | `0x00c0-0x00ff` |

Every `rx_status_code` was restored to the Controller-safe value `0x0101`. Authentic firmware returned all 256 intended `subscription_status_code` values unchanged through 16 ordinary ARC `0x3000` responses. All source and receiver names were verified, all four guests advertised ARC over mDNS, and all four were left running together for manual Controller observation.

The complete causal procedure, bounded PCAP, pre-change RAM, targeted readbacks, QMP transcripts, exact frames, parsed records, and hashes are outside this repository at:

`../emulation/runs/controller-subscription-status-correct-field-20260813-01/FINDING.md`

The primary result JSON has SHA-256 `0a947aabd5ec1ac77c57d3e1d1d25637a202d05d5115a431d31627872e94fe59`. The 32-packet ARC verification PCAP has SHA-256 `1714eab5071df16ca3464f0a92e64f70f0d8092e04e64d2692866d5da247a7c5`.

## Incorrect receiver-status detour

A later experiment mistakenly fuzzed `rx_status_code` while trying to extend subscription-status enumeration. That field is derived from the byte at source-channel-name-relative offset `+67`, a pointed state reached through the pointer at `+74`, and at least one receiver-position-dependent input.

The original 256-combination receiver-state grid changed the injected byte and receiver position together. It is not a universal byte-to-wire mapping. With the same injected byte `0x3f` and pointed state `0x01`, authentic firmware produced `0xffff` in receiver 1 but `0xff01` in receiver 64. Targeted table and state RAM captured before and after the authentic ARC query was byte-for-byte identical, excluding a firmware rewrite as the cause.

The complete correction and crash provenance are outside this repository at:

`../emulation/runs/controller-receiver-status-0100-01ff-20260813-01/controller-crash-status-isolation-20260813-01/FINDING.md`

That finding has SHA-256 `157017bb161772ed576c95c663df87489bfccb70d6844cd6d72d8899cf839689`.

## Controller crash boundary discovered during the detour

With exactly one candidate `rx_status_code` and the other 63 receiver statuses fixed at safe `0x0101`, Controller produced these bounded outcomes:

| Controller survived | Controller crashed |
| --- | --- |
| `0x00ff` | `0x07ff` |
| `0x01ff` | `0x0fff` |
| `0x03ff` | `0x1fff` |
|  | `0x3fff` |
|  | `0x7fff` |
|  | `0xff01` |
|  | `0xff03` |
|  | `0xff07` |
|  | `0xff0f` |

Each crash value independently terminated Controller and produced a new timestamped macOS diagnostic report. A control with all 64 receiver statuses at `0x0101` survived and was manually confirmed usable. Values `0xff1f`, `0xff3f`, `0xff7f`, and `0xffff` were not tested individually.

These receiver-state crash results are useful protocol robustness evidence, but they are not subscription-status labels and must not displace the original enumeration goal.

## Required continuation

1. Keep every `rx_status_code` at the proven Controller-safe value `0x0101`.
2. Keep coherent source device names, source channel names, receiver names, subscription status of non-target records, and every other field stable.
3. Use the now-running corrected A32-000005 through A32-000008 grid to record Controller-visible meanings for `0x0040-0x00ff`; do not mutate or restart the guests during that observation.
4. Preserve each UI observation with its injected value, exact ARC response, guest RAM address, and independent semantic description.
5. Before testing `0x0100` or higher, causally locate the high-byte source for offset `+14`, or modify the finalized authentic ARC response buffer immediately before transmission while preserving every other response byte.

At the verified live-state check at `2026-08-13T11:26:23-04:00`, A32-000005 through A32-000008 were running and advertising ARC. Dante Controller was not running on `macbook.local`. These facts can drift and must be rechecked before a later continuation.
