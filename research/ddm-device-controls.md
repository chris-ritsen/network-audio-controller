# DDM device settings

## Scope and evidence

This change retains managed status in the daemon, enables the existing settings
controls, and adds an acknowledged managed reboot request. Network configuration
parsing remains in the Rust core. Allowed evidence consists of public product
documentation, the documented Managed API, existing independent NetAudio protocol
implementations, authenticated status replies, and bounded causal treatments on
the two enrolled analog AVIO adapters authorized by the user. Proprietary static
inspection, runtime memory, unrelated traffic and credentials were excluded.

| Class | Finding |
| --- | --- |
| documented | Controller exposes device latency, sample rate and preferred encoding according to device capabilities; enrollment can permit additional latency values. |
| documented | The two-channel analog AVIO input/output products support 44.1, 48, 88.2 and 96 kHz. |
| observed | Enrolled models `AVIO-DAI2` and `AVIO-DAO2`, both reporting Dante firmware `4.2.4.1`, returned latency, sample-rate/encoding choices, analog gain, interfaces and clock status through the existing managed transport. |
| observed | Both reported active/configured latency of 1 ms, minimum 1 ms and maximum 20.3125 ms, four sample rates, and 16/24/32-bit encoding choices. |
| inferred | Recreating a device per HTTP request and resolving managed publications only by IP prevented settings from being retained. Focused synthetic tests reproduce both paths. |
| inferred | The documented fixed channel counts apply across the documented rates for these two analog AVIO models. Fresh counts must match before and after a rate change. |
| causal | Both enrolled adapters accepted latency 1 → 2 → 1 ms, encoding changes to 16 bit and restoration, one supported channel-gain change and restoration, and sample rate 48 → 96 → 48 kHz. Each result was independently read back. |
| causal | The enrolled input accepted preferred-leader false → true → false. |
| causal | Changing the input to 96 kHz while its receiver remained at 48 kHz retired its automatic unicast flow. Configured receiver subscriptions were preserved. |
| causal | On the enrolled output, the existing reboot command received a correlated settings acknowledgement. A fresh connection timestamp and activation of pending DNS configuration followed. Polling did not sample a non-ready connection state. A second reboot applied restoration of the original DNS. |
| causal | The output's network status revision `0x0738` carries the same strictly validated configuration descriptor as the identified older revisions. Three status replies distinguish unchanged, pending DNS, and restored settings. |
| unknown | Behavior on unobserved Controller-service versions, device revisions and unsupported control operations is not established by passing tests. |

Public references, reviewed 2026-09-08:

- [Controller Device Config](https://dev.audinate.com/GA/dante-controller/userguide/webhelp/content/device_config_tab.htm)
- [Analog AVIO product specification, June 2025](https://www.getdante.com/wp-content/uploads/2025/06/dante_avio-analog-20250604.pdf)
- The documented Managed API schema in
  `packages/netaudio/src/netaudio/ddm/schema.json` describes clocking groups as
  owning system-generated PTP subdomains. The device view presents enrolled
  clock subdomains as managed by DDM.

## Artifact provenance

Run `managed-controls-20260908.3cfd91xc` retains the complete local provenance
outside the repository, including source device, server/domain identities,
exclusive leases, baselines, bounded manifests, operation timestamps and
restoration readbacks. Product code contains no lab endpoints or credentials.

The initial read interval was 2026-09-08 00:26:17–00:27:13 UTC. Core setting
validation ran 00:33:14–00:45:00 UTC. The network treatment ran at 00:45:31 UTC;
reboot observation ran 00:47:07–00:47:39 UTC, and DNS restoration with reboot ran
00:48:07–00:48:40 UTC. The final independent settings comparison at
00:50:00–00:50:07 UTC matched the original baseline exactly, including interface
settings and sample-rate preflight topology.

| Artifact | SHA-256 |
| --- | --- |
| `managed-settings-before.json` | `b4a68bd5c35616c2edb7377dc655eb576cf0180408c06ce0fea3e940a9a01bf7` |
| `managed-settings-after.json` | `c0451bdb1369bee8b6ef5b8c097603921b75f986c271b5ee05608b5a774d6fdf` |
| `verify_reads.py` | `e4fe2c5ecda5954c6fda4f2fd307723089397dc7ca9a38b920a02a3b88fe5224` |
| `settings-validation.json` | `6f9922398e03e3beeb7c31796f59da288bf80fb1fe7b7917d71abc06ca70a8c9` |
| `validate_settings.py` | `67e7bdee21e6f13052913866909cbefd64d7b33f580f545766fcacd073a60567` |
| `network-validation.json` | `2653e0955405682deb56062a1f020c97da607696f3193613dfec0f16f1154ae7` |
| `reboot-validation.json` | `813c2adeb72c88e6effcebf3d98e0b3acac55fea0c3935274e8087cc91a2a364` |
| `network-restoration.json` | `f4c69c1b18615483d4f371004bb7c77e7c81ac34d3b58e25eea487eebea22581` |
| `final-managed-settings.json` | `ddef26390f7b102fcad60ad4febd5926d8cfc752255ad8dae980a72ffe1dc8e8` |
| `network-status-observation.json` | `d77f4f954e9cf6ba5873b15de1985ba2f2e2a2d6bcdd30890ffce78de3a54d78` |
| `observe_network_status.py` | `12f612ff03e46b0ffc304d9538ef6299e63c34e209bc643a46b229715e4fcf0f` |
| `network-http-validation.json` | `d1386b3a89615921ed4a38f8b3d0aeb78d4177a1060db6e1039be88b02af4646` |
| `validate_network_http.py` | `07a692bfc6dce5de36c4f12118c0f9faa4fc52f76b045a666622753ead2f839d` |
| `deployed-managed-settings.json` | `664ad0ecdca74f892f0f84c500c68aa7d18ab6bd434e3f30e6947ff5acf06736` |

The observation script selects identity/configuration fields from
`DanteDeviceSerializer` and saves typed preflight results as sorted JSON.
These are application observations, not packet captures. The first network
readback incorrectly appeared restored because the parser substituted active
settings for configured settings. Its manifest records this subsequent finding;
restoration was established only after the second reboot and final comparison.

A later bounded DNS treatment produced three 96-byte settings replies. The
minimal sanitized replies are promoted in
`tests/fixtures/managed_network_configuration.json`. Each records its exact
source artifact, timestamp, source SHA-256, transformed SHA-256 and explicit
identity/address substitutions. Framing, lengths, descriptors and offsets are
unchanged. No authentication or session frames are included. The pending reply
changes the configuration record while retaining active DNS; the restoration
reply clears that record. The corrected Rust parser verifies both transitions.
Complete inventories, local harnesses and complete runs remain outside the tree.

After deployment, the installed HTTP interface accepted the configured DNS
change, reported it pending, accepted restoration and returned the exact saved
network baseline. No reboot was sent for this final validation. A fresh complete
managed-settings comparison matched the original baseline, and subscription
endpoints remained unchanged.

## Behavior and limits

Managed device objects retain settings across inventory polls. Server, context,
domain and device identities scope status delivery. Inventory removal invalidates
managed-only objects; losing a manager never permits direct-control fallback.
Discovery and periodic refresh populate settings without an open browser or a
direct device address.

Latency writes require a matching active-latency readback. Custom entry is bounded
by the reported range, which does not guarantee every intermediate value is
accepted. Pull-up choices use tuning labels and omit unknown values.

Sample-rate preflight supports the identified analog AVIO models and the existing
A32 profile. Managed snapshots validate fresh channel counts and typed modern
flow records. Zero channel counts avoid querying nonexistent directions. Modern
records do not report frames per packet, so it remains unknown. Changes verify
rate, capacity, configured subscriptions and retained flow membership. Automatic
modern unicast flows may disappear when capacity does not contract; multicast
membership remains strict. The browser requires a separate action before a
reported destructive membership change. Unsupported models fail closed.

Managed reboot requires a fresh reset capability and ready connection, an
advertised v2 Controller API, matching domain, target selection and correlated
acknowledgement. The HTTP response remains accepted-but-unverified; request
acceptance does not prove the device has finished restarting. No automatic retry
or factory reset is added.

Network writes support the identified `0x0738` configuration descriptor and
require matching configured readback without automatically rebooting. Unknown
revisions do not invent configured values. Active and pending settings are shown
separately, including whether the observed change requires reboot.

Managed device lock, multicast allocation, detailed metering and redundancy
changes retain their unsupported paths. AES67 and clock-domain configuration
follow DDM's domain-level rules. This is not a claim that every managed operation
or every Dante device model has complete parity.
