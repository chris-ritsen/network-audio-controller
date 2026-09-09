# Network configuration evidence

The digest-bound packets and their source capture IDs, frame numbers,
timestamps, transformations, and SHA-256 values are in
[`network_configuration.json`](../tests/fixtures/network_configuration.json).
The source captures remain outside the repository. These observations use
Controller's ordinary interface and device notifications, not static inspection
of proprietary software or firmware. No research artifact is required at runtime.

The observed managed Wing publication in
[`managed_wing_network.json`](../tests/fixtures/managed_wing_network.json)
uses a shorter target-name field than AVIO publications. Its declared packet
offset locates the native response; the parser validates both lengths.

The observed Wing readback in
[`wing_network_fields.json`](../tests/fixtures/wing_network_fields.json)
was matched against Controller's address, netmask, DNS and gateway labels for
both interfaces. DNS and gateway fields remain distinct. Controller accepts
configured gateways outside the configured subnet. The same observation
identifies active Redundant with configured Switched in the network mode flags.

## Observed and causal behavior

Controller 4.16.1.5 and an A32 reporting Dante 4.0.8.2 supplied the redundancy
and dual-interface observations. User-performed Switched/Redundant changes and
reboots distinguish current from configured mode and change the active interface
count. A separate user-performed reboot applies previously configured primary
and secondary static addresses. These transitions are causal evidence; their
captured requests and notifications are observed wire evidence.

The A32's network status carries ordered primary and secondary active records
and a configuration descriptor referencing one stored record per interface.
The secondary static record retains configured gateway and DNS values after
reboot while its active record omits those values. Retention alone therefore
does not establish a pending reboot. Static secondary readback compares the
reported active mode, address, and subnet mask.

An AD4D reporting Dante 4.2.0.28 supplied the mode-choice notification and the
Controller request for Split/Redundant. Its notification distinguishes active
Switched from configured Split/Redundant. The later observation is Switched
again after the user restored that mode. There is no captured active
Split/Redundant readback in this fixture set.

## Inferences and limits

The independent current/configured bit interpretation for the A32 and the
unobserved pending transition back to Switched are inferred from the captured
transitions. The AD4D Switched setter's value is inferred from its mode-choice
table and the observed Split/Redundant request. Synthetic tests exercise these
inferences without upgrading their evidence class.

The A32 secondary-address capture establishes reads only. Wing secondary writes
are supported by the causal requests and readbacks in
[`secondary_network_configuration.json`](../tests/fixtures/secondary_network_configuration.json):
a static request changes only secondary configured DNS; an installed-app DHCP
request preserves primary configuration, active addresses and redundancy.
These are NetAudio experiments, not captured Controller writes. Other secondary
network revisions remain unsupported.

The configured interface and freshly read revision select the Rust encoder.
The web form offers only the modes supported by that interface.

The Rust implementation recognizes the captured network-status revisions and
validates the configuration descriptor before interpreting stored settings.
Previously captured AVIO primary-interface status remains covered by the core
response fixtures. Unknown redundancy variants do not enable writes. A
single-interface zero mode field does not by itself establish redundancy
capability.

Product mutations require fresh preflight and matching configured-state
readback. Verification also compares the other interface settings, active
addresses and reported redundancy against the saved preflight snapshot.
They serialize against other topology changes, do not automatically
retry a write, and never reboot implicitly. Offline tests do not establish that
NetAudio-issued writes have been exercised on physical devices.

## Managed Wing redundancy

**Causal:** the ordinary application API restored configured Redundant while
preserving both DHCP interface records and active Redundant. Fresh readback
reported no pending reboot. Support is limited to the observed Wing revision.

Source: disposable `wing-dante64`, Dante 4.3.1.8, run
`managed-redundancy-app-20260909.l41l53c8`, request at
`2026-09-09T13:35:05.614071+00:00`, readback at
`2026-09-09T13:35:10.812797+00:00`.
Allowed evidence: application requests and network readback. Excluded:
proprietary static contents, runtime memory and authentication data.
The following untransformed API artifacts remain outside the source tree.

| Artifact | SHA-256 |
| --- | --- |
| `wing-before.json` | `11a8651c94d492484e64acbcaabca9675e9110158ed722877fe678542cc44c8c` |
| `redundancy-response.json` | `bc1d8e260512d8b9d6c9e1fc7da9fe8fce102559d7fb752f3535ca4d49d6b92b` |
| `wing-after.json` | `0d61ccf5f653db4ae6fd395a39dec421408ccbd94513c65901c04144f7563b80` |
