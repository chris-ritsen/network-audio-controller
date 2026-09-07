# Network configuration evidence

The digest-bound packets and their source capture IDs, frame numbers,
timestamps, transformations, and SHA-256 values are in
[`network_configuration.json`](../tests/fixtures/network_configuration.json).
The source captures remain outside the repository. These observations use
Controller's ordinary interface and device notifications, not static inspection
of proprietary software or firmware. No research artifact is required at runtime.

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

The secondary-address configuration capture began after the user saved its
settings. It establishes secondary reads, not the secondary write request.
Secondary writes remain unavailable; NetAudio does not guess a selector or
send a primary-address command for a secondary target.

The Rust implementation recognizes the captured network-status revisions and
validates the configuration descriptor before interpreting stored settings.
Previously captured AVIO primary-interface status remains covered by the core
response fixtures. Unknown redundancy variants do not enable writes. A
single-interface zero mode field does not by itself establish redundancy
capability.

Product mutations require fresh preflight and matching configured-state
readback. They serialize against other topology changes, do not automatically
retry a write, and never reboot implicitly. Offline tests do not establish that
NetAudio-issued writes have been exercised on physical devices.
