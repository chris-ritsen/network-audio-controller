# Flow performance and configuration storage

NetAudio exposes four typed ARC `0x1101` performance operations:

- receive-flow performance: latency `0x8301` and frames per packet `0x0310`;
- transmit-flow performance: latency `0x8204` and frames per packet `0x0210`;
- unicast performance: the advertised subset of configured latency `0x8205`, configured frames per packet `0x0211`, active latency `0x8301`, and active frames per packet `0x0310`;
- receive-flow default slots: `0x0303`.

The serializers use the negotiated ARC protocol identifier capped at `0x2809`. The operation fails closed when the protocol cohort is unsupported, the property directory has not been observed, or a required property is absent. Latency inputs are unsigned integer microseconds and are checked before conversion to big-endian `u32` nanoseconds. Frames per packet and default slots are big-endian `u16` values. A latency-only call never supplies a frames-per-packet value.

For software versions below `3.0.0`, receive-flow writes also set `0x8304` to `1`, and unicast writes set it to `1000`. NetAudio requires an exact observed `x.y.z` software version before choosing this behavior. It does not guess when the version is missing or has another shape.

Every operation sends one write request, records its ARC reply as request acknowledgement, and issues a fresh `0x1100` query for every affected property. A matching reply sets `effective_state_confirmation` to true. A correlated mismatch sets it to false. The ARC reply does not populate `device_confirmation` or `persistence_confirmation`.

The existing generic latency command remains the capture-backed serializer for its recorded device cohort. The typed performance interfaces use the property-level serializers above; no equivalence between them is assumed.

Configuration storage uses ARC `0x1f01` with no opcode-specific fields and the same protocol cap. Its reply is reported as `persistence_request_acknowledgement`. `persistence_confirmation` remains null until an independent persistence signal or post-reboot readback is available.

The device CLI exposes these operations under `netaudio device config`:

```text
receive-flow-performance LATENCY_MICROSECONDS FRAMES_PER_PACKET
transmit-flow-performance LATENCY_MICROSECONDS FRAMES_PER_PACKET
unicast-performance LATENCY_MICROSECONDS FRAMES_PER_PACKET
receive-flow-default-slots SLOTS
store-current-configuration
```

The HTTP API exposes corresponding `POST` routes named `/set-receive-flow-performance`, `/set-transmit-flow-performance`, `/set-unicast-performance`, `/set-receive-flow-default-slots`, and `/store-current-configuration`. Browser controls consume `performance_operation_availability` and remain unavailable until the required observations exist. Managed devices fail closed because no managed write transport has been established for these operations.

Preset schema version 3 stores typed performance values in the NetAudio extension. Preset application can request storage after every changed setting for a device has a successful effective-state readback. The storage acknowledgement is retained separately and never promoted to persistence confirmation.

## Evidence status

The property identifiers, widths, compatibility values, protocol cap, and storage opcode are **documented** here from the sanitized implementation specification supplied for this task. Deterministic offline tests establish that NetAudio follows that specification. Physical-device interoperability remains **unknown** because this review used no independent permitted capture or device execution. No device, service, VM, route, interface, capture, or media path was contacted while implementing or validating these paths.
