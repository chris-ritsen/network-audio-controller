# AVIO clock-status evidence

`avio-aes3.hex` is the complete UDP payload of frame 56 from
`unicast-delay-read-20260904-01/controller-device-config-refresh-1.pcap`.
Extraction: `tshark -r CAPTURE -Y 'frame.number==56' -T fields -e data.data`.
Hex decoding reproduces the original payload without alteration. The full
capture remains in the external research run; tests require only this fixture.

- Timestamp: 2026-09-04T15:50:06.515127-04:00.
- Device: avio-aes3-1, AVIO-AES3, 192.168.1.18.
- Controller host: macOS, 192.168.1.62, interface en0.
- Controller version and device firmware: not recorded in this run; unknown.
- Action: Device View reload with Device Config selected; read-only.
- Request: frame 20, ConMon 0x0021. Response: frame 56, ConMon 0x0020.
- Repetition: refresh-2 capture frames 16 and 41 have the same table geometry.
- Original PCAP SHA-256:
  `0192fe367749a2f3a7a17da30dbd78ba27d31dc47d8b7646c11a1a583eddc507`.
- Decoded fixture SHA-256:
  `321d937b413ace35bab9e5bd08d9ea53a64e2e334c47bcbf355266433a686b0d`.

Observed layout, offsets relative to the record at UDP payload offset 24:

| Field | Offset | Observed bytes |
| --- | ---: | --- |
| Record protocol | 0 | 0738 |
| Port descriptor | 84 | 0060 0c00 0000000c 00980020 |
| Port table header | 96 | 0003 0000 0068 1000 |
| Three port records | 104 | 3 × 16 bytes |
| Additional data | 152 | 44 bytes |

The descriptor's target is 96, twelve bytes after its start. The record target
is 104, eight bytes after the table header. The stride encodings differ from
the previously supported four-byte descriptor layout. Calling these byte-sized
strides with zero padding is an inference; only the observed encoding is
accepted. The additional data must not prevent parsing the preceding port
records. Its semantics remain unknown.

Port fields use NetAudio's existing mappings. This fixture extends their
structural coverage; it does not independently establish those semantic names.
It does not prove a Unicast Delay Requests boolean or a configuration write.
Controller showed Disabled during the physical baseline, but matching that UI
value is insufficient to assign a packet field.

Allowed evidence: captured network traffic and normal Controller UI actions.
No proprietary executable or firmware inspection was used. No physical state
was changed, so restoration was unnecessary.
