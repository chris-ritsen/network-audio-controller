# Sample-rate capability implementation note

This file is intentionally local and excluded by `.gitignore`. It records the evidence and deferred capture work behind the app-facing sample-rate capability implementation.

## Current evidence

ConMon/Settings multicast message `0x0080` is a periodic sample-rate status announcement on `224.0.0.231:8702`.

Tagged evidence already recorded in `tests/fixtures/provenance/facts.json`:

- `sample_rate_discovery:28101`, LX-Dante, 72 bytes
- `fact_catalog_expansion:709014`, Ferrofish A32, 72 bytes
- `fact_catalog_expansion:678575`, Ferrofish A32, 72 bytes

The verified fields are:

- Current sample rate: big-endian `u32` at `0x24`
- Supported-rate list start: `0x30`

All three packets use this structure:

- `0x20`: `0x0018`
- `0x22`: `0x0006`
- `0x24`: current sample rate
- `0x30`: six big-endian `u32` values
- Advertised rates: `44100`, `48000`, `88200`, `96000`, `176400`, `192000`

The implementation interprets `0x20` as the list byte length and `0x22` as the list element count. The equality `24 == 6 * 4`, the packet length, and the following six values strongly support that interpretation. Those two metadata fields have not yet been separately promoted to verified fields in `facts.json`.

## Deferred evidence work

No committed AVIO capture currently contains message `0x0080`. AVIO devices are expected to advertise a single-element `[48000]` list, but that expectation is not claimed as verified.

When protocol investigation becomes the priority again:

1. Start daemon capture, capture collection, wire capture, and a named capture session.
2. Collect at least one `0x0080` heartbeat from an AVIO adapter.
3. Tag the packet as evidence in the active capture session.
4. Verify the values at `0x20`, `0x22`, `0x24`, and `0x30`.
5. Promote list byte length and list count into `facts.json` through the fact CLI.
6. Add device-variant tests if AVIO framing differs.

## App-facing contract

The shared core parse kind is `sample_rate_status` and returns:

```json
{
  "current_sample_rate": 48000,
  "supported_sample_rates": [44100, 48000, 88200, 96000, 176400, 192000]
}
```

The Python device and relay field is `supported_sample_rates`. Absence means the current discovery session has not received a valid `0x0080` announcement. An empty list, if ever advertised, remains distinct from absence.

The application must not substitute the global six-rate command whitelist when this field is absent and must not persist the field as live device state across discovery sessions.

## iOS development handoff

The iOS repository is `../netaudio-ios`. Its worktree is already extensively modified, including the integration files and vendored core binary. Patch the existing worktree and do not restore files from `HEAD`.

Direct mode:

- Add `CoreSampleRateStatus` with `currentSampleRate: UInt32` and `supportedSampleRates: [UInt32]` in `netaudio/Core/CoreModels.swift`.
- In `netaudio/Services/ConmonNotificationService.swift`, handle opcode `0x0080` with `CoreFunctions.parseResponse(kind: "sample_rate_status", data: data)`.
- Assign the parsed current rate and supported list to the current-session device.

Shared device state:

- Add `supportedSampleRates: [Int]?` beside `sampleRate` in `netaudio/Models/DanteDevice.swift`.
- Keep `nil` distinct from an empty list. `nil` means no valid announcement has arrived during the current discovery session.
- Do not add this field to `DeviceCache`; a cached capability must never be presented as current.
- Clear the field in both offline paths: relay `onDeviceRemoved` and direct `markMissingBrowserDeviceOffline`.

Relay mode:

- Add optional `supported_sample_rates` decoding to `RelayDevice` in `netaudio/Services/RelayClient.swift`.
- Assign the decoded value, including `nil`, during device synchronization in `netaudio/Services/DeviceManager.swift` so switching transports cannot retain stale data.
- The Python relay now emits a deduplicated `device_updated` SSE event whenever the `0x0080` current rate or supported list changes.

UI and mutation validation:

- Replace the hard-coded per-device choices in `SampleRatePickerView` in `netaudio/Views/DeviceDetailView.swift` with `device.supportedSampleRates`.
- Replace the per-device hard-coded menus in `NetworkConfigView` in `netaudio/Views/DeviceListView.swift`.
- A one-element list should be displayed as fixed-rate.
- Unknown capabilities should display as unavailable or still discovering, without falling back to the global list.
- Viewing the supported list is informational and must not require the configure entitlement. Only mutation remains gated.
- For network-wide changes, expose only the intersection after every online device has reported a capability list.
- Add the central known-list rejection to `DeviceManager.setSampleRate`. Preserve existing behavior when the capability list is `nil` for compatibility with older relays and non-UI callers.

Verification:

- Add relay JSON decoding coverage for `supported_sample_rates`.
- Add direct ConMon coverage using the tagged 72-byte packet recorded above.
- Copy the complete regenerated response golden, which now includes `sample_rate_status_lx-dante`, after rebuilding the vendored core.
- Add debug-server output for `supportedSampleRates` so `/devices` can verify it without screenshots.
- Fixture UI data may use `[48000]` for an AVIO simulation, but the test must not describe that value as captured protocol evidence.
- Verify that LX-Dante exposes 96 kHz and the simulated AVIO does not.

Core binary and provenance:

- The `0x0080` parser adds no new C symbol or signature change, so the generic C ABI remains version 2.
- The currently vendored iOS XCFramework and its parity assertion still identify ABI 1. Replacing it therefore performs the already-pending ABI-1-to-ABI-2 migration; this parser does not require ABI 3.
- Ask the user to commit the exact netaudio core source before running `scripts/build_xcframework.sh`.
- Rebuild and replace the XCFramework only from that committed source. Use the build script's generated `Vendor/netaudio_core.xcframework/PROVENANCE.md`; do not fabricate provenance manually.
- Update the iOS ABI assertion to 2 and synchronize the complete regenerated command and response goldens, not only the sample-rate case.
- Land the other ABI-2 migration work in the same iOS change: lock/unlock key-length arguments in `CoreClient.swift`, `netaudio_status_name(Int32(...))` in `CoreError.swift`, the ABI assertion, complete goldens, and CI checksums.
- Update the framework checksum expectations in `.github/workflows/ios.yml`.
