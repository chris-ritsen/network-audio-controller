# NetAudio — unofficial Dante Controller alternative

NetAudio is an unofficial Dante Controller alternative for discovering,
routing, configuring, and monitoring Dante network audio devices. The project
provides open-source command-line tools, a web interface, a persistent daemon,
and a protocol library for Linux, macOS, and Windows, alongside a native
**iPhone and iPad app for iOS and iPadOS**.

**NetAudio is a native Dante Controller alternative for iOS and iPadOS,
available now for iPhone and iPad through
[TestFlight](https://testflight.apple.com/join/GcuDerST).** The app communicates
directly with Dante devices for live discovery, audio subscription routing, device and channel configuration, live device metering, clock and
status monitoring, and diagnostics. Local direct-device control runs on the
phone or tablet without a separate computer, remote desktop session, or NetAudio
daemon. The app also supports Dante Domain Manager (DDM) networks and connections
to a NetAudio daemon.

[Website and iOS app](https://netaudio.app/) ·
[Install the CLI and web interface](#installation) ·
[Documentation](#documentation)

## Interfaces and architecture

NetAudio serves both interactive device control and software automation. A
shared Rust protocol core implements Dante packet encoding and parsing for the
Python application and native iOS client.

| Interface | Use |
| --- | --- |
| Native iOS/iPadOS app | Discover, route, configure, and monitor devices from an iPhone or iPad using direct control or a NetAudio daemon. Available as a TestFlight beta. |
| Command-line interface (CLI) | Inspect devices, manage subscriptions and settings, apply presets, and integrate with scripts using structured JSON output. Supports direct device access and daemon integration. |
| Web interface | Control and monitor the network in a browser, served by the NetAudio daemon. |
| Background daemon | Maintain network discovery, device inventory, monitoring, event history, and control services across client sessions. |
| HTTP API and live events | Integrate network state and device control into other applications, with server-sent events (SSE) for live updates. |
| Protocol library | Use the Rust core directly or through its C interface; see the [C and Swift examples](packages/netaudio-core/examples). |

The CLI and daemon run on Linux, macOS, and Windows. Automated checks cover all
three platforms, including native libraries and installed Python packages.

## Capabilities

- **Discovery and routing:** discover Dante devices and their receive/transmit
  channels, inspect subscription status, and add or remove audio subscriptions.
- **Device configuration:** device and channel names, sample rate, encoding,
  latency, analog gain, device locking, preferred clock leader, AES67 settings,
  and primary/secondary network configuration on supported devices.
- **Audio flows:** inspect receive and transmit flows, plan and manage supported
  native Dante multicast transmit flows, discover external RTP flows through
  SAP/SDP, and subscribe compatible receivers to them.
- **Device metering and monitoring:** live signal levels, clock status, interface
  statistics and errors, device diagnostics, monitoring issues, and event
  history through the CLI, API, and browser.
- **Dante Domain Manager:** discover managed inventory and control supported
  settings on DDM-enrolled devices using configured credentials and operation
  permissions.
- **Presets and automation:** save and apply device/routing presets, map device
  identities, inspect application results, and consume structured output from
  scripts and other control systems.

Device capabilities and the selected control transport determine which
operations are available. The linked documentation describes specific support,
including multicast flow authoring and managed-device operations.

## Installation

### iOS and iPadOS (iPhone and iPad)

Install the native NetAudio iOS/iPadOS app through
[TestFlight](https://testflight.apple.com/join/GcuDerST). See
[netaudio.app](https://netaudio.app/) for app information and support.

### Linux, macOS, and Windows

Install the CLI, daemon, and web interface from PyPI:

```bash
uv tool install netaudio
```

Or with pip:

```bash
pip install netaudio
```

### Arch Linux

Install the [netaudio AUR package](https://aur.archlinux.org/packages/netaudio).

### From source

A source checkout requires Python 3.9 or newer, `uv`, and a Rust toolchain:

```bash
uv sync
uv run netaudio --help
```

## Quick start

Start the daemon and open the web interface:

```bash
netaudio daemon start
netaudio daemon web --open
```

Use `netaudio --help` for the command list and `netaudio COMMAND --help` for
individual commands. From a source checkout, use `uv run netaudio`.

For example, inspect a device by name and request JSON output:

```bash
netaudio -n avio-usb-1 device show
netaudio --json -n avio-usb-1 device show
```

Use your device's name in place of `avio-usb-1`. See the
[CLI guide](packages/netaudio/README.md#selecting-devices-and-channels) for
selection syntax, channel references, subscriptions, and presets. Generate the
command reference with `make man` from a source checkout.

## Documentation

- [Daemon, HTTPS, and DDM operation permissions](docs/daemon.md)
- [CLI device selection and command examples](packages/netaudio/README.md)
- [External RTP discovery and interface statistics](docs/external-rtp-and-interface-statistics.md)
- [Monitoring event journal](docs/monitoring-event-journal.md)
- [Transmit-flow lifecycle and supported operations](docs/transmit-flow-lifecycle.md)
- [Flow performance and configuration storage](docs/performance-configuration.md)
- [Presets and monitoring issues](docs/presets-and-issues.md)

## Development

The project develops an independent implementation and understanding of Dante
control protocols. Protocol work is recorded in reviewed source, focused tests,
and evidence records identifying observed behavior and remaining unknowns.
Research tooling is separate from normal application use.

Run the offline Python and Rust tests:

```bash
uv run pytest -q
cargo test --manifest-path packages/netaudio-core/Cargo.toml
```

Run Python lint checks or the broader source-quality checks:

```bash
uv run ruff check .
make quality
```

The [CI workflow](.github/workflows/quality.yml) also runs browser tests and
checks package installation across supported platforms.

## Project background

NetAudio is an independent interoperability project and is not affiliated with
or endorsed by Audinate.

The project grew out of packet capture replay and modification experiments
first written up in a
[Gearspace discussion](https://gearspace.com/board/music-computers/1221989-dante-routing-without-dante-controller-possible.html).
