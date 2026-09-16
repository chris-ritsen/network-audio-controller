
### Description

NetAudio is an open-source, cross-platform implementation for discovering,
monitoring, configuring, and routing Dante network audio devices.

A NetAudio iOS client is in development:
[Dante control for iPhone and iPad](https://netaudio.app/#ios-app), available
now as a [TestFlight beta](https://testflight.apple.com/join/GcuDerST).

It provides a command-line interface, browser interface, HTTP API, and
background daemon, making Dante networks accessible from Linux, macOS, Windows,
scripts, automation systems, and other software. NetAudio communicates directly
with Dante devices and supports both unmanaged networks and devices enrolled in
Dante Domain Manager (DDM).

Current functionality includes device and channel discovery, subscription
routing, device and channel naming, sample rate, encoding and latency
configuration, analog gain control, device locking, network configuration,
multicast transmit-flow management, presets, AES67 and external RTP flows,
interface statistics, monitoring and issue history, and DDM-managed device
control.

NetAudio is designed both as a standalone Dante control application and as an
interoperability layer for software that needs programmatic access to Dante
networks. Commands support structured JSON output, and the daemon exposes the
same network state and control capabilities through an HTTP API for
integration with other applications and automation systems.

NetAudio is an independent interoperability project and is not affiliated with
or endorsed by Audinate.

The project grew out of packet capture replay and modification experiments
first written up in a
[gearspace thread](https://gearspace.com/board/music-computers/1221989-dante-routing-without-dante-controller-possible.html).

### Features

#### Current

- AVIO input/output gain control
- AES67 readiness in the browser: supported and active modes, pending changes,
  and DDM-reported RTP flow availability
- Add/remove subscriptions
- CLI and browser interface
- Cross-platform daemon
- DDM-enrolled device settings in the browser, including latency, encoding,
  analog gain, preferred leader, network configuration, reboot, and sample-rate
  controls for supported models
- Primary and secondary DHCP/static network configuration on supported Wing
  devices, through the browser, CLI and API, including DDM-managed control
- Device lock/unlock
- Display active subscriptions, Rx and Tx channels, devices names and
  addresses, subscription status
- JSON output
- Bounded monitoring event journal with CLI, JSON export, API, and browser history
- Lifecycle-managed monitoring issues with current/history CLI, API, SSE, and browser views
- SAP/SDP external RTP flow discovery and direct receiver subscription
- ConMon interface statistics with raw records, rates, errors, and freshness
- Canonical transmit-flow planning, verified create/delete, HTTP API, browser controls, and presets
- Versioned preset round trips with explicit identity mapping, multi-interface support, and verified apply reports
- Set device latency, sample rate, encoding
- Set/reset channel names, device names
- mDNS device discovery

### Installation

To install from PyPI:

```bash
uv tool install netaudio
```

Or with pip/pipx:

```bash
pip install netaudio
```

To run from a clone (needs `uv` and `cargo`):

```bash
uv sync
uv run netaudio
```

#### Arch Linux

To install from AUR, build the package with
[aur/netaudio](https://aur.archlinux.org/packages/netaudio).

### Usage

Run `netaudio` if installed globally, or `uv run netaudio` from a clone.
Use `--help` to list commands and options.

To open the browser interface:

```bash
netaudio daemon start
netaudio daemon web --open
```

The server reports its installed version at `GET /server-info` and during
local-network discovery. A Git revision is included when the installation
metadata identifies a Git commit.

Run tests:

```bash
uv run pytest -q
cargo test --manifest-path packages/netaudio-core/Cargo.toml
```

Lint and format:

```bash
uv run ruff check .
uv run ruff format .
```

##### HTTPS

The daemon serves plain HTTP by default. If you already have a certificate for
the machine, the daemon can additionally serve HTTPS on a second port; nothing
is generated or managed for you:

```toml
[daemon]
tls_certificate = "/etc/netaudio/daemon.crt"
tls_key = "/etc/netaudio/daemon.key"
tls_port = 9443
```

Relative paths resolve against the config file's directory, `tls_port`
defaults to 9443, and the HTTP port keeps working for local tools. `netaudio
daemon tls` shows the active configuration and the certificate's SHA-256
fingerprint; `netaudio daemon web` lists the HTTPS addresses alongside HTTP.
The Bonjour record carries the HTTPS port as `tls_port`. Clients trust the
certificate through their normal trust store, so a certificate signed by a CA
your devices already trust is the least friction.

##### Managed operation permissions

Writes to an enrolled device fail closed until its saved DDM context explicitly
permits each operation. Add `operation_permissions` to the context in the
NetAudio configuration file:

```toml
[ddm.contexts.studio]
server = "manager"
domain_id = "0123456789abcdef0123456789abcdef"
operation_permissions = ["identify", "sample_rate", "encoding"]
```

Accepted operation names are `identify`, `sample_rate`, `encoding`,
`sample_rate_pullup`, `aes67`, `static_ipv4`, `redundancy`, `codec_control`, and
`locking`. Omitting the setting means permission has not been configured; an
empty array explicitly permits no operations. Device capability, current
configuration state, lock state, and transport support can still prevent a
listed operation. Managed lock and unlock remain unavailable because NetAudio
has no verified DDM transport for them.

### Documentation

Receiver flow queries retain partial replies and identify them as incomplete. The
last complete inventory remains available, but a partial readback cannot confirm
missing flows or completed changes. Automatic retrieval of receiver-flow
continuation pages is not yet supported.

- [External RTP discovery and interface statistics](docs/external-rtp-and-interface-statistics.md)
- [Monitoring event journal](docs/monitoring-event-journal.md)
- [Transmit-flow lifecycle](docs/transmit-flow-lifecycle.md)
- [Presets and monitoring issues](docs/presets-and-issues.md)
- [Examples](https://github.com/chris-ritsen/network-audio-controller/wiki/Examples)
- [Technical details](https://github.com/chris-ritsen/network-audio-controller/wiki/Technical-details)
- [Testing](https://github.com/chris-ritsen/network-audio-controller/wiki/Testing)
