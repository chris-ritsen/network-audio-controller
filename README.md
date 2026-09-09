
### Description

This is a python program for controlling Dante network audio devices (and
possibly others in the future).  It's early, so expect things to break or
switches to change.  Use this at your own risk; it's not ready for anything
other than a test environment and could make the devices behave unexpectedly.
The first goal is to do everything that Dante Controller can do that would be
useful for control of the devices from a command-line interface or within
scripts.

For more information, check out the [gearspace discussion](https://gearspace.com/board/music-computers/1221989-dante-routing-without-dante-controller-possible.html).

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

To install from a clone (requires Python 3.9+ and a Rust toolchain):

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

### Documentation

- [Examples](https://github.com/chris-ritsen/network-audio-controller/wiki/Examples)
- [Technical details](https://github.com/chris-ritsen/network-audio-controller/wiki/Technical-details)
- [Testing](https://github.com/chris-ritsen/network-audio-controller/wiki/Testing)
