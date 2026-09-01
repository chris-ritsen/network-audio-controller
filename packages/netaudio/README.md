
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
- Add/remove subscriptions
- CLI
- Cross-platform foreground daemon plus installable boot service
- Device lock/unlock through the native Rust protocol core
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

To install from a clone (requires Python 3.9+ and a Rust toolchain, since the
native core is compiled from source):

```bash
uv sync
uv run netaudio
```

#### Arch Linux

To install from AUR, build the package with
[aur/netaudio](https://aur.archlinux.org/packages/netaudio).

### Usage

Run `netaudio` if installed globally, or `uv run netaudio` from a clone.

#### Selecting devices and channels

Every command selects devices with the same global filters: `-n/--name`
(glob), `-s/--server-name` (glob), `-m/--mac`, and `--host` (IP address).
Commands that act on one device report `device not found` or
`multiple devices matched` when the filters do not narrow to exactly one.
`-h` is an alias for `--help` everywhere.

```bash
netaudio -n avio-usb-1 device show
netaudio -n avio-usb-1 flow list
netaudio --host 192.168.1.50 lock set 1234
```

Channels are written as `tx:1`, `rx:1`, `tx:NAME`, `rx:NAME`, or a bare
channel name. A bare name searches both directions and is rejected when it
matches both a transmitter and a receiver channel.

```bash
netaudio -n avio-usb-1 channel name rx:1
netaudio -n avio-usb-1 channel name rx:1 vocal-in
netaudio -n avio-usb-1 channel gain tx:1 3
netaudio subscription add --tx tx:1@stagebox --rx rx:1@avio-usb-1
netaudio subscription add --tx 1@stagebox --rx 1@avio-usb-1
netaudio subscription remove --rx rx:1@avio-usb-1
```

With `--tx` and `--rx` the direction is implied, so `1@DEVICE` is accepted as
shorthand for `tx:1@DEVICE` and `rx:1@DEVICE` respectively.

Presets are stored in the preset directory (`presets/` next to
`config.toml`, or `preset_directory` in `config.toml`); `preset save NAME`,
`preset show NAME`, and `preset load NAME` use it unless given an explicit
`.xml` path, and `preset list` shows what is saved there.

```bash
netaudio -n 'avio-*' preset save stage
netaudio preset list
netaudio config show
```

Run tests:

```bash
uv lock --check
uv run --python 3.9 --no-project python -m compileall -q packages/netaudio/src/netaudio
cargo test --manifest-path packages/netaudio-core/Cargo.toml
uv run pytest -q
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
