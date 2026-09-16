# NetAudio CLI guide

NetAudio is an unofficial Dante Controller alternative for discovering,
routing, configuring, and monitoring Dante network audio devices. This package
provides the Python CLI, background daemon, and browser interface, using the
shared Rust protocol core on Linux, macOS, and Windows.

NetAudio also provides a native Dante Controller alternative for iOS and iPadOS
on iPhone and iPad, available through
[TestFlight](https://testflight.apple.com/join/GcuDerST). It supports direct
Dante device control and connections to a NetAudio daemon.

See the [main README](../../README.md) for capabilities, installation,
architecture, and development instructions, or [netaudio.app](https://netaudio.app/)
for the iOS app and support.

## Selecting devices and channels

Every command selects devices with the same global filters: `-n/--name`
(glob), `-s/--server-name` (glob), `-m/--mac`, and `--host` (IP address).
Commands that act on one device report `device not found` or
`multiple devices matched` when the filters do not narrow to exactly one.
`-h` is an alias for `--help` everywhere.

```bash
netaudio -n avio-usb-1 device show
netaudio -n avio-usb-1 flow inspect
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

## Further documentation

- [Daemon, HTTPS, and DDM operation permissions](../../docs/daemon.md)
- [Presets and monitoring issues](../../docs/presets-and-issues.md)
- [All documentation](../../README.md#documentation)
