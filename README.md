
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

#### Latency configuration and monitoring

Read the complete device-wide latency state for one device:

```bash
netaudio -n avio-aes3-1 device config latency
```

The output distinguishes active, configured, and default values, the
device-reported minimum/maximum range, and the latency options produced by
filtering the standard option set through that range. JSON, YAML, and XML
output include both milliseconds and the original nanosecond values. An
active or configured value remains visible when it is inside the reported
range but absent from the ordinary option list, or even outside the reported
range.

Set latency in milliseconds and require matching active readback:

```bash
netaudio -n avio-aes3-1 device config latency 2
```

`netaudio -n DEVICE device show` keeps three different layers separate:
device-wide configuration, each receiver flow's latency setting and frames per
packet, and receiver-flow current/average/peak measurements. Flow settings and
live measurements are not treated as aliases of the device-wide value.

#### Modern ARC status JSON

Modern channel-status reads follow every observed `0x8112` continuation and
return one merged record list. The result includes `page_capacity`,
`page_count`, `page_capacities`, and `total_record_count`. The deprecated
`maximum_transmitter_channels` and `maximum_receiver_channels` aliases remain
available, but describe a page capacity rather than the device's total channel
count. Channel records expose the global `channel_number` separately from
`media_type` and `media_local_channel_id`.

Transmitter-flow records similarly expose `global_flow_id`, `media_type`, and
`media_local_flow_id`. `transmitter_channel_ids_by_slot` preserves the ordered
slot vector, including zero-valued empty slots, while `populated_slot_count` is
derived from its nonzero entries. The old `flow_number` alias remains equal to
`global_flow_id`. The deprecated `channel_count` field remains for JSON
compatibility but is now derived from populated slots; it is never read from
the media-local ID field.

For the packet-observed frontends, an exact mDNS `arcp_vers` of `2.8.15`
selects protocol `0x280f`; the existing `2.8.9` frontend uses `0x2809`.
Unrecognized versions retain the `0x2809` compatibility default.

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
