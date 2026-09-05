
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
- Browser interface served by the daemon
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

#### Subscription status

Subscription JSON keeps the raw 16-bit subscription `status.code`, separate
`rx_channel_status.code`, and any additional `status_message` warnings. A
`connected` subscription can carry warnings; it does not prove audio delivery.
Managed responses retain `ddm_status`, `ddm_status_message` and `ddm_summary`
separately. Their semantic state comes from the shared Rust definitions, while
severity follows the managed summary. A null managed status remains unknown,
with its available message, summary and channel metadata preserved. GraphQL
errors remain full objects in daemon status and leave inventory degraded;
unrelated API failures are not treated as successful queries.

Numeric definitions and identifier classification live in `netaudio-core`.
The Python client requires native ABI 5 for these functions. Source checkouts
must rebuild the native core after this update. Subscription definitions no
longer come from the optional local label file.

The [status observation fixture](tests/fixtures/subscription/README.md) records
the evidence scope: a synthetic 0x0000–0x00ff sweep at receiver health 0x0101,
observed through one DDM deployment on 2026-09-05. `observed_summary` is populated
for recognized values in the observed receiver contexts. `interpretation`
identifies observed pairs, unverified receiver contexts, unknown numeric values,
or code 1 requiring receiver context. Code 1 resolves to DYNAMIC at health
0x0101 and UNRESOLVED at health 0x0000; other contexts remain unresolved.
Higher values keep all bits and remain unknown. The sweep does not establish a
complete receiver-health precedence rule or naturally occurring hardware faults.

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

#### Channel and flow status JSON

Channel-status reads follow every continuation page and return one merged
record list with `page_capacity`, `page_count`, and `total_record_count`.
Channel records carry `channel_number`, `media_type`, and
`media_local_channel_id`; flow records carry `global_flow_id`, `media_type`,
`media_local_flow_id`, and `transmitter_channel_ids_by_slot`.

Modern ARC channel-status media type codes are `3` for audio, `4` for video,
and `5` for ancillary data. The ancillary label is a causal black-box finding:
Dante Controller classified devices publishing code `5` with its ancillary
capability filter. The finding establishes Controller's interpretation of the
field; the tested devices were synthetic, so it does not establish which
physical device families publish ancillary channels.

For the packet-observed frontends, an exact mDNS `arcp_vers` of `2.8.15`
selects protocol `0x280f`; the existing `2.8.9` frontend uses `0x2809`.
Unrecognized or missing versions fail closed instead of selecting a presumed
protocol. Enrolled DDM devices use the separately observed `0x2809` managed
transport contract.

#### Dante Domain Manager devices

When the daemon's merged inventory marks a device as enrolled, ordinary device
commands automatically use DDM. Unenrolled devices continue to use their local
ARC/settings services. A device that has both a local address and enrolled DDM
metadata still uses DDM, so commands do not accidentally bypass domain policy.

The guided login discovers DDM servers over mDNS when `--url` is omitted,
authenticates, reads the visible domains, prompts when there is more than one
choice, and saves the first context as the default:

```bash
netaudio ddm login --username operator
netaudio ddm context list
netaudio ddm context use studio-main
```

An existing Managed API credential can be used without a password prompt:

```bash
netaudio ddm login --url https://ddm.example/graphql --server-profile studio \
  --credential-file ~/.config/netaudio/studio.credential
```

Each context binds one server profile, one credential file, and one domain ID.
The resulting `config.toml` uses this shape:

```toml
[ddm]
default_context = "studio-main"

[ddm.servers.studio]
url = "https://ddm.example/graphql"
credential_file = "credentials/studio.credential"

[ddm.contexts.studio-main]
server = "studio"
domain_id = "0123456789abcdef0123456789abcdef"
domain_name = "Main Studio"
```

Use `netaudio --context CONTEXT ...` or `NETAUDIO_CONTEXT` for a one-command
override. The daemon polls every configured server, while each managed device
record retains its originating server profile, context, domain ID, and device
ID. This keeps devices distinct when separate sites reuse names or IP address
ranges and ensures credentials are sent only to the configured server.

The URL is used as configured; there are no separate certificate, hostname, or
internal-port settings. Low-level Managed API access is grouped by intent and
resource so it does not overwhelm the ordinary DDM commands:

```bash
netaudio ddm api read domains
netaudio ddm api write device set-name --device-id DEVICE_ID --name NAME
netaudio ddm api schema
```

Use the normal device commands for ordinary control. `ddm api write` exposes
schema-derived administrative mutations and should be used deliberately.

The documented GraphQL API supplies inventory and device-name, preferred-leader,
and subscription changes. Capture-derived, version-scoped Controller-service
support supplies Identify and native ARC/settings requests. The normal CLI paths
currently cover device/channel/flow status, device and channel names, latency,
sample rate, encoding, gain, AES67, clock state/subdomain, network interface
status/configuration, subscriptions, receiver port ranges, and the observed
modern flow inventory/delete form. Reads were exercised against enrolled AVIO
input and output adapters; mutation paths retain their normal readback and
capability checks.

Operations without an established managed request and completion model fail
closed instead of trying the unmanaged device address. These currently include
lock/unlock, reboot and factory reset, capability/log exports, modern multicast
flow creation, channel-name reset, and the `0x2729` transmitter-channel-capability
query. Sample-rate pull-up is implemented through the managed settings envelope,
but the tested AVIO family did not publish a response and is reported as
unavailable.

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

#### Browser interface

The daemon serves a browser interface from its own HTTP port, so no extra
process or build step is involved. Start the daemon and open the address it
reports:

```bash
netaudio daemon start
netaudio daemon web --open
```

The page is a single-page application backed entirely by the daemon HTTP API and
its `/events` stream, so device, subscription, metering, and Shure state update
live without polling. It uses real browser history routing, so every view,
device, and device tab is a linkable address. It follows Dante Controller's
layout and vocabulary so it is immediately familiar:

- Routing: the full network subscription matrix, Dante receivers down and Dante
  transmitters across, with devices collapsed by default. Click a device cell to
  subscribe one-to-one, expand a device to work channel by channel, and filter
  either axis by device or channel name. The matrix is canvas-rendered and stays
  responsive across tens of thousands of cells; pending changes, subscribed,
  warning, and error states are drawn as in Dante Controller.
- Device info, Clock status, Network status: the network-view tables with the
  columns Dante Controller operators expect. Every table lets columns be hidden,
  shown, and dragged into a new order, remembered per table in the browser.
- Device view: Receive, Transmit, Status, Latency, Device config, Network
  config, AES67 config, Transmit flows, Device lock, and Domain tabs. Receivers
  subscribe through a searchable picker; channels can be renamed and gains set
  where the device supports it.
- Metering: live meters for every declared channel, drawn on canvas with peak
  hold. Both metering protocols are surfaced: detailed per-channel levels
  streamed after an explicit start, and the passive signal-presence records a
  device already broadcasts.
- Flows: transmit flow inventory per device, with multicast flow creation and
  deletion.
- Domains: Dante Domain Manager status, domains, and the managed inventory.
- Shure: discovered receivers, channel state, transmitter and battery detail,
  and live meter values.
- Events: the daemon event stream, held in memory only and excluding meter
  samples.

Press Ctrl+K (Command+K on macOS) to jump to any device or view. Defaults show
what an operator needs; every field the daemon knows remains available behind
column menus and expandable detail sections, never as raw JSON.

The client is Preact with signals, vendored into the package alongside the rest
of the assets, so the interface loads with no build step, no bundler, and no
network access beyond the daemon itself. `make test-webapp` renders every view
and device tab against captured device records under Node, so template and
formatting regressions are caught before deployment.

Device mutations use the same verified-write paths as the CLI, so a control that
reports success has been read back from the device. The daemon binds every
interface; restrict access at the network layer if that is not wanted.

### Documentation

- [Examples](https://github.com/chris-ritsen/network-audio-controller/wiki/Examples)
- [Technical details](https://github.com/chris-ritsen/network-audio-controller/wiki/Technical-details)
- [Testing](https://github.com/chris-ritsen/network-audio-controller/wiki/Testing)
