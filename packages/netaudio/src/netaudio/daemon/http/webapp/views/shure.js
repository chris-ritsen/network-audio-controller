import {Fields, Notice, OnlineState, Panel, Pill, Value} from "../components.js";
import { ConfigurableTable } from "../table.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";
import { navigate } from "../router.js";
import { shureDevices, shureMeters } from "../store.js";

const CHANNEL_COLUMNS = [
  { align: "right", cell: (row) => row.number, id: "number", label: "#" },
  { cell: (row) => html`<${Value} value=${row.channel.name} />`, id: "name", label: "Name" },
  { cell: (row) => html`<${Value} value=${row.channel.frequency} />`, id: "frequency", label: "Frequency" },
  { cell: (row) => html`<${Value} value=${row.channel.group_channel} />`, id: "group-channel", label: "Group/channel", defaultHidden: true },
  { cell: (row) => html`<${Value} value=${row.channel.audio_gain} />`, id: "gain", label: "Gain" },
  { cell: (row) => html`<${Value} value=${row.channel.audio_mute ?? row.channel.rf_mute} />`, id: "muted", label: "Muted" },
  {
    cell: (row) =>
      html`<${Value} value=${row.live.audio_level_peak ?? row.channel.audio_level_peak ?? row.channel.audio_in_level} />`,
    id: "peak",
    label: "Peak",
  },
  {
    cell: (row) => html`<${Value} value=${row.live.audio_level_rms ?? row.channel.audio_level_rms} />`,
    id: "rms",
    label: "RMS", defaultHidden: true,
  },
  { cell: (row) => html`<${Value} value=${row.channel.signal_quality} />`, id: "signal-quality", label: "Signal quality", defaultHidden: true },
  { cell: (row) => html`<${Value} value=${row.channel.antenna_status} />`, id: "antenna", label: "Antenna", defaultHidden: true },
  { cell: (row) => html`<${Value} value=${row.channel.encryption_status} />`, id: "encryption", label: "Encryption", defaultHidden: true },
  { cell: (row) => html`<${Value} value=${row.channel.interference_status} />`, id: "interference", label: "Interference", defaultHidden: true },
  { cell: (row) => html`<${Value} value=${(row.channel.transmitter || {}).model} />`, id: "transmitter", label: "Transmitter", defaultHidden: true },
  {
    cell: (row) => html`<${Value} value=${(row.channel.transmitter || {}).battery_hours} />`,
    id: "battery-hours",
    label: "Battery hours", defaultHidden: true,
  },
  {
    cell: (row) => html`<${Value} value=${(row.channel.transmitter || {}).battery_charge_percent} />`,
    id: "battery-percent",
    label: "Battery percent",
  },
  {
    cell: (row) => html`<${Value} value=${(row.channel.transmitter || {}).power_level} />`,
    id: "power-level",
    label: "Power level", defaultHidden: true,
  },
];

const RECEIVER_COLUMNS = [
  { cell: (device) => html`<${OnlineState} online=${device.online} />`, id: "state", label: "State" },
  { cell: (device) => html`<${Value} value=${device.name} />`, id: "name", label: "Name" },
  { cell: (device) => html`<${Value} value=${device.model} />`, id: "model", label: "Model" },
  { cell: (device) => html`<${Value} value=${device.device_type} />`, id: "type", label: "Type", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.ip} />`, id: "address", label: "Address" },
  { cell: (device) => html`<${Value} value=${device.mac} />`, id: "mac", label: "MAC address", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.dante_mac} />`, id: "dante-mac", label: "Dante MAC address", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.firmware_version} />`, id: "firmware", label: "Firmware" },
  { cell: (device) => html`<${Value} value=${device.rf_band} />`, id: "rf-band", label: "RF band", defaultHidden: true },
  { cell: (device) => format.timestamp(device.last_seen), id: "last-seen", label: "Last seen", defaultHidden: true },
];

function deviceByIdentifier(identifier) {
  for (const device of Object.values(shureDevices.value)) {
    if (device.mac === identifier || device.name === identifier) {
      return device;
    }
  }
  return null;
}

function ChannelTable({ device }) {
  const channels = device.channels || {};
  const meters = shureMeters.value[device.mac] || {};
  const rows = Object.keys(channels)
    .map(Number)
    .sort((first, second) => first - second)
    .map((number) => ({ channel: channels[number], live: meters[number] || {}, number }));
  if (!rows.length) {
    return html`<${Notice}>This receiver reports no channels.<//>`;
  }
  return html`<${ConfigurableTable}
    tableId="shure-channels"
    columns=${CHANNEL_COLUMNS}
    rows=${rows}
    rowKey=${(row) => row.number}
  />`;
}

function ShureView({ location }) {
  const all = format.sortedShureDevices(shureDevices.value);
  const identifier = location.parameters.device;

  if (identifier) {
    const device = deviceByIdentifier(identifier);
    if (!device) {
      return html`<${Notice}>No Shure device named ${identifier} has been discovered.<//>`;
    }
    return html`
      <div class="stack">
        <div class="content-header">
          <div>
            <div class="content-title">${device.name || device.mac}</div>
            <div class="content-subtitle">${html`<${Value} value=${device.model} />`} · ${html`<${Value} value=${device.ip} />`}</div>
          </div>
          <${Pill} tone=${device.online ? "good" : "bad"}>${device.online ? "online" : "offline"}<//>
        </div>
        <${Panel} title="Receiver">
          <${Fields}
            entries=${[
              ["Name", html`<${Value} value=${device.name} />`],
              ["Model", html`<${Value} value=${device.model} />`],
              ["Device type", html`<${Value} value=${device.device_type} />`],
              ["Address", html`<${Value} value=${device.ip} />`],
              ["MAC address", html`<${Value} value=${device.mac} />`],
              ["Dante MAC address", html`<${Value} value=${device.dante_mac} />`],
              ["Firmware version", html`<${Value} value=${device.firmware_version} />`],
              ["RF band", html`<${Value} value=${device.rf_band} />`],
              ["Transmission mode", html`<${Value} value=${device.transmission_mode} />`],
              ["Quadversity mode", html`<${Value} value=${device.quadversity_mode} />`],
              ["Encryption mode", html`<${Value} value=${device.encryption_mode} />`],
              ["Last seen", format.timestamp(device.last_seen)],
            ]}
          />
        <//>
        <${Panel} title="Channels">
          <${ChannelTable} device=${device} />
        <//>
      </div>
    `;
  }

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Shure</div>
          <div class="content-subtitle">Wireless receivers discovered on the network.</div>
        </div>
      </div>
      <${Panel} title=${`Receivers (${all.length})`}>
        ${all.length === 0
          ? html`<${Notice}>No Shure devices have been discovered by the daemon.<//>`
          : html`<${ConfigurableTable}
              tableId="shure-receivers"
              columns=${RECEIVER_COLUMNS}
              rows=${all}
              rowKey=${(device) => device.mac}
              onRowClick=${(device) => navigate(`/shure/${encodeURIComponent(device.name || device.mac)}`)}
            />`}
      <//>
    </div>
  `;
}

export const shureView = {
  component: ShureView,
  id: "shure",
  label: "Shure",
};
