import { Notice, Panel } from "../components.js";
import { ConfigurableTable } from "../table.js";
import * as format from "../format.js";
import { Fragment, html, useCallback, useMemo, useState } from "../lib/preact.js";
import { api } from "../api.js";
import { ChannelStripToggle, ChannelStrips } from "../channel-strips.js";
import {
  AD4D_AUDIO_FIELDS,
  AD4D_DEVICE_FIELDS,
  AD4D_RF_FIELDS,
  AD4D_TRANSMITTER_FIELDS,
  P10T_CHANNEL_FIELDS,
  P10T_DEVICE_FIELDS,
  SLOT_COLUMNS,
  bits,
  decode,
} from "../shure-catalog.js";
import { ON_AIR_SOURCES, slotRows } from "../shure-slots.js";
import { shureDevices, shureLevelsFor, shureMeters, visibleShureDevices } from "../store.js";

const DEVICE_TYPE_LABELS = { ad4d: "Axient Digital receiver", p10t: "PSM 1000 transmitter" };

function modelOf(device) {
  return device.model || (device.device_type ? device.device_type.toUpperCase() : null);
}

const DEVICE_COLUMNS = [
  { cell: (device) => device.name || "", id: "name", label: "Name" },
  { cell: (device) => modelOf(device) || "", id: "model", label: "Model" },
  { cell: (device) => DEVICE_TYPE_LABELS[device.device_type] || "", id: "type", label: "Type" },
  { cell: (device) => device.ip || "", id: "address", label: "Address" },
  { cell: (device) => device.mac || "", id: "mac", label: "MAC address", defaultHidden: true },
  { cell: (device) => device.firmware_version || "", id: "firmware", label: "Firmware" },
  { cell: (device) => device.rf_band || "", id: "rf-band", label: "RF band", defaultHidden: true },
  { cell: (device) => (device.last_seen ? format.timestamp(device.last_seen) : ""), id: "last-seen", label: "Last seen", defaultHidden: true },
];

function deviceByIdentifier(identifier) {
  for (const device of Object.values(shureDevices.value)) {
    if (device.mac === identifier || device.name === identifier) return device;
  }
  return null;
}

function channelNumbers(device) {
  return Object.keys(device.channels || {})
    .map(Number)
    .sort((first, second) => first - second);
}

function channelProperties(device, number, live) {
  const channel = (device.channels || {})[number] || {};
  const merged = { ...(channel.properties || {}) };
  for (const [key, value] of Object.entries(live || {})) {
    merged[key] = value && typeof value === "object" ? { ...(merged[key] || {}), ...value } : value;
  }
  return merged;
}

function useSetter(device) {
  return useCallback(
    (request) => api.setShureValue({ device: device.mac, ...request }),
    [device.mac],
  );
}

function errorText(error) {
  return error && error.message ? error.message : String(error);
}

function TextEditor({ field, value, run, initial }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const shown = (field.decode || decode.text)(value);
  if (!editing) {
    return html`<span class="shure-value">${shown ?? ""}</span>
      <button type="button" class="btn btn-xs" onClick=${() => { setDraft(initial ?? decode.text(value) ?? ""); setEditing(true); }}>Edit</button>`;
  }
  const save = async (event) => {
    event.preventDefault();
    if (await run(draft)) setEditing(false);
  };
  return html`<form class="shure-edit" onSubmit=${save}>
    <input
      class="input input-sm"
      type=${field.edit.kind === "number" ? "number" : "text"}
      value=${draft}
      maxlength=${field.edit.maxLength || null}
      aria-label=${field.label}
      onInput=${(event) => setDraft(event.target.value)}
      autofocus
    />
    <button type="submit" class="btn btn-xs btn-primary">Save</button>
    <button type="button" class="btn btn-xs" onClick=${() => setEditing(false)}>Cancel</button>
  </form>`;
}

function FieldEditor({ field, value, request }) {
  const set = request.set;
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const run = async (next) => {
    setPending(true);
    setError("");
    try {
      await set({ setting: field.edit.setting, value: next, channel: request.channel, slot: request.slot });
      return true;
    } catch (exception) {
      setError(errorText(exception));
      return false;
    } finally {
      setPending(false);
    }
  };
  const edit = field.edit;
  let control;
  if (edit.kind === "select") {
    const current = edit.raw ? edit.raw(value) : value;
    const known = edit.options.some(([option]) => option === current);
    control = html`<select
      class="select select-sm"
      aria-label=${field.label}
      disabled=${pending}
      value=${known ? current : ""}
      onChange=${(event) => run(event.target.value)}
    >
      ${known ? null : html`<option value="">${(field.decode || decode.text)(value) ?? ""}</option>`}
      ${edit.options.map(([option, label]) => html`<option key=${option} value=${option}>${label}</option>`)}
    </select>`;
  } else if (edit.kind === "toggle") {
    const on = edit.isOn(value);
    control = html`<button
      type="button"
      class=${`btn btn-xs${on ? " btn-error" : ""}`}
      aria-pressed=${on ? "true" : "false"}
      disabled=${pending}
      onClick=${() => run(on ? "off" : "on")}
    >${on ? "On" : "Off"}</button>`;
  } else {
    const initial = edit.kind === "frequency" ? (decode.frequency(value) || "").replace(" MHz", "") : edit.kind === "group" ? (decode.text(value) === "--,--" ? "" : decode.text(value)) : undefined;
    control = html`<${TextEditor} field=${field} value=${value} run=${run} initial=${initial} />`;
  }
  return html`<span class="shure-field">${control}${error ? html`<span class="text-error text-sm" role="alert">${error}</span>` : null}</span>`;
}

function Leds({ value, colors }) {
  const lit = bits(value, colors.length);
  return html`<span class="shure-leds" aria-label=${`LED bitmap ${value}`}>
    ${colors.map((color, index) => html`<span key=${index} class="shure-led" style=${lit[index] ? `background:${color};box-shadow:0 0 4px ${color}` : ""}></span>`)}
  </span>`;
}

const AUDIO_LED_COLORS = ["#2fe36a", "#2fe36a", "#2fe36a", "#2fe36a", "#ffc400", "#ffc400", "#ffc400", "#ff2323"];
const RF_LED_COLORS = ["#ffc400", "#ffc400", "#ffc400", "#ffc400", "#ffc400", "#ff2323"];
const ANTENNA_COLORS = { B: "#3b82f6", R: "#ff2323", X: null };
const ANTENNA_LETTERS = ["A", "B", "C", "D"];

function KindValue({ field, value }) {
  if (field.kind === "audio-leds") return html`<${Leds} value=${value} colors=${AUDIO_LED_COLORS} />`;
  if (field.kind === "rf-leds") {
    const entries = Object.entries(value || {});
    if (!entries.length) return null;
    return html`<span class="shure-inline">${entries.map(([antenna, bitmap]) => html`<span key=${antenna}>${ANTENNA_LETTERS[antenna - 1] || antenna} <${Leds} value=${bitmap} colors=${RF_LED_COLORS} /></span>`)}</span>`;
  }
  if (field.kind === "rssi") {
    const entries = Object.entries(value || {});
    if (!entries.length) return null;
    return html`<span class="shure-inline">${entries.map(([antenna, raw]) => html`<span key=${antenna}>${ANTENNA_LETTERS[antenna - 1] || antenna} ${decode.dbm(raw)}</span>`)}</span>`;
  }
  if (field.kind === "antennas") {
    const text = decode.text(value);
    if (!text) return null;
    return html`<span class="shure-inline">${[...text].map((state, index) => html`<span key=${index} class="shure-antenna">
      <span class="shure-led" style=${ANTENNA_COLORS[state] ? `background:${ANTENNA_COLORS[state]};box-shadow:0 0 4px ${ANTENNA_COLORS[state]}` : ""}></span>
      ${ANTENNA_LETTERS[index]} ${state === "B" ? "blue" : state === "R" ? "red" : "off"}
    </span>`)}</span>`;
  }
  return html`<span>${(field.decode || decode.text)(value)}</span>`;
}

function hasValue(field, value) {
  if (field.kind === "audio-leds") return value !== undefined && value !== null && value !== "";
  if (field.kind === "rf-leds" || field.kind === "rssi") return Object.keys(value || {}).length > 0;
  if (field.kind === "antennas") return decode.text(value) !== null;
  return (field.decode || decode.text)(value) !== null;
}

function FieldList({ fields, properties, request }) {
  const visible = fields.filter(
    (field) => (!field.onlyWhen || field.onlyWhen(properties)) && ((field.edit && request) || hasValue(field, properties[field.key])),
  );
  if (!visible.length) return null;
  return html`<dl class="fields shure-fields">
    ${visible.map((field) => html`<${Fragment} key=${field.key}>
      <dt>${field.label}</dt>
      <dd>${field.edit && request ? html`<${FieldEditor} field=${field} value=${properties[field.key]} request=${request} />` : html`<${KindValue} field=${field} value=${properties[field.key]} />`}</dd>
    <//>`)}
  </dl>`;
}

function IdentifyButton({ set, channel, label }) {
  const [state, setState] = useState("");
  const run = async () => {
    setState("pending");
    try {
      await set({ setting: "FLASH", value: "on", channel });
      setState("");
    } catch (exception) {
      setState(errorText(exception));
    }
  };
  return html`<button type="button" class="btn btn-xs" disabled=${state === "pending"} onClick=${run}>${label}</button>
    ${state && state !== "pending" ? html`<span class="text-error text-sm" role="alert">${state}</span>` : null}`;
}

function slotCell(column, { values, onAir }) {
  const active = values.SLOT_STATUS === "LINKED.ACTIVE";
  const registered = values.SLOT_STATUS && values.SLOT_STATUS !== "EMPTY";
  const relevant =
    column.key === "SLOT_STATUS" ||
    active ||
    (registered && (column.key === "SLOT_TX_MODEL" || column.key === "SLOT_TX_DEVICE_ID")) ||
    (onAir && column.key in ON_AIR_SOURCES);
  if (!relevant) return { editable: false, shown: null };
  const shown = (column.decode || decode.text)(values[column.key]);
  return {
    editable: Boolean(column.edit && active),
    shown: column.key === "SLOT_STATUS" && onAir && shown ? `${shown}, on air` : shown,
  };
}

function SlotTable({ properties, request }) {
  const rows = slotRows(properties).filter(({ values }) => values.SLOT_STATUS && values.SLOT_STATUS !== "EMPTY");
  if (!rows.length) return null;
  const columns = SLOT_COLUMNS.filter((column) =>
    rows.some((row) => {
      const cell = slotCell(column, row);
      return cell.editable || cell.shown !== null;
    }),
  );
  return html`<h3 class="shure-heading">Transmitter slots</h3>
  <div class="table-wrapper">
    <table class="data shure-slots">
      <thead><tr><th>Slot</th>${columns.map((column) => html`<th key=${column.key}>${column.label}</th>`)}</tr></thead>
      <tbody>
        ${rows.map((row) => html`<tr key=${row.slot}>
          <td data-label="Slot">${row.slot}</td>
          ${columns.map((column) => {
            const cell = slotCell(column, row);
            return html`<td key=${column.key} data-label=${column.label}>
              ${cell.editable
                ? html`<${FieldEditor} field=${column} value=${row.values[column.key]} request=${{ ...request, slot: row.slot }} />`
                : cell.shown}
            </td>`;
          })}
        </tr>`)}
      </tbody>
    </table>
  </div>`;
}

function parseNetwork(text) {
  const [mode, address, mask, gateway, mac] = String(text || "").split(/\s+/);
  const ip = (value) => (value && /^\d+\.\d+\.\d+\.\d+$/.test(value) ? value.split(".").map((part) => String(Number(part))).join(".") : value);
  return { mode, address: ip(address), mask: ip(mask), gateway: ip(gateway) === "0.0.0.0" ? "None" : ip(gateway), mac };
}

const INTERFACE_LABELS = { SC: "Shure control", D1: "Dante primary", D2: "Dante secondary" };

function NetworkRow({ interfaceName, settings, set }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(null);
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);
  const begin = () => {
    setDraft({ mode: settings.mode || "AUTO", address: settings.address || "", subnet_mask: settings.mask || "", gateway: settings.gateway === "None" ? "0.0.0.0" : settings.gateway || "" });
    setEditing(true);
  };
  const save = async (event) => {
    event.preventDefault();
    const warning = interfaceName === "SC"
      ? "The receiver will move to the new control address and netaudio will reconnect to it."
      : "The receiver reboots to apply Dante network settings. Its audio stops until it is back.";
    if (!window.confirm(`Change ${INTERFACE_LABELS[interfaceName]} network settings? ${warning}`)) return;
    setPending(true);
    setError("");
    try {
      await set({ setting: "NET_SETTINGS", value: { interface: interfaceName, ...draft } });
      setEditing(false);
    } catch (exception) {
      setError(errorText(exception));
    } finally {
      setPending(false);
    }
  };
  return html`<tr>
    <td data-label="Interface">${INTERFACE_LABELS[interfaceName]}</td>
    ${editing
      ? html`<td colspan="5">
          <form class="shure-edit" onSubmit=${save}>
            <select class="select select-sm" value=${draft.mode} onChange=${(event) => setDraft({ ...draft, mode: event.target.value })} aria-label="Mode">
              <option value="AUTO">Automatic</option>
              <option value="MANUAL">Manual</option>
            </select>
            ${draft.mode === "MANUAL"
              ? ["address", "subnet_mask", "gateway"].map((name) => html`<input key=${name} class="input input-sm" value=${draft[name]} aria-label=${name.replace("_", " ")} placeholder=${name.replace("_", " ")} onInput=${(event) => setDraft({ ...draft, [name]: event.target.value })} />`)
              : null}
            <button type="submit" class="btn btn-xs btn-primary" disabled=${pending}>Apply</button>
            <button type="button" class="btn btn-xs" onClick=${() => setEditing(false)}>Cancel</button>
            ${error ? html`<span class="text-error text-sm" role="alert">${error}</span>` : null}
          </form>
        </td>`
      : html`<td data-label="Mode">${settings.mode === "AUTO" ? "Automatic" : settings.mode === "MANUAL" ? "Manual" : settings.mode}</td>
          <td data-label="Address">${settings.address}</td>
          <td data-label="Subnet mask">${settings.mask}</td>
          <td data-label="Gateway">${settings.gateway}</td>
          <td data-label="MAC address">${settings.mac} <button type="button" class="btn btn-xs" onClick=${begin}>Edit</button></td>`}
  </tr>`;
}

function NetworkPanel({ device, set }) {
  const network = (device.properties || {}).NET_SETTINGS || {};
  const interfaces = ["SC", "D1", "D2"].filter((name) => network[name]);
  if (!interfaces.length) return null;
  return html`<${Panel} title="Network">
    <div class="table-wrapper">
      <table class="data">
        <thead><tr><th>Interface</th><th>Mode</th><th>Address</th><th>Subnet mask</th><th>Gateway</th><th>MAC address</th></tr></thead>
        <tbody>${interfaces.map((name) => html`<${NetworkRow} key=${name} interfaceName=${name} settings=${parseNetwork(network[name])} set=${set} />`)}</tbody>
      </table>
    </div>
  <//>`;
}

function MuteControl({ device, strip, set }) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const properties = ((device.channels || {})[strip.channel] || {}).properties || {};
  const p10t = device.device_type === "p10t";
  const muted = p10t ? properties.RF_MUTE === "1" : properties.AUDIO_MUTE === "ON";
  const toggle = async () => {
    setPending(true);
    setError("");
    try {
      await set({ setting: p10t ? "RF_MUTE" : "AUDIO_MUTE", value: muted ? "off" : "on", channel: strip.channel });
    } catch (exception) {
      setError(errorText(exception));
    } finally {
      setPending(false);
    }
  };
  return html`<${ChannelStripToggle}
    pressed=${muted}
    pending=${pending}
    label=${p10t ? "RF mute" : "Mute"}
    title=${(error ? `${error}. ` : "") + (p10t ? (muted ? "RF muted. Click to unmute" : "RF mute") : muted ? "Muted. Click to unmute" : "Mute")}
    onToggle=${toggle}
  />`;
}

function LevelsPanel({ device, set }) {
  const p10t = device.device_type === "p10t";
  const strips = useMemo(
    () =>
      channelNumbers(device).map((number) => {
        const properties = ((device.channels || {})[number] || {}).properties || {};
        return {
          key: `${device.mac}:${number}`,
          channel: number,
          number,
          name: decode.text(properties.CHAN_NAME) || "",
          tracks: p10t ? 2 : 1,
          trackLabels: p10t ? ["L", "R"] : null,
        };
      }),
    [device.mac, device.channels, p10t],
  );
  const read = useCallback(
    (strip) => ({ levels: shureLevelsFor(device.mac, strip.channel) }),
    [device.mac],
  );
  if (!strips.length) return null;
  return html`<${Panel} title="Levels">
    <${ChannelStrips} strips=${strips} read=${read} label=${`${device.name} levels`} controls=${(strip) => html`<${MuteControl} device=${device} strip=${strip} set=${set} />`} />
  <//>`;
}

function Ad4dChannel({ device, number, set }) {
  const live = (shureMeters.value[device.mac] || {})[number];
  const properties = channelProperties(device, number, live);
  const request = { set, channel: number };
  const name = decode.text(properties.CHAN_NAME);
  return html`<${Panel}
    title=${`Channel ${number}${name ? ` · ${name}` : ""}`}
    headerActions=${html`<${IdentifyButton} set=${set} channel=${number} label="Identify" />`}
  >
    <div class="shure-columns">
      <section><h3 class="shure-heading">Audio</h3><${FieldList} fields=${AD4D_AUDIO_FIELDS} properties=${properties} request=${request} /></section>
      <section><h3 class="shure-heading">RF</h3><${FieldList} fields=${AD4D_RF_FIELDS} properties=${properties} request=${request} /></section>
      ${!properties.TX_MODEL || properties.TX_MODEL === "UNKNOWN"
        ? null
        : html`<section>
            <h3 class="shure-heading">Transmitter</h3>
            <${FieldList} fields=${AD4D_TRANSMITTER_FIELDS} properties=${properties} />
          </section>`}
    </div>
    <${SlotTable} properties=${properties} request=${request} />
  <//>`;
}

function P10tChannels({ device, set }) {
  const numbers = channelNumbers(device);
  const meters = shureMeters.value[device.mac] || {};
  return html`<${Panel} title="Channels">
    <div class="shure-columns">
      ${numbers.map((number) => {
        const properties = channelProperties(device, number, meters[number]);
        const name = decode.text(properties.CHAN_NAME);
        return html`<section key=${number}>
          <h3 class="shure-heading">Channel ${number}${name ? ` · ${name}` : ""}</h3>
          <${FieldList} fields=${P10T_CHANNEL_FIELDS} properties=${properties} request=${{ set, channel: number }} />
        </section>`;
      })}
    </div>
  <//>`;
}

function DeviceView({ device }) {
  const set = useSetter(device);
  const p10t = device.device_type === "p10t";
  const properties = device.properties || {};
  const address = [modelOf(device), DEVICE_TYPE_LABELS[device.device_type], device.ip].filter(Boolean).join(" · ");
  const deviceFields = p10t ? P10T_DEVICE_FIELDS : AD4D_DEVICE_FIELDS;
  return html`<div class="flex flex-col gap-4">
    <div class="content-header">
      <div>
        <div class="content-title">${device.name || device.mac}</div>
        <div class="content-subtitle">${address}</div>
      </div>
    </div>
    ${device.online ? null : html`<${Notice}>Not reachable since ${format.timestamp(device.last_seen)}.<//>`}
    <${LevelsPanel} device=${device} set=${set} />
    <${Panel} title="Device" headerActions=${p10t ? null : html`<${IdentifyButton} set=${set} label="Identify" />`}>
      <${FieldList}
        fields=${[...deviceFields, { key: "__address", label: "Address" }, { key: "__mac", label: "MAC address" }]}
        properties=${{ ...properties, __address: device.ip, __mac: device.mac }}
        request=${{ set }}
      />
    <//>
    ${p10t ? null : html`<${NetworkPanel} device=${device} set=${set} />`}
    ${p10t
      ? html`<${P10tChannels} device=${device} set=${set} />`
      : channelNumbers(device).map((number) => html`<${Ad4dChannel} key=${number} device=${device} number=${number} set=${set} />`)}
  </div>`;
}

function ShureView({ location }) {
  const identifier = location.parameters.device;
  if (identifier) {
    const device = deviceByIdentifier(identifier);
    if (!device) {
      return html`<${Notice}>${identifier} is not on the network.<//>`;
    }
    return html`<${DeviceView} device=${device} />`;
  }
  const all = format.sortedShureDevices(visibleShureDevices.value);
  if (!all.length) return null;
  return html`<div class="flex flex-col gap-4">
    <${Panel} title=${`Shure devices (${all.length})`}>
      <${ConfigurableTable}
        tableId="shure-receivers"
        columns=${DEVICE_COLUMNS}
        rows=${all}
        rowKey=${(device) => device.mac}
        rowHref=${(device) => `/shure/${encodeURIComponent(device.name || device.mac)}`}
      />
    <//>
  </div>`;
}

export const shureView = {
  filters: false,
  component: ShureView,
  id: "shure",
  label: "Shure",
};
