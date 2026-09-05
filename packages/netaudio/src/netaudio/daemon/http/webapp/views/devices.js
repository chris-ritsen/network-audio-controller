import { Notice, OnlineState, Panel, Pill } from "../components.js";
import { ReceiveSection, TransmitSection } from "../device/channels.js";
import { Aes67Section, DeviceConfigSection, LatencySection } from "../device/config.js";
import { FlowsSection } from "../device/flows.js";
import { isManaged, ManagedSection } from "../device/managed.js";
import { NetworkSection } from "../device/network.js";
import { LockSection } from "../device/security.js";
import { StatusSection } from "../device/status.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";
import { devicePath, navigate, setQueryParameter } from "../router.js";
import { deviceByName, devices } from "../store.js";
import { ConfigurableTable } from "../table.js";

const INFO_COLUMNS = [
  { cell: (device) => html`<${OnlineState} online=${device.online} />`, id: "state", label: "State" },
  { cell: (device) => format.deviceLabel(device), id: "name", label: "Device name" },
  { cell: (device) => format.text(device.model), id: "model", label: "Model name" },
  { cell: (device) => format.text(device.manufacturer), id: "manufacturer", label: "Manufacturer", defaultHidden: true },
  { cell: (device) => format.text(device.product_version), id: "product-version", label: "Product version", defaultHidden: true },
  { cell: (device) => format.text(device.firmware_version), id: "dante-version", label: "Dante firmware" },
  { cell: (device) => format.text(device.software_version), id: "software-version", label: "Dante software", defaultHidden: true },
  { cell: (device) => (device.is_locked ? "locked" : "unlocked"), id: "lock", label: "Device lock" },
  { cell: (device) => format.text(device.ipv4), id: "primary-address", label: "Primary address" },
  { cell: (device) => (device.link_speed_mbps ? `${device.link_speed_mbps} Mbps` : format.ABSENT), id: "link-speed", label: "Primary link speed" },
  { cell: (device) => format.sampleRate(device.sample_rate_hz), id: "sample-rate", label: "Sample rate" },
  { cell: (device) => (device.encoding ? `PCM ${device.encoding}` : format.ABSENT), id: "encoding", label: "Encoding", defaultHidden: true },
  { cell: (device) => format.latency(device.latency_ms), id: "latency", label: "Latency" },
  { align: "right", cell: (device) => format.text(device.tx_count), id: "transmit", label: "Tx channels" },
  { align: "right", cell: (device) => format.text(device.rx_count), id: "receive", label: "Rx channels" },
  {
    align: "right",
    cell: (device) => (device.subscriptions || []).filter((entry) => entry.tx_device).length,
    id: "subscriptions",
    label: "Subscriptions",
  },
  { cell: (device) => format.text(device.mac_address), id: "mac", label: "MAC address", defaultHidden: true },
  { cell: (device) => format.text(device.dante_model), id: "dante-model", label: "Dante model", defaultHidden: true },
  { cell: (device) => format.text(device.kind), id: "kind", label: "Kind", defaultHidden: true },
  { cell: (device) => format.text(device.inventory_sources), id: "sources", label: "Inventory sources", defaultHidden: true },
  { cell: (device) => format.text(device.ddm_domain_name), id: "domain", label: "Domain", defaultHidden: true },
  { cell: (device) => format.timestamp(device.last_seen), id: "last-seen", label: "Last seen", defaultHidden: true },
];

function DeviceInfo({ location }) {
  const filter = location.query.filter || "";
  const all = format.sortedDevices(devices.value);
  const needle = filter.trim().toLowerCase();
  const visible = all.filter((device) => !needle || format.deviceHaystack(device).includes(needle));
  const online = all.filter((device) => device.online).length;

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Device info</div>
          <div class="content-subtitle">${online} of ${all.length} devices online</div>
        </div>
      </div>
      <${Panel} title="Devices">
        ${all.length === 0
          ? html`<${Notice}>No Dante devices have been discovered yet. The daemon browses mDNS continuously.<//>`
          : html`<${ConfigurableTable}
              tableId="device-info"
              columns=${INFO_COLUMNS}
              rows=${visible}
              rowKey=${(device) => device.server_name || device.name}
              onRowClick=${(device) => navigate(devicePath("devices", format.deviceLabel(device), "receive"))}
              toolbar=${html`
                <input
                  type="search"
                  placeholder="Filter by name, address, model, or domain"
                  size="34"
                  value=${filter}
                  onInput=${(event) => setQueryParameter("filter", event.target.value)}
                />
                <span class="nav-count">${visible.length} of ${all.length}</span>
              `}
            />`}
      <//>
    </div>
  `;
}

export const DEVICE_TABS = [
  { id: "receive", label: "Receive" },
  { id: "transmit", label: "Transmit" },
  { id: "status", label: "Status" },
  { id: "latency", label: "Latency" },
  { id: "device-config", label: "Device config" },
  { id: "network-config", label: "Network config" },
  { id: "aes67-config", label: "AES67 config" },
  { id: "flows", label: "Transmit flows" },
  { id: "lock", label: "Device lock" },
];

function tabContent(tab, device) {
  if (tab === "transmit") {
    return html`<${TransmitSection} device=${device} />`;
  }
  if (tab === "status") {
    return html`<${StatusSection} device=${device} />`;
  }
  if (tab === "latency") {
    return html`<${LatencySection} device=${device} />`;
  }
  if (tab === "device-config") {
    return html`<${DeviceConfigSection} device=${device} />`;
  }
  if (tab === "network-config") {
    return html`<${NetworkSection} device=${device} />`;
  }
  if (tab === "aes67-config") {
    return html`<${Aes67Section} device=${device} />`;
  }
  if (tab === "flows") {
    return html`<${FlowsSection} device=${device} />`;
  }
  if (tab === "lock") {
    return html`<${LockSection} device=${device} />`;
  }
  if (tab === "domain") {
    return html`<${ManagedSection} device=${device} />`;
  }
  return html`<${ReceiveSection} device=${device} />`;
}

function DeviceView({ location }) {
  const deviceName = location.parameters.device;
  const device = deviceByName(deviceName);
  if (!device) {
    return html`<${Notice}>
      No device named ${deviceName} is in the daemon inventory. It may have gone offline or been forgotten.
    <//>`;
  }
  const tabs = isManaged(device) ? [...DEVICE_TABS, { id: "domain", label: "Domain" }] : DEVICE_TABS;
  const tab = location.parameters.section || "receive";
  const subscriptions = (device.subscriptions || []).filter((entry) => entry.tx_device);

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">${format.deviceLabel(device)}</div>
          <div class="content-subtitle">
            ${format.text(device.model)} · ${format.text(device.ipv4)} · ${format.sampleRate(device.sample_rate_hz)} ·
            ${format.latency(device.latency_ms)} · ${format.text(device.clock_role)} ·
            ${subscriptions.length} subscriptions
          </div>
        </div>
        <${Pill} tone=${device.online ? "good" : "bad"}>${device.online ? "online" : "offline"}<//>
      </div>
      <nav class="tabs">
        ${tabs.map(
          (entry) => html`
            <a
              key=${entry.id}
              class="tab${entry.id === tab ? " active" : ""}"
              aria-current=${entry.id === tab ? "page" : null}
              href=${devicePath("devices", format.deviceLabel(device), entry.id)}
            >
              ${entry.label}
            </a>
          `,
        )}
      </nav>
      ${tabContent(tab, device)}
    </div>
  `;
}

function DevicesView({ location }) {
  if (location.parameters.device) {
    return html`<${DeviceView} location=${location} />`;
  }
  return html`<${DeviceInfo} location=${location} />`;
}

export const devicesView = {
  component: DevicesView,
  id: "devices",
  label: "Device info",
};
