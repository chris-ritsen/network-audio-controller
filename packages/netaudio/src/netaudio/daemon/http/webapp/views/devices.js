import { Notice, OnlineState, Panel } from "../components.js";
import { ReceiveSection, TransmitSection } from "../device/channels.js";
import { Aes67Section, DeviceConfigSection } from "../device/config.js";
import { isEnrolled, ManagedSection } from "../device/managed.js";
import { NetworkSection } from "../device/network.js";
import { LockSection } from "../device/security.js";
import { StatusSection } from "../device/status.js";
import { DeviceMeters } from "../device/meters.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";
import { devicePath, navigate, setQueryParameter } from "../router.js";
import { deviceByName, scopedDevices as devices } from "../store.js";
import { ConfigurableTable } from "../table.js";

const INFO_COLUMNS = [
  { cell: (device) => html`<${OnlineState} online=${device.online} />`, id: "state", label: "State" },
  { cell: (device) => format.deviceLabel(device), id: "name", label: "Name" },
  { cell: (device) => format.text(format.deviceModelName(device)), id: "model", label: "Model name" },
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
  { cell: (device) => format.macAddress(device), id: "mac", label: "MAC address" },
  { cell: (device) => format.text(device.board_name), id: "dante-model", label: "Dante model", defaultHidden: true },
  { cell: (device) => format.text(device.kind), id: "kind", label: "Kind", defaultHidden: true },
  { cell: (device) => format.text(device.inventory_sources), id: "sources", label: "Inventory sources", defaultHidden: true },
  { cell: (device) => format.text(device.ddm_domain_name), id: "domain", label: "Domain", defaultHidden: true },
  { cell: (device) => format.timestamp(device.last_seen), id: "last-seen", label: "Last seen", defaultHidden: true },
  { cell: (device) => format.text(device.clock_role), id: "clock-role", label: "Clock role" },
  { cell: (device) => format.clockLeaderName(device, devices.value), id: "clock-leader", label: "Clock leader", defaultHidden: true },
  { cell: (device) => format.preferredLeader(device.preferred_leader), id: "preferred-leader", label: "Preferred leader", defaultHidden: true },
  { cell: (device) => format.clockSourceCode(device.clock_source_code), id: "clock-source", label: "Clock source", defaultHidden: true },
  { cell: (device) => format.clockSubdomain(device.clock_subdomain), id: "clock-subdomain", label: "Clock subdomain", defaultHidden: true },
  { cell: (device) => format.text(device.ddm_clocking_state?.locked), id: "clock-sync", label: "Clock sync", defaultHidden: true },
  { cell: (device) => device.clock_frequency_offset_parts_per_billion == null ? format.ABSENT : `${device.clock_frequency_offset_parts_per_billion} ppb`, id: "frequency-offset", label: "Frequency offset", defaultHidden: true },
  { cell: (device) => device.aes67_current == null ? format.ABSENT : device.aes67_current ? "Enabled" : "Disabled", id: "aes67", label: "AES67", defaultHidden: true },
  { cell: (device) => format.text(device.interfaces?.[0]?.mode), id: "primary-mode", label: "Primary mode", defaultHidden: true },
  { cell: (device) => format.text(device.interfaces?.[1]?.ip_address), id: "secondary-address", label: "Secondary address", defaultHidden: true },
  { cell: (device) => {
    const speed = device.interfaces?.[1]?.link_speed_mbps ?? device.interfaces?.[1]?.speed;
    return speed == null ? format.ABSENT : `${speed} Mbps`;
  }, id: "secondary-link-speed", label: "Secondary link speed", defaultHidden: true },
  { cell: (device) => format.text(device.interfaces?.[0]?.gateway), id: "gateway", label: "Gateway", defaultHidden: true },
  { cell: (device) => format.text(device.interfaces?.[0]?.dns_server), id: "dns", label: "DNS", defaultHidden: true },
  { cell: (device) => format.text(device.interface_reboot_required), id: "reboot-required", label: "Reboot required", defaultHidden: true },
  ...[["tx", "transmit"], ["rx", "receive"]].map(([direction, field]) => ({
    id: `${direction}-bandwidth`, label: `${direction === "tx" ? "Tx" : "Rx"} bandwidth`, defaultHidden: true,
    cell: (device) => {
      const value = device.network_interface_traffic?.[`estimated_total_${field}_bits_per_second`];
      return value == null || !Number.isFinite(Number(value)) ? format.ABSENT : `${(Number(value) / 1_000_000).toFixed(2)} Mbps`;
    },
  })),
];

function DeviceInfo({ location }) {
  const filter = location.query.filter || "";
  const all = format.sortedDevices(devices.value);
  const needle = filter.trim().toLowerCase();
  const visible = all.filter((device) => !needle || format.deviceHaystack(device).includes(needle));
  const online = all.filter((device) => device.online).length;

  return html`
    <div class="flex flex-col gap-4 pb-6">
      <div class="content-header">
        <div>
          <div class="content-title">Devices</div>
          <div class="content-subtitle">${online} of ${all.length} online</div>
        </div>
      </div>
      <${Panel}>
        ${all.length === 0
          ? html`<${Notice}>No Dante devices have been discovered yet. The daemon browses mDNS continuously.<//>`
          : html`<${ConfigurableTable}
              tableId="devices"
              mobileSummary=${(device) => ({ title: format.deviceLabel(device), detail: html`<${OnlineState} online=${device.online} />` })}
              columns=${INFO_COLUMNS}
              rows=${visible}
              rowKey=${(device) => device.server_name || device.name}
              rowHref=${(device) => devicePath("devices", format.deviceLabel(device), "receive")}
              toolbar=${html`
                <input
                  type="search"
                  placeholder="Filter"
                  aria-label="Find device by name, address, model, or domain"
                  size="18"
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
  { id: "metering", label: "Metering" },
  { id: "status", label: "Status" },
  { id: "device-config", label: "Device config" },
  { id: "network-config", label: "Network config" },
  { id: "aes67-config", label: "AES67 config" },
  { id: "lock", label: "Device lock" },
];

function tabContent(tab, device) {
  if (tab === "metering") {
    return html`<${DeviceMeters} device=${device} />`;
  }
  if (tab === "transmit") {
    return html`<${TransmitSection} device=${device} />`;
  }
  if (tab === "status") {
    return html`<${StatusSection} device=${device} />`;
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
  if (!device.online) {
    return html`<div class="flex flex-col gap-4">
      <h1 class="content-title">${format.deviceLabel(device)}</h1>
      <${Notice}>This device is offline. Controls are unavailable until it reconnects.<//>
    </div>`;
  }
  const tabs = isEnrolled(device) ? [...DEVICE_TABS, { id: "domain", label: "Domain" }] : DEVICE_TABS;
  const tab = location.parameters.section || "receive";

  return html`
    <div class="flex flex-col gap-4">
      <div class="device-header">
        <div class="device-heading">
          <h1 class="content-title">${format.deviceLabel(device)}</h1>
          <${OnlineState} online=${device.online} />
        </div>
        ${format.deviceModelName(device) ? html`<div class="device-model">${format.deviceModelName(device)}</div>` : null}
        ${device.ipv4 ? html`<div class="device-address"><span>Address</span> ${device.ipv4}</div>` : null}
      </div>
      <label class="device-section-menu">
        <span class="sr-only">Device section</span>
        <select aria-label="Device section" value=${tab}
          onChange=${(event) => navigate(devicePath("devices", format.deviceLabel(device), event.target.value))}>
          ${tabs.map((entry) => html`<option value=${entry.id}>${entry.label}</option>`)}
        </select>
      </label>
      <nav class="tabs device-tabs" aria-label="Device sections">
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
      ${tabs.some((entry) => entry.id === tab) ? tabContent(tab, device) : html`<${Notice}>This section is unavailable for this device.<//>`}
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
  label: "Devices",
};
