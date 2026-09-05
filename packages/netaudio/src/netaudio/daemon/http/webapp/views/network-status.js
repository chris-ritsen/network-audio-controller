import { Notice, OnlineState, Panel } from "../components.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";
import { devicePath, navigate } from "../router.js";
import { devices } from "../store.js";
import { ConfigurableTable } from "../table.js";

function primaryInterface(device) {
  return Array.isArray(device.interfaces) && device.interfaces.length ? device.interfaces[0] : null;
}

function secondaryInterface(device) {
  return Array.isArray(device.interfaces) && device.interfaces.length > 1 ? device.interfaces[1] : null;
}

function linkSpeed(value) {
  return value ? `${value} Mbps` : format.ABSENT;
}

function bitsPerSecond(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) {
    return format.ABSENT;
  }
  return `${(Number(value) / 1_000_000).toFixed(2)} Mbps`;
}

function traffic(device, key) {
  const record = device.network_interface_traffic;
  return record && typeof record === "object" ? bitsPerSecond(record[key]) : format.ABSENT;
}

const COLUMNS = [
  { cell: (device) => html`<${OnlineState} online=${device.online} />`, id: "state", label: "State" },
  { cell: (device) => format.deviceLabel(device), id: "name", label: "Device name" },
  { cell: (device) => format.text(device.ipv4), id: "primary-address", label: "Primary address" },
  {
    cell: (device) => {
      const record = primaryInterface(device);
      return record ? format.text(record.mode) : format.ABSENT;
    },
    id: "primary-mode",
    label: "Primary mode",
  },
  { cell: (device) => linkSpeed(device.link_speed_mbps), id: "primary-link-speed", label: "Primary link speed" },
  {
    cell: (device) => {
      const record = secondaryInterface(device);
      return record ? format.text(record.ip_address) : format.ABSENT;
    },
    id: "secondary-address",
    label: "Secondary address",
  },
  {
    cell: (device) => {
      const record = secondaryInterface(device);
      return record ? linkSpeed(record.link_speed_mbps ?? record.speed) : format.ABSENT;
    },
    id: "secondary-link-speed",
    label: "Secondary link speed",
    defaultHidden: true,
  },
  { cell: (device) => traffic(device, "estimated_total_transmit_bits_per_second"), id: "tx-bandwidth", label: "Tx bandwidth" },
  { cell: (device) => traffic(device, "estimated_total_receive_bits_per_second"), id: "rx-bandwidth", label: "Rx bandwidth" },
  { cell: (device) => format.text(device.mac_address), id: "mac", label: "MAC address", defaultHidden: true },
  {
    cell: (device) => {
      const record = primaryInterface(device);
      return record ? format.text(record.gateway) : format.ABSENT;
    },
    id: "gateway",
    label: "Gateway",
    defaultHidden: true,
  },
  {
    cell: (device) => {
      const record = primaryInterface(device);
      return record ? format.text(record.dns_server) : format.ABSENT;
    },
    id: "dns",
    label: "DNS",
    defaultHidden: true,
  },
  { cell: (device) => format.text(device.interface_reboot_required), id: "reboot-required", label: "Reboot required", defaultHidden: true },
];

function NetworkStatusView() {
  const all = format.sortedDevices(devices.value);
  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Network status</div>
          <div class="content-subtitle">${all.filter((device) => device.online).length} of ${all.length} devices online</div>
        </div>
      </div>
      <${Panel} title="Devices">
        ${all.length === 0
          ? html`<${Notice}>No Dante devices have been discovered yet.<//>`
          : html`<${ConfigurableTable}
              tableId="network-status"
              columns=${COLUMNS}
              rows=${all}
              rowKey=${(device) => device.server_name || device.name}
              onRowClick=${(device) => navigate(devicePath("devices", format.deviceLabel(device), "network-config"))}
            />`}
      <//>
    </div>
  `;
}

export const networkStatusView = {
  component: NetworkStatusView,
  id: "network-status",
  label: "Network status",
};
