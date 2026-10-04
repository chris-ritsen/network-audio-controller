import { operationWritable } from "../device/availability.js";
import { Notice, Panel } from "../components.js";
import { ReceiveSection, TransmitSection } from "../device/channels.js";
import { Aes67Section } from "../device/aes67.js";
import { DeviceConfigSection } from "../device/config.js";
import { aes67Status } from "../aes67.js";
import { isEnrolled, ManagedSection } from "../device/managed.js";
import { NetworkSection } from "../device/network.js";
import { LockSection } from "../device/security.js";
import { clockLeaderLabel, StatusSection } from "../device/status.js";
import { DeviceMeters } from "../device/meters.js";
import * as format from "../format.js";
import { html, useLayoutEffect } from "../lib/preact.js";
import { devicePath, navigate, setQueryParameter } from "../router.js";
import { deviceByName, inventoryReady, scopedDevices as devices } from "../store.js";
import { ConfigurableTable } from "../table.js";

function shown(value) {
  if (value === null || value === undefined || value === "") return "";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (Array.isArray(value)) return value.map(shown).filter(Boolean).join(", ");
  if (typeof value === "object") return "";
  return String(value);
}

function words(value) {
  const text = shown(value).replaceAll("_", " ");
  if (!text || ["unknown", "none", "unset", "unavailable"].includes(text.toLowerCase())) return "";
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function present(value) {
  return value === format.ABSENT ? "" : value;
}

function DeviceState({ online }) {
  return html`<span class="state-inline">
    <span class="status-dot${online ? " online" : ""}" aria-hidden="true"></span>
    ${online ? "Online" : "Offline"}
  </span>`;
}

function clockSync(device) {
  const synchronization = device.clock_status?.synchronization;
  if (format.clockStatusFresh(device) && synchronization === "synchronized") return "Synchronized";
  if (format.clockStatusFresh(device) && synchronization === "lost") return "Not synchronized";
  const locked = device.ddm_clocking_state?.locked;
  if (locked === true || locked === "LOCKED") return "Synchronized";
  if (locked === false || locked === "UNLOCKED") return "Not synchronized";
  return "";
}

function megabits(value) {
  return value == null || !Number.isFinite(Number(value)) ? "" : `${value} Mbps`;
}

const INFO_COLUMNS = [
  {
    cell: (device) => html`<${DeviceState} online=${device.online} />`,
    sortValue: (device) => (device.online ? "Online" : "Offline"),
    id: "state",
    label: "State",
  },
  { cell: (device) => format.deviceLabel(device), id: "name", label: "Name" },
  {
    cell: (device) => format.deviceModelName(device),
    id: "model",
    label: "Model name",
  },
  {
    cell: (device) => shown(device.manufacturer),
    id: "manufacturer",
    label: "Manufacturer",
    defaultHidden: true,
  },
  {
    cell: (device) =>
      shown(
        device.friendly_product_version ||
          device.product_version ||
          device.ddm_product_version,
      ),
    id: "product-version",
    label: "Product version",
    defaultHidden: true,
  },
  {
    cell: (device) =>
      shown(device.platform_software_version || device.ddm_dante_version),
    id: "dante-version",
    label: "Dante software/firmware",
  },
  {
    cell: (device) =>
      shown(
        device.platform_hardware_version || device.ddm_dante_hardware_version,
      ),
    id: "hardware-version",
    label: "Hardware version",
    defaultHidden: true,
  },
  {
    cell: (device) =>
      device.is_locked == null ? "" : device.is_locked ? "Locked" : "Unlocked",
    id: "lock",
    label: "Device lock",
  },
  {
    cell: (device) => shown(device.ipv4),
    id: "primary-address",
    label: "Primary address",
  },
  {
    cell: (device) => (device.link_speed_mbps ? megabits(device.link_speed_mbps) : ""),
    id: "link-speed",
    label: "Primary link speed",
  },
  {
    cell: (device) => present(format.sampleRate(device.sample_rate_hz)),
    id: "sample-rate",
    label: "Sample rate",
  },
  {
    cell: (device) => (device.encoding ? `PCM ${device.encoding}` : ""),
    id: "encoding",
    label: "Encoding",
    defaultHidden: true,
  },
  {
    cell: (device) => present(format.latency(device.latency_ms)),
    id: "latency",
    label: "Latency",
  },
  {
    align: "right",
    cell: (device) => shown(device.tx_count),
    id: "transmit",
    label: "Tx channels",
  },
  {
    align: "right",
    cell: (device) => shown(device.rx_count),
    id: "receive",
    label: "Rx channels",
  },
  {
    align: "right",
    cell: (device) =>
      (device.subscriptions || []).filter((entry) => entry.tx_device).length,
    id: "subscriptions",
    label: "Subscriptions",
  },
  {
    cell: (device) => present(format.macAddress(device)),
    id: "mac",
    label: "MAC address",
  },
  {
    cell: (device) => shown(device.platform_model_name),
    id: "dante-model",
    label: "Dante platform model",
    defaultHidden: true,
  },
  {
    cell: (device) => shown(device.ddm_domain_name),
    id: "domain",
    label: "Domain",
    defaultHidden: true,
  },
  {
    cell: (device) => present(format.timestamp(device.last_seen)),
    id: "last-seen",
    label: "Last seen",
    defaultHidden: true,
  },
  {
    cell: (device) => words(device.clock_role),
    id: "clock-role",
    label: "Clock role",
  },
  {
    cell: (device) => clockLeaderLabel(device, devices.value),
    id: "clock-leader",
    label: "Clock leader",
    defaultHidden: true,
  },
  {
    cell: (device) => shown(device.preferred_leader),
    id: "preferred-leader",
    label: "Preferred leader",
    defaultHidden: true,
  },
  {
    cell: (device) => words(device.clock_source),
    id: "clock-source",
    label: "Clock source",
    defaultHidden: true,
  },
  {
    cell: (device) => words(device.clock_subdomain_presentation?.label),
    id: "clock-subdomain",
    label: "Clock subdomain",
    defaultHidden: true,
  },
  {
    cell: (device) => clockSync(device),
    id: "clock-sync",
    label: "Clock sync",
    defaultHidden: true,
  },
  {
    cell: (device) =>
      device.clock_frequency_offset_parts_per_billion == null
        ? ""
        : `${Number((device.clock_frequency_offset_parts_per_billion / 1000).toFixed(3))} ppm`,
    id: "frequency-offset",
    label: "Frequency offset",
    defaultHidden: true,
  },
  {
    cell: (device) => aes67Label(device),
    id: "aes67",
    label: "AES67",
    defaultHidden: true,
  },
  {
    cell: (device) => ({ dynamic: "DHCP", static: "Static" })[device.interfaces?.[0]?.mode] || "",
    id: "primary-mode",
    label: "Primary mode",
    defaultHidden: true,
  },
  {
    cell: (device) => shown(device.interfaces?.[1]?.ip_address),
    id: "secondary-address",
    label: "Secondary address",
    defaultHidden: true,
  },
  {
    cell: (device) => {
      const speed =
        device.interfaces?.[1]?.link_speed_mbps ??
        device.interfaces?.[1]?.speed;
      return megabits(speed);
    },
    id: "secondary-link-speed",
    label: "Secondary link speed",
    defaultHidden: true,
  },
  {
    cell: (device) => shown(device.interfaces?.[0]?.gateway),
    id: "gateway",
    label: "Gateway",
    defaultHidden: true,
  },
  {
    cell: (device) => shown(device.interfaces?.[0]?.dns_server),
    id: "dns",
    label: "DNS",
    defaultHidden: true,
  },
  {
    cell: (device) => shown(device.interface_reboot_required),
    id: "reboot-required",
    label: "Reboot required",
    defaultHidden: true,
  },
  ...[
    ["tx", "transmit"],
    ["rx", "receive"],
  ].map(([direction, field]) => ({
    id: `${direction}-bandwidth`,
    label: `${direction === "tx" ? "Tx" : "Rx"} bandwidth`,
    defaultHidden: true,
    cell: (device) => {
      const value =
        device.network_interface_traffic?.[
          `total_${field}_rate_bits_per_second`
        ];
      return value == null || !Number.isFinite(Number(value))
        ? ""
        : `${(Number(value) / 1_000_000).toFixed(2)} Mbps`;
    },
  })),
];

const AES67_STATES = new Set([
  "Enabled",
  "Disabled",
  "Enable pending",
  "Disable pending",
  "Configured enabled",
  "Configured disabled",
]);

function aes67Label(device) {
  const label = aes67Status(device).label;
  return AES67_STATES.has(label) ? label : "";
}

const INVENTORY_VIEWS = {
  devices: {
    columns: [
      "state",
      "name",
      "model",
      "primary-address",
      "sample-rate",
      "receive",
      "transmit",
      "lock",
      "domain",
    ],
  },
  "clock-status": {
    columns: [
      "state",
      "name",
      "clock-role",
      "clock-leader",
      "preferred-leader",
      "clock-source",
      "clock-sync",
      "sample-rate",
      "domain",
    ],
  },
  "network-status": {
    columns: [
      "state",
      "name",
      "primary-address",
      "link-speed",
      "secondary-address",
      "secondary-link-speed",
      "tx-bandwidth",
      "rx-bandwidth",
      "reboot-required",
    ],
  },
};

function DeviceInfo({ location }) {
  const mode = INVENTORY_VIEWS[location.view] ? location.view : "devices";
  const view = INVENTORY_VIEWS[mode];
  const columns = [...INFO_COLUMNS]
    .sort((a, b) => {
      const rank = (column) =>
        view.columns.includes(column.id)
          ? view.columns.indexOf(column.id)
          : view.columns.length;
      return rank(a) - rank(b);
    })
    .map((column) => ({
      ...column,
      defaultHidden: !view.columns.includes(column.id),
    }));
  const filter = location.query.filter || "";
  const all = format.sortedDevices(devices.value);
  const needle = filter.trim().toLowerCase();
  const visible = all.filter(
    (device) => !needle || format.deviceHaystack(device).includes(needle),
  );
  const online = all.filter((device) => device.online).length;

  return html`
    <div class="flex flex-col gap-4 pb-6">
      <${Panel}>
        ${
          all.length === 0
            ? html`<${Notice}>No Dante devices found.<//>`
            : html`<${ConfigurableTable}
                key=${mode}
                tableId=${mode}
                mobileSummary=${(device) => ({ title: format.deviceLabel(device), detail: html`<${DeviceState} online=${device.online} />` })}
                columns=${columns}
                rows=${visible}
                rowKey=${(device) => device.server_name || device.name}
                rowHref=${(device) => devicePath("devices", format.deviceLabel(device), deviceTabs(device)[0].id)}
                toolbar=${html`
                  <input
                    type="search"
                    placeholder="Find a device…"
                    aria-label="Find device by name, address, model, or domain"
                    size="18"
                    value=${filter}
                    onInput=${(event) => setQueryParameter("filter", event.target.value)}
                  />
                  <span class="nav-count"
                    >${visible.length} devices · ${online} online</span
                  >
                `}
              />`
        }
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

function channelCount(device, direction) {
  const declared = device.channels?.[direction === "tx" ? "transmitters" : "receivers"];
  const listed = declared && typeof declared === "object" ? Object.keys(declared).length : 0;
  const reported = Number(direction === "tx" ? device.tx_count : device.rx_count);
  return Math.max(listed, Number.isFinite(reported) ? reported : 0);
}

function flowCount(flows) {
  return Array.isArray(flows) ? flows.length : flows && typeof flows === "object" ? Object.keys(flows).length : 0;
}

const TAB_AVAILABILITY = {
  receive: (device) => channelCount(device, "rx") > 0 || flowCount(device.receiver_flows) > 0,
  transmit: (device) => channelCount(device, "tx") > 0 || flowCount(device.transmitter_flows) > 0,
  metering: (device) => channelCount(device, "rx") > 0 || channelCount(device, "tx") > 0,
  "aes67-config": (device) =>
    operationWritable(device, "aes67") ||
    (isEnrolled(device)
      ? device.ddm_capabilities?.rtp_audio_supported === true
      : device.aes67_configuration_supported === true),
  lock: (device) =>
    device.device_locking_supported === true ||
    device.operation_availability?.locking?.supported === true,
};

export function deviceTabs(device) {
  const tabs = isEnrolled(device)
    ? [...DEVICE_TABS, { id: "domain", label: "Domain" }]
    : DEVICE_TABS;
  return tabs.filter((entry) => !TAB_AVAILABILITY[entry.id] || TAB_AVAILABILITY[entry.id](device));
}

function SectionRedirect({ path }) {
  useLayoutEffect(() => {
    navigate(path, { replace: true });
  }, [path]);
  return null;
}

function DeviceView({ location }) {
  const deviceName = location.parameters.device;
  const device = deviceByName(deviceName);
  if (!device) {
    return html`<${Notice}>No device named ${deviceName}.<//>`;
  }
  if (!device.online) {
    return html`<div class="flex flex-col gap-4">
      <h1 class="content-title">${format.deviceLabel(device)}</h1>
      <${Notice}>This device is offline.<//>
    </div>`;
  }
  const requested = location.parameters.section;
  const available = deviceTabs(device);
  const known = DEVICE_TABS.some((entry) => entry.id === requested) || requested === "domain";
  const waiting = !inventoryReady.value && known && !available.some((entry) => entry.id === requested);
  const tabs = waiting
    ? [...DEVICE_TABS, { id: "domain", label: "Domain" }].filter(
        (entry) => entry.id === requested || available.some((item) => item.id === entry.id),
      )
    : available;
  const tab = tabs.some((entry) => entry.id === requested) ? requested : tabs[0].id;
  const redirect = tab === requested ? null : devicePath("devices", format.deviceLabel(device), tab);

  return html`
    <div class="flex flex-col gap-4">
      <div class="device-header">
        <div class="device-heading">
          <h1 class="content-title">${format.deviceLabel(device)}</h1>
          <label class="device-switcher"
            >Device
            <select
              aria-label="Switch device"
              value=${deviceRequestIdentifier(device)}
              onChange=${(event) => navigate(devicePath("devices", event.target.value, tab))}
            >
              ${format.sortedDevices(devices.value).map((entry) => html`<option value=${deviceRequestIdentifier(entry)}>${format.deviceLabel(entry)}</option>`)}
            </select>
          </label>
        </div>
        ${format.deviceModelName(device) ? html`<div class="device-model">${format.deviceModelName(device)}</div>` : null}
        ${device.ipv4 ? html`<div class="device-address"><span>Address</span> ${device.ipv4}</div>` : null}
      </div>
      <label class="device-section-menu">
        <span class="sr-only">Device section</span>
        <select
          aria-label="Device section"
          value=${tab}
          onChange=${(event) => navigate(devicePath("devices", format.deviceLabel(device), event.target.value))}
        >
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
      ${redirect ? html`<${SectionRedirect} path=${redirect} />` : null}
      ${tabContent(tab, device)}
    </div>
  `;
}

function deviceRequestIdentifier(device) {
  return device.server_name || device.name;
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
  label: "Device Info",
};

export const clockStatusView = {
  component: DeviceInfo,
  id: "clock-status",
  label: "Clock Status",
};
export const networkStatusView = {
  component: DeviceInfo,
  id: "network-status",
  label: "Network Status",
};
