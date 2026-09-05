import { Notice, OnlineState, Panel } from "../components.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";
import { devicePath, navigate } from "../router.js";
import { devices } from "../store.js";
import { ConfigurableTable } from "../table.js";

function syncText(device) {
  const state = device.ddm_clocking_state;
  if (state && typeof state === "object" && state.locked) {
    return String(state.locked).toLowerCase();
  }
  return format.text(device.clock_role);
}

function frequencyOffset(device) {
  const offset = device.clock_frequency_offset_parts_per_billion;
  if (offset === null || offset === undefined) {
    return format.ABSENT;
  }
  return `${offset} ppb`;
}

const COLUMNS = [
  { cell: (device) => html`<${OnlineState} online=${device.online} />`, id: "state", label: "State" },
  { cell: (device) => format.deviceLabel(device), id: "name", label: "Device name" },
  { cell: (device) => format.text(device.clock_role), id: "role", label: "Clock role" },
  { cell: (device) => syncText(device), id: "sync", label: "Sync" },
  { cell: (device) => format.preferredLeader(device.preferred_leader), id: "preferred-leader", label: "Preferred leader" },
  { cell: (device) => format.clockSourceCode(device.clock_source_code), id: "clock-source", label: "Clock source" },
  { cell: (device) => format.clockSubdomain(device.clock_subdomain), id: "subdomain", label: "Clock subdomain" },
  { cell: (device) => format.text(device.clock_identity), id: "identity", label: "Clock identity", defaultHidden: true },
  { cell: (device) => format.text(device.leader_clock_identity), id: "leader", label: "Leader clock identity" },
  { align: "right", cell: (device) => frequencyOffset(device), id: "offset", label: "Frequency offset" },
  { cell: (device) => format.text(device.clock_port_state_code), id: "port-state", label: "Port state code", defaultHidden: true },
  {
    cell: (device) => (device.aes67_supported === false ? "not supported" : format.text(device.aes67_current)),
    id: "aes67",
    label: "AES67",
    defaultHidden: true,
  },
  { cell: (device) => format.sampleRate(device.sample_rate_hz), id: "sample-rate", label: "Sample rate", defaultHidden: true },
];

function ClockStatusView() {
  const all = format.sortedDevices(devices.value);
  const leaders = all.filter((device) => String(device.clock_role).toLowerCase() === "leader");
  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Clock status</div>
          <div class="content-subtitle">
            ${leaders.length === 1
              ? `Leader: ${format.deviceLabel(leaders[0])}`
              : leaders.length === 0
                ? "No clock leader reported"
                : `${leaders.length} devices report the leader role`}
          </div>
        </div>
      </div>
      <${Panel} title="Devices">
        ${all.length === 0
          ? html`<${Notice}>No Dante devices have been discovered yet.<//>`
          : html`<${ConfigurableTable}
              tableId="clock-status"
              columns=${COLUMNS}
              rows=${all}
              rowKey=${(device) => device.server_name || device.name}
              onRowClick=${(device) => navigate(devicePath("devices", format.deviceLabel(device), "device-config"))}
            />`}
      <//>
    </div>
  `;
}

export const clockStatusView = {
  component: ClockStatusView,
  id: "clock-status",
  label: "Clock status",
};
