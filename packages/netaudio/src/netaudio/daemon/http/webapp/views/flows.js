import {Notice, OnlineState, Panel, Value} from "../components.js";
import { FlowsSection } from "../device/flows.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";
import { devicePath, navigate } from "../router.js";
import { deviceByName, devices } from "../store.js";
import { ConfigurableTable } from "../table.js";

const FLOW_DEVICE_COLUMNS = [
  { cell: (device) => html`<${OnlineState} online=${device.online} />`, id: "state", label: "State" },
  { cell: (device) => format.deviceLabel(device), id: "device", label: "Device" },
  { align: "right", cell: (device) => html`<${Value} value=${device.tx_flow_count} />`, id: "transmit-flows", label: "Transmit flows" },
  { align: "right", cell: (device) => html`<${Value} value=${device.rx_flow_count} />`, id: "receive-flows", label: "Receive flows" },
  { align: "right", cell: (device) => html`<${Value} value=${device.tx_count} />`, id: "transmit-channels", label: "Tx channels", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.model} />`, id: "model", label: "Model", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.ipv4} />`, id: "address", label: "Address", defaultHidden: true },
];

function FlowsView({ location }) {
  const all = format.sortedDevices(devices.value);
  const deviceName = location.parameters.device;

  if (deviceName) {
    const device = deviceByName(deviceName);
    if (!device) {
      return html`<${Notice}>No device named ${deviceName} is in the daemon inventory.<//>`;
    }
    return html`
      <div class="stack">
        <div class="content-header">
          <div>
            <div class="content-title">${format.deviceLabel(device)}</div>
            <div class="content-subtitle">Transmit flow slots read directly from the device over ARC.</div>
          </div>
        </div>
        <${FlowsSection} device=${device} />
      </div>
    `;
  }

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Flows</div>
          <div class="content-subtitle">Choose a device to read and manage its transmit flows.</div>
        </div>
      </div>
      <${Panel} title="Devices">
        ${all.length === 0
          ? html`<${Notice}>No devices have been discovered yet.<//>`
          : html`<${ConfigurableTable}
              tableId="flows-devices"
              columns=${FLOW_DEVICE_COLUMNS}
              rows=${all}
              rowKey=${(device) => device.server_name || device.name}
              onRowClick=${(device) => navigate(devicePath("flows", format.deviceLabel(device)))}
            />`}
      <//>
    </div>
  `;
}

export const flowsView = {
  component: FlowsView,
  id: "flows",
  label: "Flows",
};
