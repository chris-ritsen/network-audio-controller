import { api } from "../api.js";
import { AsyncButton, Button, Notice, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { html, useState } from "../lib/preact.js";
import { buildMatrixModel, expanded, RoutingMatrix, setAllExpanded } from "../matrix.js";
import { devicePath, navigate } from "../router.js";
import { deviceRequestName, devices } from "../store.js";
import { ConfigurableTable } from "../table.js";

function subscriptionColumns() {
  return [
    { cell: (row) => format.deviceLabel(row.device), id: "receiver", label: "Receiver" },
    { cell: (row) => html`<${Value} value=${row.subscription.rx_channel} />`, id: "receive-channel", label: "Rx channel" },
    { cell: (row) => html`<${Value} value=${row.subscription.tx_device} />`, id: "transmitter", label: "Transmitter" },
    { cell: (row) => html`<${Value} value=${row.subscription.tx_channel} />`, id: "transmit-channel", label: "Tx channel" },
    {
      cell: (row) => {
        const status = row.subscription.status || null;
        return html`<span class=${status ? `state-${format.statusTone(status.severity)}` : ""}>
          ${status ? format.subscriptionStatusText(row.subscription) : format.ABSENT}
        </span>`;
      },
      id: "status",
      label: "Status",
    },
    { cell: (row) => html`<${Value} value=${row.subscription.ddm_summary} />`, id: "managed", label: "Domain status", defaultHidden: true },
    {
      cell: (row) =>
        row.receiveNumber === undefined
          ? format.ABSENT
          : html`<${AsyncButton}
              small
              description=${`unsubscribe ${format.deviceLabel(row.device)} ${row.subscription.rx_channel}`}
              onRun=${() =>
                api.unsubscribe({ rx_channel: row.receiveNumber, rx_device: deviceRequestName(row.device) })}
            >
              Unsubscribe
            <//>`,
      id: "action",
      label: "",
    },
  ];
}

function SubscriptionTable({ all }) {
  const rows = [];
  for (const device of all) {
    const receiveChannels = device.channels ? device.channels.receivers || {} : {};
    const numbers = format.sortedChannelNumbers(receiveChannels);
    for (const subscription of device.subscriptions || []) {
      if (!subscription.tx_device || !subscription.tx_channel) {
        continue;
      }
      rows.push({
        device,
        receiveNumber: numbers.find((number) => receiveChannels[number].name === subscription.rx_channel),
        subscription,
      });
    }
  }
  return html`
    <${Panel} title=${`Subscriptions (${rows.length})`}>
      ${rows.length === 0
        ? html`<${Notice}>No Dante receiver is currently subscribed.<//>`
        : html`<${ConfigurableTable}
            tableId="subscriptions"
            columns=${subscriptionColumns()}
            rows=${rows}
            rowKey=${(row) => `${row.device.server_name}:${row.subscription.rx_channel}`}
            short
          />`}
    <//>
  `;
}

function RoutingView() {
  const all = format.sortedDevices(devices.value);
  const [receiverFilter, setReceiverFilter] = useState("");
  const [transmitterFilter, setTransmitterFilter] = useState("");
  const state = expanded.value;

  if (!all.length) {
    return html`<${Notice}>No Dante devices have been discovered yet.<//>`;
  }

  const model = buildMatrixModel({
    devices: devices.value,
    expandedReceivers: state.receivers,
    expandedTransmitters: state.transmitters,
    receiverFilter,
    transmitterFilter,
  });
  const receiverLabels = model.rows.filter((row) => row.kind === "device").map((row) => row.label);
  const transmitterLabels = model.columns.filter((column) => column.kind === "device").map((column) => column.label);
  const anyExpanded = receiverLabels.some((label) => state.receivers.has(label)) || transmitterLabels.some((label) => state.transmitters.has(label));

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Routing</div>
          <div class="content-subtitle">
            ${receiverLabels.length} receivers · ${transmitterLabels.length} transmitters
          </div>
        </div>
        <div class="toolbar">
          <label class="inline">
            Rx filter
            <input type="search" size="16" value=${receiverFilter} onInput=${(event) => setReceiverFilter(event.target.value)} />
          </label>
          <label class="inline">
            Tx filter
            <input type="search" size="16" value=${transmitterFilter} onInput=${(event) => setTransmitterFilter(event.target.value)} />
          </label>
          <${Button}
            small
            onClick=${() => {
              setAllExpanded("receivers", receiverLabels, !anyExpanded);
              setAllExpanded("transmitters", transmitterLabels, !anyExpanded);
            }}
          >
            ${anyExpanded ? "Collapse all" : "Expand all"}
          <//>
        </div>
      </div>
      ${model.rows.length === 0 || model.columns.length === 0
        ? html`<${Notice}>No devices match the current filters.<//>`
        : html`<${RoutingMatrix}
            rows=${model.rows}
            columns=${model.columns}
            subscriptionIndex=${model.subscriptionIndex}
            onOpenDevice=${(label, tab) => navigate(devicePath("devices", label, tab))}
          />`}
      <${SubscriptionTable} all=${all} />
    </div>
  `;
}

export const routingView = {
  component: RoutingView,
  id: "routing",
  label: "Routing",
};
