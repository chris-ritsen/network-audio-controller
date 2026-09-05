import { api } from "../api.js";
import {AsyncButton, Button, Notice, Panel, Value} from "../components.js";
import * as format from "../format.js";
import { html, useRef, useState } from "../lib/preact.js";
import { RoutePicker } from "../route-picker.js";
import { deviceRequestName } from "../store.js";
import { ConfigurableTable } from "../table.js";

function gainChannelType(device) {
  if (device.gain_device_type === "input") {
    return "tx";
  }
  if (device.gain_device_type === "output") {
    return "rx";
  }
  return null;
}

function NameCell({ channel, channelNumber, channelType, requestName }) {
  const input = useRef(null);
  return html`
    <span class="cell-actions">
      <input
        key=${`channel-name-${requestName}-${channelType}-${channelNumber}`}
        ref=${input}
        type="text"
        size="16"
        defaultValue=${channel.name || ""}
      />
      <${AsyncButton}
        small
        description=${`rename ${channelType} channel ${channelNumber} on ${requestName}`}
        onRun=${() => api.renameChannel(requestName, channelType, channelNumber, input.current.value)}
      >
        Set
      <//>
      <${AsyncButton}
        small
        description=${`reset ${channelType} channel ${channelNumber} name on ${requestName}`}
        onRun=${() => api.renameChannel(requestName, channelType, channelNumber, "")}
      >
        Reset
      <//>
    </span>
  `;
}

function GainCell({ channel, channelNumber, channelType, device, requestName }) {
  const select = useRef(null);
  const choices = device.gain_level_choices;
  if (!choices || gainChannelType(device) !== channelType) {
    return html`<span>${html`<${Value} value=${channel.gain_level_label} />`}</span>`;
  }
  return html`
    <span class="cell-actions">
      <select key=${`gain-${requestName}-${channelType}-${channelNumber}`} ref=${select}>
        ${choices.map((choice) => {
          const value = choice !== null && typeof choice === "object" ? choice.value : choice;
          const label = choice !== null && typeof choice === "object" ? choice.label : String(choice);
          return html`<option key=${value} value=${value} selected=${Number(value) === Number(channel.gain_level)}>
            ${label}
          </option>`;
        })}
      </select>
      <${AsyncButton}
        small
        description=${`set gain on ${requestName} channel ${channelNumber}`}
        onRun=${() => api.setGain(requestName, channelNumber, Number(select.current.value), device.gain_device_type)}
      >
        Set
      <//>
    </span>
  `;
}

function subscriptionForChannel(device, channel) {
  return (device.subscriptions || []).find((entry) => entry.rx_channel === channel.name) || null;
}

function receiveColumns(device, requestName, onRoute) {
  return [
    { align: "right", cell: (row) => row.number, id: "number", label: "#" },
    {
      cell: (row) =>
        html`<${NameCell} channel=${row.channel} channelNumber=${row.number} channelType="rx" requestName=${requestName} />`,
      id: "name",
      label: "Name",
    },
    { cell: (row) => format.subscriptionSource(row.subscription), id: "subscription", label: "Subscription" },
    {
      cell: (row) => {
        const status = row.subscription && row.subscription.status ? row.subscription.status : null;
        return html`<span class=${status ? `state-${format.statusTone(status.severity)}` : ""}>
          ${status ? format.subscriptionStatusText(row.subscription) : format.ABSENT}
        </span>`;
      },
      id: "status",
      label: "Status",
    },
    {
      cell: (row) => html`
        <span class="cell-actions">
          <${Button} small onClick=${() => onRoute(row)}>Subscribe…<//>
          ${row.subscription && row.subscription.tx_device
            ? html`<${AsyncButton}
                small
                description=${`unsubscribe ${requestName} channel ${row.number}`}
                onRun=${() => api.unsubscribe({ rx_channel: row.number, rx_device: requestName })}
              >
                Unsubscribe
              <//>`
            : null}
        </span>
      `,
      id: "subscribe",
      label: "Subscribe",
    },
    {
      cell: (row) =>
        html`<${GainCell}
          channel=${row.channel}
          channelNumber=${row.number}
          channelType="rx"
          device=${device}
          requestName=${requestName}
        />`,
      id: "gain",
      label: "Gain",
    },
    { cell: (row) => html`<${Value} value=${row.channel.volume} />`, id: "volume", label: "Volume", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.muted} />`, id: "muted", label: "Muted", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.media_type} />`, id: "media-type", label: "Media type", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.status_text} />`, id: "channel-status", label: "Channel status", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.ddm_summary} />`, id: "managed", label: "Managed", defaultHidden: true },
  ];
}

function transmitColumns(device, requestName) {
  return [
    { align: "right", cell: (row) => row.number, id: "number", label: "#" },
    {
      cell: (row) =>
        html`<${NameCell} channel=${row.channel} channelNumber=${row.number} channelType="tx" requestName=${requestName} />`,
      id: "name",
      label: "Name",
    },
    { cell: (row) => html`<${Value} value=${row.channel.friendly_name} />`, id: "friendly-name", label: "Friendly name", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.factory_name} />`, id: "factory-name", label: "Factory name", defaultHidden: true },
    {
      cell: (row) =>
        html`<${GainCell}
          channel=${row.channel}
          channelNumber=${row.number}
          channelType="tx"
          device=${device}
          requestName=${requestName}
        />`,
      id: "gain",
      label: "Gain",
    },
    { cell: (row) => format.sampleRate(row.channel.sample_rate), id: "sample-rate", label: "Sample rate", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.encoding} />`, id: "encoding", label: "Encoding", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.bit_depth} />`, id: "bit-depth", label: "Bit depth", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.media_type} />`, id: "media-type", label: "Media type", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.status_text} />`, id: "channel-status", label: "Channel status", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.ddm_summary} />`, id: "managed", label: "Managed", defaultHidden: true },
  ];
}

function receiveRows(device) {
  const receiveChannels = device.channels ? device.channels.receivers || {} : {};
  return format.sortedChannelNumbers(receiveChannels).map((number) => ({
    channel: receiveChannels[number],
    number,
    subscription: subscriptionForChannel(device, receiveChannels[number]),
  }));
}

function transmitRows(device) {
  const transmitChannels = device.channels ? device.channels.transmitters || {} : {};
  return format.sortedChannelNumbers(transmitChannels).map((number) => ({ channel: transmitChannels[number], number }));
}

export function ReceiveSection({ device }) {
  const requestName = deviceRequestName(device);
  const [routing, setRouting] = useState(null);
  const rows = receiveRows(device);
  return html`
    <div class="stack">
      ${routing
        ? html`<${RoutePicker}
            receiver=${device}
            receiveChannelNumber=${routing.number}
            receiveChannelName=${routing.channel.name}
            subscription=${routing.subscription}
            onClose=${() => setRouting(null)}
          />`
        : null}
      <${Panel}
        title=${`Receivers (${rows.length})`}
        actions=${rows.length
          ? html`<${AsyncButton}
              small
              variant="danger"
              description=${`unsubscribe all receivers on ${requestName}`}
              onRun=${() => api.unsubscribe({ rx_channels: rows.map((row) => row.number), rx_device: requestName })}
            >
              Unsubscribe all
            <//>`
          : null}
      >
        ${rows.length === 0
          ? html`<${Notice}>This device reports no Dante receivers.<//>`
          : html`<${ConfigurableTable}
              tableId="device-receive-channels"
              columns=${receiveColumns(device, requestName, setRouting)}
              rows=${rows}
              rowKey=${(row) => row.number}
            />`}
      <//>
    </div>
  `;
}

export function TransmitSection({ device }) {
  const requestName = deviceRequestName(device);
  const rows = transmitRows(device);
  return html`
    <${Panel} title=${`Transmitters (${rows.length})`}>
      ${rows.length === 0
        ? html`<${Notice}>This device reports no Dante transmitters.<//>`
        : html`<${ConfigurableTable}
            tableId="device-transmit-channels"
            columns=${transmitColumns(device, requestName)}
            rows=${rows}
            rowKey=${(row) => row.number}
          />`}
    <//>
  `;
}
