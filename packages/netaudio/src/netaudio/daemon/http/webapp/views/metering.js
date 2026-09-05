import { api } from "../api.js";
import {AsyncButton, Fields, Notice, OnlineState, Panel, Pill, Value} from "../components.js";
import { ConfigurableTable } from "../table.js";
import * as format from "../format.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { MeterBank } from "../meters.js";
import { devicePath, navigate } from "../router.js";
import { deviceByName, deviceRequestName, devices, meterRevision, meterValuesFor } from "../store.js";

const CLIENT_IDENTIFIER = "netaudio_webapp";

const PROTOCOLS = {
  detailed: {
    label: "detailed",
    summary: "Per-channel levels streamed after an explicit metering start. Highest resolution, stops when idle.",
    tone: "good",
  },
  signal_presence: {
    label: "signal presence",
    summary: "Passive conmon records the device already broadcasts. Coarser, but needs no metering session.",
    tone: "warn",
  },
};

function protocolPill(source) {
  const protocol = PROTOCOLS[source];
  if (!protocol) {
    return html`<${Pill}>none<//>`;
  }
  return html`<${Pill} tone=${protocol.tone}>${protocol.label}<//>`;
}

function ProtocolLegend() {
  return html`
    <${Panel} title="Metering protocols">
      <${Fields}
        entries=${[
          [
            "Detailed",
            html`<span>${PROTOCOLS.detailed.summary}</span>`,
          ],
          [
            "Signal presence",
            html`<span>${PROTOCOLS.signal_presence.summary}</span>`,
          ],
          [
            "Selection",
            html`<span>
              The daemon prefers a fresh detailed sample and falls back to signal presence when detailed metering is
              not running or has gone stale.
            </span>`,
          ],
        ]}
      />
    <//>
  `;
}

function MeteringIndex() {
  meterRevision.value;
  const all = format.sortedDevices(devices.value);
  const [status, setStatus] = useState(null);

  useEffect(() => {
    let cancelled = false;
    api
      .getMeteringStatus()
      .then((result) => {
        if (!cancelled) {
          setStatus(result);
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  const columns = [
    { cell: (device) => html`<${OnlineState} online=${device.online} />`, id: "state", label: "State" },
    { cell: (device) => format.deviceLabel(device), id: "device", label: "Device" },
    {
      cell: (device) => {
        const values = meterValuesFor(device.server_name);
        return values && values.metering_source ? PROTOCOLS[values.metering_source].label : format.ABSENT;
      },
      id: "protocol",
      label: "Protocol",
    },
    {
      cell: (device) => {
        const entry = status ? Object.values(status).find((record) => record.server_name === device.server_name) : null;
        return entry ? format.text(entry.receiving) : format.ABSENT;
      },
      id: "receiving",
      label: "Receiving",
    },
    {
      align: "right",
      cell: (device) => {
        const values = meterValuesFor(device.server_name);
        return values ? Object.keys(values.tx || {}).length : 0;
      },
      id: "transmit",
      label: "Transmit",
    },
    {
      align: "right",
      cell: (device) => {
        const values = meterValuesFor(device.server_name);
        return values ? Object.keys(values.rx || {}).length : 0;
      },
      id: "receive",
      label: "Receive",
    },
    {
      cell: (device) => {
        const values = meterValuesFor(device.server_name);
        return values ? format.timestamp(values.wall_time) : format.ABSENT;
      },
      id: "last-sample",
      label: "Last sample",
    },
  ];

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Metering</div>
          <div class="content-subtitle">Choose a device to open its live meters.</div>
        </div>
      </div>
      <${Panel} title="Devices">
        ${all.length === 0
          ? html`<${Notice}>No devices have been discovered yet.<//>`
          : html`<${ConfigurableTable}
              tableId="metering-devices"
              columns=${columns}
              rows=${all}
              rowKey=${(device) => device.server_name || device.name}
              onRowClick=${(device) => navigate(devicePath("metering", format.deviceLabel(device)))}
            />`}
      <//>
      <${ProtocolLegend} />
    </div>
  `;
}

function DeviceMetering({ deviceName }) {
  meterRevision.value;
  const device = deviceByName(deviceName);
  if (!device) {
    return html`<${Notice}>No device named ${deviceName} is in the daemon inventory.<//>`;
  }
  const requestName = deviceRequestName(device);
  const values = meterValuesFor(device.server_name);
  const [snapshot, setSnapshot] = useState(null);

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">${format.deviceLabel(device)}</div>
          <div class="content-subtitle">
            ${values ? `last sample ${format.timestamp(values.wall_time)}` : "no metering data received yet"}
          </div>
        </div>
        <div class="toolbar">
          ${protocolPill(values ? values.metering_source : null)}
          <${AsyncButton}
            variant="primary"
            small
            description=${`start detailed metering on ${requestName}`}
            onRun=${() => api.startMetering(requestName, CLIENT_IDENTIFIER)}
          >
            Start detailed
          <//>
          <${AsyncButton}
            small
            description=${`stop detailed metering on ${requestName}`}
            onRun=${() => api.stopMetering(requestName, CLIENT_IDENTIFIER)}
          >
            Stop detailed
          <//>
          <${AsyncButton}
            small
            description=${`snapshot metering on ${requestName}`}
            onRun=${async () => {
              const result = await api.getMeteringSnapshot(requestName);
              setSnapshot(result);
              return result;
            }}
          >
            Snapshot
          <//>
        </div>
      </div>
      <${Fields}
        entries=${[
          ["Protocol", values ? format.text(values.metering_source) : format.ABSENT],
          [
            "Source address",
            values && values.source_ip ? `${values.source_ip}:${html`<${Value} value=${values.source_port} />`}` : format.ABSENT,
          ],
          ["Last sample", values ? format.timestamp(values.wall_time) : format.ABSENT],
        ]}
      />
      <div class="split">
        <${Panel} title="Transmit levels">
          <${MeterBank} device=${device} direction="tx" serverName=${device.server_name} />
        <//>
        <${Panel} title="Receive levels">
          <${MeterBank} device=${device} direction="rx" serverName=${device.server_name} />
        <//>
      </div>
      ${snapshot
        ? html`<${Panel} title="On-demand snapshot">
            <${Fields}
              entries=${[
                ["Protocol", html`<${Value} value=${snapshot.metering_source} />`],
                ["Captured", format.timestamp(snapshot.wall_time)],
                ["Transmit channels", String(Object.keys(snapshot.tx || {}).length)],
                ["Receive channels", String(Object.keys(snapshot.rx || {}).length)],
              ]}
            />
          <//>`
        : null}
      <${ProtocolLegend} />
    </div>
  `;
}

function MeteringView({ location }) {
  if (location.parameters.device) {
    return html`<${DeviceMetering} deviceName=${location.parameters.device} />`;
  }
  return html`<${MeteringIndex} />`;
}

export const meteringView = {
  component: MeteringView,
  id: "metering",
  label: "Metering",
};
