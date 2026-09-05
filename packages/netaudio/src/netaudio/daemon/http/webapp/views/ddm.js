import { api } from "../api.js";
import {AsyncButton, DataTable, Fields, Metric, Notice, Panel, Value} from "../components.js";
import { ConfigurableTable } from "../table.js";
import * as format from "../format.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { devices } from "../store.js";

const DOMAIN_HEADERS = [
  "Name",
  "Identifier",
  "Context",
  "Server profile",
  "Summary",
  "Clocking",
  "Connectivity",
  "Latency",
  "Subscriptions",
  "Devices",
];

const SERVER_HEADERS = [
  "Server",
  "State",
  "Enabled",
  "Address",
  "Fresh",
  "Domains",
  "Devices",
  "Enrolled",
  "Unenrolled",
  "Last attempt",
  "Last success",
  "Last error",
];

function StatusPanel({ onLoaded, status }) {
  return html`
    <${Panel}
      title="Managed status"
      actions=${html`
        <${AsyncButton} small description="read managed status" onRun=${() => api.getManagedStatus().then(onLoaded)}>
          Read status
        <//>
        <${AsyncButton}
          small
          description="refresh managed inventory"
          onRun=${async () => {
            const result = await api.managedRefresh(null);
            onLoaded(await api.getManagedStatus());
            return result;
          }}
        >
          Refresh inventory
        <//>
      `}
    >
      ${status === null
        ? html`<${Notice}>Reading managed status…<//>`
        : html`
            <div class="metric-row">
              <${Metric} label="Domains" value=${html`<${Value} value=${status.domain_count} />`} />
              <${Metric} label="Devices" value=${html`<${Value} value=${status.device_count} />`} />
              <${Metric} label="Enrolled" value=${html`<${Value} value=${status.enrolled_device_count} />`} />
              <${Metric} label="Unenrolled" value=${html`<${Value} value=${status.unenrolled_device_count} />`} />
              <${Metric} label="Servers" value=${html`<${Value} value=${status.server_count} />`} />
            </div>
            <${Fields}
              entries=${[
                ["Enabled", html`<${Value} value=${status.enabled} />`],
                ["State", html`<${Value} value=${status.state} />`],
                ["Default context", html`<${Value} value=${status.default_context} />`],
                ["Inventory fresh", html`<${Value} value=${status.fresh} />`],
              ]}
            />
            ${status.servers
              ? html`<${DataTable}
                  headers=${SERVER_HEADERS}
                  rows=${Object.entries(status.servers).map(
                    ([name, server]) => html`
                      <tr key=${name}>
                        <td>${name}</td>
                        <td>${html`<${Value} value=${server.state} />`}</td>
                        <td>${html`<${Value} value=${server.enabled} />`}</td>
                        <td>${html`<${Value} value=${server.url} />`}</td>
                        <td>${html`<${Value} value=${server.fresh} />`}</td>
                        <td class="numeric">${html`<${Value} value=${server.domain_count} />`}</td>
                        <td class="numeric">${html`<${Value} value=${server.device_count} />`}</td>
                        <td class="numeric">${html`<${Value} value=${server.enrolled_device_count} />`}</td>
                        <td class="numeric">${html`<${Value} value=${server.unenrolled_device_count} />`}</td>
                        <td>${format.timestamp(server.last_attempt)}</td>
                        <td>${format.timestamp(server.last_success)}</td>
                        <td class=${server.last_error ? "state-bad" : ""}>${html`<${Value} value=${server.last_error} />`}</td>
                      </tr>
                    `,
                  )}
                  short
                />`
              : null}
          `}
    <//>
  `;
}

function DomainsPanel({ domains, onLoaded }) {
  return html`
    <${Panel}
      title="Domains"
      actions=${html`<${AsyncButton} small description="read managed domains" onRun=${() => api.getManagedDomains(null).then(onLoaded)}>
        Read domains
      <//>`}
    >
      ${domains === null
        ? html`<${Notice}>Reading domains…<//>`
        : domains.length === 0
          ? html`<${Notice}>The managed server reported no domains.<//>`
          : html`<${DataTable}
              headers=${DOMAIN_HEADERS}
              rows=${domains.map(
                (domain) => html`
                  <tr key=${domain.id || domain.name}>
                    <td>${html`<${Value} value=${domain.name} />`}</td>
                    <td>${html`<${Value} value=${domain.id} />`}</td>
                    <td>${html`<${Value} value=${domain.ddm_context} />`}</td>
                    <td>${html`<${Value} value=${domain.ddm_server_profile} />`}</td>
                    <td>${html`<${Value} value=${domain.status ? domain.status.summary : null} />`}</td>
                    <td>${html`<${Value} value=${domain.status ? domain.status.clocking : null} />`}</td>
                    <td>${html`<${Value} value=${domain.status ? domain.status.connectivity : null} />`}</td>
                    <td>${html`<${Value} value=${domain.status ? domain.status.latency : null} />`}</td>
                    <td>${html`<${Value} value=${domain.status ? domain.status.subscriptions : null} />`}</td>
                    <td class="numeric">${(domain.devices || []).length}</td>
                  </tr>
                `,
              )}
              short
            />`}
    <//>
  `;
}

const MANAGED_COLUMNS = [
  { cell: (device) => format.deviceLabel(device), id: "device", label: "Device" },
  { cell: (device) => html`<${Value} value=${device.ddm_domain_name} />`, id: "domain", label: "Domain" },
  { cell: (device) => html`<${Value} value=${device.ddm_context} />`, id: "context", label: "Context", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.ddm_enrolment_state} />`, id: "enrolment", label: "Enrolment" },
  { cell: (device) => html`<${Value} value=${device.ddm_connection_state} />`, id: "connection", label: "Connection" },
  { cell: (device) => html`<${Value} value=${device.ddm_status} />`, id: "status", label: "Status" },
  { cell: (device) => format.timestamp(device.ddm_last_sync), id: "last-sync", label: "Last sync", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.ipv4} />`, id: "address", label: "Address", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.ddm_device_id} />`, id: "identifier", label: "Managed identifier", defaultHidden: true },
];

function ManagedDevicesPanel() {
  const managed = format
    .sortedDevices(devices.value)
    .filter((device) => (device.inventory_sources || []).includes("ddm"));
  return html`
    <${Panel} title=${`Managed devices (${managed.length})`}>
      ${managed.length === 0
        ? html`<${Notice}>No device in the daemon inventory came from a managed domain.<//>`
        : html`<${ConfigurableTable}
            tableId="ddm-managed-devices"
            columns=${MANAGED_COLUMNS}
            rows=${managed}
            rowKey=${(device) => device.server_name || device.name}
            short
          />`}
    <//>
  `;
}

function DdmView() {
  const [status, setStatus] = useState(null);
  const [domains, setDomains] = useState(null);

  useEffect(() => {
    let cancelled = false;
    api
      .getManagedStatus()
      .then((result) => !cancelled && setStatus(result))
      .catch(() => !cancelled && setStatus(false));
    api
      .getManagedDomains(null)
      .then((result) => !cancelled && setDomains(result))
      .catch(() => !cancelled && setDomains([]));
    return () => {
      cancelled = true;
    };
  }, []);

  return html`
    <div class="stack">
      <div class="content-header">
        <div>
          <div class="content-title">Dante Domain Manager</div>
          <div class="content-subtitle">Managed inventory read through the documented Managed API.</div>
        </div>
      </div>
      ${status === false
        ? html`<${Notice}>The daemon could not read managed status. Managed support may be disabled.<//>`
        : html`<${StatusPanel} status=${status} onLoaded=${setStatus} />`}
      <${DomainsPanel} domains=${domains} onLoaded=${setDomains} />
      <${ManagedDevicesPanel} />
    </div>
  `;
}

export const ddmView = {
  component: DdmView,
  id: "ddm",
  label: "Domains",
};
