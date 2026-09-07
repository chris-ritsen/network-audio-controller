import { DdmConnections } from "../ddm-connections.js";
import { api } from "../api.js";
import {DataTable, Notice, Panel, Value} from "../components.js";
import { ConfigurableTable } from "../table.js";
import * as format from "../format.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { scopedDevices as devices, managedDomains, inventoryReady } from "../store.js";
import { Icon } from "../icons.js";

const DOMAIN_HEADERS = [
  "Name",
  "Context",
  "Server profile",
  "Summary",
  "Clocking",
  "Connectivity",
  "Latency",
  "Subscriptions",
  "Devices",
];


function DomainsPanel({ domains }) {
  return html`
    <${Panel}
      title="Domains"

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
            />`}
    <//>
  `;
}

function EnrollmentControl({ device }) {
  const [domain, setDomain] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const domains = managedDomains.value.filter((entry) => entry.ddm_server_profile === device.ddm_server_profile);
  const enrolled = Boolean(device.ddm_domain_id);
  const selected = domain || domains[0]?.id || "";
  const act = async () => {
    if (busy) return;
    const action = enrolled ? "unenroll" : "enroll";
    if (!enrolled && !window.confirm(`Enroll ${format.deviceLabel(device)} in ${domains.find((entry) => entry.id === selected)?.name}? This may interrupt audio. Device configuration will be retained.`)) return;
    setBusy(true); setError("");
    try {
      await api.setDdmEnrollment({ server: device.ddm_server_profile, device_id: device.ddm_device_id, action, domain_id: selected });
    } catch (failure) { setError(failure.message); }
    finally { setBusy(false); }
  };
  return html`<div class="flex items-center gap-2">
    ${!enrolled ? html`<select aria-label=${`Domain for ${format.deviceLabel(device)}`} value=${selected} disabled=${busy} onChange=${(event) => setDomain(event.target.value)}>
      ${!domains.length ? html`<option value="">No domains available</option>` : null}
      ${domains.map((entry) => html`<option value=${entry.id}>${entry.name || "Unnamed domain"}</option>`)}
    </select>` : null}
    <button type="button" class="btn btn-sm" disabled=${busy || !device.online || !inventoryReady.value || (!enrolled && !selected)} onClick=${act}>${!enrolled ? html`<${Icon} name="plus" />` : null}${busy ? "Applying…" : enrolled ? "Unenroll" : "Enroll"}</button>
    ${error ? html`<span class="text-error text-sm" role="alert">${error}</span>` : null}
  </div>`;
}

const MANAGED_COLUMNS = [
  { cell: (device) => format.deviceLabel(device), id: "device", label: "Device" },
  { cell: (device) => html`<${Value} value=${device.ddm_domain_name} />`, id: "domain", label: "Domain" },
  { cell: (device) => html`<${Value} value=${device.ddm_context} />`, id: "context", label: "Context", defaultHidden: true },
  { cell: (device) => format.stateLabel(device.ddm_enrolment_state), id: "enrolment", label: "Enrolment" },
  { cell: (device) => format.stateLabel(device.ddm_connection_state), id: "connection", label: "Connection" },
  { cell: (device) => format.stateLabel(device.ddm_status?.summary), id: "status", label: "Status" },
  { cell: (device) => html`<${EnrollmentControl} key=${device.server_name} device=${device} />`, id: "actions", label: "Actions" },
  { cell: (device) => format.timestamp(device.ddm_last_sync), id: "last-sync", label: "Last sync", defaultHidden: true },
  { cell: (device) => html`<${Value} value=${device.ipv4} />`, id: "address", label: "Address", defaultHidden: true },
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
          />`}
    <//>
  `;
}

function DdmView() {
  const domains = managedDomains.value;

  useEffect(() => {
    let cancelled = false;
    api
      .getManagedDomains(null)
      .then((result) => !cancelled && (managedDomains.value = result))
      .catch(() => !cancelled && (managedDomains.value = []));
    return () => {
      cancelled = true;
    };
  }, []);

  return html`
    <div class="flex flex-col gap-4">
      <div class="content-header">
        <div>
          <div class="content-title">Dante Domain Manager</div>
        </div>
      </div>
      <${DdmConnections} />
      <${DomainsPanel} domains=${domains} />
      <${ManagedDevicesPanel} />
    </div>
  `;
}

export const ddmView = {
  component: DdmView,
  id: "ddm",
  label: "Domains",
};
