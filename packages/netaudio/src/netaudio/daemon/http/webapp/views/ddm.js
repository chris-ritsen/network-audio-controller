import { DdmConnections } from "../ddm-connections.js";
import { api } from "../api.js";
import { DataTable, Notice, Panel } from "../components.js";
import { ConfigurableTable } from "../table.js";
import * as format from "../format.js";
import { html, useEffect, useState } from "../lib/preact.js";
import { scopedDevices as devices, managedDomains, inventoryReady } from "../store.js";
import { Icon } from "../icons.js";
import { t } from "../i18n.js";

const DOMAIN_HEADERS = [
  t("Name"),
  t("Context"),
  t("Server profile"),
  t("Summary"),
  t("Clocking"),
  t("Connectivity"),
  t("Latency"),
  t("Subscriptions"),
  t("Devices"),
];

const word = (value) => (value === null || value === undefined || value === "" ? "" : format.stateLabel(value));

function DomainsPanel({ domains }) {
  if (Array.isArray(domains) && !domains.length) return null;
  return html`
    <${Panel} title=${t("Domains")}>
      ${domains === null
        ? html`<${Notice}>${t("Reading domains…")}<//>`
        : html`<${DataTable}
              headers=${DOMAIN_HEADERS}
              rows=${domains.map(
                (domain) => html`
                  <tr key=${domain.id || domain.name}>
                    <td data-label=${t("Name")}>${domain.name || ""}</td>
                    <td data-label=${t("Context")}>${domain.ddm_context || ""}</td>
                    <td data-label=${t("Server profile")}>${domain.ddm_server_profile || ""}</td>
                    <td data-label=${t("Summary")}>${word(domain.status?.summary)}</td>
                    <td data-label=${t("Clocking")}>${word(domain.status?.clocking)}</td>
                    <td data-label=${t("Connectivity")}>${word(domain.status?.connectivity)}</td>
                    <td data-label=${t("Latency")}>${word(domain.status?.latency)}</td>
                    <td data-label=${t("Subscriptions")}>${word(domain.status?.subscriptions)}</td>
                    <td class="numeric" data-label=${t("Devices")}>${(domain.devices || []).length}</td>
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
    if (!enrolled && !window.confirm(t("Enroll {device} in {domain}? This may interrupt audio. Device configuration will be retained.", { device: format.deviceLabel(device), domain: domains.find((entry) => entry.id === selected)?.name }))) return;
    setBusy(true); setError("");
    try {
      await api.setDdmEnrollment({ server: device.ddm_server_profile, device_id: device.ddm_device_id, action, domain_id: selected });
    } catch (failure) { setError(failure.message); }
    finally { setBusy(false); }
  };
  return html`<div class="flex items-center gap-2">
    ${!enrolled ? html`<select aria-label=${t("Domain for {device}", { device: format.deviceLabel(device) })} value=${selected} disabled=${busy} onChange=${(event) => setDomain(event.target.value)}>
      ${!domains.length ? html`<option value="">${t("No domains available")}</option>` : null}
      ${domains.map((entry) => html`<option value=${entry.id}>${entry.name}</option>`)}
    </select>` : null}
    <button type="button" class="btn btn-sm" disabled=${busy || !device.online || !inventoryReady.value || (!enrolled && !selected)} onClick=${act}>${!enrolled ? html`<${Icon} name="plus" />` : null}${busy ? t("Applying…") : enrolled ? t("Unenroll") : t("Enroll")}</button>
    ${error ? html`<span class="text-error text-sm" role="alert">${t(error)}</span>` : null}
  </div>`;
}

const MANAGED_COLUMNS = [
  { cell: (device) => format.deviceLabel(device), id: "device", label: t("Device") },
  { cell: (device) => device.ddm_domain_name || "", id: "domain", label: t("Domain") },
  { cell: (device) => device.ddm_context || "", id: "context", label: t("Context"), defaultHidden: true },
  { cell: (device) => word(device.ddm_enrolment_state), id: "enrollment", label: t("Enrollment") },
  { cell: (device) => word(device.ddm_connection_state), id: "connection", label: t("Connection") },
  { cell: (device) => word(device.ddm_status?.summary), id: "status", label: t("Status") },
  { cell: (device) => html`<${EnrollmentControl} key=${device.server_name} device=${device} />`, id: "actions", label: t("Actions"), sortable: false },
  { cell: (device) => (device.ddm_last_sync ? format.timestamp(device.ddm_last_sync) : ""), id: "last-sync", label: t("Last sync"), defaultHidden: true },
  { cell: (device) => device.ipv4 || "", id: "address", label: t("Address"), defaultHidden: true },
];

function ManagedDevicesPanel() {
  const managed = format
    .sortedDevices(devices.value)
    .filter((device) => (device.inventory_sources || []).includes("ddm"));
  if (!managed.length) return null;
  return html`
    <${Panel} title=${t("Managed devices ({count})", { count: managed.length })}>
      <${ConfigurableTable}
        tableId="ddm-managed-devices"
        columns=${MANAGED_COLUMNS}
        rows=${managed}
        rowKey=${(device) => device.server_name || device.name}
      />
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
      <${DdmConnections} />
      <${DomainsPanel} domains=${domains} />
      <${ManagedDevicesPanel} />
    </div>
  `;
}

export const ddmView = {
  component: DdmView,
  id: "ddm",
  label: t("Domains"),
};
