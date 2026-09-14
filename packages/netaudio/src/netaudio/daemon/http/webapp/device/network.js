import { api } from "../api.js";
import { AsyncButton, Fields, FieldRow, Panel } from "../components.js";
import { html, useEffect, useRef, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";
import { operationReasonText, operationWritable } from "./availability.js";

const modeLabel = (value) => ({
  switched: "Switched", redundant: "Redundant", split_redundant: "Split/Redundant",
  dynamic: "DHCP", static: "Static",
}[value] || "Unavailable");

const rawModeLabel = (evidence) => {
  if (evidence?.mode) return modeLabel(evidence.mode);
  if (evidence?.raw_label) return `Unknown (${evidence.raw_label})`;
  if (Number.isInteger(evidence?.raw_code)) return `Unknown (0x${evidence.raw_code.toString(16).padStart(4, "0")})`;
  return "Unavailable";
};

function InterfaceCard({ device, entry, modes, requestName, onReadback, single }) {
  const configured = entry.configured;
  const role = entry.interface;
  const roleTitle = role === "primary" ? "Primary" : role === "secondary" ? "Secondary" : "Network interface";
  const title = single ? "IPv4 address" : roleTitle;
  const [mode, setMode] = useState(configured?.mode === "static" ? "static" : "dhcp");
  const [saving, setSaving] = useState(false);
  const ip = useRef(null), mask = useRef(null), gateway = useRef(null), dns = useRef(null);
  const writable = operationWritable(device, "static_ipv4");
  const editable = writable && modes?.length > 0 && configured != null;
  return html`
    <section class="network-section">
      <h3 class="section-label">${title}</h3>
      <${Fields} entries=${[
        ["Active mode", modeLabel(entry.mode)],
        ["Active address", entry.ip_address],
        ["Subnet mask", entry.netmask],
        ...(entry.gateway && entry.gateway !== "0.0.0.0" ? [["Gateway", entry.gateway]] : []),
        ...(entry.dns_server && entry.dns_server !== "0.0.0.0" ? [["DNS server", entry.dns_server]] : []),
        ["MAC address", entry.mac_address],
        ...(configured ? [
          ["Configured mode", modeLabel(configured.mode)],
          ...(configured.mode === "static" ? [
            ["Configured address", configured.ip_address],
            ["Configured subnet mask", configured.netmask],
            ["Configured gateway", configured.gateway],
            ["Configured DNS server", configured.dns_server],
          ] : []),
        ] : []),
      ]} />
      ${entry.reboot_required ? html`<p role="status">Pending network change — reboot required.</p>` : null}
      ${editable ? html`
        <${FieldRow} label="Address mode">
          <select disabled=${saving} aria-label=${roleTitle + " address mode"} value=${mode} onChange=${(event) => setMode(event.currentTarget.value)}>
            ${modes.map((value) => html`<option value=${value}>${value === "dhcp" ? "DHCP" : "Static"}</option>`)}
          </select>
        <//>
        ${mode === "static" ? html`
          <${FieldRow} label="IP address"><input disabled=${saving} ref=${ip} aria-label=${roleTitle + " IP address"} defaultValue=${configured.ip_address || entry.ip_address || ""} /><//>
          <${FieldRow} label="Subnet mask"><input disabled=${saving} ref=${mask} aria-label=${roleTitle + " subnet mask"} defaultValue=${configured.netmask || entry.netmask || ""} /><//>
          <${FieldRow} label="Gateway"><input disabled=${saving} ref=${gateway} aria-label=${roleTitle + " gateway"} defaultValue=${configured.gateway || ""} /><//>
          <${FieldRow} label="DNS server"><input disabled=${saving} ref=${dns} aria-label=${roleTitle + " DNS server"} defaultValue=${configured.dns_server || ""} /><//>
        ` : null}
        <${AsyncButton} description=${"save " + roleTitle.toLowerCase() + " network settings"}
          onRun=${async () => {
            setSaving(true);
            try {
              const result = await api.setInterface({
                device: requestName, interface: role, mode,
                ip: ip.current?.value, netmask: mask.current?.value,
                gateway: gateway.current?.value, dns: dns.current?.value,
              });
              onReadback(result);
              return result;
            } finally {
              setSaving(false);
            }
          }}>Save ${roleTitle.toLowerCase()} settings<//>
      ` : configured ? html`<p>Network changes are unavailable for this interface. ${writable
        ? "No configuration modes were reported."
        : operationReasonText(device, "static_ipv4")}</p>` : null}
    </section>
  `;
}

function Redundancy({ device, status, requestName, onReadback }) {
  const pending = useRef(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  const [selection, setSelection] = useState(status?.configured_mode);
  const writable = operationWritable(device, "redundancy");
  const knownModes = (status?.available_modes || []).filter((choice) => choice?.mode);
  const unknownModes = (status?.available_modes || []).filter((choice) => !choice?.mode);
  const changeMode = async (event) => {
    const mode = event.currentTarget.value;
    if (pending.current || mode === status.configured_mode) return;
    pending.current = true;
    setBusy(true);
    setError(null);
    setSelection(mode);
    try {
      const result = await api.setRedundancy({ device: requestName, mode });
      setSelection(result.redundancy?.configured_mode ?? status.configured_mode);
      onReadback(result);
    } catch (error) {
      setSelection(status.configured_mode);
      setError(error.message);
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  if (!status) return html`<p>Dante Redundancy capability is unknown.</p>`;
  const capability = status.advertised_support === true
    ? "Supported"
    : status.advertised_support === false ? "Unsupported" : "Unknown";
  return html`
    <section class="network-section">
      <h3 class="section-label">Dante Redundancy</h3>
      <${Fields} entries=${[
        ["Capability", capability],
        ["Active", rawModeLabel(status.current_mode_evidence)],
        ["Configured", rawModeLabel(status.configured_mode_evidence)],
        ["Interface inventory", status.interface_inventory?.completeness || "unknown"],
        ...(status.licensed_redundancy?.enabled == null
          ? [] : [["Licensed redundancy diagnostic", status.licensed_redundancy.enabled ? "Enabled" : "Disabled"]]),
      ]} />
      ${status.reboot_required ? html`<p role="status">Pending redundancy change — reboot required.</p>` : null}
      ${unknownModes.length ? html`<p>Unknown advertised choices: ${unknownModes.map((choice) =>
        `${choice.label || "unlabelled"} (0x${choice.code.toString(16).padStart(4, "0")})`).join(", ")}.</p>` : null}
      ${writable && knownModes.length ? html`
        <${FieldRow} label="Dante Redundancy">
          <select aria-label="Dante Redundancy" value=${selection} disabled=${busy} onChange=${changeMode}>
            ${knownModes.map((choice) => html`<option value=${choice.mode}>${choice.label}</option>`)}
          </select>
        <//>
        ${busy ? html`<p role="status">Applying redundancy mode…</p>` : null}
        ${error ? html`<p role="alert">Could not change redundancy mode: ${error}</p>` : null}
      ` : html`<p>Dante Redundancy changes are unavailable. ${writable
        ? "No supported modes were reported."
        : operationReasonText(device, "redundancy")}</p>`}
    </section>
  `;
}

export function NetworkSection({ device }) {
  const requestName = deviceRequestName(device);
  const [probe, setProbe] = useState(null);
  const [loadError, setLoadError] = useState(false);
  useEffect(() => {
    let active = true;
    setProbe(null);
    setLoadError(false);
    if (device.online !== false) api.getInterfaces(requestName).then(
      (result) => { if (active) setProbe(result); },
      () => { if (active) setLoadError(true); },
    );
    return () => { active = false; };
  }, [requestName]);
  const onReadback = (result) => {
    setProbe((previous) => ({ ...previous, ...result }));
    setLoadError(false);
  };
  const interfaces = probe?.interfaces ?? device.interfaces ?? [];
  const status = probe?.redundancy ?? device.network_redundancy;
  const speed = probe?.link_speed_mbps ?? device.link_speed_mbps;
  const interfaceModes = probe?.interface_configuration_modes ?? device.interface_configuration_modes ?? {};
  const availabilityDevice = probe?.operation_availability
    ? { ...device, operation_availability: probe.operation_availability }
    : device;
  return html`
    <div class="network-config">
    <${Panel} title="Network config">
      ${loadError ? html`<p role="status">The device did not respond. Showing last-known network settings.</p>` : null}
      ${speed ? html`<p>Link speed: ${speed} Mbps</p>` : null}
      ${interfaces.length ? interfaces.map((entry) => html`
        <${InterfaceCard} key=${requestName + entry.interface + JSON.stringify(entry.configured)} device=${availabilityDevice} entry=${entry} modes=${interfaceModes[entry.interface]} requestName=${requestName} onReadback=${onReadback} single=${interfaces.length < 2 && entry.interface === "primary"} />
      `) : html`<p>Network settings are unavailable.</p>`}
      <${Redundancy} key=${requestName + status?.configured_mode} device=${availabilityDevice} status=${status} requestName=${requestName} onReadback=${onReadback} />
    <//>
    </div>
  `;
}
