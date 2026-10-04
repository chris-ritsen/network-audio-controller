import { api } from "../api.js";
import { AsyncButton, Fields, FieldRow, Panel } from "../components.js";
import { html, useEffect, useRef, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";
import { operationWritable } from "./availability.js";

const modeLabel = (value) =>
  ({
    switched: "Switched",
    redundant: "Redundant",
    split_redundant: "Split/Redundant",
    dynamic: "DHCP",
    dhcp: "DHCP",
    static: "Static",
  })[value];

const usableAddress = (value) => (value && value !== "0.0.0.0" ? value : undefined);

function InterfaceCard({
  device,
  entry,
  modes,
  requestName,
  onReadback,
  single,
}) {
  const configured = entry.configured;
  const role = entry.interface;
  const roleTitle =
    role === "primary"
      ? "Primary"
      : role === "secondary"
        ? "Secondary"
        : "Network interface";
  const title = single ? "IPv4 address" : roleTitle;
  const [mode, setMode] = useState(
    configured?.mode === "static" ? "static" : "dhcp",
  );
  const [saving, setSaving] = useState(false);
  const ip = useRef(null),
    mask = useRef(null),
    gateway = useRef(null),
    dns = useRef(null);
  const writable = operationWritable(device, "static_ipv4");
  const editable = writable && modes?.length > 0 && configured != null;
  return html`
    <section class="network-section">
      <h3 class="section-label">${title}</h3>
      <${Fields}
        entries=${[
        ["Mode", modeLabel(entry.mode)],
        ["Address", usableAddress(entry.ip_address)],
        ["Subnet mask", usableAddress(entry.netmask)],
        ["Gateway", usableAddress(entry.gateway)],
        ["DNS server", usableAddress(entry.dns_server)],
        ["MAC address", entry.mac_address || undefined],
        ...(configured && !editable && configured.mode !== entry.mode
          ? [["Mode after reboot", modeLabel(configured.mode)]]
          : []),
        ...(configured?.mode === "static" && !editable && configured.ip_address !== entry.ip_address
          ? [
              ["Address after reboot", usableAddress(configured.ip_address)],
              ["Subnet mask after reboot", usableAddress(configured.netmask)],
              ["Gateway after reboot", usableAddress(configured.gateway)],
              ["DNS server after reboot", usableAddress(configured.dns_server)],
            ]
          : []),
      ]}
      />
      ${entry.reboot_required ? html`<p role="status">Reboot to apply the new network settings.</p>` : null}
      ${
        editable
          ? html`
              <${FieldRow} label="Address mode">
                <select
                  disabled=${saving}
                  aria-label=${roleTitle + " address mode"}
                  value=${mode}
                  onChange=${(event) => setMode(event.currentTarget.value)}
                >
                  ${modes.map((value) => html`<option value=${value}>${value === "dhcp" ? "DHCP" : "Static"}</option>`)}
                </select>
              <//>
              ${
          mode === "static"
            ? html`
                <${FieldRow} label="IP address"
                  ><input
                    disabled=${saving}
                    ref=${ip}
                    aria-label=${roleTitle + " IP address"}
                    defaultValue=${configured.ip_address || entry.ip_address || ""}
                /><//>
                <${FieldRow} label="Subnet mask"
                  ><input
                    disabled=${saving}
                    ref=${mask}
                    aria-label=${roleTitle + " subnet mask"}
                    defaultValue=${configured.netmask || entry.netmask || ""}
                /><//>
                <${FieldRow} label="Gateway"
                  ><input
                    disabled=${saving}
                    ref=${gateway}
                    aria-label=${roleTitle + " gateway"}
                    defaultValue=${configured.gateway || ""}
                /><//>
                <${FieldRow} label="DNS server"
                  ><input
                    disabled=${saving}
                    ref=${dns}
                    aria-label=${roleTitle + " DNS server"}
                    defaultValue=${configured.dns_server || ""}
                /><//>
              `
            : null
        }
              <${AsyncButton}
                description=${"save " + roleTitle.toLowerCase() + " network settings"}
                onRun=${async () => {
            setSaving(true);
            try {
              const result = await api.setInterface({
                device: requestName,
                interface: role,
                mode,
                ip: ip.current?.value,
                netmask: mask.current?.value,
                gateway: gateway.current?.value,
                dns: dns.current?.value,
              });
              onReadback(result);
              return result;
            } finally {
              setSaving(false);
            }
          }}
                >Save ${roleTitle.toLowerCase()} settings<//
              >
            `
          : null
      }
    </section>
  `;
}

function Redundancy({ device, status, requestName, onReadback }) {
  const pending = useRef(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  const [selection, setSelection] = useState(status?.configured_mode);
  const writable = operationWritable(device, "redundancy");
  const knownModes = (status?.available_modes || []).filter(
    (choice) => choice?.mode,
  );
  const changeMode = async (event) => {
    const mode = event.currentTarget.value;
    if (pending.current || mode === status.configured_mode) return;
    pending.current = true;
    setBusy(true);
    setError(null);
    setSelection(mode);
    try {
      const result = await api.setRedundancy({ device: requestName, mode });
      setSelection(
        result.redundancy?.configured_mode ?? status.configured_mode,
      );
      onReadback(result);
    } catch (error) {
      setSelection(status.configured_mode);
      setError(error.message);
    } finally {
      pending.current = false;
      setBusy(false);
    }
  };
  const editable = writable && knownModes.length > 0;
  if (!status || (status.advertised_support !== true && !editable)) return null;
  const active = modeLabel(status.current_mode_evidence?.mode);
  const configured = modeLabel(status.configured_mode_evidence?.mode);
  return html`
    <section class="network-section">
      <h3 class="section-label">Dante Redundancy</h3>
      ${
        editable
          ? html`
              <${FieldRow} label="Mode">
                <select
                  aria-label="Dante Redundancy"
                  value=${selection}
                  disabled=${busy}
                  onChange=${changeMode}
                >
                  ${knownModes.map((choice) => html`<option value=${choice.mode}>${choice.label}</option>`)}
                </select>
              <//>
              ${busy ? html`<p role="status">Applying redundancy mode…</p>` : null}
              ${error ? html`<p role="alert">Could not change redundancy mode: ${error}</p>` : null}
            `
          : html`<${Fields}
              entries=${[
                ["Mode", active],
                ["Mode after reboot", configured && configured !== active ? configured : undefined],
              ]}
            />`
      }
      ${status.reboot_required ? html`<p role="status">Reboot to apply the new redundancy mode.</p>` : null}
    </section>
  `;
}

export function NetworkSection({ device }) {
  const requestName = deviceRequestName(device);
  const [probe, setProbe] = useState(null);
  useEffect(() => {
    let active = true;
    setProbe(null);
    if (device.online !== false)
      api.getInterfaces(requestName).then(
        (result) => {
          if (active) setProbe(result);
        },
        (error) => console.error(error),
      );
    return () => {
      active = false;
    };
  }, [requestName]);
  const onReadback = (result) => {
    setProbe((previous) => ({ ...previous, ...result }));
  };
  const interfaces = probe?.interfaces ?? device.interfaces ?? [];
  const status = probe?.redundancy ?? device.network_redundancy;
  const speed = probe?.link_speed_mbps ?? device.link_speed_mbps;
  const interfaceModes =
    probe?.interface_configuration_modes ??
    device.interface_configuration_modes ??
    {};
  const availabilityDevice = probe?.operation_availability
    ? { ...device, operation_availability: probe.operation_availability }
    : device;
  return html`
    <div class="network-config">
      <${Panel} title="Network config">
        ${speed ? html`<${Fields} entries=${[["Link speed", `${speed} Mbps`]]} />` : null}
        ${
        interfaces.map(
              (entry) => html`
                <${InterfaceCard}
                  key=${requestName + entry.interface + JSON.stringify(entry.configured)}
                  device=${availabilityDevice}
                  entry=${entry}
                  modes=${interfaceModes[entry.interface]}
                  requestName=${requestName}
                  onReadback=${onReadback}
                  single=${interfaces.length < 2 && entry.interface === "primary"}
                />
              `,
            )
      }
        <${Redundancy}
          key=${requestName + status?.configured_mode}
          device=${availabilityDevice}
          status=${status}
          requestName=${requestName}
          onReadback=${onReadback}
        />
      <//>
    </div>
  `;
}
