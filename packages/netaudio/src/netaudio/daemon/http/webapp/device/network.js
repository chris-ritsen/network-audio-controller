import { api } from "../api.js";
import { AsyncButton, Fields, FieldRow, Panel } from "../components.js";
import { t } from "../i18n.js";
import { html, useEffect, useRef, useState } from "../lib/preact.js";
import { deviceRequestName } from "../store.js";
import { operationWritable } from "./availability.js";

const modeLabel = (value) =>
  ({
    switched: t("Switched"),
    redundant: t("Redundant"),
    split_redundant: t("Split/Redundant"),
    dynamic: "DHCP",
    dhcp: "DHCP",
    static: t("Static"),
  })[value];

const usableAddress = (value) => (value && value !== "0.0.0.0" ? value : undefined);

const INTERFACE_TEXT = {
  primary: {
    title: t("Primary"),
    addressMode: t("Primary address mode"),
    ipAddress: t("Primary IP address"),
    subnetMask: t("Primary subnet mask"),
    gateway: t("Primary gateway"),
    dnsServer: t("Primary DNS server"),
    description: t("save primary network settings"),
    save: t("Save primary settings"),
  },
  secondary: {
    title: t("Secondary"),
    addressMode: t("Secondary address mode"),
    ipAddress: t("Secondary IP address"),
    subnetMask: t("Secondary subnet mask"),
    gateway: t("Secondary gateway"),
    dnsServer: t("Secondary DNS server"),
    description: t("save secondary network settings"),
    save: t("Save secondary settings"),
  },
  other: {
    title: t("Network interface"),
    addressMode: t("Network interface address mode"),
    ipAddress: t("Network interface IP address"),
    subnetMask: t("Network interface subnet mask"),
    gateway: t("Network interface gateway"),
    dnsServer: t("Network interface DNS server"),
    description: t("save network interface network settings"),
    save: t("Save network interface settings"),
  },
};

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
  const roleText =
    role === "primary"
      ? INTERFACE_TEXT.primary
      : role === "secondary"
        ? INTERFACE_TEXT.secondary
        : INTERFACE_TEXT.other;
  const title = single ? t("IPv4 address") : roleText.title;
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
        [t("Mode"), modeLabel(entry.mode)],
        [t("Address"), usableAddress(entry.ip_address)],
        [t("Subnet mask"), usableAddress(entry.netmask)],
        [t("Gateway"), usableAddress(entry.gateway)],
        [t("DNS server"), usableAddress(entry.dns_server)],
        [t("MAC address"), entry.mac_address || undefined],
        ...(configured && !editable && configured.mode !== entry.mode
          ? [[t("Mode after reboot"), modeLabel(configured.mode)]]
          : []),
        ...(configured?.mode === "static" && !editable && configured.ip_address !== entry.ip_address
          ? [
              [t("Address after reboot"), usableAddress(configured.ip_address)],
              [t("Subnet mask after reboot"), usableAddress(configured.netmask)],
              [t("Gateway after reboot"), usableAddress(configured.gateway)],
              [t("DNS server after reboot"), usableAddress(configured.dns_server)],
            ]
          : []),
      ]}
      />
      ${entry.reboot_required ? html`<p role="status">${t("Reboot to apply the new network settings.")}</p>` : null}
      ${
        editable
          ? html`
              <${FieldRow} label=${t("Address mode")}>
                <select
                  disabled=${saving}
                  aria-label=${roleText.addressMode}
                  value=${mode}
                  onChange=${(event) => setMode(event.currentTarget.value)}
                >
                  ${modes.map((value) => html`<option value=${value}>${value === "dhcp" ? "DHCP" : t("Static")}</option>`)}
                </select>
              <//>
              ${
          mode === "static"
            ? html`
                <${FieldRow} label=${t("IP address")}
                  ><input
                    disabled=${saving}
                    ref=${ip}
                    aria-label=${roleText.ipAddress}
                    defaultValue=${configured.ip_address || entry.ip_address || ""}
                /><//>
                <${FieldRow} label=${t("Subnet mask")}
                  ><input
                    disabled=${saving}
                    ref=${mask}
                    aria-label=${roleText.subnetMask}
                    defaultValue=${configured.netmask || entry.netmask || ""}
                /><//>
                <${FieldRow} label=${t("Gateway")}
                  ><input
                    disabled=${saving}
                    ref=${gateway}
                    aria-label=${roleText.gateway}
                    defaultValue=${configured.gateway || ""}
                /><//>
                <${FieldRow} label=${t("DNS server")}
                  ><input
                    disabled=${saving}
                    ref=${dns}
                    aria-label=${roleText.dnsServer}
                    defaultValue=${configured.dns_server || ""}
                /><//>
              `
            : null
        }
              <${AsyncButton}
                description=${roleText.description}
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
                >${roleText.save}<//
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
      <h3 class="section-label">${t("Dante Redundancy")}</h3>
      ${
        editable
          ? html`
              <${FieldRow} label=${t("Mode")}>
                <select
                  aria-label=${t("Dante Redundancy")}
                  value=${selection}
                  disabled=${busy}
                  onChange=${changeMode}
                >
                  ${knownModes.map((choice) => html`<option value=${choice.mode}>${t(choice.label)}</option>`)}
                </select>
              <//>
              ${busy ? html`<p role="status">${t("Applying redundancy mode…")}</p>` : null}
              ${error ? html`<p role="alert">${t("Could not change redundancy mode: {error}", { error: t(error) })}</p>` : null}
            `
          : html`<${Fields}
              entries=${[
                [t("Mode"), active],
                [t("Mode after reboot"), configured && configured !== active ? configured : undefined],
              ]}
            />`
      }
      ${status.reboot_required ? html`<p role="status">${t("Reboot to apply the new redundancy mode.")}</p>` : null}
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
      <${Panel} title=${t("Network config")}>
        ${speed ? html`<${Fields} entries=${[[t("Link speed"), `${speed} Mbps`]]} />` : null}
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
